/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package disruption_test

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sync/atomic"

	"github.com/awslabs/operatorpkg/status"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/state/virtualpods"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// sweepClient counts Node and NodeClaim Gets and can fail Patches and NodeClaim Creates.
type sweepClient struct {
	client.Client
	nodeGets, nodeClaimGets atomic.Int32
	patchErr                func(client.Object) error // Node spec patches
	statusPatchErr          func(client.Object) error // NodeClaim status patches
	createErr               func(client.Object) error
}

func (c *sweepClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	switch obj.(type) {
	case *corev1.Node:
		c.nodeGets.Add(1)
	case *v1.NodeClaim:
		c.nodeClaimGets.Add(1)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *sweepClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if c.patchErr != nil {
		if err := c.patchErr(obj); err != nil {
			return err
		}
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *sweepClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if c.createErr != nil {
		if err := c.createErr(obj); err != nil {
			return err
		}
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c *sweepClient) Status() client.SubResourceWriter {
	return &sweepStatusWriter{SubResourceWriter: c.Client.Status(), c: c}
}

type sweepStatusWriter struct {
	client.SubResourceWriter
	c *sweepClient
}

func (w *sweepStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if w.c.statusPatchErr != nil {
		if err := w.c.statusPatchErr(obj); err != nil {
			return err
		}
	}
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

func conflict(obj client.Object) error {
	return apierrors.NewConflict(schema.GroupResource{}, obj.GetName(), fmt.Errorf("injected conflict"))
}

// legacySweep is the sweep as it was inlined in Controller.Reconcile before MarkSweeper: every node in cluster state
// that is not queued, MarkedForDeletion or rebooting is read from the API server.
func legacySweep(ctx context.Context, kubeClient client.Client) error {
	outdatedNodes := lo.Reject(cluster.DeepCopyNodes(), func(s *state.StateNode, _ int) bool {
		return queue.HasAny(s.ProviderID()) || s.MarkedForDeletion() || s.RebootInProgress()
	})
	if err := state.RequireNoScheduleTaint(ctx, kubeClient, false, outdatedNodes...); err != nil {
		return err
	}
	return state.ClearNodeClaimsCondition(ctx, kubeClient, env.Clock, v1.ConditionTypeDisruptionReason, outdatedNodes...)
}

func keepMarks(s *state.StateNode) bool {
	return queue.HasAny(s.ProviderID()) || s.MarkedForDeletion() || s.RebootInProgress()
}

func hasDisruptionTaint(n *corev1.Node) bool {
	return lo.ContainsBy(n.Spec.Taints, func(t corev1.Taint) bool { return t.MatchTaint(&v1.DisruptedNoScheduleTaint) })
}

var _ = Describe("Disruption Mark Sweep", func() {
	var nodePool *v1.NodePool
	var kc *sweepClient

	// newMarkedNode creates an initialized, cluster-state-tracked node whose API objects carry the given marks.
	newMarkedNode := func(taint, condition bool, mutate func(*v1.NodeClaim, *corev1.Node)) (*v1.NodeClaim, *corev1.Node) {
		nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
					corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
				},
			},
			Status: v1.NodeClaimStatus{
				ProviderID:  test.RandomProviderID(),
				Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("32"), corev1.ResourcePods: resource.MustParse("100")},
			},
		})
		if taint {
			node.Spec.Taints = append(node.Spec.Taints, v1.DisruptedNoScheduleTaint)
		}
		if condition {
			nodeClaim.StatusConditions().SetTrueWithReason(v1.ConditionTypeDisruptionReason, string(v1.DisruptionReasonDrifted), string(v1.DisruptionReasonDrifted))
		}
		if mutate != nil {
			mutate(nodeClaim, node)
		}
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		return nodeClaim, node
	}
	marks := func(nodeClaim *v1.NodeClaim, node *corev1.Node) (bool, bool) {
		return hasDisruptionTaint(ExpectExists(ctx, env.Client, node)), ExpectExists(ctx, env.Client, nodeClaim).StatusConditions().Get(v1.ConditionTypeDisruptionReason) != nil
	}

	BeforeEach(func() {
		nodePool = test.NodePool()
		// Keep the disruption methods from acting on the test nodes, so only the sweep writes marks.
		nodePool.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration("Never")
		ExpectApplied(ctx, env.Client, nodePool)
		kc = &sweepClient{Client: env.Client}
	})

	Context("Reconcile", func() {
		var dc *disruption.Controller
		BeforeEach(func() {
			dc = disruption.NewController(ctx, env.Clock, kc, prov, cloudProvider, recorder, cluster, queue, clusterCost, disruption.WithMethods(NewMethodsWithNopValidator()...))
		})
		DescribeTable("removes stale marks",
			func(taint, condition bool) {
				nodeClaim, node := newMarkedNode(taint, condition, nil)
				ExpectSingletonReconciled(ctx, dc)
				t, c := marks(nodeClaim, node)
				Expect(t).To(BeFalse())
				Expect(c).To(BeFalse())
			},
			Entry("taint and condition", true, true),
			Entry("taint only", true, false),
			Entry("condition only", false, true),
		)
		It("keeps the marks of queued, MarkedForDeletion and rebooting nodes", func() {
			queuedNC, queuedN := newMarkedNode(true, true, nil)
			queue.ProviderIDToCommand[queuedNC.Status.ProviderID] = &disruption.Command{}
			markedNC, markedN := newMarkedNode(true, true, nil)
			cluster.MarkForDeletion(markedNC.Status.ProviderID)
			rebootingNC, rebootingN := newMarkedNode(false, true, func(nc *v1.NodeClaim, _ *corev1.Node) {
				nc.StatusConditions().SetTrue(v1.ConditionTypeRebooting)
			})
			staleNC, staleN := newMarkedNode(true, true, nil)

			ExpectSingletonReconciled(ctx, dc)
			for _, p := range [][2]any{{queuedNC, queuedN}, {markedNC, markedN}} {
				t, c := marks(p[0].(*v1.NodeClaim), p[1].(*corev1.Node))
				Expect(t).To(BeTrue())
				Expect(c).To(BeTrue())
			}
			_, c := marks(rebootingNC, rebootingN)
			Expect(c).To(BeTrue())
			t, c := marks(staleNC, staleN)
			Expect(t).To(BeFalse())
			Expect(c).To(BeFalse())
		})
		It("requeues on a conflict removing the taint", func() {
			nodeClaim, node := newMarkedNode(true, true, nil)
			kc.patchErr = func(obj client.Object) error {
				if _, ok := obj.(*corev1.Node); ok {
					return conflict(obj)
				}
				return nil
			}
			res, err := dc.Reconcile(ctx)
			Expect(err).ToNot(HaveOccurred())
			Expect(res.Requeue).To(BeTrue())
			// The condition stage doesn't run after the taint stage fails.
			t, c := marks(nodeClaim, node)
			Expect(t).To(BeTrue())
			Expect(c).To(BeTrue())
		})
		It("requeues on a conflict clearing the condition", func() {
			nodeClaim, node := newMarkedNode(true, true, nil)
			kc.statusPatchErr = func(obj client.Object) error {
				if _, ok := obj.(*v1.NodeClaim); ok {
					return conflict(obj)
				}
				return nil
			}
			res, err := dc.Reconcile(ctx)
			Expect(err).ToNot(HaveOccurred())
			Expect(res.Requeue).To(BeTrue())
			t, c := marks(nodeClaim, node)
			Expect(t).To(BeFalse())
			Expect(c).To(BeTrue())
		})
		It("returns other errors", func() {
			newMarkedNode(true, false, nil)
			kc.patchErr = func(obj client.Object) error { return fmt.Errorf("injected") }
			_, err := dc.Reconcile(ctx)
			Expect(err).To(MatchError(ContainSubstring("removing taint from nodes")))
		})
		It("removes the marks left by a command that failed to start, before cluster state observes them", func() {
			// Cluster state is only updated by the test, so it never observes the marks StartCommand writes.
			nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "100%"}}
			ExpectApplied(ctx, env.Client, nodePool)
			rs := test.ReplicaSet()
			ExpectApplied(ctx, env.Client, rs)
			Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(rs), rs)).To(Succeed())
			pod := test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: new(true), BlockOwnerDeletion: new(true),
			}}}})
			nodeClaim, node := newMarkedNode(false, false, func(nc *v1.NodeClaim, _ *corev1.Node) {
				nc.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
			})
			ExpectApplied(ctx, env.Client, pod)
			ExpectManualBinding(ctx, env.Client, pod, node)

			// Fail creating the replacement, after StartCommand has marked the candidate.
			kc.createErr = func(obj client.Object) error {
				if _, ok := obj.(*v1.NodeClaim); ok {
					return fmt.Errorf("injected create failure")
				}
				return nil
			}
			failingProv := provisioning.NewProvisioner(kc, recorder, cloudProvider, cluster, env.Clock, draController, virtualpods.NewVirtualPodCache(env.Client), store)
			q := disruption.NewQueue(kc, recorder, cluster, env.Clock, failingProv)
			dc = disruption.NewController(ctx, env.Clock, kc, failingProv, cloudProvider, recorder, cluster, q, clusterCost,
				disruption.WithMethods(disruption.NewDrift(kc, cluster, failingProv, recorder, env.Clock)))
			_, err := dc.Reconcile(ctx)
			Expect(err).To(MatchError(ContainSubstring("launching replacement nodeclaim")))
			t, c := marks(nodeClaim, node)
			Expect(t).To(BeTrue())
			Expect(c).To(BeTrue())
			Expect(cluster.DeepCopyNodes()[0].NoScheduleTaintRemovalNeeded()).To(BeFalse()) // cluster state lags

			// Stop drift from re-marking the node, then reconcile again: the sweep reads the node from the API server.
			nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "0"}}
			ExpectApplied(ctx, env.Client, nodePool)
			_, err = dc.Reconcile(ctx)
			Expect(err).ToNot(HaveOccurred())
			t, c = marks(nodeClaim, node)
			Expect(t).To(BeFalse())
			Expect(c).To(BeFalse())
		})
	})

	Context("MarkSweeper", func() {
		var sweeper *disruption.MarkSweeper
		BeforeEach(func() {
			sweeper = disruption.NewMarkSweeper(kc, cluster, env.Clock)
		})
		It("reads only nodes whose cluster state shows a mark", func() {
			for range 5 {
				newMarkedNode(false, false, nil)
			}
			taintedNC, taintedN := newMarkedNode(true, false, nil)
			conditionNC, conditionN := newMarkedNode(false, true, nil)
			kc.nodeGets.Store(0)
			kc.nodeClaimGets.Store(0)
			Expect(sweeper.Sweep(ctx, keepMarks)).To(Succeed())
			Expect(kc.nodeGets.Load()).To(BeEquivalentTo(1))
			Expect(kc.nodeClaimGets.Load()).To(BeEquivalentTo(1))
			t, _ := marks(taintedNC, taintedN)
			Expect(t).To(BeFalse())
			_, c := marks(conditionNC, conditionN)
			Expect(c).To(BeFalse())

			// The legacy sweep reads every node twice.
			kc.nodeGets.Store(0)
			kc.nodeClaimGets.Store(0)
			Expect(legacySweep(ctx, kc)).To(Succeed())
			Expect(kc.nodeGets.Load()).To(BeEquivalentTo(7))
			Expect(kc.nodeClaimGets.Load()).To(BeEquivalentTo(7))
		})
		It("still writes the conditions that StatusConditions() initializes on a NodeClaim missing one", func() {
			nodeClaim, _ := newMarkedNode(false, false, nil)
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			stored := nodeClaim.DeepCopy()
			nodeClaim.Status.Conditions = lo.Reject(nodeClaim.Status.Conditions, func(c status.Condition, _ int) bool { return c.Type == v1.ConditionTypeLaunched })
			Expect(env.Client.Status().Patch(ctx, nodeClaim, client.MergeFrom(stored))).To(Succeed())
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nodeClaim))
			Expect(ExpectExists(ctx, env.Client, nodeClaim).StatusConditions(status.WithObservedOnly()).Get(v1.ConditionTypeLaunched)).To(BeNil())

			Expect(sweeper.Sweep(ctx, keepMarks)).To(Succeed())
			Expect(ExpectExists(ctx, env.Client, nodeClaim).StatusConditions(status.WithObservedOnly()).Get(v1.ConditionTypeLaunched)).ToNot(BeNil())
		})
		It("does not remove a mark that cluster state has not observed until it does", func() {
			nodeClaim, node := newMarkedNode(false, false, nil)
			// Another actor taints the node and sets the condition; the informers haven't delivered it yet.
			stored := node.DeepCopy()
			node.Spec.Taints = append(node.Spec.Taints, v1.DisruptedNoScheduleTaint)
			Expect(env.Client.Patch(ctx, node, client.MergeFrom(stored))).To(Succeed())
			storedNC := nodeClaim.DeepCopy()
			nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeDisruptionReason)
			Expect(env.Client.Status().Patch(ctx, nodeClaim, client.MergeFrom(storedNC))).To(Succeed())

			kc.nodeGets.Store(0)
			Expect(sweeper.Sweep(ctx, keepMarks)).To(Succeed())
			Expect(kc.nodeGets.Load()).To(BeEquivalentTo(0))
			t, c := marks(nodeClaim, node)
			Expect(t).To(BeTrue())
			Expect(c).To(BeTrue())

			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nodeClaim))
			Expect(sweeper.Sweep(ctx, keepMarks)).To(Succeed())
			t, c = marks(nodeClaim, node)
			Expect(t).To(BeFalse())
			Expect(c).To(BeFalse())
		})
		It("reads rechecked nodes from the API server until a sweep has removed their marks", func() {
			nodeClaim, node := newMarkedNode(false, false, nil)
			taint := func() {
				n := ExpectExists(ctx, env.Client, node)
				stored := n.DeepCopy()
				n.Spec.Taints = append(n.Spec.Taints, v1.DisruptedNoScheduleTaint)
				Expect(env.Client.Patch(ctx, n, client.MergeFrom(stored))).To(Succeed())
			}
			sweeper.Recheck(nodeClaim.Status.ProviderID)
			taint()
			// Kept nodes stay rechecked.
			cluster.MarkForDeletion(nodeClaim.Status.ProviderID)
			Expect(sweeper.Sweep(ctx, keepMarks)).To(Succeed())
			t, _ := marks(nodeClaim, node)
			Expect(t).To(BeTrue())

			cluster.UnmarkForDeletion(nodeClaim.Status.ProviderID)
			Expect(sweeper.Sweep(ctx, keepMarks)).To(Succeed())
			t, _ = marks(nodeClaim, node)
			Expect(t).To(BeFalse())

			// Swept: back to trusting cluster state, which still lags.
			taint()
			Expect(sweeper.Sweep(ctx, keepMarks)).To(Succeed())
			t, _ = marks(nodeClaim, node)
			Expect(t).To(BeTrue())
		})
		It("makes the same writes as the legacy sweep when cluster state is current", func() {
			type scenario struct {
				taint, condition, queued, marked, rebooting, deleting, missingCondition bool
			}
			r := rand.New(rand.NewSource(GinkgoRandomSeed())) //nolint:gosec
			scenarios := make([]scenario, 40)
			for i := range scenarios {
				scenarios[i] = scenario{taint: r.Intn(2) == 0, condition: r.Intn(2) == 0, queued: r.Intn(6) == 0, marked: r.Intn(6) == 0,
					rebooting: r.Intn(6) == 0, deleting: r.Intn(6) == 0, missingCondition: r.Intn(6) == 0}
			}
			// What the sweep wrote (resourceVersion changed) and the marks and conditions it left.
			type outcome struct {
				nodeWritten, nodeClaimWritten bool
				taint                         bool
				conditions                    []string
			}
			run := func(sweep func() error) []outcome {
				var ncs []*v1.NodeClaim
				var ns []*corev1.Node
				for _, s := range scenarios {
					nc, n := newMarkedNode(s.taint, s.condition, func(nc *v1.NodeClaim, n *corev1.Node) {
						if s.rebooting {
							nc.StatusConditions().SetTrue(v1.ConditionTypeRebooting)
						}
						if s.deleting {
							n.Finalizers = append(n.Finalizers, "karpenter.sh/test-finalizer")
						}
					})
					if s.missingCondition {
						nc = ExpectExists(ctx, env.Client, nc)
						stored := nc.DeepCopy()
						nc.Status.Conditions = lo.Reject(nc.Status.Conditions, func(c status.Condition, _ int) bool { return c.Type == v1.ConditionTypeRegistered })
						Expect(env.Client.Status().Patch(ctx, nc, client.MergeFrom(stored))).To(Succeed())
					}
					if s.deleting {
						Expect(env.Client.Delete(ctx, n)).To(Succeed())
					}
					ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(n))
					ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nc))
					if s.queued {
						queue.ProviderIDToCommand[nc.Status.ProviderID] = &disruption.Command{}
					}
					if s.marked {
						cluster.MarkForDeletion(nc.Status.ProviderID)
					}
					ncs, ns = append(ncs, nc), append(ns, n)
				}
				before := lo.Map(scenarios, func(_ scenario, i int) [2]string {
					return [2]string{ExpectExists(ctx, env.Client, ns[i]).ResourceVersion, ExpectExists(ctx, env.Client, ncs[i]).ResourceVersion}
				})
				Expect(sweep()).To(Succeed())
				return lo.Map(scenarios, func(_ scenario, i int) outcome {
					n, nc := ExpectExists(ctx, env.Client, ns[i]), ExpectExists(ctx, env.Client, ncs[i])
					conditions := lo.Map(nc.Status.Conditions, func(c status.Condition, _ int) string { return c.Type + "=" + string(c.Status) })
					slices.Sort(conditions)
					return outcome{nodeWritten: n.ResourceVersion != before[i][0], nodeClaimWritten: nc.ResourceVersion != before[i][1],
						taint: hasDisruptionTaint(n), conditions: conditions}
				})
			}
			cleanup := func() {
				ExpectCleanedUp(ctx, env.Client)
				cluster.Reset()
				*queue = lo.FromPtr(disruption.NewQueue(env.Client, recorder, cluster, env.Clock, prov))
				nodePool.ResourceVersion, nodePool.UID = "", ""
				ExpectApplied(ctx, env.Client, nodePool)
			}
			legacy := run(func() error { return legacySweep(ctx, env.Client) })
			cleanup()
			swept := run(func() error { return sweeper.Sweep(ctx, keepMarks) })
			Expect(swept).To(HaveLen(len(legacy)))
			for i := range legacy {
				Expect(swept[i]).To(Equal(legacy[i]), "node %d: %+v", i, scenarios[i])
			}
		})
	})
})
