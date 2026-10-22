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
	"maps"

	"github.com/awslabs/operatorpkg/singleton"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// spyMethod wraps a Method and runs a hook before the wrapped ComputeCommands, so a spec can observe the candidates,
// budgets, and cluster state a later method sees.
type spyMethod struct {
	disruption.Method
	before func(budgets map[string]int, candidates []*disruption.Candidate)
}

func (s *spyMethod) ComputeCommands(ctx context.Context, budgets map[string]int, candidates ...*disruption.Candidate) ([]disruption.Command, error) {
	s.before(maps.Clone(budgets), candidates)
	return s.Method.ComputeCommands(ctx, budgets, candidates...)
}

// stubMethod returns at most one command, built by build from the first candidate, and records how often it ran.
type stubMethod struct {
	reason v1.DisruptionReason
	target string
	build  func(*disruption.Candidate) disruption.Command
	calls  int
}

func (s *stubMethod) ShouldDisrupt(_ context.Context, c *disruption.Candidate) bool {
	return c.Name() == s.target
}

func (s *stubMethod) ComputeCommands(_ context.Context, _ map[string]int, candidates ...*disruption.Candidate) ([]disruption.Command, error) {
	s.calls++
	if s.build == nil || len(candidates) == 0 {
		return nil, nil
	}
	return []disruption.Command{s.build(candidates[0])}, nil
}

func (s *stubMethod) Reason() v1.DisruptionReason { return s.reason }
func (s *stubMethod) Class() string               { return disruption.GracefulDisruptionClass }
func (s *stubMethod) ConsolidationType() string   { return "" }

var _ = Describe("Continue After Delete-Only Commands", func() {
	var nodePool *v1.NodePool

	// newNodes returns n initialized-on-apply nodes in nodePool. Empty nodes are consolidatable; drifted nodes are not, so
	// only Drift acts on them.
	newNodes := func(n int, cpu string, drifted bool) ([]*v1.NodeClaim, []*corev1.Node) {
		nodeClaims, nodes := test.NodeClaimsAndNodes(n, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
					corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
				},
			},
			Status: v1.NodeClaimStatus{
				Allocatable: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:  resource.MustParse(cpu),
					corev1.ResourcePods: resource.MustParse("100"),
				},
			},
		})
		for _, nc := range nodeClaims {
			if drifted {
				nc.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
			} else {
				nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
			}
		}
		return nodeClaims, nodes
	}
	// apply applies the nodes, binds one pod per node (podCPU != ""), and informs cluster state.
	apply := func(nodeClaims []*v1.NodeClaim, nodes []*corev1.Node, podCPU string) {
		for i := range nodeClaims {
			ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
			if podCPU != "" {
				pod := test.Pod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(podCPU)},
				}})
				ExpectApplied(ctx, env.Client, pod)
				ExpectManualBinding(ctx, env.Client, pod, nodes[i])
			}
		}
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
	}
	// useMethods installs methods in the order NewMethods registers them (without Repair), with no validation wait.
	useMethods := func(wrap func([]disruption.Method) []disruption.Method) {
		c := disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue)
		methods := []disruption.Method{
			disruption.NewEmptiness(c, disruption.WithValidator(NopValidator{})),
			disruption.NewStaticDrift(cluster, prov, cloudProvider, recorder),
			disruption.NewDrift(env.Client, cluster, prov, recorder, env.Clock),
			disruption.NewMultiNodeConsolidation(c, disruption.WithValidator(NopValidator{})),
			disruption.NewSingleNodeConsolidation(c, disruption.WithValidator(NopValidator{})),
		}
		if wrap != nil {
			methods = wrap(methods)
		}
		disruptionController = disruption.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost, disruption.WithMethods(methods...))
	}
	commandsByReason := func() map[v1.DisruptionReason][]*disruption.Command {
		return lo.GroupBy(queue.GetCommands(), func(cmd *disruption.Command) v1.DisruptionReason { return cmd.Reason() })
	}
	names := func(nodes []*corev1.Node) []string {
		return lo.Map(nodes, func(n *corev1.Node, _ int) string { return n.Name })
	}

	BeforeEach(func() {
		nodePool = test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					Budgets:             []v1.Budget{{Nodes: "100%"}},
				},
			},
		})
		ExpectApplied(ctx, env.Client, nodePool)
	})

	It("should run Drift in the same Reconcile after Emptiness deletes nodes, seeing the deletions", func() {
		emptyClaims, emptyNodes := newNodes(3, "32", false)
		driftClaims, driftNodes := newNodes(1, "32", true)
		apply(emptyClaims, emptyNodes, "")
		apply(driftClaims, driftNodes, "20")

		var driftCandidates []string
		var driftBudgets map[string]int
		markedWhenDriftRan := 0
		useMethods(func(ms []disruption.Method) []disruption.Method {
			ms[2] = &spyMethod{Method: ms[2], before: func(budgets map[string]int, candidates []*disruption.Candidate) {
				driftBudgets = budgets
				driftCandidates = lo.Map(candidates, func(c *disruption.Candidate, _ int) string { return c.Name() })
				markedWhenDriftRan = lo.CountBy(cluster.DeepCopyNodes(), func(n *state.StateNode) bool { return n.MarkedForDeletion() })
			}}
			return ms
		})
		ExpectSingletonReconciled(ctx, disruptionController)

		cmds := commandsByReason()
		Expect(cmds[v1.DisruptionReasonEmpty]).To(HaveLen(1))
		Expect(lo.Map(cmds[v1.DisruptionReasonEmpty][0].Candidates, func(c *disruption.Candidate, _ int) string { return c.Name() })).To(ConsistOf(names(emptyNodes)))
		Expect(cmds[v1.DisruptionReasonDrifted]).To(HaveLen(1))
		Expect(cmds[v1.DisruptionReasonDrifted][0].Decision()).To(Equal(disruption.ReplaceDecision))
		Expect(cmds[v1.DisruptionReasonDrifted][0].Candidates[0].Name()).To(Equal(driftNodes[0].Name))

		// Drift ran after the empty nodes were MarkedForDeletion and queued: they were not its candidates, and its
		// budget (4 nodes * 100% - 3 in flight) already counted them.
		Expect(markedWhenDriftRan).To(Equal(3))
		Expect(driftCandidates).To(ConsistOf(driftNodes[0].Name))
		Expect(driftBudgets[nodePool.Name]).To(Equal(1))
	})
	It("should still end the Reconcile after a command that launches a replacement", func() {
		// The drifted node's pod doesn't fit elsewhere, so Drift replaces it. Single-node consolidation could delete
		// the underutilized node, but must wait for the replacement to launch.
		driftClaims, driftNodes := newNodes(1, "32", true)
		underClaims, underNodes := newNodes(2, "4", false)
		apply(driftClaims, driftNodes, "20")
		apply(underClaims, underNodes, "1")
		consolidationRan := false
		useMethods(func(ms []disruption.Method) []disruption.Method {
			for i := 3; i < len(ms); i++ {
				ms[i] = &spyMethod{Method: ms[i], before: func(map[string]int, []*disruption.Candidate) { consolidationRan = true }}
			}
			return ms
		})
		ExpectSingletonReconciled(ctx, disruptionController)

		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Reason()).To(Equal(v1.DisruptionReasonDrifted))
		Expect(cmds[0].Decision()).To(Equal(disruption.ReplaceDecision))
		Expect(consolidationRan).To(BeFalse())

		// The replacement is not launched, so cluster state is not synced and the next Reconcile does nothing.
		Expect(cluster.Synced(ctx)).To(BeFalse())
		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(queue.GetCommands()).To(HaveLen(1))
		Expect(consolidationRan).To(BeFalse())

		// Once it launches, consolidation gets its turn.
		ExpectMakeNewNodeClaimsReady(ctx, env.Client, env.Clock, cluster, cloudProvider, cmds[0])
		Expect(cluster.Synced(ctx)).To(BeTrue())
		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(consolidationRan).To(BeTrue())
	})
	It("should not over-commit the budget when Emptiness consumes it and Drift follows", func() {
		nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "3"}}
		ExpectApplied(ctx, env.Client, nodePool)
		emptyClaims, emptyNodes := newNodes(2, "32", false)
		driftClaims, driftNodes := newNodes(3, "32", true)
		apply(emptyClaims, emptyNodes, "")
		apply(driftClaims, driftNodes, "20")
		useMethods(nil)

		ExpectSingletonReconciled(ctx, disruptionController)
		cmds := commandsByReason()
		Expect(cmds[v1.DisruptionReasonEmpty]).To(HaveLen(1))
		Expect(cmds[v1.DisruptionReasonEmpty][0].Candidates).To(HaveLen(2))
		Expect(cmds[v1.DisruptionReasonDrifted]).To(HaveLen(1))
		ExpectMetricGaugeValue(disruption.NodePoolNodesConsumingBudgets, 2, map[string]string{metrics.NodePoolLabel: nodePool.Name, metrics.ReasonLabel: "drifted"})

		// 3 in flight: the budget is spent. Launching the replacement lets the next Reconcile run, but nothing more starts.
		ExpectMakeNewNodeClaimsReady(ctx, env.Client, env.Clock, cluster, cloudProvider, cmds[v1.DisruptionReasonDrifted][0])
		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(queue.GetCommands()).To(HaveLen(2))
		inFlight := lo.CountBy(cluster.DeepCopyNodes(), func(n *state.StateNode) bool { return n.MarkedForDeletion() })
		Expect(inFlight).To(Equal(3))
	})
	It("should block Drift entirely when Emptiness spends the whole budget", func() {
		nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "2"}}
		ExpectApplied(ctx, env.Client, nodePool)
		emptyClaims, emptyNodes := newNodes(2, "32", false)
		driftClaims, driftNodes := newNodes(1, "32", true)
		apply(emptyClaims, emptyNodes, "")
		apply(driftClaims, driftNodes, "20")
		useMethods(nil)

		ExpectSingletonReconciled(ctx, disruptionController)
		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Reason()).To(Equal(v1.DisruptionReasonEmpty))
		Expect(cluster.Synced(ctx)).To(BeTrue())
	})
	It("should not let a later method take a node an earlier method already deleted", func() {
		// The node is both empty and drifted: Emptiness takes it, and Drift must not see it (no second command, no
		// "candidate is being disrupted" error from the queue).
		claims, nodes := newNodes(1, "32", false)
		claims[0].StatusConditions().SetTrue(v1.ConditionTypeDrifted)
		apply(claims, nodes, "")
		var driftCandidates []string
		useMethods(func(ms []disruption.Method) []disruption.Method {
			ms[2] = &spyMethod{Method: ms[2], before: func(_ map[string]int, candidates []*disruption.Candidate) {
				driftCandidates = lo.Map(candidates, func(c *disruption.Candidate, _ int) string { return c.Name() })
			}}
			return ms
		})

		result := ExpectSingletonReconciled(ctx, disruptionController)
		Expect(result.RequeueAfter).To(Equal(singleton.RequeueImmediately))
		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Reason()).To(Equal(v1.DisruptionReasonEmpty))
		// Drift found no candidates, so it never reached ComputeCommands.
		Expect(driftCandidates).To(BeEmpty())
	})

	Context("Pass policy", func() {
		var first, second *stubMethod
		var nodes []*corev1.Node

		BeforeEach(func() {
			var claims []*v1.NodeClaim
			claims, nodes = newNodes(2, "32", false)
			apply(claims, nodes, "")
			first = &stubMethod{reason: v1.DisruptionReasonEmpty, target: nodes[0].Name}
			second = &stubMethod{reason: v1.DisruptionReasonDrifted, target: nodes[1].Name}
			disruptionController = disruption.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost, disruption.WithMethods(first, second))
		})
		DescribeTable("whether the next method runs after a method's commands",
			func(build func(*disruption.Candidate) disruption.Command, nextRuns bool) {
				first.build = build
				ExpectSingletonReconciled(ctx, disruptionController)
				Expect(first.calls).To(Equal(1))
				Expect(second.calls).To(Equal(lo.Ternary(nextRuns, 1, 0)))
			},
			Entry("no command: next method runs (unchanged)", nil, true),
			Entry("delete: next method runs (new)", func(c *disruption.Candidate) disruption.Command {
				return disruption.Command{Candidates: []*disruption.Candidate{c}}
			}, true),
			Entry("terminate-first: pass ends (unchanged)", func(c *disruption.Candidate) disruption.Command {
				return disruption.Command{Candidates: []*disruption.Candidate{c}, TerminateFirstReason: disruption.TerminateFirstNoReservedCapacity}
			}, false),
			Entry("reboot: pass ends (unchanged)", func(c *disruption.Candidate) disruption.Command {
				return disruption.Command{Candidates: []*disruption.Candidate{c}, Reboot: true}
			}, false),
		)
		It("should end the pass after a delete if cluster state stopped being synced", func() {
			// Something else (e.g. the provisioner) creates a NodeClaim while the first method runs.
			first.build = func(c *disruption.Candidate) disruption.Command {
				unlaunched := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name}}})
				unlaunched.Status.ProviderID = ""
				cluster.UpdateNodeClaim(unlaunched)
				return disruption.Command{Candidates: []*disruption.Candidate{c}}
			}
			ExpectSingletonReconciled(ctx, disruptionController)
			Expect(first.calls).To(Equal(1))
			Expect(queue.GetCommands()).To(HaveLen(1))
			Expect(cluster.Synced(ctx)).To(BeFalse())
			Expect(second.calls).To(Equal(0))
		})
		It("should run methods in order and requeue immediately after continuing", func() {
			order := []string{}
			first.build = func(c *disruption.Candidate) disruption.Command {
				order = append(order, "first")
				return disruption.Command{Candidates: []*disruption.Candidate{c}}
			}
			second.build = func(c *disruption.Candidate) disruption.Command {
				order = append(order, "second")
				return disruption.Command{Candidates: []*disruption.Candidate{c}}
			}
			result := ExpectSingletonReconciled(ctx, disruptionController)
			Expect(order).To(Equal([]string{"first", "second"}))
			Expect(result.RequeueAfter).To(Equal(singleton.RequeueImmediately))
			Expect(sets.New(lo.Map(queue.GetCommands(), func(cmd *disruption.Command, _ int) string { return cmd.Candidates[0].Name() })...)).
				To(Equal(sets.New(names(nodes)...)))
		})
	})

	Context("Evidence", func() {
		// launchReplacements launches and initializes the command's replacements and informs cluster state, so the
		// next pass is synced and can schedule onto them.
		launchReplacements := func(cmd *disruption.Command) {
			for _, r := range cmd.Replacements {
				nc := &v1.NodeClaim{}
				Expect(env.Client.Get(ctx, types.NamespacedName{Name: r.Name}, nc)).To(Succeed())
				nc, n := ExpectNodeClaimDeployedAndStateUpdated(ctx, env.Client, cluster, cloudProvider, nc)
				ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{n}, []*v1.NodeClaim{nc})
			}
		}
		// newPassTracker returns a func reporting the reasons of commands started since its previous call; it launches
		// their replacements so the next pass is synced.
		newPassTracker := func() func() []v1.DisruptionReason {
			seen := sets.New[string]()
			return func() []v1.DisruptionReason {
				var reasons []v1.DisruptionReason
				for _, cmd := range queue.GetCommands() {
					if !seen.Has(cmd.ID.String()) {
						seen.Insert(cmd.ID.String())
						reasons = append(reasons, cmd.Reason())
						fmt.Fprintf(GinkgoWriter, "EVIDENCE   started %s\n", cmd)
						launchReplacements(cmd)
					}
				}
				return reasons
			}
		}
		It("should finish 10 empty + 3 drifted nodes in fewer passes", func() {
			emptyClaims, emptyNodes := newNodes(10, "32", false)
			driftClaims, driftNodes := newNodes(3, "32", true)
			apply(emptyClaims, emptyNodes, "")
			apply(driftClaims, driftNodes, "20")
			useMethods(nil)
			started := newPassTracker()
			passes := 0
			for i := 0; i < 10; i++ {
				fmt.Fprintf(GinkgoWriter, "EVIDENCE pass %d\n", i+1)
				ExpectSingletonReconciled(ctx, disruptionController)
				if len(started()) == 0 {
					break
				}
				passes++
			}
			fmt.Fprintf(GinkgoWriter, "EVIDENCE passes to finish 10 empty + 3 drifted: %d\n", passes)
			Expect(queue.GetCommands()).To(HaveLen(4))
			// Emptiness and the first drift share a pass; each further drift replacement needs its own.
			Expect(passes).To(Equal(3))
		})
		It("should give consolidation a turn while Emptiness keeps finding empty nodes", func() {
			// Two lightly used nodes: consolidation can remove one. A new empty node appears before every pass, so
			// Emptiness acts in every pass.
			underClaims, underNodes := newNodes(2, "32", false)
			apply(underClaims, underNodes, "1")
			useMethods(nil)
			started := newPassTracker()
			const passes = 5
			consolidationPasses := 0
			for i := 0; i < passes; i++ {
				claims, nodes := newNodes(1, "32", false)
				apply(claims, nodes, "")
				ExpectSingletonReconciled(ctx, disruptionController)
				reasons := started()
				Expect(reasons).To(ContainElement(v1.DisruptionReasonEmpty))
				if lo.Contains(reasons, v1.DisruptionReasonUnderutilized) {
					consolidationPasses++
				}
			}
			fmt.Fprintf(GinkgoWriter, "EVIDENCE passes (of %d) where consolidation acted while emptiness acted every pass: %d\n", passes, consolidationPasses)
			Expect(consolidationPasses).To(Equal(1))
		})
	})
})
