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

package health

import (
	"context"
	"fmt"
	"strings"
	"time"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/awslabs/operatorpkg/object"
	"github.com/awslabs/operatorpkg/status"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Uninitialized Node Repair", func() {
	var controller *Controller
	var nodePool *v1.NodePool
	var nodeClaim *v1.NodeClaim
	var node *corev1.Node

	// newNodeClaimAndNode returns a NodePool-owned NodeClaim/Node pair whose finalizers keep a deleted NodeClaim
	// observable.
	newNodeClaimAndNode := func() (*v1.NodeClaim, *corev1.Node) {
		return test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
			Labels:     map[string]string{v1.NodePoolLabelKey: nodePool.Name},
			Finalizers: []string{v1.TerminationFinalizer},
		}})
	}
	// register applies a NodeClaim/Node pair that registered but never initialized because the Node went NotReady at
	// the current clock time.
	register := func(nc *v1.NodeClaim, n *corev1.Node) {
		n.Labels[v1.NodeRegisteredLabelKey] = "true"
		n.Spec.Taints = lo.Reject(n.Spec.Taints, func(t corev1.Taint, _ int) bool { return t.MatchTaint(&v1.UnregisteredNoExecuteTaint) })
		nc.StatusConditions().SetTrue(v1.ConditionTypeLaunched)
		nc.StatusConditions().SetTrue(v1.ConditionTypeRegistered)
		ExpectApplied(ctx, env.Client, nc, n)
		// The API server stamps creation with real time; a condition can't predate it, so keep the fake clock at or past it.
		if env.Clock.Now().Before(n.CreationTimestamp.Time) {
			env.Clock.SetTime(n.CreationTimestamp.Time)
		}
		ExpectMakeNodesNotReady(ctx, env.Client, env.Clock, n)
	}
	// initialize makes a NodeClaim/Node pair initialized and healthy.
	initialize := func(nc *v1.NodeClaim, n *corev1.Node) {
		n.Labels[v1.NodeRegisteredLabelKey] = "true"
		n.Labels[v1.NodeInitializedLabelKey] = "true"
		nc.StatusConditions().SetTrue(v1.ConditionTypeLaunched)
		nc.StatusConditions().SetTrue(v1.ConditionTypeRegistered)
		nc.StatusConditions().SetTrue(v1.ConditionTypeInitialized)
		ExpectApplied(ctx, env.Client, nc, n)
		ExpectMakeNodesReady(ctx, env.Client, env.Clock, n)
	}
	expectDeleted := func(nc *v1.NodeClaim) {
		GinkgoHelper()
		Expect(ExpectExists(ctx, env.Client, nc).DeletionTimestamp.IsZero()).To(BeFalse())
	}
	expectNotDeleted := func(nc *v1.NodeClaim) {
		GinkgoHelper()
		Expect(ExpectExists(ctx, env.Client, nc).DeletionTimestamp.IsZero()).To(BeTrue())
	}

	BeforeEach(func() {
		env.Clock.SetTime(time.Now().Truncate(time.Second))
		cloudProvider.Reset()
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: corev1.NodeReady, ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, Action: cloudprovider.ReplaceNode},
		}
		recorder.Reset()
		controller = NewController(env.Clock, env.Client, cloudProvider, recorder)
		nodePool = test.NodePool()
		ExpectApplied(ctx, env.Client, nodePool)
		nodeClaim, node = newNodeClaimAndNode()
	})
	AfterEach(func() {
		ExpectCleanedUp(ctx, env.Client)
		metrics.NodeClaimsDisruptedTotal.Reset()
		NodeClaimsUnhealthyDisruptedTotal.Reset()
	})

	It("should forcefully delete a registered node that never initialized once the toleration elapses", func() {
		register(nodeClaim, node)
		env.Clock.Step(31 * time.Minute)

		ExpectObjectReconciled(ctx, env.Client, controller, node)

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.DeletionTimestamp.IsZero()).To(BeFalse())
		Expect(nodeClaim.Annotations).To(HaveKeyWithValue(v1.NodeClaimTerminationTimestampAnnotationKey, env.Clock.Now().Format(time.RFC3339)))
		ExpectMetricCounterValue(metrics.NodeClaimsDisruptedTotal, 1, map[string]string{
			metrics.ReasonLabel:          metrics.UnhealthyReason,
			metrics.NodePoolLabel:        nodePool.Name,
			metrics.TerminationModeLabel: metrics.TerminationModeForceful,
		})
		ExpectMetricCounterValue(NodeClaimsUnhealthyDisruptedTotal, 1, map[string]string{
			RepairCondition.Name:  "ready",
			metrics.NodePoolLabel: nodePool.Name,
		})
	})
	It("should replace rather than reboot a registered node that never initialized", func() {
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: corev1.NodeReady, ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, Action: cloudprovider.ReplaceNode},
			{ConditionType: corev1.NodeReady, ConditionStatus: corev1.ConditionFalse, ReasonRegex: "^NotReady$", TolerationDuration: 30 * time.Minute, Action: cloudprovider.RebootNode},
		}
		controller = NewController(env.Clock, env.Client, cloudProvider, recorder)
		register(nodeClaim, node)
		env.Clock.Step(31 * time.Minute)

		ExpectObjectReconciled(ctx, env.Client, controller, node)

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.DeletionTimestamp.IsZero()).To(BeFalse())
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting)).To(BeNil())
		Expect(cloudProvider.RebootCalls).To(BeEmpty())
	})
	It("should requeue a registered node that never initialized until the toleration elapses", func() {
		register(nodeClaim, node)
		env.Clock.Step(10 * time.Minute)

		result := ExpectObjectReconciled(ctx, env.Client, controller, node)

		expectNotDeleted(nodeClaim)
		Expect(result.RequeueAfter).To(Equal(20 * time.Minute))
	})
	It("should not requeue a healthy registered node that hasn't initialized", func() {
		register(nodeClaim, node)
		ExpectMakeNodesReady(ctx, env.Client, env.Clock, node)
		env.Clock.Step(31 * time.Minute)

		result := ExpectObjectReconciled(ctx, env.Client, controller, node)

		expectNotDeleted(nodeClaim)
		Expect(result.RequeueAfter).To(BeZero())
	})
	It("should not delete a node that hasn't registered", func() {
		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeLaunched)
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		ExpectMakeNodesNotReady(ctx, env.Client, env.Clock, node)
		env.Clock.Step(31 * time.Minute)

		ExpectObjectReconciled(ctx, env.Client, controller, node)

		expectNotDeleted(nodeClaim)
	})
	It("should not delete a node whose NodeClaim is initialized", func() {
		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeInitialized)
		register(nodeClaim, node)
		env.Clock.Step(31 * time.Minute)

		ExpectObjectReconciled(ctx, env.Client, controller, node)

		expectNotDeleted(nodeClaim)
	})
	It("should not delete a node labeled initialized before its NodeClaim condition catches up", func() {
		node.Labels[v1.NodeInitializedLabelKey] = "true"
		register(nodeClaim, node)
		env.Clock.Step(31 * time.Minute)

		ExpectObjectReconciled(ctx, env.Client, controller, node)

		expectNotDeleted(nodeClaim)
	})
	It("should not delete a node that is rebooting", func() {
		nodeClaim.StatusConditions().SetTrueWithReason(v1.ConditionTypeRebooting, v1.RebootReasonIssued, "rebooting")
		register(nodeClaim, node)
		env.Clock.Step(31 * time.Minute)

		ExpectObjectReconciled(ctx, env.Client, controller, node)

		expectNotDeleted(nodeClaim)
	})
	It("should delete a node that goes unhealthy after a reboot completes but before it re-initializes", func() {
		nodeClaim.StatusConditions(status.WithClock(env.Clock)).SetFalse(v1.ConditionTypeRebooting, v1.RebootReasonSucceeded, "node rebooted and rejoined the cluster")
		register(nodeClaim, node)
		env.Clock.Step(31 * time.Minute)

		ExpectObjectReconciled(ctx, env.Client, controller, node)

		expectDeleted(nodeClaim)
	})
	It("should only count unhealthy time after a reboot completes", func() {
		// The condition predates the reboot, e.g. an agent hasn't re-reported it since the node came back.
		register(nodeClaim, node)
		env.Clock.Step(31 * time.Minute)
		stored := ExpectExists(ctx, env.Client, nodeClaim)
		stored.StatusConditions(status.WithClock(env.Clock)).SetFalse(v1.ConditionTypeRebooting, v1.RebootReasonSucceeded, "node rebooted and rejoined the cluster")
		ExpectApplied(ctx, env.Client, stored)
		env.Clock.Step(10 * time.Minute)

		result := ExpectObjectReconciled(ctx, env.Client, controller, node)

		expectNotDeleted(nodeClaim)
		Expect(result.RequeueAfter).To(Equal(20 * time.Minute))

		env.Clock.Step(20 * time.Minute)
		ExpectObjectReconciled(ctx, env.Client, controller, node)
		expectDeleted(nodeClaim)
	})
	DescribeTable("should not delete a node annotated do-not-repair",
		func(onNode bool) {
			annotations := map[string]string{v1.DoNotRepairAnnotationKey: "true"}
			if onNode {
				node.Annotations = lo.Assign(node.Annotations, annotations)
			} else {
				nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, annotations)
			}
			register(nodeClaim, node)
			env.Clock.Step(31 * time.Minute)

			ExpectObjectReconciled(ctx, env.Client, controller, node)

			expectNotDeleted(nodeClaim)
		},
		Entry("on the Node", true),
		Entry("on the NodeClaim", false),
	)
	It("should delete a node annotated do-not-disrupt", func() {
		node.Annotations = lo.Assign(node.Annotations, map[string]string{v1.DoNotDisruptAnnotationKey: "true"})
		register(nodeClaim, node)
		env.Clock.Step(31 * time.Minute)

		ExpectObjectReconciled(ctx, env.Client, controller, node)

		expectDeleted(nodeClaim)
	})
	It("should not delete a node without a NodePool", func() {
		delete(nodeClaim.Labels, v1.NodePoolLabelKey)
		delete(node.Labels, v1.NodePoolLabelKey)
		register(nodeClaim, node)
		env.Clock.Step(31 * time.Minute)

		ExpectObjectReconciled(ctx, env.Client, controller, node)

		expectNotDeleted(nodeClaim)
	})
	It("should not delete a node when the NodeRepair feature gate is disabled", func() {
		register(nodeClaim, node)
		env.Clock.Step(31 * time.Minute)
		gateOffCtx := options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodeRepair: lo.ToPtr(false)}}))

		ExpectObjectReconciled(gateOffCtx, env.Client, controller, node)

		expectNotDeleted(nodeClaim)
	})
	It("should not delete a NodeClaim that initialized after it was evaluated", func() {
		register(nodeClaim, node)
		env.Clock.Step(31 * time.Minute)
		// Initialize the NodeClaim between the controller's read and its delete.
		racing := NewController(env.Clock, &beforeDeleteClient{Client: env.Client, beforeDelete: func() {
			stored := ExpectExists(ctx, env.Client, nodeClaim)
			stored.StatusConditions().SetTrue(v1.ConditionTypeInitialized)
			ExpectApplied(ctx, env.Client, stored)
		}}, cloudProvider, recorder)

		result := ExpectObjectReconciled(ctx, env.Client, racing, node)

		Expect(result.Requeue).To(BeTrue()) //nolint:staticcheck
		expectNotDeleted(nodeClaim)
		Expect(ExpectExists(ctx, env.Client, nodeClaim).Annotations).ToNot(HaveKey(v1.NodeClaimTerminationTimestampAnnotationKey))
		ExpectObjectReconciled(ctx, env.Client, controller, node)
		expectNotDeleted(nodeClaim)
	})
	It("should not extend an earlier termination deadline", func() {
		earlier := env.Clock.Now().Add(-time.Hour).Format(time.RFC3339)
		nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{v1.NodeClaimTerminationTimestampAnnotationKey: earlier})
		register(nodeClaim, node)
		env.Clock.Step(31 * time.Minute)

		ExpectObjectReconciled(ctx, env.Client, controller, node)

		Expect(ExpectExists(ctx, env.Client, nodeClaim).Annotations).To(HaveKeyWithValue(v1.NodeClaimTerminationTimestampAnnotationKey, earlier))
	})
	Context("Event Ordering", func() {
		It("should delete once the NodeClaim is marked Registered after the Node was labeled registered", func() {
			// Registration labels the Node first; the NodeClaim's Registered condition lands in a later status patch.
			nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeLaunched)
			node.Labels[v1.NodeRegisteredLabelKey] = "true"
			ExpectApplied(ctx, env.Client, nodeClaim, node)
			ExpectMakeNodesNotReady(ctx, env.Client, env.Clock, node)
			env.Clock.Step(31 * time.Minute)

			result := ExpectObjectReconciled(ctx, env.Client, controller, node)
			expectNotDeleted(nodeClaim)
			Expect(result.RequeueAfter).To(BeZero())

			stored := ExpectExists(ctx, env.Client, nodeClaim)
			updated := stored.DeepCopy()
			updated.StatusConditions().SetTrue(v1.ConditionTypeRegistered)
			Expect(passes(nodeClaimPredicates(cloudProvider), stored, updated)).To(BeTrue())
			ExpectApplied(ctx, env.Client, updated)

			ExpectObjectReconciled(ctx, env.Client, controller, node)
			expectDeleted(nodeClaim)
		})
		It("should delete once a reboot reaches a terminal outcome with the node still unhealthy", func() {
			nodeClaim.StatusConditions().SetTrueWithReason(v1.ConditionTypeRebooting, v1.RebootReasonIssued, "rebooting")
			register(nodeClaim, node)
			env.Clock.Step(31 * time.Minute)

			ExpectObjectReconciled(ctx, env.Client, controller, node)
			expectNotDeleted(nodeClaim)

			// The reboot controller writes only the NodeClaim; the Node's conditions don't change.
			stored := ExpectExists(ctx, env.Client, nodeClaim)
			updated := stored.DeepCopy()
			updated.StatusConditions(status.WithClock(env.Clock)).SetFalse(v1.ConditionTypeRebooting, v1.RebootReasonSucceeded, "node rebooted and rejoined the cluster")
			Expect(passes(nodeClaimPredicates(cloudProvider), stored, updated)).To(BeTrue())
			ExpectApplied(ctx, env.Client, updated)

			// The node gets a full toleration after the reboot before it is replaced.
			result := ExpectObjectReconciled(ctx, env.Client, controller, node)
			expectNotDeleted(nodeClaim)
			Expect(result.RequeueAfter).To(Equal(30 * time.Minute))
			env.Clock.Step(30 * time.Minute)
			ExpectObjectReconciled(ctx, env.Client, controller, node)
			expectDeleted(nodeClaim)
		})
	})
	Context("Predicates", func() {
		It("should admit Node updates that can change eligibility", func() {
			register(nodeClaim, node)
			stored := ExpectExists(ctx, env.Client, node)
			mutations := map[string]func(n *corev1.Node){
				"registered":       func(n *corev1.Node) { delete(n.Labels, v1.NodeRegisteredLabelKey) },
				"condition reason": func(n *corev1.Node) { n.Status.Conditions[0].Reason = "KubeletNotReady" },
				"condition status": func(n *corev1.Node) { n.Status.Conditions[0].Status = corev1.ConditionUnknown },
				"condition added": func(n *corev1.Node) {
					n.Status.Conditions = append(n.Status.Conditions, corev1.NodeCondition{Type: "BadNode"})
				},
				"do-not-repair": func(n *corev1.Node) {
					n.Annotations = lo.Assign(n.Annotations, map[string]string{v1.DoNotRepairAnnotationKey: "true"})
				},
				"initialized label removed": func(n *corev1.Node) {},
			}
			for name, mutate := range mutations {
				old, updated := stored.DeepCopy(), stored.DeepCopy()
				if name == "initialized label removed" {
					old.Labels[v1.NodeInitializedLabelKey] = "true"
				}
				mutate(updated)
				Expect(passes(nodePredicates(cloudProvider), old, updated)).To(BeTrue(), name)
			}
		})
		It("should admit a Node that becomes managed after creation", func() {
			register(nodeClaim, node)
			updated := ExpectExists(ctx, env.Client, node)
			old := updated.DeepCopy()
			for k := range old.Labels {
				if strings.HasSuffix(k, "/"+strings.ToLower(object.GVK(cloudProvider.GetSupportedNodeClasses()[0]).Kind)) {
					delete(old.Labels, k)
				}
			}
			Expect(passes(nodePredicates(cloudProvider), old, updated)).To(BeTrue())
		})
		It("should drop Node heartbeats and initialized Nodes", func() {
			register(nodeClaim, node)
			stored := ExpectExists(ctx, env.Client, node)
			heartbeat := stored.DeepCopy()
			heartbeat.Status.Conditions[0].LastHeartbeatTime = metav1.NewTime(env.Clock.Now().Add(time.Minute))
			Expect(passes(nodePredicates(cloudProvider), stored, heartbeat)).To(BeFalse())

			initialized := stored.DeepCopy()
			initialized.Labels[v1.NodeInitializedLabelKey] = "true"
			Expect(passes(nodePredicates(cloudProvider), stored, initialized)).To(BeFalse())
		})
		It("should admit NodeClaim updates that can change eligibility and drop the rest", func() {
			register(nodeClaim, node)
			stored := ExpectExists(ctx, env.Client, nodeClaim)
			for name, mutate := range map[string]func(nc *v1.NodeClaim){
				"registered": func(nc *v1.NodeClaim) { nc.StatusConditions().SetUnknown(v1.ConditionTypeRegistered) },
				"rebooting": func(nc *v1.NodeClaim) {
					nc.StatusConditions().SetTrueWithReason(v1.ConditionTypeRebooting, v1.RebootReasonIssued, "")
				},
				"do-not-repair": func(nc *v1.NodeClaim) {
					nc.Annotations = lo.Assign(nc.Annotations, map[string]string{v1.DoNotRepairAnnotationKey: "true"})
				},
			} {
				updated := stored.DeepCopy()
				mutate(updated)
				Expect(passes(nodeClaimPredicates(cloudProvider), stored, updated)).To(BeTrue(), name)
			}
			unrelated := stored.DeepCopy()
			unrelated.Status.LastPodEventTime = metav1.NewTime(env.Clock.Now())
			Expect(passes(nodeClaimPredicates(cloudProvider), stored, unrelated)).To(BeFalse())
			// Initialized only returns to Unknown when a reboot is issued, which also changes the Node.
			initialized := stored.DeepCopy()
			initialized.StatusConditions().SetTrue(v1.ConditionTypeInitialized)
			Expect(passes(nodeClaimPredicates(cloudProvider), stored, initialized)).To(BeFalse())
		})
		It("should not mutate the cached NodeClaim when filtering", func() {
			register(nodeClaim, node)
			stored := ExpectExists(ctx, env.Client, nodeClaim)
			stored.Status.Conditions = lo.Filter(stored.Status.Conditions, func(c status.Condition, _ int) bool { return c.Type == v1.ConditionTypeRegistered })
			before := stored.DeepCopy()

			passes(nodeClaimPredicates(cloudProvider), stored, stored)

			Expect(stored.Status.Conditions).To(Equal(before.Status.Conditions))
		})
	})
	Context("Deleting NodeClaims", func() {
		// markDeleting deletes the NodeClaim out from under the controller; its finalizer keeps it observable.
		markDeleting := func(nc *v1.NodeClaim) {
			Expect(env.Client.Delete(ctx, nc)).To(Succeed())
		}
		It("should stamp the deadline on an eligible NodeClaim that is already deleting", func() {
			register(nodeClaim, node)
			env.Clock.Step(31 * time.Minute)
			markDeleting(nodeClaim)

			ExpectObjectReconciled(ctx, env.Client, controller, node)

			Expect(ExpectExists(ctx, env.Client, nodeClaim).Annotations).To(HaveKeyWithValue(v1.NodeClaimTerminationTimestampAnnotationKey, env.Clock.Now().Format(time.RFC3339)))
			// Only this controller's own delete counts as a disruption.
			_, found := FindMetricWithLabelValues(ExpectMetricName(metrics.NodeClaimsDisruptedTotal.(*opmetrics.PrometheusCounter)), map[string]string{metrics.ReasonLabel: metrics.UnhealthyReason, metrics.NodePoolLabel: nodePool.Name})
			Expect(found).To(BeFalse())
		})
		It("should retry the deadline when stamping fails after the delete", func() {
			register(nodeClaim, node)
			env.Clock.Step(31 * time.Minute)
			failing := NewController(env.Clock, &failPatchClient{Client: env.Client}, cloudProvider, recorder)

			_ = ExpectObjectReconcileFailed(ctx, env.Client, failing, node)
			expectDeleted(nodeClaim)
			Expect(ExpectExists(ctx, env.Client, nodeClaim).Annotations).ToNot(HaveKey(v1.NodeClaimTerminationTimestampAnnotationKey))

			ExpectObjectReconciled(ctx, env.Client, controller, node)
			Expect(ExpectExists(ctx, env.Client, nodeClaim).Annotations).To(HaveKeyWithValue(v1.NodeClaimTerminationTimestampAnnotationKey, env.Clock.Now().Format(time.RFC3339)))
			ExpectMetricCounterValue(metrics.NodeClaimsDisruptedTotal, 1, map[string]string{metrics.ReasonLabel: metrics.UnhealthyReason, metrics.NodePoolLabel: nodePool.Name})
		})
		It("should not stamp a deleting NodeClaim that isn't eligible", func() {
			register(nodeClaim, node)
			env.Clock.Step(10 * time.Minute)
			markDeleting(nodeClaim)

			ExpectObjectReconciled(ctx, env.Client, controller, node)

			Expect(ExpectExists(ctx, env.Client, nodeClaim).Annotations).ToNot(HaveKey(v1.NodeClaimTerminationTimestampAnnotationKey))
		})
		It("should not stamp a deleting NodeClaim annotated do-not-repair", func() {
			nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{v1.DoNotRepairAnnotationKey: "true"})
			register(nodeClaim, node)
			env.Clock.Step(31 * time.Minute)
			markDeleting(nodeClaim)

			ExpectObjectReconciled(ctx, env.Client, controller, node)

			Expect(ExpectExists(ctx, env.Client, nodeClaim).Annotations).ToNot(HaveKey(v1.NodeClaimTerminationTimestampAnnotationKey))
		})
	})
	Context("Circuit Breaker", func() {
		// pool builds a ten-node NodePool with the given number of registered-but-unhealthy nodes, all past toleration,
		// and returns the first unhealthy pair.
		pool := func(unhealthy int) (*v1.NodeClaim, *corev1.Node) {
			for range 10 - unhealthy {
				nc, n := newNodeClaimAndNode()
				initialize(nc, n)
			}
			register(nodeClaim, node)
			for range unhealthy - 1 {
				nc, n := newNodeClaimAndNode()
				register(nc, n)
			}
			env.Clock.Step(31 * time.Minute)
			return nodeClaim, node
		}
		It("should delete when the NodePool is at the unhealthy threshold", func() {
			nc, n := pool(2)

			ExpectObjectReconciled(ctx, env.Client, controller, n)

			expectDeleted(nc)
		})
		It("should block and requeue when the NodePool is above the unhealthy threshold", func() {
			nc, n := pool(3)

			result := ExpectObjectReconciled(ctx, env.Client, controller, n)

			expectNotDeleted(nc)
			Expect(result.RequeueAfter).To(Equal(breakerRequeue))
			Expect(recorder.Calls(events.NodeRepairBlocked)).To(BeNumerically(">", 0))
		})
		It("should count initialized unhealthy nodes toward the threshold", func() {
			for range 7 {
				nc, n := newNodeClaimAndNode()
				initialize(nc, n)
			}
			for range 2 {
				nc, n := newNodeClaimAndNode()
				initialize(nc, n)
				ExpectMakeNodesNotReady(ctx, env.Client, env.Clock, n)
			}
			register(nodeClaim, node)
			env.Clock.Step(31 * time.Minute)

			ExpectObjectReconciled(ctx, env.Client, controller, node)

			expectNotDeleted(nodeClaim)
		})
	})
})

var _ = Describe("RepairResult NextEligibleAt", func() {
	It("should return the earliest future eligibility across matching policies", func() {
		matcher, err := NewRepairPolicyMatcher([]cloudprovider.RepairPolicy{
			{ConditionType: corev1.NodeReady, ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, Action: cloudprovider.ReplaceNode},
			{ConditionType: corev1.NodeReady, ConditionStatus: corev1.ConditionFalse, ReasonRegex: "^Kubelet", TolerationDuration: 10 * time.Minute, Action: cloudprovider.ReplaceNode},
			{ConditionType: "DiskPressure", ConditionStatus: corev1.ConditionTrue, ReasonRegex: ".*", TolerationDuration: 5 * time.Minute, Action: cloudprovider.ReplaceNode},
			{ConditionType: "MemoryPressure", ConditionStatus: corev1.ConditionTrue, ReasonRegex: ".*", TolerationDuration: time.Hour, Action: cloudprovider.ReplaceNode},
		}, supportedRepairActions)
		Expect(err).ToNot(HaveOccurred())
		now := time.Now().Truncate(time.Second)
		node := &corev1.Node{Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: corev1.ConditionFalse, Reason: "KubeletDown", LastTransitionTime: metav1.NewTime(now.Add(-8 * time.Minute))},
			{Type: "DiskPressure", Status: corev1.ConditionTrue, Reason: "Full", LastTransitionTime: metav1.NewTime(now.Add(-6 * time.Minute))},
			// Pending later than Ready, and evaluated after it, so the earliest pending eligibility must win.
			{Type: "MemoryPressure", Status: corev1.ConditionTrue, Reason: "Full", LastTransitionTime: metav1.NewTime(now)},
		}}}

		result := matcher.Evaluate(node, now)

		// DiskPressure is already eligible; the reason-specific Ready policy (not the 30m fallback) is next.
		Expect(result.Action).To(Equal(cloudprovider.ReplaceNode))
		Expect(result.NextEligibleAt).To(Equal(now.Add(2 * time.Minute)))
	})
	It("should measure toleration from no earlier than notBefore", func() {
		matcher, err := NewRepairPolicyMatcher([]cloudprovider.RepairPolicy{
			{ConditionType: corev1.NodeReady, ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, Action: cloudprovider.ReplaceNode},
		}, supportedRepairActions)
		Expect(err).ToNot(HaveOccurred())
		now := time.Now().Truncate(time.Second)
		node := &corev1.Node{Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: corev1.ConditionFalse, LastTransitionTime: metav1.NewTime(now.Add(-time.Hour))},
		}}}

		Expect(matcher.Evaluate(node, now).Action).To(Equal(cloudprovider.ReplaceNode))
		result := matcher.EvaluateSince(node, now, now.Add(-10*time.Minute))
		Expect(result.Action).To(BeEmpty())
		Expect(result.NextEligibleAt).To(Equal(now.Add(20 * time.Minute)))
		// A notBefore that predates the condition changes nothing.
		Expect(matcher.EvaluateSince(node, now, now.Add(-2*time.Hour)).Action).To(Equal(cloudprovider.ReplaceNode))
	})
	It("should return false when every matching policy is already eligible", func() {
		matcher, err := NewRepairPolicyMatcher([]cloudprovider.RepairPolicy{
			{ConditionType: corev1.NodeReady, ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, Action: cloudprovider.ReplaceNode},
		}, supportedRepairActions)
		Expect(err).ToNot(HaveOccurred())
		now := time.Now().Truncate(time.Second)
		node := &corev1.Node{Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: corev1.ConditionFalse, LastTransitionTime: metav1.NewTime(now.Add(-time.Hour))},
		}}}

		Expect(matcher.Evaluate(node, now).NextEligibleAt.IsZero()).To(BeTrue())
	})
})

// beforeDeleteClient runs beforeDelete ahead of each Delete so tests can race a write against the controller's delete.
type beforeDeleteClient struct {
	client.Client
	beforeDelete func()
}

func (c *beforeDeleteClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	c.beforeDelete()
	return c.Client.Delete(ctx, obj, opts...)
}

// passes reports whether an update event from old to updated passes every predicate.
func passes(predicates []predicate.Predicate, old, updated client.Object) bool {
	return lo.EveryBy(predicates, func(p predicate.Predicate) bool {
		return p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: updated})
	})
}

// failPatchClient fails every Patch so tests can fail the termination deadline stamp after a successful delete.
type failPatchClient struct {
	client.Client
}

func (c *failPatchClient) Patch(context.Context, client.Object, client.Patch, ...client.PatchOption) error {
	return fmt.Errorf("injected patch failure")
}
