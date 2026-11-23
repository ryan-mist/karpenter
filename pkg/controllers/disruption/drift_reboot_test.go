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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/awslabs/operatorpkg/singleton"
	"github.com/awslabs/operatorpkg/status"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	nodeclaimdisruption "sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/disruption"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

const amiDrift cloudprovider.DriftReason = "AMIDrift"

var rebootDriftStatus = cloudprovider.DriftStatus{Action: cloudprovider.RebootNode, Reason: amiDrift}

// rebootingNodeClaims returns the names of the NodeClaims with a committed reboot.
func rebootingNodeClaims() []string {
	GinkgoHelper()
	return lo.FilterMap(ExpectNodeClaims(ctx, env.Client), func(nc *v1.NodeClaim, _ int) (string, bool) {
		return nc.Name, nc.StatusConditions().Get(v1.ConditionTypeRebooting).IsTrue()
	})
}

// driftedNodeClaimsAndNodes returns n NodeClaims and Nodes in the NodePool, marked drifted for a provider reason.
func driftedNodeClaimsAndNodes(n int, nodePool *v1.NodePool) ([]*v1.NodeClaim, []*corev1.Node) {
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
				corev1.ResourceCPU:  resource.MustParse("32"),
				corev1.ResourcePods: resource.MustParse("100"),
			},
		},
	})
	for _, nc := range nodeClaims {
		nc.StatusConditions().SetTrueWithReason(v1.ConditionTypeDrifted, string(amiDrift), string(amiDrift))
		Expect(nc.StatusConditions().Clear(v1.ConditionTypeConsolidatable)).To(Succeed())
	}
	return nodeClaims, nodes
}

func applyAndInitialize(nodePool *v1.NodePool, nodeClaims []*v1.NodeClaim, nodes []*corev1.Node) {
	GinkgoHelper()
	ExpectApplied(ctx, env.Client, nodePool)
	for i := range nodeClaims {
		ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
	}
	ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
}

var _ = Describe("Drift In-Place Reboot", func() {
	var nodePool *v1.NodePool

	BeforeEach(func() {
		nodePool = test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidateAfter: v1.MustParseNillableDuration("Never"),
					Budgets:          []v1.Budget{{Nodes: "100%"}},
				},
			},
		})
	})

	It("should commit an in-place reboot when the provider resolves drift with RebootNode", func() {
		nodeClaims, nodes := driftedNodeClaimsAndNodes(1, nodePool)
		cloudProvider.DriftStatuses[nodeClaims[0].Name] = rebootDriftStatus
		applyAndInitialize(nodePool, nodeClaims, nodes)

		result := ExpectSingletonReconciled(ctx, disruptionController)
		Expect(result.RequeueAfter).To(Equal(singleton.RequeueImmediately))

		// Reboot is in-place: nothing is queued, no replacement is launched and the node isn't tainted or deleted.
		Expect(queue.GetCommands()).To(BeEmpty())
		Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(1))
		Expect(ExpectExists(ctx, env.Client, nodes[0]).Spec.Taints).ToNot(ContainElement(v1.DisruptedNoScheduleTaint))

		nc := ExpectExists(ctx, env.Client, nodeClaims[0])
		Expect(nc.DeletionTimestamp.IsZero()).To(BeTrue())
		cond := nc.StatusConditions().Get(v1.ConditionTypeRebooting)
		Expect(cond.IsTrue()).To(BeTrue())
		Expect(cond.Reason).To(Equal(v1.RebootReasonRequested))
		Expect(cond.Message).To(Equal(fmt.Sprintf("rebooting to resolve drift (%s)", amiDrift)))
		Expect(nc.StatusConditions().Get(v1.ConditionTypeDisruptionReason).Reason).To(Equal(string(v1.DisruptionReasonDrifted)))
		// No NodeClaim TGP means an unbounded drain.
		Expect(nc.Annotations).ToNot(HaveKey(v1.RebootTerminationGracePeriodAnnotationKey))

		ExpectMetricCounterValue(disruption.DecisionsPerformedTotal, 1, map[string]string{
			"decision":          string(disruption.RebootDecision),
			metrics.ReasonLabel: strings.ToLower(string(v1.DisruptionReasonDrifted)),
		})
		ExpectMetricCounterValue(metrics.NodeClaimsDisruptedTotal, 1, map[string]string{
			metrics.ReasonLabel:   strings.ToLower(string(v1.DisruptionReasonDrifted)),
			metrics.NodePoolLabel: nodePool.Name,
		})

		// The next pass sees the reboot in cluster state, so it neither recommits it nor clears its DisruptionReason.
		result = ExpectSingletonReconciled(ctx, disruptionController)
		Expect(result.RequeueAfter).ToNot(Equal(singleton.RequeueImmediately))
		nc = ExpectExists(ctx, env.Client, nodeClaims[0])
		Expect(nc.StatusConditions().Get(v1.ConditionTypeDisruptionReason).IsTrue()).To(BeTrue())
		Expect(nc.StatusConditions().Get(v1.ConditionTypeRebooting).Reason).To(Equal(v1.RebootReasonRequested))
	})
	It("should stamp the NodeClaim's TGP as the reboot drain bound and admit candidates with blocking pods", func() {
		nodeClaims, nodes := driftedNodeClaimsAndNodes(1, nodePool)
		nodeClaims[0].Spec.TerminationGracePeriod = &metav1.Duration{Duration: 7 * time.Minute}
		cloudProvider.DriftStatuses[nodeClaims[0].Name] = rebootDriftStatus
		podLabels := map[string]string{"test": "value"}
		pod := test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: podLabels}})
		pdb := test.PodDisruptionBudget(test.PDBOptions{Labels: podLabels, MaxUnavailable: fromInt(0)})
		ExpectApplied(ctx, env.Client, pod, pdb)
		applyAndInitialize(nodePool, nodeClaims, nodes)
		ExpectManualBinding(ctx, env.Client, pod, nodes[0])
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)

		ExpectSingletonReconciled(ctx, disruptionController)

		nc := ExpectExists(ctx, env.Client, nodeClaims[0])
		Expect(nc.StatusConditions().Get(v1.ConditionTypeRebooting).IsTrue()).To(BeTrue())
		Expect(nc.Annotations).To(HaveKeyWithValue(v1.RebootTerminationGracePeriodAnnotationKey, "7m0s"))
	})
	It("should remove a stale reboot TGP annotation left by an earlier reboot when the NodeClaim has no TGP", func() {
		nodeClaims, nodes := driftedNodeClaimsAndNodes(1, nodePool)
		// A forceful repair policy left a 0s bound behind; a drift reboot must not inherit it and skip the drain.
		nodeClaims[0].Annotations = lo.Assign(nodeClaims[0].Annotations, map[string]string{v1.RebootTerminationGracePeriodAnnotationKey: "0s"})
		cloudProvider.DriftStatuses[nodeClaims[0].Name] = rebootDriftStatus
		applyAndInitialize(nodePool, nodeClaims, nodes)

		ExpectSingletonReconciled(ctx, disruptionController)

		nc := ExpectExists(ctx, env.Client, nodeClaims[0])
		Expect(nc.StatusConditions().Get(v1.ConditionTypeRebooting).IsTrue()).To(BeTrue())
		Expect(nc.Annotations).ToNot(HaveKey(v1.RebootTerminationGracePeriodAnnotationKey))
	})
	It("should replace through the existing path when the provider returns ReplaceNode", func() {
		nodeClaims, nodes := driftedNodeClaimsAndNodes(1, nodePool)
		cloudProvider.DriftStatuses[nodeClaims[0].Name] = cloudprovider.DriftStatus{Action: cloudprovider.ReplaceNode, Reason: amiDrift}
		applyAndInitialize(nodePool, nodeClaims, nodes)

		ExpectSingletonReconciled(ctx, disruptionController)

		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Decision()).To(Equal(disruption.DeleteDecision)) // the node is empty, so nothing needs a replacement
		Expect(cmds[0].Reboot).To(BeFalse())
		Expect(rebootingNodeClaims()).To(BeEmpty())
		Expect(ExpectExists(ctx, env.Client, nodes[0]).Spec.Taints).To(ContainElement(v1.DisruptedNoScheduleTaint))
	})
	It("should skip a candidate whose drift status errors and move on to the next one", func() {
		nodeClaims, nodes := driftedNodeClaimsAndNodes(2, nodePool)
		// The first candidate drifted earlier, so it's evaluated first.
		Expect(nodeClaims[0].StatusConditions().Clear(v1.ConditionTypeDrifted)).To(Succeed())
		nodeClaims[0].StatusConditions(status.WithClock(clocktesting.NewFakeClock(env.Clock.Now().Add(-time.Hour)))).SetTrueWithReason(
			v1.ConditionTypeDrifted, string(amiDrift), string(amiDrift))
		cloudProvider.DriftStatusErrors[nodeClaims[0].Name] = fmt.Errorf("in-place operation is unresolved")
		cloudProvider.DriftStatuses[nodeClaims[1].Name] = rebootDriftStatus
		applyAndInitialize(nodePool, nodeClaims, nodes)

		ExpectSingletonReconciled(ctx, disruptionController)

		Expect(cloudProvider.GetDriftStatusCalls).To(ContainElement(nodeClaims[0].Name))
		Expect(rebootingNodeClaims()).To(ConsistOf(nodeClaims[1].Name))
		Expect(queue.GetCommands()).To(BeEmpty())
		Expect(ExpectExists(ctx, env.Client, nodes[0]).Spec.Taints).ToNot(ContainElement(v1.DisruptedNoScheduleTaint))
	})
	DescribeTable("should skip a candidate the provider doesn't consider drifted",
		func(driftStatus cloudprovider.DriftStatus) {
			nodeClaims, nodes := driftedNodeClaimsAndNodes(1, nodePool)
			cloudProvider.DriftStatuses[nodeClaims[0].Name] = driftStatus
			applyAndInitialize(nodePool, nodeClaims, nodes)

			ExpectSingletonReconciled(ctx, disruptionController)

			Expect(cloudProvider.GetDriftStatusCalls).To(ContainElement(nodeClaims[0].Name))
			Expect(queue.GetCommands()).To(BeEmpty())
			Expect(rebootingNodeClaims()).To(BeEmpty())
			Expect(ExpectExists(ctx, env.Client, nodes[0]).Spec.Taints).ToNot(ContainElement(v1.DisruptedNoScheduleTaint))
		},
		Entry("NoOp", cloudprovider.NotDrifted),
		Entry("an empty action", cloudprovider.DriftStatus{}),
		Entry("an unsupported action", cloudprovider.DriftStatus{Action: "Teleport", Reason: amiDrift}),
	)
	DescribeTable("should replace drift that core detected without asking the provider",
		func(reason cloudprovider.DriftReason) {
			nodeClaims, nodes := driftedNodeClaimsAndNodes(1, nodePool)
			nodeClaims[0].StatusConditions().SetTrueWithReason(v1.ConditionTypeDrifted, string(reason), string(reason))
			// A reboot can't resolve drift from the NodePool, so the provider's answer must not matter.
			cloudProvider.DriftStatuses[nodeClaims[0].Name] = rebootDriftStatus
			applyAndInitialize(nodePool, nodeClaims, nodes)

			ExpectSingletonReconciled(ctx, disruptionController)

			Expect(cloudProvider.GetDriftStatusCalls).ToNot(ContainElement(nodeClaims[0].Name))
			Expect(queue.GetCommands()).To(HaveLen(1))
			Expect(rebootingNodeClaims()).To(BeEmpty())
		},
		Entry("NodePoolDrifted", nodeclaimdisruption.NodePoolDrifted),
		Entry("RequirementsDrifted", nodeclaimdisruption.RequirementsDrifted),
		Entry("InstanceTypeNotFound", nodeclaimdisruption.InstanceTypeNotFound),
	)
	It("should respect the budget across passes before the informer observes the committed reboot", func() {
		nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "1"}}
		nodeClaims, nodes := driftedNodeClaimsAndNodes(3, nodePool)
		for _, nc := range nodeClaims {
			cloudProvider.DriftStatuses[nc.Name] = rebootDriftStatus
		}
		applyAndInitialize(nodePool, nodeClaims, nodes)

		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(rebootingNodeClaims()).To(HaveLen(1))

		// Cluster state was updated by the commit itself, without reconciling the NodeClaim state controller.
		ExpectSingletonReconciled(ctx, disruptionController)
		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(rebootingNodeClaims()).To(HaveLen(1))
		ExpectMetricGaugeValue(disruption.NodePoolNodesConsumingBudgets, 1, map[string]string{
			metrics.NodePoolLabel: nodePool.Name,
			metrics.ReasonLabel:   strings.ToLower(string(v1.DisruptionReasonDrifted)),
		})
	})
	It("should not commit a reboot for a NodeClaim that is already rebooting in the API server", func() {
		nodeClaims, nodes := driftedNodeClaimsAndNodes(1, nodePool)
		cloudProvider.DriftStatuses[nodeClaims[0].Name] = rebootDriftStatus
		applyAndInitialize(nodePool, nodeClaims, nodes)
		// Cluster state lags: the API server already has a reboot that the state NodeClaim doesn't show yet.
		nc := ExpectExists(ctx, env.Client, nodeClaims[0])
		nc.StatusConditions().SetTrueWithReason(v1.ConditionTypeRebooting, v1.RebootReasonIssued, "rebooting for another reason")
		ExpectApplied(ctx, env.Client, nc)

		ExpectSingletonReconciled(ctx, disruptionController)

		nc = ExpectExists(ctx, env.Client, nodeClaims[0])
		Expect(nc.StatusConditions().Get(v1.ConditionTypeRebooting).Reason).To(Equal(v1.RebootReasonIssued))
		Expect(nc.StatusConditions().Get(v1.ConditionTypeRebooting).Message).To(Equal("rebooting for another reason"))
		Expect(queue.GetCommands()).To(BeEmpty())
	})
})

var _ = Describe("StaticDrift In-Place Reboot", func() {
	var nodePool *v1.NodePool

	BeforeEach(func() {
		nodePool = test.StaticNodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Replicas:   new(int64(3)),
				Disruption: v1.Disruption{Budgets: []v1.Budget{{Nodes: "100%"}}},
			},
		})
	})

	It("should reboot at the node limit without reserving it or reporting the candidates blocked", func() {
		nodePool.Spec.Limits = v1.Limits{resources.Node: resource.MustParse("3")}
		nodeClaims, nodes := driftedNodeClaimsAndNodes(3, nodePool)
		for _, nc := range nodeClaims {
			cloudProvider.DriftStatuses[nc.Name] = rebootDriftStatus
		}
		applyAndInitialize(nodePool, nodeClaims, nodes)

		result := ExpectSingletonReconciled(ctx, disruptionController)
		Expect(result.RequeueAfter).To(Equal(singleton.RequeueImmediately))

		Expect(rebootingNodeClaims()).To(HaveLen(3))
		Expect(queue.GetCommands()).To(BeEmpty())
		Expect(recorder.Calls(events.DisruptionBlocked)).To(BeZero())
		for _, nc := range nodeClaims {
			Expect(ExpectExists(ctx, env.Client, nc).StatusConditions().Get(v1.ConditionTypeDisruptionReason).Reason).To(Equal(string(v1.DisruptionReasonDrifted)))
		}
		// Nothing was reserved against limits.nodes: the one slot a limit of 4 leaves is still free.
		Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 4, 1)).To(Equal(int64(1)))
		ExpectMetricCounterValue(disruption.DecisionsPerformedTotal, 3, map[string]string{
			"decision":          string(disruption.RebootDecision),
			metrics.ReasonLabel: strings.ToLower(string(v1.DisruptionReasonDrifted)),
		})
	})
	It("should reboot while the NodePool is scaling down", func() {
		nodePool.Spec.Replicas = new(int64(1))
		nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "1"}}
		nodeClaims, nodes := driftedNodeClaimsAndNodes(2, nodePool)
		for _, nc := range nodeClaims {
			cloudProvider.DriftStatuses[nc.Name] = rebootDriftStatus
		}
		applyAndInitialize(nodePool, nodeClaims, nodes)

		ExpectSingletonReconciled(ctx, disruptionController)

		Expect(rebootingNodeClaims()).To(HaveLen(1))
		Expect(queue.GetCommands()).To(BeEmpty())
	})
	It("should consume the budget across passes before the informer observes the committed reboots", func() {
		nodePool.Spec.Replicas = new(int64(5))
		nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "2"}}
		nodeClaims, nodes := driftedNodeClaimsAndNodes(5, nodePool)
		for _, nc := range nodeClaims {
			cloudProvider.DriftStatuses[nc.Name] = rebootDriftStatus
		}
		applyAndInitialize(nodePool, nodeClaims, nodes)

		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(rebootingNodeClaims()).To(HaveLen(2))
		// Only the candidates the budget allows are asked.
		Expect(cloudProvider.GetDriftStatusCalls).To(HaveLen(2))

		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(rebootingNodeClaims()).To(HaveLen(2))
	})
	It("should split the budget between reboots and replacements", func() {
		nodePool.Spec.Replicas = new(int64(4))
		nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "2"}}
		nodeClaims, nodes := driftedNodeClaimsAndNodes(4, nodePool)
		applyAndInitialize(nodePool, nodeClaims, nodes)
		// Candidate order isn't fixed, so every candidate reboots except one the provider replaces.
		replaced := nodeClaims[0].Name
		for _, nc := range nodeClaims[1:] {
			cloudProvider.DriftStatuses[nc.Name] = rebootDriftStatus
		}
		cloudProvider.DriftStatuses[replaced] = cloudprovider.DriftStatus{Action: cloudprovider.ReplaceNode, Reason: amiDrift}

		ExpectSingletonReconciled(ctx, disruptionController)

		rebooting := rebootingNodeClaims()
		cmds := queue.GetCommands()
		// Two disruptions in total, whatever the split.
		Expect(len(rebooting) + len(cmds)).To(Equal(2))
		Expect(rebooting).ToNot(ContainElement(replaced))
		for _, cmd := range cmds {
			Expect(cmd.Decision()).To(Equal(disruption.ReplaceDecision))
			Expect(cmd.Candidates[0].NodeClaim.Name).To(Equal(replaced))
		}
	})
	It("should reboot one candidate and block the replacement of another at the node limit", func() {
		nodePool.Spec.Replicas = new(int64(2))
		nodePool.Spec.Limits = v1.Limits{resources.Node: resource.MustParse(strconv.Itoa(2))}
		nodeClaims, nodes := driftedNodeClaimsAndNodes(2, nodePool)
		cloudProvider.DriftStatuses[nodeClaims[0].Name] = rebootDriftStatus
		cloudProvider.DriftStatuses[nodeClaims[1].Name] = cloudprovider.DriftStatus{Action: cloudprovider.ReplaceNode, Reason: amiDrift}
		applyAndInitialize(nodePool, nodeClaims, nodes)

		ExpectSingletonReconciled(ctx, disruptionController)

		Expect(rebootingNodeClaims()).To(ConsistOf(nodeClaims[0].Name))
		Expect(queue.GetCommands()).To(BeEmpty())
		Expect(recorder.Calls(events.DisruptionBlocked)).To(BeNumerically(">", 0))
	})
	It("should skip candidates whose drift status errors or is NoOp", func() {
		nodeClaims, nodes := driftedNodeClaimsAndNodes(3, nodePool)
		cloudProvider.DriftStatusErrors[nodeClaims[0].Name] = fmt.Errorf("in-place operation is unresolved")
		cloudProvider.DriftStatuses[nodeClaims[1].Name] = cloudprovider.NotDrifted
		cloudProvider.DriftStatuses[nodeClaims[2].Name] = rebootDriftStatus
		applyAndInitialize(nodePool, nodeClaims, nodes)

		ExpectSingletonReconciled(ctx, disruptionController)

		Expect(rebootingNodeClaims()).To(ConsistOf(nodeClaims[2].Name))
		Expect(queue.GetCommands()).To(BeEmpty())
	})
})
