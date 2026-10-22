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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Evaluation Metrics", func() {
	var nodePool *v1.NodePool
	var nodeClaims []*v1.NodeClaim
	var nodes []*corev1.Node

	phaseMetric := "karpenter_voluntary_disruption_decision_evaluation_phase_duration_seconds"
	simulationsMetric := "karpenter_voluntary_disruption_decision_evaluation_scheduling_simulations"
	evaluationMetric := "karpenter_voluntary_disruption_decision_evaluation_duration_seconds"
	gateMetric := "karpenter_voluntary_disruption_cluster_state_gate_duration_seconds"

	emptyLabels := map[string]string{metrics.ReasonLabel: "empty", disruption.ConsolidationTypeLabel: "empty"}
	driftLabels := map[string]string{metrics.ReasonLabel: "drifted"}
	multiLabels := map[string]string{metrics.ReasonLabel: "underutilized", disruption.ConsolidationTypeLabel: "multi"}
	singleLabels := map[string]string{metrics.ReasonLabel: "underutilized", disruption.ConsolidationTypeLabel: "single"}
	withPhase := func(labels map[string]string, phase string) map[string]string {
		return lo.Assign(labels, map[string]string{"phase": phase})
	}
	histogramSum := func(name string, labels map[string]string) float64 {
		GinkgoHelper()
		m, ok := FindMetricWithLabelValues(name, labels)
		Expect(ok).To(BeTrue(), "Metric "+name+" should be available")
		return m.Histogram.GetSampleSum()
	}

	BeforeEach(func() {
		nodePool = test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
					Budgets:             []v1.Budget{{Nodes: "100%"}},
				},
			},
		})
		nodeClaims, nodes = test.NodeClaimsAndNodes(1, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: leastExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        leastExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
					corev1.LabelTopologyZone:       leastExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
				},
			},
			Status: v1.NodeClaimStatus{
				Allocatable: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:  resource.MustParse("32"),
					corev1.ResourcePods: resource.MustParse("100"),
				},
			},
		})
		nodeClaims[0].StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
	})
	It("should record every phase of the method that acts, and not count the methods after a delete-only command as skipped", func() {
		pipelineDifference("R84", "PR 01's phase, simulation, skipped and gated-pass metrics (and R61's decision_evaluation_duration_seconds) are not emitted with the gate on")
		ExpectApplied(ctx, env.Client, nodePool, nodeClaims[0], nodes[0])
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
		ExpectSingletonReconciled(ctx, disruptionController)
		ExpectMetricCounterValue(disruption.DecisionsPerformedTotal, 1, map[string]string{"decision": "delete", metrics.ReasonLabel: "empty", disruption.ConsolidationTypeLabel: "empty"})

		// Emptiness acted, so each of its phases is observed exactly once, alongside the whole-evaluation histogram.
		for _, phase := range []string{"candidates", "budgets", "compute", "validate", "start"} {
			ExpectMetricHistogramSampleCountValue(phaseMetric, 1, withPhase(emptyLabels, phase))
		}
		ExpectMetricHistogramSampleCountValue(evaluationMetric, 1, emptyLabels)
		// Emptiness deletes without simulating, and the nop validator doesn't re-simulate.
		ExpectMetricHistogramSampleCountValue(simulationsMetric, 1, emptyLabels)
		Expect(histogramSum(simulationsMetric, emptyLabels)).To(BeZero())

		// StaticDrift and Drift ran first (sharing reason=drifted), found no candidates, and stopped after the build.
		ExpectMetricHistogramSampleCountValue(phaseMetric, 2, withPhase(driftLabels, "candidates"))
		_, found := FindMetricWithLabelValues(phaseMetric, withPhase(driftLabels, "budgets"))
		Expect(found).To(BeFalse())
		ExpectMetricHistogramSampleCountValue(simulationsMetric, 2, driftLabels)

		// Emptiness's command only deletes, so the pass went on to multi- and single-node consolidation, which were
		// evaluated (and found the node already marked for deletion). Nothing was skipped. The drift spec below covers
		// a command that ends the pass.
		for _, labels := range []map[string]string{emptyLabels, driftLabels, multiLabels, singleLabels} {
			ExpectMetricCounterValue(disruption.SkippedEvaluationsTotal, 0, labels)
			ExpectMetricGaugeValue(disruption.LastEvaluationTimestampSeconds, float64(env.Clock.Now().Unix()), labels)
		}
	})
	It("should count the scheduling simulations of an evaluation", func() {
		pipelineDifference("R84", "PR 01's phase, simulation, skipped and gated-pass metrics (and R61's decision_evaluation_duration_seconds) are not emitted with the gate on")
		nodeClaims[0].StatusConditions().SetTrue(v1.ConditionTypeDrifted)
		Expect(nodeClaims[0].StatusConditions().Clear(v1.ConditionTypeConsolidatable)).To(Succeed())
		pods := test.Pods(2, test.PodOptions{})
		ExpectApplied(ctx, env.Client, nodePool, nodeClaims[0], nodes[0], pods[0], pods[1])
		ExpectManualBinding(ctx, env.Client, pods[0], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[1], nodes[0])
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
		ExpectSingletonReconciled(ctx, disruptionController)
		ExpectMetricCounterValue(disruption.DecisionsPerformedTotal, 1, map[string]string{"decision": "replace", metrics.ReasonLabel: "drifted"})

		// StaticDrift (no static NodePool, no candidates) and Drift (one simulation of the drifted node's pods).
		ExpectMetricHistogramSampleCountValue(simulationsMetric, 2, driftLabels)
		Expect(histogramSum(simulationsMetric, driftLabels)).To(BeNumerically(">=", 1))
		ExpectMetricHistogramSampleCountValue(phaseMetric, 1, withPhase(driftLabels, "start"))
		// Drift doesn't validate, so it has no validate phase.
		_, found := FindMetricWithLabelValues(phaseMetric, withPhase(driftLabels, "validate"))
		Expect(found).To(BeFalse())
		for _, labels := range []map[string]string{emptyLabels, multiLabels, singleLabels} {
			ExpectMetricCounterValue(disruption.SkippedEvaluationsTotal, 1, labels)
		}
	})
	It("should report the commands in flight in the orchestration queue", func() {
		ExpectApplied(ctx, env.Client, nodePool, nodeClaims[0], nodes[0])
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
		ExpectSingletonReconciled(ctx, disruptionController)

		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))
		ExpectMetricGaugeValue(disruption.QueueCommands, 1, nil)
		queue.CompleteCommand(cmds[0])
		ExpectMetricGaugeValue(disruption.QueueCommands, 0, nil)
	})
	It("should measure how long the loop is gated on cluster state sync", func() {
		pipelineDifference("R84", "PR 01's phase, simulation, skipped and gated-pass metrics (and R61's decision_evaluation_duration_seconds) are not emitted with the gate on")
		// The NodeClaim exists in the API server but not yet in cluster state, so state is not synced.
		ExpectApplied(ctx, env.Client, nodePool, nodeClaims[0])
		ExpectSingletonReconciled(ctx, disruptionController)
		env.Clock.Step(30 * time.Second)
		ExpectSingletonReconciled(ctx, disruptionController)
		ExpectMetricCounterValue(disruption.ClusterStateGatedPassesTotal, 2, nil)
		// The gate period is only observed once the loop gets past the gate.
		_, found := FindMetricWithLabelValues(gateMetric, nil)
		Expect(found).To(BeFalse())
		_, found = FindMetricWithLabelValues(evaluationMetric, emptyLabels)
		Expect(found).To(BeFalse())

		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nodeClaims[0]))
		ExpectSingletonReconciled(ctx, disruptionController)
		ExpectMetricCounterValue(disruption.ClusterStateGatedPassesTotal, 2, nil)
		ExpectMetricHistogramSampleCountValue(gateMetric, 1, nil)
		Expect(histogramSum(gateMetric, nil)).To(BeNumerically("==", 30))

		// Further synced passes don't observe again.
		ExpectSingletonReconciled(ctx, disruptionController)
		ExpectMetricHistogramSampleCountValue(gateMetric, 1, nil)
	})
})
