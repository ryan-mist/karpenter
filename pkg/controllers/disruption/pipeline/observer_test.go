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

package pipeline_test

import (
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Observer", func() {
	var w *world
	var empty *stubPlanner
	var c *pipeline.Controller
	fast := map[string]string{"lane": "fast"}
	BeforeEach(func() {
		for _, m := range []interface{ Reset() }{pipeline.RoundsTotal, pipeline.PlansTotal, pipeline.GatedSecondsTotal, pipeline.UnresolvedPlans,
			pipeline.LedgerReservedNodes, pipeline.LastAdmittedTimestampSeconds, pipeline.RoundDurationSeconds, pipeline.PlanWaitSeconds,
			pipeline.AdmissionResimsTotal, disruption.LastEvaluationTimestampSeconds, disruption.ClusterStateGateDurationSeconds,
			disruption.NodePoolAllowedDisruptions, disruption.NodePoolNodesConsumingBudgets} {
			m.Reset()
		}
		w = newWorld(3, 2)
		empty = newStubPlanner("emptiness")
		empty.shape, empty.reason = types.ShapePodFree, v1.DisruptionReasonEmpty
		empty.filter = func(c *disruption.Candidate) bool { return len(c.ReschedulablePods()) == 0 }
		empty.plan = func(in pipeline.PlanInput) (pipeline.PlanOutput, error) {
			p := deletePlan(in.Candidates...)
			p.Method = disruption.NewEmptiness(disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue))
			p.Footprint.BudgetNodes = map[string]sets.Set[string]{w.nodePool.Name: p.Footprint.Removed}
			return pipeline.PlanOutput{Plans: []*pipeline.Plan{p}}, nil
		}
		c = pipeline.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			pipeline.WithLanes(pipeline.LaneSpec{ID: types.LaneFast, Planners: []pipeline.Planner{empty}}))
	})

	It("reports rounds, plans and their waits, and the disruption controller's metrics that carry over", func() {
		ExpectSingletonReconciled(ctx, c)
		ExpectMetricCounterValue(pipeline.RoundsTotal, 1, lo.Assign(fast, map[string]string{"result": "planned"}))
		ExpectMetricCounterValue(pipeline.PlansTotal, 1, map[string]string{"planner": "emptiness", "result": "admitted", "reason": ""})
		ExpectMetricGaugeValue(pipeline.UnresolvedPlans, 0, fast)
		ExpectMetricGaugeValue(pipeline.LastAdmittedTimestampSeconds, float64(env.Clock.Now().Unix()), map[string]string{"planner": "emptiness"})
		ExpectMetricHistogramSampleCountValue("karpenter_voluntary_disruption_pipeline_plan_wait_seconds", 1, map[string]string{"lane": "fast", "planner": "emptiness"})
		for _, phase := range []string{"snapshot", "plan", "admit"} {
			ExpectMetricHistogramSampleCountValue("karpenter_voluntary_disruption_pipeline_round_duration_seconds", 1, lo.Assign(fast, map[string]string{"phase": phase}))
		}
		ExpectMetricGaugeValue(disruption.LastEvaluationTimestampSeconds, float64(env.Clock.Now().Unix()), map[string]string{"reason": "empty", "consolidation_type": ""})
		ExpectMetricGaugeValue(disruption.NodePoolAllowedDisruptions, 3, map[string]string{"nodepool": w.nodePool.Name, "reason": "empty"})
		// The budget gauges are set before the commit loop starts the round's plans.
		ExpectMetricGaugeValue(pipeline.LedgerReservedNodes, 2, map[string]string{"nodepool": w.nodePool.Name})
		ExpectSingletonReconciled(ctx, c)
		ExpectMetricGaugeValue(pipeline.LedgerReservedNodes, 0, map[string]string{"nodepool": w.nodePool.Name})
		ExpectMetricGaugeValue(disruption.NodePoolNodesConsumingBudgets, 2, map[string]string{"nodepool": w.nodePool.Name, "reason": "empty"})
	})
	It("counts gated rounds and publishes the budget event for a NodePool that allows no disruption", func() {
		w.nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "0"}}
		ExpectApplied(ctx, env.Client, w.nodePool)
		ExpectSingletonReconciled(ctx, c)
		env.Clock.Step(10 * time.Second)
		ExpectSingletonReconciled(ctx, c)
		ExpectMetricCounterValue(pipeline.RoundsTotal, 2, lo.Assign(fast, map[string]string{"result": "gated"}))
		ExpectMetricCounterValue(pipeline.GatedSecondsTotal, 10, lo.Assign(fast, map[string]string{"gate": "budget_zero"}))
		Expect(empty.planned()).To(BeEmpty())
	})
	It("counts a round's gate once however many planners it gates", func() {
		w.nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "0", Reasons: []v1.DisruptionReason{v1.DisruptionReasonEmpty, v1.DisruptionReasonDrifted}}}
		ExpectApplied(ctx, env.Client, w.nodePool)
		drift := newStubPlanner("drift")
		single := newStubPlanner("single")
		single.reason = v1.DisruptionReasonUnderutilized
		single.filter = func(*disruption.Candidate) bool { return false }
		c = pipeline.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			pipeline.WithLanes(pipeline.LaneSpec{ID: types.LaneFast, Planners: []pipeline.Planner{empty, drift, single}}))
		ExpectSingletonReconciled(ctx, c)
		env.Clock.Step(10 * time.Second)
		ExpectSingletonReconciled(ctx, c)
		ExpectMetricCounterValue(pipeline.GatedSecondsTotal, 10, lo.Assign(fast, map[string]string{"gate": "budget_zero"}))
	})
	It("keeps unresolved_plans at 0 after a plan is rejected at Submit", func() {
		w.nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "1"}}
		ExpectApplied(ctx, env.Client, w.nodePool)
		ExpectSingletonReconciled(ctx, c)
		ExpectMetricCounterValue(pipeline.PlansTotal, 1, map[string]string{"planner": "emptiness", "result": "rejected", "reason": "budget"})
		ExpectMetricGaugeValue(pipeline.UnresolvedPlans, 0, fast)
	})
	It("keeps unresolved_plans at 0 after a back-pressure rejection", func() {
		// The lane counts every submitted plan in its round, so every resolution, back-pressure too, must count down.
		obs := pipeline.NewMetricsObserver(env.Clock, env.Client, recorder, pipeline.NewLedger(env.Client, cluster, cloudProvider, env.Clock, queue),
			pipeline.LaneSpec{ID: types.LaneFast, Planners: []pipeline.Planner{empty}})
		p := &pipeline.Plan{ID: uuid.New(), Planner: empty.Name()}
		obs.RoundStarted(types.LaneFast)
		obs.Resolved(p, pipeline.Resolution{PlanID: p.ID, Kind: pipeline.Rejected, Reason: types.ReasonBackpressure}, 0)
		obs.RoundFinished(ctx, types.LaneFast, pipeline.StepResult{Lane: types.LaneFast, Plans: 1, EndedAt: env.Clock.Now()}, nil)
		ExpectMetricGaugeValue(pipeline.UnresolvedPlans, 0, fast)
	})
	It("counts consolidation's failed validations per candidate", func() {
		disruption.FailedValidationsTotal.Reset()
		plan, planned := empty.plan, false
		empty.plan = func(in pipeline.PlanInput) (pipeline.PlanOutput, error) {
			planned = true
			return plan(in)
		}
		// The candidates turn ineligible between planning and admission, so admission rejects the plan.
		empty.filter = func(c *disruption.Candidate) bool { return !planned && len(c.ReschedulablePods()) == 0 }
		ExpectSingletonReconciled(ctx, c)
		ExpectMetricCounterValue(pipeline.PlansTotal, 1, map[string]string{"planner": "emptiness", "result": "rejected", "reason": string(types.ReasonCandidateIneligible)})
		ExpectMetricCounterValue(disruption.FailedValidationsTotal, 2, map[string]string{disruption.ConsolidationTypeLabel: "empty"})
	})
	It("explains once that a planner waits for budget", func() {
		w.nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "1"}}
		ExpectApplied(ctx, env.Client, w.nodePool)
		// Another reason's in-flight node uses the whole budget, so the round plans nothing.
		cluster.MarkForDeletion(w.providerIDs()[2])
		other := newStubPlanner("drift")
		c = pipeline.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			pipeline.WithLanes(pipeline.LaneSpec{ID: types.LaneFast, Planners: []pipeline.Planner{empty, other}}))
		ExpectSingletonReconciled(ctx, c)
		ExpectSingletonReconciled(ctx, c)
		Expect(recorder.Calls(events.DisruptionBudgetExhausted)).To(Equal(2)) // once for each planner
		Expect(recorder.DetectedEvent("emptiness is waiting for budget: 1 of 1 allowed nodes are disrupting (reason Empty; in-flight nodes count against every reason)")).To(BeTrue())
	})
	It("publishes the round's buffered candidate events", func() {
		pod := test.Pod(test.PodOptions{NodeName: w.nodes[0].Name, Phase: corev1.PodRunning, ObjectMeta: metav1.ObjectMeta{
			Annotations:     map[string]string{v1.DoNotDisruptAnnotationKey: "true"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs", UID: "rs", Controller: lo.ToPtr(true)}},
		}})
		ExpectApplied(ctx, env.Client, pod)
		ExpectReconcileSucceeded(ctx, podStateController, client.ObjectKeyFromObject(pod))
		recorder.Reset()
		ExpectSingletonReconciled(ctx, c)
		Expect(lo.Map(recorder.Events(), func(e events.Event, _ int) string { return e.Reason + ": " + e.Message })).To(ContainElement(
			ContainSubstring(`has "karpenter.sh/do-not-disrupt" annotation`)))
	})
	It("observes how long cluster state was unsynced, once it syncs", func() {
		unlaunched := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: w.nodePool.Name}}})
		unlaunched.Status.ProviderID = ""
		cluster.UpdateNodeClaim(unlaunched)
		ExpectSingletonReconciled(ctx, c)
		env.Clock.Step(30 * time.Second)
		ExpectSingletonReconciled(ctx, c)
		_, found := FindMetricWithLabelValues("karpenter_voluntary_disruption_cluster_state_gate_duration_seconds", nil)
		Expect(found).To(BeFalse())
		cluster.DeleteNodeClaim(unlaunched.Name)
		ExpectSingletonReconciled(ctx, c)
		ExpectMetricHistogramSampleCountValue("karpenter_voluntary_disruption_cluster_state_gate_duration_seconds", 1, nil)
	})
})
