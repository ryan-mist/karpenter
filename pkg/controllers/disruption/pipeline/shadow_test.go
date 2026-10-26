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
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/util/sets"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// disruptionMetrics are the disruption controller's series (everything but the pipeline's), by name and labels.
func disruptionMetrics() map[string]string {
	out := map[string]string{}
	for _, mf := range lo.Must(crmetrics.Registry.Gather()) {
		if !strings.HasPrefix(mf.GetName(), "karpenter_voluntary_disruption_") || strings.Contains(mf.GetName(), "_pipeline_") {
			continue
		}
		for _, m := range mf.Metric {
			out[mf.GetName()+fmt.Sprint(m.GetLabel())] = m.String()
		}
	}
	return out
}

// emptyFirst plans nothing for its first n calls, then as its planner does.
type emptyFirst struct {
	pipeline.Planner
	n, calls int
}

func (p *emptyFirst) Plan(ctx context.Context, in pipeline.PlanInput) (pipeline.PlanOutput, error) {
	p.calls++
	if p.calls <= p.n {
		return pipeline.PlanOutput{}, nil
	}
	return p.Planner.Plan(ctx, in)
}

var _ = Describe("Shadow", func() {
	var shadow *pipeline.Shadow
	var legacy *disruption.Controller
	var writes atomic.Int64
	var logged []string
	drift := map[string]string{"planner": "drift"}
	BeforeEach(func() {
		for _, m := range []interface{ Reset() }{pipeline.ShadowComparisonsTotal, pipeline.ShadowMismatchesTotal, pipeline.ShadowAgreementRatio} {
			m.Reset()
		}
		// More drifted nodes than any spec has passes, so a recheck on fresh state always finds one to plan, whatever
		// the disruption controller started meanwhile.
		w := newWorld(5, 0, 1, 2, 3, 4)
		w.only(v1.ConditionTypeDrifted)
		writes.Store(0)
		logged = nil
		// The shadow gets its own client, so its writes are counted apart from the disruption controller's.
		shadow = pipeline.NewShadow(ctx, env.Clock, writeCounter{Client: env.Client, writes: &writes}, prov, cloudProvider, cluster, queue, clusterCost, 1)
		shadow.SetShadowLogger(funcr.New(func(prefix, args string) { logged = append(logged, args) }, funcr.Options{}))
		legacy = disruption.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			disruption.WithComputeObserver(shadow))
	})
	It("plans as the disruption controller computes, and writes nothing", func() {
		ExpectSingletonReconciled(ctx, legacy)
		shadow.WaitIdle()
		Expect(queue.GetCommands()).To(HaveLen(1))
		ExpectMetricCounterValue(pipeline.ShadowComparisonsTotal, 1, map[string]string{"level": "reference", "planner": "drift"})
		ExpectMetricCounterValue(pipeline.ShadowComparisonsTotal, 1, map[string]string{"level": "snapshot", "planner": "drift"})
		// Emptiness has no candidates: its snapshot is compared too, and there is nothing to plan.
		ExpectMetricCounterValue(pipeline.ShadowComparisonsTotal, 1, map[string]string{"level": "snapshot", "planner": "emptiness"})
		ExpectMetricGaugeValue(pipeline.ShadowAgreementRatio, 1, drift)
		_, found := FindMetricWithLabelValues("karpenter_voluntary_disruption_pipeline_shadow_mismatches_total", nil)
		Expect(found).To(BeFalse())
		Expect(writes.Load()).To(BeZero())
		Expect(logged).To(BeEmpty())
	})
	It("counts a mismatch the recheck confirms as logic, and logs it once per 10 minutes", func() {
		// A drift planner that never plans: the recheck's core still finds the command, so the logic differs.
		shadow.SetShadowPlanner("drift", &emptyFirst{Planner: shadow.ShadowPlanner("drift"), n: 1 << 30})
		ExpectSingletonReconciled(ctx, legacy)
		shadow.WaitIdle()
		ExpectSingletonReconciled(ctx, legacy)
		shadow.WaitIdle()
		ExpectMetricCounterValue(pipeline.ShadowMismatchesTotal, 2, map[string]string{"level": "reference", "planner": "drift", "kind": "commands", "cause": "logic"})
		ExpectMetricGaugeValue(pipeline.ShadowAgreementRatio, 0, drift)
		Expect(logged).To(HaveLen(1))
		env.Clock.Step(10 * time.Minute)
		ExpectSingletonReconciled(ctx, legacy)
		shadow.WaitIdle()
		Expect(logged).To(HaveLen(2))
	})
	It("counts a mismatch the recheck doesn't confirm as a race", func() {
		// The planner plans nothing while the shadow compares (a sample that plans nothing isn't resampled), and as it
		// should in the recheck.
		shadow.SetShadowPlanner("drift", &emptyFirst{Planner: shadow.ShadowPlanner("drift"), n: 1})
		ExpectSingletonReconciled(ctx, legacy)
		shadow.WaitIdle()
		ExpectMetricCounterValue(pipeline.ShadowMismatchesTotal, 1, map[string]string{"level": "reference", "planner": "drift", "kind": "commands", "cause": "race"})
	})
	It("rechecks a mismatch with its own objects: the disruption controller's method state, events and metrics don't change", func() {
		w := newWorld(3, 0, 1, 2)
		w.only(v1.ConditionTypeConsolidatable)
		w.nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "0"}}
		ExpectApplied(ctx, env.Client, w.nodePool)
		methods := disruption.NewMethods(ctx, env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue)
		single, _ := lo.Find(methods, func(m disruption.Method) bool { _, ok := m.(*disruption.SingleNodeConsolidation); return ok })
		single.(*disruption.SingleNodeConsolidation).PreviouslyUnseenNodePools = sets.New("unseen")
		cs, err := disruption.NewCandidateSnapshot(ctx, cluster, env.Client, recorder, env.Clock, cloudProvider, queue)
		Expect(err).ToNot(HaveOccurred())
		cands, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, env.Clock, cloudProvider, single.ShouldDisrupt, single.Class(), queue)
		Expect(err).ToNot(HaveOccurred())
		Expect(cands).To(HaveLen(3))
		recorder.Reset()
		before := disruptionMetrics()
		// The shadow plans nothing at budget 0; a command it didn't plan makes it recheck.
		shadow.Computing(ctx, single, cs, cands, map[string]int{w.nodePool.Name: 0})
		shadow.Computed(ctx, single, []disruption.Command{{Candidates: cands[:1]}}, nil)
		shadow.WaitIdle()
		ExpectMetricCounterValue(pipeline.ShadowMismatchesTotal, 1, map[string]string{"level": "reference", "planner": "single", "kind": "commands", "cause": "race"})
		Expect(single.(*disruption.SingleNodeConsolidation).PreviouslyUnseenNodePools.UnsortedList()).To(ConsistOf("unseen"))
		Expect(recorder.Events()).To(BeEmpty())
		Expect(disruptionMetrics()).To(Equal(before))
		Expect(writes.Load()).To(BeZero())
	})
	It("compares nothing when no computation is sampled", func() {
		shadow = pipeline.NewShadow(ctx, env.Clock, env.Client, prov, cloudProvider, cluster, queue, clusterCost, 0.0000001)
		shadow.SetShadowLogger(logr.Discard())
		legacy = disruption.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			disruption.WithComputeObserver(shadow))
		ExpectSingletonReconciled(ctx, legacy)
		_, found := FindMetricWithLabelValues("karpenter_voluntary_disruption_pipeline_shadow_comparisons_total", nil)
		Expect(found).To(BeFalse())
	})
	DescribeTable("reads its sample rate from the environment",
		func(value string, rate float64) {
			GinkgoT().Setenv(pipeline.ShadowEnv, value)
			Expect(pipeline.ShadowRate()).To(Equal(rate))
		},
		Entry("unset", "", 0.0),
		Entry("not a number", "yes", 0.0),
		Entry("negative", "-1", 0.0),
		Entry("a fraction", "0.1", 0.1),
		Entry("more than 1", "5", 1.0),
		Entry("NaN", "NaN", 0.0),
		Entry("infinite", "+Inf", 0.0),
	)
})
