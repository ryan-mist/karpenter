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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
)

var _ = Describe("Drift fairness", func() {
	var a, b *world
	var cands []*disruption.Candidate
	BeforeEach(func() {
		a, b = newWorld(3), newWorld(2)
		s, err := pipeline.NewSnapshotBuilder(env.Client, cluster, env.Clock, cloudProvider, queue, clusterCost, nil).Build(ctx, pipeline.Scope{})
		Expect(err).ToNot(HaveOccurred())
		cands = s.CandidatesFor(ctx, newStubPlanner("drift"))
		Expect(cands).To(HaveLen(5))
	})
	It("interleaves NodePools, least recently served first", func() {
		f := pipeline.NewPoolFairness(env.Clock, func() float64 { return 0.5 })
		first, second := lo.Ternary(a.nodePool.Name < b.nodePool.Name, a, b), lo.Ternary(a.nodePool.Name < b.nodePool.Name, b, a)
		f.Order(cands)
		Expect(pools(cands)[:4]).To(Equal([]string{first.nodePool.Name, second.nodePool.Name, first.nodePool.Name, second.nodePool.Name}))
		f.Started(first.nodePool.Name)
		f.Order(cands)
		Expect(pools(cands)[0]).To(Equal(second.nodePool.Name))
	})
	It("backs off a NodePool after a failed command, doubling to 10 minutes, and resets on success", func() {
		f := pipeline.NewPoolFairness(env.Clock, func() float64 { return 0.5 }) // no jitter
		f.Finished(false, a.nodePool.Name)
		Expect(pools(f.Eligible(cands))).To(ConsistOf(b.nodePool.Name, b.nodePool.Name))
		env.Clock.Step(time.Minute)
		Expect(f.Eligible(cands)).To(HaveLen(5))
		f.Finished(false, a.nodePool.Name)
		env.Clock.Step(time.Minute + time.Second)
		Expect(f.Eligible(cands)).To(HaveLen(2))
		env.Clock.Step(time.Minute)
		Expect(f.Eligible(cands)).To(HaveLen(5))
		for range 6 {
			f.Finished(false, a.nodePool.Name)
		}
		env.Clock.Step(10*time.Minute - time.Second)
		Expect(f.Eligible(cands)).To(HaveLen(2))
		env.Clock.Step(time.Second)
		Expect(f.Eligible(cands)).To(HaveLen(5))
		f.Finished(false, a.nodePool.Name)
		f.Finished(true, a.nodePool.Name)
		Expect(f.Eligible(cands)).To(HaveLen(5))
	})
	It("jitters the backoff by up to 10%", func() {
		f := pipeline.NewPoolFairness(env.Clock, func() float64 { return 1 }) // +10%
		f.Finished(false, a.nodePool.Name)
		env.Clock.Step(65 * time.Second)
		Expect(f.Eligible(cands)).To(HaveLen(2))
		env.Clock.Step(2 * time.Second)
		Expect(f.Eligible(cands)).To(HaveLen(5))
	})
	It("backs off the NodePools of a failed drift command through the queue", func() {
		a.only("Drifted")
		b.only("Drifted")
		a.withPods("1", 0, 1, 2)
		b.withPods("1", 0, 1)
		planners := pipeline.DefaultPlanners(ctx, env.Clock, cluster, env.Client, prov, cloudProvider, queue, &pipeline.RoundEvents{}, nil)
		drift := planners["drift"]
		plan := func() []*pipeline.Plan {
			GinkgoHelper()
			s, err := pipeline.NewSnapshotBuilder(env.Client, cluster, env.Clock, cloudProvider, queue, clusterCost, nil).Build(ctx, pipeline.Scope{})
			Expect(err).ToNot(HaveOccurred())
			out, err := drift.Plan(ctx, pipeline.PlanInput{Candidates: s.CandidatesFor(ctx, drift), Now: env.Clock.Now(), Reserved: newFakeExecutor().view(), Snapshot: s})
			Expect(err).ToNot(HaveOccurred())
			return out.Plans
		}
		planned := plan()
		Expect(planned).ToNot(BeEmpty())
		inA := lo.Filter(lo.FlatMap(planned, func(p *pipeline.Plan, _ int) []*disruption.Candidate { return p.Candidates }),
			func(c *disruption.Candidate, _ int) bool { return c.NodePool.Name == a.nodePool.Name })
		Expect(inA).ToNot(BeEmpty())
		// One of drift's commands in NodePool a fails (its replacement never launched).
		queue.CompleteCommand(&disruption.Command{Method: planned[0].Method, Candidates: inA[:1]})
		Expect(pools(lo.FlatMap(plan(), func(p *pipeline.Plan, _ int) []*disruption.Candidate { return p.Candidates }))).To(HaveEach(b.nodePool.Name))
	})
	It("backs off the NodePools of a drift plan whose start failed", func() {
		a.only("Drifted")
		b.only("Drifted")
		a.withPods("1", 0, 1, 2)
		b.withPods("1", 0, 1)
		planners := pipeline.DefaultPlanners(ctx, env.Clock, cluster, env.Client, prov, cloudProvider, queue, &pipeline.RoundEvents{}, nil)
		drift := planners["drift"]
		plan := func() []*pipeline.Plan {
			GinkgoHelper()
			s, err := pipeline.NewSnapshotBuilder(env.Client, cluster, env.Clock, cloudProvider, queue, clusterCost, nil).Build(ctx, pipeline.Scope{})
			Expect(err).ToNot(HaveOccurred())
			out, err := drift.Plan(ctx, pipeline.PlanInput{Candidates: s.CandidatesFor(ctx, drift), Now: env.Clock.Now(), Reserved: newFakeExecutor().view(), Snapshot: s})
			Expect(err).ToNot(HaveOccurred())
			return out.Plans
		}
		inA := lo.Filter(lo.FlatMap(plan(), func(p *pipeline.Plan, _ int) []*disruption.Candidate { return p.Candidates }),
			func(c *disruption.Candidate, _ int) bool { return c.NodePool.Name == a.nodePool.Name })
		Expect(inA).ToNot(BeEmpty())
		// Review C-1: a plan in NodePool a failed to start, so it never reached the queue and the completion hook never runs.
		drift.(interface {
			Resolved(*pipeline.Plan, pipeline.Resolution)
		}).Resolved(&pipeline.Plan{Candidates: inA[:1]}, pipeline.Resolution{Kind: pipeline.Rejected, Reason: types.ReasonStartFailed})
		Expect(pools(lo.FlatMap(plan(), func(p *pipeline.Plan, _ int) []*disruption.Candidate { return p.Candidates }))).To(HaveEach(b.nodePool.Name))
	})
})

var _ = Describe("Consolidation dedup", func() {
	It("skips while cluster state is unchanged since nothing was found", func() {
		d := pipeline.NewStateDedup(cluster)
		Expect(d.IsConsolidated()).To(BeFalse())
		d.MarkConsolidated()
		Expect(d.IsConsolidated()).To(BeTrue())
		env.Clock.Step(time.Second)
		cluster.MarkUnconsolidated()
		Expect(d.IsConsolidated()).To(BeFalse())
	})
})
