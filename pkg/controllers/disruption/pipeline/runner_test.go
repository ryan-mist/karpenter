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
	"errors"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
)

var _ = Describe("PollTrigger", func() {
	now := time.Now()
	DescribeTable("paces a lane",
		func(t pipeline.PollTrigger, last pipeline.StepResult, want time.Duration) {
			last.EndedAt = now
			Expect(t.Pace(last, 0)).To(Equal(want))
		},
		Entry("fast: right away after a start", pipeline.PollTrigger{}, pipeline.StepResult{Admitted: 1, Took: time.Second}, time.Duration(0)),
		Entry("background: its duty cycle after a start", pipeline.PollTrigger{Background: true}, pipeline.StepResult{Admitted: 1, Took: 2 * time.Second}, 6*time.Second),
		Entry("background: at least a second", pipeline.PollTrigger{Background: true}, pipeline.StepResult{Admitted: 1, Took: time.Millisecond}, time.Second),
		Entry("a second while plans resolve", pipeline.PollTrigger{}, pipeline.StepResult{Plans: 2}, time.Second),
		Entry("a second while cluster state syncs", pipeline.PollTrigger{}, pipeline.StepResult{Gated: types.GateSynced}, time.Second),
		Entry("the polling period when idle", pipeline.PollTrigger{}, pipeline.StepResult{}, 10*time.Second),
		Entry("the polling period when budget is zero", pipeline.PollTrigger{}, pipeline.StepResult{Gated: types.GateBudgetZero}, 10*time.Second),
		Entry("the polling period after an error", pipeline.PollTrigger{}, pipeline.StepResult{Err: errors.New("boom")}, 10*time.Second),
		Entry("a planner's earlier timer", pipeline.PollTrigger{}, pipeline.StepResult{NextTimer: now.Add(3 * time.Second)}, 3*time.Second),
	)
	It("never wakes a lane early", func() {
		Expect(pipeline.PollTrigger{}.Wake()).To(BeNil())
	})
})

var _ = Describe("DefaultLanes", func() {
	It("runs repair, emptiness, static drift and drift in Fast, and consolidation in Background", func() {
		planners := map[string]pipeline.Planner{}
		for _, n := range []string{pipeline.SingleNodePlanner, pipeline.DriftPlanner, pipeline.RepairPlanner, pipeline.MultiNodePlanner, pipeline.EmptinessPlanner, pipeline.StaticDriftPlanner} {
			planners[n] = newStubPlanner(n)
		}
		lanes := pipeline.DefaultLanes(planners)
		names := func(l pipeline.LaneSpec) []string {
			return lo.Map(l.Planners, func(p pipeline.Planner, _ int) string { return p.Name() })
		}
		Expect(lanes).To(HaveLen(2))
		Expect(lanes[0].ID).To(Equal(types.LaneFast))
		Expect(names(lanes[0])).To(Equal([]string{"repair", "emptiness", "static_drift", "drift"}))
		Expect(lanes[0].SkipWhileUnsynced).To(BeFalse())
		Expect(lanes[1].ID).To(Equal(types.LaneBackground))
		Expect(names(lanes[1])).To(Equal([]string{"multi", "single"}))
		Expect(lanes[1].SkipWhileUnsynced).To(BeTrue())
		Expect(lanes[1].Trigger).To(Equal(pipeline.PollTrigger{Background: true}))
	})
	It("leaves out missing planners and empty lanes", func() {
		lanes := pipeline.DefaultLanes(map[string]pipeline.Planner{pipeline.DriftPlanner: newStubPlanner("drift")})
		Expect(lanes).To(HaveLen(1))
		Expect(lanes[0].Planners).To(HaveLen(1))
	})
})

// countingExecutor counts its steps and reports what it is told to.
type countingExecutor struct {
	steps     atomic.Int64
	cancelled atomic.Int64
	ready     chan struct{}
	result    pipeline.CommitStepResult
	onStep    func()
}

func (e *countingExecutor) Submit(context.Context, ...*pipeline.Plan) <-chan pipeline.Resolution {
	return nil
}
func (e *countingExecutor) Step(ctx context.Context) pipeline.CommitStepResult {
	e.steps.Add(1)
	if ctx.Err() != nil {
		e.cancelled.Add(1)
	}
	if e.onStep != nil {
		e.onStep()
	}
	return e.result
}
func (e *countingExecutor) Ready() <-chan struct{} { return e.ready }

// addRecorder is a manager that only records Add.
type addRecorder struct {
	manager.Manager
	mu    sync.Mutex
	added []manager.Runnable
}

func (m *addRecorder) Add(r manager.Runnable) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.added = append(m.added, r)
	return nil
}

var _ = Describe("Asynchronous runners", func() {
	var exec *countingExecutor
	var c *pipeline.Controller
	var runnables []manager.Runnable
	var p *stubPlanner
	BeforeEach(func() {
		exec = &countingExecutor{ready: make(chan struct{}, 1)}
		p = newStubPlanner("drift")
		p.plan = func(pipeline.PlanInput) (pipeline.PlanOutput, error) { return pipeline.PlanOutput{}, nil }
		c = pipeline.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			pipeline.WithExecutor(exec), pipeline.WithLanes(pipeline.DefaultLanes(map[string]pipeline.Planner{"drift": p})...))
		m := &addRecorder{}
		Expect(c.Register(ctx, m)).To(Succeed())
		runnables = m.added
		newWorld(1)
	})
	start := func(r manager.Runnable) (context.CancelFunc, chan struct{}) {
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			Expect(r.Start(runCtx)).To(Succeed())
			close(done)
		}()
		return cancel, done
	}
	It("registers one leader-elected runner per lane and one for the commit loop", func() {
		Expect(runnables).To(HaveLen(2))
		for _, r := range runnables {
			Expect(r.(manager.LeaderElectionRunnable).NeedLeaderElection()).To(BeTrue())
		}
	})
	It("runs a lane round, then waits for its pace", func() {
		cancel, done := start(runnables[0])
		Eventually(p.planned).Should(HaveLen(1))
		Eventually(env.Clock.HasWaiters).Should(BeTrue())
		// Idle: the next round comes after the polling period.
		env.Clock.Step(9 * time.Second)
		Consistently(p.planned).Should(HaveLen(1))
		env.Clock.Step(time.Second)
		Eventually(p.planned).Should(HaveLen(2))
		cancel()
		Eventually(done).Should(BeClosed())
	})
	It("runs a commit step when a plan is submitted, and drops the waiting plans when the lease is lost", func() {
		cancel, done := start(runnables[1])
		Eventually(func() int64 { return exec.steps.Load() }).Should(Equal(int64(1)))
		exec.ready <- struct{}{}
		Eventually(func() int64 { return exec.steps.Load() }).Should(Equal(int64(2)))
		env.Clock.Step(pipeline.SweepTick)
		Eventually(func() int64 { return exec.steps.Load() }).Should(Equal(int64(3)))
		cancel()
		Eventually(done).Should(BeClosed())
		Expect(exec.cancelled.Load()).To(BeNumerically(">=", 1))
	})
	It("drops the waiting plans when the lease is lost during a step", func() {
		runCtx, cancel := context.WithCancel(ctx)
		var once sync.Once
		exec.onStep = func() { once.Do(cancel) } // the lease is lost after the step checked its context
		Expect(runnables[1].Start(runCtx)).To(Succeed())
		Expect(exec.cancelled.Load()).To(Equal(int64(1)))
	})
})
