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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
)

var _ = Describe("Wake-ups", func() {
	It("coalesces a lane's unread wake-ups", func() {
		hub := pipeline.NewWakeHub()
		fast, bg := hub.Subscribe(types.LaneFast), hub.Subscribe(types.LaneBackground)
		hub.Publish(types.LaneFast, types.WakeResolution)
		hub.Publish("", types.WakeLedgerRelease)
		Expect(fast).To(Receive(Equal(types.WakeResolution | types.WakeLedgerRelease)))
		Expect(bg).To(Receive(Equal(types.WakeLedgerRelease)))
		Expect(fast).ToNot(Receive())
	})
	It("wakes a plan's lane when it resolves, every lane when a slot frees or cluster state syncs", func() {
		hub := pipeline.NewWakeHub()
		fast, bg := hub.Subscribe(types.LaneFast), hub.Subscribe(types.LaneBackground)
		o := pipeline.NewWakeObserver(hub, pipeline.DefaultLanes(map[string]pipeline.Planner{"drift": newStubPlanner("drift"), "multi": newStubPlanner("multi")})...)
		o.Resolved(&pipeline.Plan{Planner: "drift"}, pipeline.Resolution{Kind: pipeline.Started}, 0)
		Expect(fast).To(Receive(Equal(types.WakeResolution)))
		Expect(bg).ToNot(Receive())
		o.Resolved(&pipeline.Plan{Planner: "multi"}, pipeline.Resolution{Kind: pipeline.Rejected, Reason: types.ReasonBudget}, 0)
		Expect(fast).To(Receive(Equal(types.WakeLedgerRelease)))
		Expect(bg).To(Receive(Equal(types.WakeResolution | types.WakeLedgerRelease)))
		// A plan rejected at Submit held no slot: no wake-up, or a lane would re-plan the same rejection in a loop.
		o.Resolved(&pipeline.Plan{Planner: "drift"}, pipeline.Resolution{Kind: pipeline.Rejected, Reason: types.ReasonConflict, AtSubmit: true}, 0)
		Expect(fast).ToNot(Receive())
		Expect(bg).ToNot(Receive())
		o.CommitStepFinished(pipeline.CommitStepResult{}, false, 0)
		Expect(fast).ToNot(Receive())
		o.CommitStepFinished(pipeline.CommitStepResult{}, true, 0)
		Expect(fast).To(Receive(Equal(types.WakeSynced)))
		Expect(bg).To(Receive(Equal(types.WakeSynced)))
		o.CommitStepFinished(pipeline.CommitStepResult{}, true, 0)
		Expect(fast).ToNot(Receive())
	})
	It("runs a lane early when a command completes, instead of after the polling period", func() {
		newWorld(1)
		p := newStubPlanner("drift")
		p.plan = func(pipeline.PlanInput) (pipeline.PlanOutput, error) { return pipeline.PlanOutput{}, nil }
		c := pipeline.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			pipeline.WithExecutor(newFakeExecutor()), pipeline.WithLanes(pipeline.DefaultLanes(map[string]pipeline.Planner{"drift": p})...))
		m := &addRecorder{}
		Expect(c.Register(ctx, m)).To(Succeed())
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			defer GinkgoRecover()
			Expect(m.added[0].(manager.Runnable).Start(runCtx)).To(Succeed())
		}()
		Eventually(p.planned).Should(HaveLen(1))
		Consistently(p.planned).Should(HaveLen(1))
		queue.CompleteCommand(&disruption.Command{Candidates: nil, Succeeded: true})
		Eventually(p.planned).Should(HaveLen(2))
	})
	It("doesn't re-plan in a loop when every start fails", func() {
		newWorld(1)
		p := newStubPlanner("drift")
		c := pipeline.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			pipeline.WithStarter(failingStarter{}), pipeline.WithLanes(pipeline.DefaultLanes(map[string]pipeline.Planner{"drift": p})...))
		m := &addRecorder{}
		Expect(c.Register(ctx, m)).To(Succeed())
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		for _, r := range m.added {
			go func() {
				defer GinkgoRecover()
				Expect(r.Start(runCtx)).To(Succeed())
			}()
		}
		Eventually(p.planned).Should(HaveLen(1))
		// The failed start wakes the lane; it must not re-plan the same candidate right away.
		Consistently(p.planned, 2*time.Second).Should(HaveLen(1))
		// A minute later it has retried about once per polling period, as the disruption controller would.
		for range 60 {
			env.Clock.Step(time.Second)
			time.Sleep(10 * time.Millisecond)
		}
		Eventually(func() int { return len(p.planned()) }).Should(BeNumerically(">=", 3))
		Expect(len(p.planned())).To(BeNumerically("<=", 7))
	})
})

// failingStarter fails every start, as a webhook rejecting replacement NodeClaims would.
type failingStarter struct{}

func (failingStarter) Start(context.Context, *pipeline.Plan) error { return errors.New("denied") }

var _ = Describe("Wake trigger", func() {
	now := time.Now()
	It("cuts the Fast lane's wait short on a freed slot, a sync or a resolution, and the Background lane's to its duty cycle", func() {
		c := pipeline.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			pipeline.WithLanes(pipeline.DefaultLanes(map[string]pipeline.Planner{"drift": newStubPlanner("drift"), "multi": newStubPlanner("multi")})...))
		fast, bg := pipeline.TriggerFor(c, types.LaneFast), pipeline.TriggerFor(c, types.LaneBackground)
		idle := pipeline.StepResult{EndedAt: now, Took: 2 * time.Second}
		Expect(fast.Pace(idle, 0)).To(Equal(10 * time.Second))
		for _, w := range []types.WakeSet{types.WakeLedgerRelease, types.WakeSynced, types.WakeResolution} {
			Expect(fast.Pace(idle, w)).To(BeZero())
		}
		Expect(bg.Pace(idle, 0)).To(Equal(10 * time.Second))
		Expect(bg.Pace(idle, types.WakeLedgerRelease)).To(Equal(6 * time.Second))
		Expect(bg.Pace(idle, types.WakeSynced)).To(Equal(10 * time.Second))
	})
})
