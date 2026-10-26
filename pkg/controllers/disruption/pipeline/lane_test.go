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
	"sync/atomic"
	"time"

	"github.com/awslabs/operatorpkg/singleton"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/test"
)

var _ = Describe("Lane", func() {
	var exec *fakeExecutor
	newLane := func(spec pipeline.LaneSpec, mutate ...func(*pipeline.LaneDeps)) pipeline.Lane {
		deps := pipeline.LaneDeps{
			Clock:     env.Clock,
			Cluster:   cluster,
			Snapshots: pipeline.NewSnapshotBuilder(env.Client, cluster, env.Clock, cloudProvider, queue, clusterCost, exec.view()),
			Executor:  exec,
			Reserved:  exec.view(),
		}
		for _, m := range mutate {
			m(&deps)
		}
		return pipeline.NewLane(spec, deps)
	}
	BeforeEach(func() {
		exec = newFakeExecutor()
	})

	It("runs planners in order, each planning around the plans the previous ones submitted", func() {
		w := newWorld(4)
		first, second := newStubPlanner("first"), newStubPlanner("second")
		ids := w.providerIDs()
		first.filter = func(c *disruption.Candidate) bool { return c.ProviderID() == ids[0] || c.ProviderID() == ids[1] }
		l := newLane(pipeline.LaneSpec{ID: types.LaneFast, Planners: []pipeline.Planner{first, second}})
		r := l.Step(ctx)
		Expect(r.Err).ToNot(HaveOccurred())
		Expect(r.Plans).To(Equal(4))
		Expect(first.planned()).To(HaveLen(1))
		Expect(first.planned()[0]).To(ConsistOf(ids[0], ids[1]))
		// The first planner's plans were reserved when they were submitted, so the second never saw those nodes.
		Expect(second.planned()).To(HaveLen(1))
		Expect(second.planned()[0]).To(ConsistOf(ids[2], ids[3]))
		Expect(exec.submittedIDs()).To(Equal(append(first.planned()[0], second.planned()[0]...)))
		// The lane owns these plan fields.
		for _, p := range exec.submitted {
			Expect(p.ID).ToNot(BeZero())
			Expect(p.BatchID).To(Equal(p.ID))
			Expect(p.Reason).To(Equal(v1.DisruptionReasonDrifted))
			Expect(p.ComputedAt).ToNot(BeZero())
		}
		Expect(exec.submitted[0].Planner).To(Equal("first"))
		Expect(exec.submitted[3].Planner).To(Equal("second"))
	})
	It("doesn't plan for a planner whose last plans are unresolved, and does once they resolve", func() {
		w := newWorld(2)
		busy, other := newStubPlanner("busy"), newStubPlanner("other")
		busy.filter = func(c *disruption.Candidate) bool { return c.ProviderID() == w.providerIDs()[0] }
		other.plan = func(pipeline.PlanInput) (pipeline.PlanOutput, error) { return pipeline.PlanOutput{}, nil }
		obs := &recordingObserver{finished: func(*pipeline.RoundEvents) {}}
		// Nothing is reserved for the snapshot, so busy's candidate stays a candidate: only back-pressure holds it back.
		l := newLane(pipeline.LaneSpec{ID: types.LaneFast, Planners: []pipeline.Planner{busy, other}}, func(d *pipeline.LaneDeps) {
			d.Snapshots = pipeline.NewSnapshotBuilder(env.Client, cluster, env.Clock, cloudProvider, queue, clusterCost, nil)
			d.Observer = obs
		})
		Expect(l.Step(ctx).Plans).To(Equal(1))
		Expect(l.Step(ctx).Plans).To(Equal(0))
		Expect(busy.planned()).To(HaveLen(1))
		Expect(obs.gated).To(Equal([]string{"busy/" + string(types.GateUnresolved)}))
		// The other planner isn't held back by busy's unresolved plan.
		Expect(other.planned()).To(HaveLen(2))
		exec.resolveAll(pipeline.Started)
		r := l.Step(ctx)
		Expect(r.Admitted).To(Equal(1))
		Expect(r.Plans).To(Equal(1))
		Expect(busy.planned()).To(HaveLen(2))
	})
	It("backs a planner off for the polling period when none of its last plans started", func() {
		w := newWorld(2)
		busy, other := newStubPlanner("busy"), newStubPlanner("other")
		busy.filter = func(c *disruption.Candidate) bool { return c.ProviderID() == w.providerIDs()[0] }
		other.plan = func(pipeline.PlanInput) (pipeline.PlanOutput, error) { return pipeline.PlanOutput{}, nil }
		obs := &recordingObserver{finished: func(*pipeline.RoundEvents) {}}
		l := newLane(pipeline.LaneSpec{ID: types.LaneFast, Planners: []pipeline.Planner{busy, other}}, func(d *pipeline.LaneDeps) {
			d.Snapshots = pipeline.NewSnapshotBuilder(env.Client, cluster, env.Clock, cloudProvider, queue, clusterCost, nil)
			d.Observer = obs
		})
		Expect(l.Step(ctx).Plans).To(Equal(1))
		exec.resolveAll(pipeline.Rejected)
		r := l.Step(ctx)
		Expect(r.Plans).To(BeZero())
		Expect(r.NextTimer).To(Equal(env.Clock.Now().Add(10 * time.Second)))
		Expect(obs.gated).To(Equal([]string{"busy/" + string(types.GatePaced)}))
		Expect(other.planned()).To(HaveLen(2)) // the back-off is the planner's, not the lane's
		env.Clock.Step(10 * time.Second)
		Expect(l.Step(ctx).Plans).To(Equal(1))
		Expect(busy.planned()).To(HaveLen(2))
		// A Submit with a plan that started doesn't back off.
		exec.resolveAll(pipeline.Started)
		Expect(l.Step(ctx).Plans).To(Equal(1))
		Expect(busy.planned()).To(HaveLen(3))
	})
	It("gives planners that move pods only pod-free candidates while cluster state is unsynced", func() {
		w := newWorld(3, 1)
		moving, podFree := newStubPlanner("moving"), newStubPlanner("pod-free")
		podFree.shape = types.ShapePodFree
		podFree.plan = func(pipeline.PlanInput) (pipeline.PlanOutput, error) { return pipeline.PlanOutput{}, nil }
		moving.plan = podFree.plan
		l := newLane(pipeline.LaneSpec{ID: types.LaneFast, Planners: []pipeline.Planner{moving, podFree}})
		ids := w.providerIDs()

		unlaunched := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: w.nodePool.Name}}})
		unlaunched.Status.ProviderID = ""
		cluster.UpdateNodeClaim(unlaunched)
		Expect(cluster.Synced(ctx)).To(BeFalse())
		r := l.Step(ctx)
		Expect(r.Err).ToNot(HaveOccurred())
		Expect(r.HeldUnsynced).To(BeTrue())
		Expect(moving.planned()[0]).To(ConsistOf(ids[0], ids[2]))
		Expect(podFree.planned()[0]).To(ConsistOf(ids))
		Expect(moving.calls[0].Synced).To(BeFalse())

		bg := newStubPlanner("background")
		Expect(newLane(pipeline.LaneSpec{ID: types.LaneBackground, Planners: []pipeline.Planner{bg}, SkipWhileUnsynced: true}).Step(ctx).Gated).To(Equal(types.GateSynced))
		Expect(bg.planned()).To(BeEmpty())
	})
	It("doesn't plan while no planner of the lane has budget left", func() {
		newWorld(2)
		drift, empty := newStubPlanner("drift"), newStubPlanner("empty")
		empty.reason = v1.DisruptionReasonEmpty
		spec := pipeline.LaneSpec{ID: types.LaneFast, Planners: []pipeline.Planner{drift, empty}}
		zero := fakeBudgets{zero: sets.New(v1.DisruptionReasonDrifted, v1.DisruptionReasonEmpty)}
		r := newLane(spec, func(d *pipeline.LaneDeps) { d.Budgets = zero }).Step(ctx)
		Expect(r.Gated).To(Equal(types.GateBudgetZero))
		Expect(r.BudgetBound).To(BeTrue())
		Expect(drift.planned()).To(BeEmpty())
		// One reason with budget is enough to run the lane; a planner whose reason has none is skipped, and the lane is
		// budget-bound.
		some := fakeBudgets{zero: sets.New(v1.DisruptionReasonDrifted)}
		r = newLane(spec, func(d *pipeline.LaneDeps) { d.Budgets = some }).Step(ctx)
		Expect(r.Gated).To(Equal(types.GateNone))
		Expect(r.BudgetBound).To(BeTrue())
		Expect(drift.planned()).To(BeEmpty())
		Expect(empty.planned()).To(HaveLen(1))
		Expect(newLane(spec).Step(ctx).BudgetBound).To(BeFalse())
	})
	It("plans for a budget-free planner with no budget left, as repair repairs uninitialized nodes at budget 0", func() {
		newWorld(1)
		repair, drift := &budgetFreePlanner{newStubPlanner("repair")}, newStubPlanner("drift")
		repair.reason = v1.DisruptionReasonUnhealthy
		zero := fakeBudgets{zero: sets.New(v1.DisruptionReasonDrifted, v1.DisruptionReasonUnhealthy)}
		r := newLane(pipeline.LaneSpec{ID: types.LaneFast, Planners: []pipeline.Planner{repair, drift}}, func(d *pipeline.LaneDeps) { d.Budgets = zero }).Step(ctx)
		Expect(r.Gated).To(Equal(types.GateNone))
		Expect(repair.planned()).To(HaveLen(1))
		Expect(drift.planned()).To(BeEmpty())
	})
	It("doesn't plan until the prediction store is hydrated", func() {
		newWorld(1)
		p := newStubPlanner("drift")
		hydrated := false
		l := newLane(pipeline.LaneSpec{ID: types.LaneFast, Planners: []pipeline.Planner{p}}, func(d *pipeline.LaneDeps) {
			d.Hydrated = func(context.Context) bool { return hydrated }
		})
		Expect(l.Step(ctx).Gated).To(Equal(types.GateHydrating))
		hydrated = true
		Expect(l.Step(ctx).Gated).To(Equal(types.GateNone))
		Expect(p.planned()).To(HaveLen(1))
	})
	It("ends the round at a planner's error", func() {
		newWorld(1)
		failing, next := newStubPlanner("failing"), newStubPlanner("next")
		failing.plan = func(pipeline.PlanInput) (pipeline.PlanOutput, error) {
			return pipeline.PlanOutput{}, errors.New("boom")
		}
		r := newLane(pipeline.LaneSpec{ID: types.LaneFast, Planners: []pipeline.Planner{failing, next}}).Step(ctx)
		Expect(r.Err).To(MatchError("boom"))
		Expect(next.planned()).To(BeEmpty())
	})
	It("records verdicts in the round's events and reports the earliest next time", func() {
		w := newWorld(2)
		a, b := newStubPlanner("a"), newStubPlanner("b")
		soon := env.Clock.Now().Add(time.Minute)
		a.filter = func(c *disruption.Candidate) bool { return c.ProviderID() != w.providerIDs()[0] }
		a.plan = func(pipeline.PlanInput) (pipeline.PlanOutput, error) {
			return pipeline.PlanOutput{Next: soon.Add(time.Hour), Verdicts: []types.Verdict{{Planner: "a", Reason: types.ReasonPodsDoNotFit}}}, nil
		}
		b.plan = func(pipeline.PlanInput) (pipeline.PlanOutput, error) { return pipeline.PlanOutput{Next: soon}, nil }
		var events *pipeline.RoundEvents
		obs := &recordingObserver{finished: func(ev *pipeline.RoundEvents) { events = ev }}
		r := newLane(pipeline.LaneSpec{ID: types.LaneFast, Planners: []pipeline.Planner{a, b}}, func(d *pipeline.LaneDeps) { d.Observer = obs }).Step(ctx)
		Expect(r.NextTimer).To(Equal(soon))
		Expect(lo.Map(events.Verdicts(), func(v types.Verdict, _ int) types.RejectReason { return v.Reason })).To(Equal(
			[]types.RejectReason{types.ReasonDoNotDisrupt, types.ReasonPodsDoNotFit}))
	})
	It("writes nothing while it plans", func() {
		newWorld(3, 0, 2)
		var writes atomic.Int64
		recording := writeCounter{Client: env.Client, writes: &writes}
		before := lo.Map(cluster.DeepCopyNodes(), func(n *state.StateNode, _ int) *state.StateNode { return n })
		p := newStubPlanner("drift")
		l := newLane(pipeline.LaneSpec{ID: types.LaneFast, Planners: []pipeline.Planner{p}}, func(d *pipeline.LaneDeps) {
			d.Snapshots = pipeline.NewSnapshotBuilder(recording, cluster, env.Clock, cloudProvider, queue, clusterCost, exec.view())
		})
		Expect(l.Step(ctx).Plans).To(Equal(3))
		Expect(writes.Load()).To(BeZero())
		Expect(recorder.Events()).To(BeEmpty())
		Expect(cluster.DeepCopyNodes()).To(ConsistOf(lo.Map(before, func(n *state.StateNode, _ int) any { return Equal(n) })...))
	})
})

var _ = Describe("Controller", func() {
	It("runs every lane, then waits for a plan's ValidateAt before the next commit step, as validation waits", func() {
		newWorld(1)
		exec := newFakeExecutor()
		at := env.Clock.Now().Add(15 * time.Second)
		exec.steps = []pipeline.CommitStepResult{{Waiting: 1, NextReady: at}, {Admitted: 1}}
		fast, bg := newStubPlanner("fast"), newStubPlanner("bg")
		c := pipeline.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			pipeline.WithExecutor(exec), pipeline.WithReservations(exec.view()),
			pipeline.WithLanes(pipeline.LaneSpec{ID: types.LaneFast, Planners: []pipeline.Planner{fast}}, pipeline.LaneSpec{ID: types.LaneBackground, Planners: []pipeline.Planner{bg}}))
		done := make(chan time.Duration)
		go func() {
			defer GinkgoRecover()
			r, err := c.Reconcile(ctx)
			Expect(err).ToNot(HaveOccurred())
			done <- r.RequeueAfter
		}()
		Eventually(env.Clock.HasWaiters).Should(BeTrue())
		Consistently(done).ShouldNot(Receive())
		env.Clock.Step(15 * time.Second)
		Eventually(done).Should(Receive(Equal(singleton.RequeueImmediately)))
		Expect(fast.planned()).To(HaveLen(1))
		// The background lane ran after the fast lane's commit steps; the fast lane's plan reserved the only node.
		Expect(bg.planned()).To(BeEmpty())
	})
	It("requeues after a second while plans wait for a gate, and after the polling period when idle", func() {
		exec := newFakeExecutor()
		exec.steps = []pipeline.CommitStepResult{{Waiting: 1}}
		c := pipeline.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			pipeline.WithExecutor(exec), pipeline.WithLanes(pipeline.LaneSpec{ID: types.LaneFast}))
		r, err := c.Reconcile(ctx)
		Expect(err).ToNot(HaveOccurred())
		Expect(r.RequeueAfter).To(Equal(time.Second))
		r, err = c.Reconcile(ctx)
		Expect(err).ToNot(HaveOccurred())
		Expect(r.RequeueAfter).To(Equal(10 * time.Second))
	})
	It("returns a planner's error", func() {
		newWorld(1)
		p := newStubPlanner("failing")
		p.plan = func(pipeline.PlanInput) (pipeline.PlanOutput, error) {
			return pipeline.PlanOutput{}, errors.New("boom")
		}
		c := pipeline.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			pipeline.WithExecutor(newFakeExecutor()), pipeline.WithLanes(pipeline.LaneSpec{ID: types.LaneFast, Planners: []pipeline.Planner{p}}))
		_, err := c.Reconcile(ctx)
		Expect(err).To(MatchError(ContainSubstring("boom")))
	})
})

type recordingObserver struct {
	pipeline.NopObserver
	finished func(*pipeline.RoundEvents)
	gated    []string
}

func (o *recordingObserver) Gated(_ types.LaneID, planner string, gate types.GateReason) {
	o.gated = append(o.gated, planner+"/"+string(gate))
}

// budgetFreePlanner is a stub planner that may plan without budget.
type budgetFreePlanner struct{ *stubPlanner }

func (budgetFreePlanner) BudgetFree() bool { return true }

func (o *recordingObserver) RoundFinished(_ context.Context, _ types.LaneID, _ pipeline.StepResult, ev *pipeline.RoundEvents) {
	o.finished(ev)
}

// writeCounter counts the writes made through it.
type writeCounter struct {
	client.Client
	writes *atomic.Int64
}

func (c writeCounter) Create(ctx context.Context, o client.Object, opts ...client.CreateOption) error {
	c.writes.Add(1)
	return c.Client.Create(ctx, o, opts...)
}
func (c writeCounter) Update(ctx context.Context, o client.Object, opts ...client.UpdateOption) error {
	c.writes.Add(1)
	return c.Client.Update(ctx, o, opts...)
}
func (c writeCounter) Patch(ctx context.Context, o client.Object, p client.Patch, opts ...client.PatchOption) error {
	c.writes.Add(1)
	return c.Client.Patch(ctx, o, p, opts...)
}
func (c writeCounter) Delete(ctx context.Context, o client.Object, opts ...client.DeleteOption) error {
	c.writes.Add(1)
	return c.Client.Delete(ctx, o, opts...)
}
func (c writeCounter) DeleteAllOf(ctx context.Context, o client.Object, opts ...client.DeleteAllOfOption) error {
	c.writes.Add(1)
	return c.Client.DeleteAllOf(ctx, o, opts...)
}
func (c writeCounter) Status() client.SubResourceWriter {
	return subResourceCounter{SubResourceWriter: c.Client.Status(), writes: c.writes}
}
func (c writeCounter) SubResource(s string) client.SubResourceClient {
	return subResourceCounter{SubResourceClient: c.Client.SubResource(s), SubResourceWriter: c.Client.SubResource(s), writes: c.writes}
}

type subResourceCounter struct {
	client.SubResourceClient
	client.SubResourceWriter
	writes *atomic.Int64
}

func (c subResourceCounter) Create(ctx context.Context, o, sub client.Object, opts ...client.SubResourceCreateOption) error {
	c.writes.Add(1)
	return c.SubResourceWriter.Create(ctx, o, sub, opts...)
}
func (c subResourceCounter) Update(ctx context.Context, o client.Object, opts ...client.SubResourceUpdateOption) error {
	c.writes.Add(1)
	return c.SubResourceWriter.Update(ctx, o, opts...)
}
func (c subResourceCounter) Patch(ctx context.Context, o client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
	c.writes.Add(1)
	return c.SubResourceWriter.Patch(ctx, o, p, opts...)
}
func (c subResourceCounter) Apply(ctx context.Context, o runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	c.writes.Add(1)
	return c.SubResourceWriter.Apply(ctx, o, opts...)
}
