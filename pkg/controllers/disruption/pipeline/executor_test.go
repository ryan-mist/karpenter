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
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/controllers/node/health"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// hookStarter runs before, then the real starter.
type hookStarter struct {
	pipeline.Starter
	before func(*pipeline.Plan) error
}

func (s hookStarter) Start(ctx context.Context, p *pipeline.Plan) error {
	if s.before != nil {
		if err := s.before(p); err != nil {
			return err
		}
	}
	return s.Starter.Start(ctx, p)
}

func drain(ch <-chan pipeline.Resolution) []pipeline.Resolution {
	var out []pipeline.Resolution
	for {
		select {
		case r, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, r)
		default:
			return out
		}
	}
}

func kinds(rs []pipeline.Resolution) []pipeline.ResolutionKind {
	return lo.Map(rs, func(r pipeline.Resolution, _ int) pipeline.ResolutionKind { return r.Kind })
}

func tainted(node *corev1.Node) bool {
	GinkgoHelper()
	n := ExpectExists(ctx, env.Client, node)
	return lo.ContainsBy(n.Spec.Taints, func(t corev1.Taint) bool { return t.MatchTaint(&v1.DisruptedNoScheduleTaint) })
}

var _ = Describe("Executor", func() {
	var w *world
	var ids []string
	var l pipeline.Ledger
	var planner *acceptingPlanner
	var exec pipeline.Executor
	var starter hookStarter
	var candidates map[string]*disruption.Candidate

	newPlan := func(ids ...string) *pipeline.Plan {
		p := deletePlan(lo.Map(ids, func(id string, _ int) *disruption.Candidate { return candidates[id] })...)
		p.ID = uuid.New()
		p.BatchID, p.Siblings = p.ID, 1
		p.Planner, p.Reason, p.Method = planner.Name(), v1.DisruptionReasonDrifted, disruption.NewDrift(env.Client, cluster, prov, recorder, env.Clock)
		p.ComputedAt = env.Clock.Now()
		p.Footprint.ComputedAt = l.Version()
		p.Footprint.BudgetNodes = map[string]sets.Set[string]{w.nodePool.Name: sets.New(ids...)}
		return p
	}
	BeforeEach(func() {
		w = newWorld(5)
		ids = w.providerIDs()
		l = pipeline.NewLedger(env.Client, cluster, cloudProvider, env.Clock, queue)
		planner = &acceptingPlanner{stubPlanner: newStubPlanner("drift")}
		starter = hookStarter{Starter: pipeline.NewQueueStarter(queue, env.Clock)}
		exec = pipeline.NewExecutor(pipeline.ExecutorDeps{
			Ledger:   l,
			Admitter: pipeline.NewAdmitter(l, pipeline.NewCandidateRebuilder(env.Client, cluster, env.Clock, cloudProvider, queue), planner),
			Starter:  &starter,
			Sweeper:  disruption.NewMarkSweeper(env.Client, cluster, env.Clock),
			Queue:    queue,
			Cluster:  cluster,
			Clock:    env.Clock,
		})
		s, err := pipeline.NewSnapshotBuilder(env.Client, cluster, env.Clock, cloudProvider, queue, clusterCost, nil).Build(ctx, pipeline.Scope{})
		Expect(err).ToNot(HaveOccurred())
		candidates = lo.SliceToMap(s.CandidatesFor(ctx, planner), func(c *disruption.Candidate) (string, *disruption.Candidate) { return c.ProviderID(), c })
	})

	It("starts an admitted plan through the queue and releases its reservation once its nodes are marked", func() {
		ch := exec.Submit(ctx, newPlan(ids[0], ids[1]))
		Expect(l.View().Unstarted().UnsortedList()).To(ConsistOf(ids[0], ids[1]))
		r := exec.Step(ctx)
		Expect(r.Admitted).To(Equal(1))
		Expect(r.Waiting).To(BeZero())
		Expect(kinds(drain(ch))).To(Equal([]pipeline.ResolutionKind{pipeline.Started}))
		Expect(queue.HasAny(ids[0], ids[1])).To(BeTrue())
		Expect(tainted(w.nodes[0])).To(BeTrue())
		ExpectStateNodeExists(cluster, w.nodes[0])
		Expect(lo.Filter(cluster.DeepCopyNodes(), func(n *state.StateNode, _ int) bool { return n.MarkedForDeletion() })).To(HaveLen(2))
		Expect(l.View().Unstarted().Len()).To(BeZero())
		Expect(l.StartedSince(0)).To(HaveLen(1))
	})
	It("rejects at Submit what doesn't fit, and marks the surviving siblings for a re-simulation", func() {
		w.nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "2"}}
		ExpectApplied(ctx, env.Client, w.nodePool)
		a, b := newPlan(ids[0], ids[1]), newPlan(ids[2])
		b.BatchID, a.Siblings, b.Siblings = a.ID, 2, 2
		ch := exec.Submit(ctx, a, b)
		Expect(drain(ch)).To(Equal([]pipeline.Resolution{{PlanID: b.ID, Kind: pipeline.Rejected, Reason: types.ReasonBudget, AtSubmit: true}}))
		exec.Step(ctx)
		Expect(planner.inputs[0].Causes).To(ContainElement(types.ResimSibling))
	})
	It("reserves nothing once the lease is lost", func() {
		lost, cancel := context.WithCancel(ctx)
		cancel()
		p := newPlan(ids[0])
		Expect(drain(exec.Submit(lost, p))).To(Equal([]pipeline.Resolution{{PlanID: p.ID, Kind: pipeline.Rejected, Reason: types.ReasonCanceled, AtSubmit: true}}))
		Expect(l.View().IsReserved(ids[0])).To(BeFalse())
		Expect(exec.Step(ctx).Waiting).To(BeZero())
	})
	It("returns every plan of a planner with unresolved plans", func() {
		a := newPlan(ids[0])
		a.ValidateAt = env.Clock.Now().Add(15 * time.Second)
		exec.Submit(ctx, a)
		b := newPlan(ids[1])
		Expect(drain(exec.Submit(ctx, b))).To(Equal([]pipeline.Resolution{{PlanID: b.ID, Kind: pipeline.Rejected, Reason: types.ReasonBackpressure, AtSubmit: true}}))
		Expect(l.View().IsReserved(ids[1])).To(BeFalse())
	})
	It("admits a plan at its ValidateAt, not before", func() {
		p := newPlan(ids[0])
		p.ValidateAt = env.Clock.Now().Add(15 * time.Second)
		ch := exec.Submit(ctx, p)
		r := exec.Step(ctx)
		Expect(r).To(Equal(pipeline.CommitStepResult{Waiting: 1, NextReady: p.ValidateAt, Swept: true}))
		Expect(drain(ch)).To(BeEmpty())
		env.Clock.Step(15 * time.Second)
		Expect(exec.Step(ctx).Admitted).To(Equal(1))
		Expect(kinds(drain(ch))).To(Equal([]pipeline.ResolutionKind{pipeline.Started}))
	})
	It("releases a plan rejected at admission", func() {
		p := newPlan(ids[0])
		ch := exec.Submit(ctx, p)
		cluster.NominateNodeForPod(ctx, ids[0])
		Expect(exec.Step(ctx).Rejected).To(Equal(1))
		Expect(drain(ch)).To(Equal([]pipeline.Resolution{{PlanID: p.ID, Kind: pipeline.Rejected, Reason: types.ReasonCandidateNominated}}))
		Expect(l.View().IsReserved(ids[0])).To(BeFalse())
		Expect(queue.HasAny(ids[0])).To(BeFalse())
	})
	It("releases a plan whose start failed and sweeps the marks it left in the same step", func() {
		exec.Step(ctx) // the startup sweep
		starter.before = func(p *pipeline.Plan) error {
			// Fail after marking, as StartCommand does when it can't launch a replacement.
			Expect(state.RequireNoScheduleTaint(ctx, env.Client, true, p.Candidates[0].StateNode)).To(Succeed())
			return errors.New("launching replacement nodeclaim")
		}
		p := newPlan(ids[0])
		ch := exec.Submit(ctx, p)
		r := exec.Step(ctx)
		Expect(r.Swept).To(BeTrue())
		Expect(drain(ch)).To(Equal([]pipeline.Resolution{{PlanID: p.ID, Kind: pipeline.Rejected, Reason: types.ReasonStartFailed}}))
		Expect(l.View().IsReserved(ids[0])).To(BeFalse())
		Expect(tainted(w.nodes[0])).To(BeFalse())
	})
	It("doesn't sweep the nodes of reserved plans, and never sweeps while a plan starts", func() {
		// A leftover taint on a node that a waiting plan reserved.
		Expect(state.RequireNoScheduleTaint(ctx, env.Client, true, candidates[ids[0]].StateNode, candidates[ids[1]].StateNode)).To(Succeed())
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(w.nodes[0]))
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(w.nodes[1]))
		waiting := newPlan(ids[0])
		waiting.ValidateAt = env.Clock.Now().Add(15 * time.Second)
		exec.Submit(ctx, waiting)
		Expect(exec.Step(ctx).Swept).To(BeTrue())
		Expect(tainted(w.nodes[0])).To(BeTrue())
		Expect(tainted(w.nodes[1])).To(BeFalse())

		// A start blocked between its taint and its queue insert, as StartCommand is between markDisrupted and the
		// queue map, while the sweep comes due.
		block, entered := make(chan struct{}), make(chan struct{})
		starter.before = func(p *pipeline.Plan) error {
			close(entered)
			<-block
			return nil
		}
		other := &acceptingPlanner{stubPlanner: newStubPlanner("other")}
		p := newPlan(ids[2])
		p.Planner = other.Name()
		exec.Submit(ctx, p)
		env.Clock.Step(pipeline.SweepTick)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			exec.Step(ctx)
		}()
		Eventually(entered).Should(BeClosed())
		swept := make(chan pipeline.CommitStepResult)
		go func() {
			defer GinkgoRecover()
			swept <- exec.Step(ctx)
		}()
		Consistently(swept).ShouldNot(Receive())
		close(block)
		wg.Wait()
		Eventually(swept).Should(Receive())
	})
	It("drops every waiting plan when its context is cancelled", func() {
		p := newPlan(ids[0])
		p.ValidateAt = env.Clock.Now().Add(15 * time.Second)
		ch := exec.Submit(ctx, p)
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		exec.Step(cancelled)
		Expect(drain(ch)).To(Equal([]pipeline.Resolution{{PlanID: p.ID, Kind: pipeline.Dropped, Reason: types.ReasonCanceled}}))
		Expect(l.View().Unstarted().Len()).To(BeZero())
	})
	It("starts nothing admitted when the lease is lost during the step", func() {
		cancelled, cancel := context.WithCancel(ctx)
		e := pipeline.NewExecutor(pipeline.ExecutorDeps{Ledger: l, Starter: &starter, Queue: queue, Cluster: cluster, Clock: env.Clock,
			Sweeper:  disruption.NewMarkSweeper(env.Client, cluster, env.Clock),
			Admitter: admitThen{Admitter: pipeline.NewAdmitter(l, pipeline.NewCandidateRebuilder(env.Client, cluster, env.Clock, cloudProvider, queue), planner), then: cancel}})
		p := newPlan(ids[0])
		ch := e.Submit(ctx, p)
		e.Step(cancelled)
		Expect(drain(ch)).To(Equal([]pipeline.Resolution{{PlanID: p.ID, Kind: pipeline.Dropped, Reason: types.ReasonCanceled}}))
		Expect(queue.HasAny(ids[0])).To(BeFalse())
		Expect(l.View().Unstarted().Len()).To(BeZero())
	})
	It("resolves a plan replaced by several as started if any of them started", func() {
		a := newPlan(ids[0], ids[1])
		a1, a2 := newPlan(ids[0]), newPlan(ids[1])
		e := pipeline.NewExecutor(pipeline.ExecutorDeps{Ledger: l, Starter: &starter, Queue: queue, Cluster: cluster, Clock: env.Clock,
			Sweeper: disruption.NewMarkSweeper(env.Client, cluster, env.Clock),
			Admitter: admitFunc(func(ctx context.Context, group []*pipeline.Pending, _ *pipeline.CommitStep) pipeline.AdmitResult {
				Expect(l.Swap(ctx, []uuid.UUID{a.ID}, []*pipeline.Plan{a1, a2})).To(Succeed())
				return pipeline.AdmitResult{Start: []*pipeline.Plan{a1, a2}, Replaced: map[uuid.UUID][]uuid.UUID{a.ID: {a1.ID, a2.ID}}}
			})})
		starter.before = func(p *pipeline.Plan) error {
			if p.ID == a2.ID {
				return errors.New("launching replacement nodeclaim")
			}
			return nil
		}
		ch := e.Submit(ctx, a)
		Expect(e.Step(ctx).Admitted).To(Equal(1))
		Expect(drain(ch)).To(Equal([]pipeline.Resolution{{PlanID: a.ID, Kind: pipeline.Started, Replanned: true}}))
		Expect(queue.HasAny(ids[0])).To(BeTrue())
		Expect(l.View().Unstarted().Len()).To(BeZero())
	})
	Context("reboot", func() {
		var repair *disruption.Repair
		reboot := func(id string) *pipeline.Plan {
			p := newPlan(id)
			p.Action, p.Method, p.Reason = types.ActionReboot, repair, v1.DisruptionReasonUnhealthy
			p.Candidates[0].RepairPolicyResult = health.RepairResult{Action: cloudprovider.RebootNode, Condition: "BadNode", Reason: "test"}
			return p
		}
		BeforeEach(func() {
			repair = disruption.NewRepair(disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue))
		})
		It("hands the node to the reboot controller on its NodeClaim read at the start", func() {
			p := reboot(ids[0])
			// The NodeClaim changes after planning; a handoff built at planning would conflict.
			nc := ExpectExists(ctx, env.Client, w.nodeClaims[0])
			nc.Labels["changed"] = "true"
			ExpectApplied(ctx, env.Client, nc)
			ch := exec.Submit(ctx, p)
			Expect(exec.Step(ctx).Admitted).To(Equal(1))
			Expect(kinds(drain(ch))).To(Equal([]pipeline.ResolutionKind{pipeline.Started}))
			nc = ExpectExists(ctx, env.Client, w.nodeClaims[0])
			Expect(nc.StatusConditions().Get(v1.ConditionTypeRebooting).IsTrue()).To(BeTrue())
			Expect(nc.StatusConditions().Get(v1.ConditionTypeDisruptionReason).Reason).To(Equal(string(v1.DisruptionReasonUnhealthy)))
		})
		It("rejects a reboot whose node started rebooting since planning", func() {
			p := reboot(ids[0])
			nc := ExpectExists(ctx, env.Client, w.nodeClaims[0])
			nc.StatusConditions().SetTrue(v1.ConditionTypeRebooting)
			ExpectApplied(ctx, env.Client, nc)
			ch := exec.Submit(ctx, p)
			exec.Step(ctx)
			Expect(drain(ch)).To(Equal([]pipeline.Resolution{{PlanID: p.ID, Kind: pipeline.Rejected, Reason: types.ReasonCandidateInvalid}}))
		})
	})
})

var _ = Describe("Pipeline controller", func() {
	It("plans, admits and starts through the shared queue, gate on", func() {
		w := newWorld(3, 2)
		empty := newStubPlanner("emptiness")
		empty.shape = types.ShapePodFree
		empty.reason = v1.DisruptionReasonEmpty
		empty.filter = func(c *disruption.Candidate) bool { return len(c.ReschedulablePods()) == 0 }
		empty.plan = func(in pipeline.PlanInput) (pipeline.PlanOutput, error) {
			p := deletePlan(in.Candidates...)
			p.Method = disruption.NewEmptiness(disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue))
			p.Footprint.BudgetNodes = map[string]sets.Set[string]{w.nodePool.Name: p.Footprint.Removed}
			return pipeline.PlanOutput{Plans: []*pipeline.Plan{p}}, nil
		}
		c := pipeline.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			pipeline.WithLanes(pipeline.LaneSpec{ID: types.LaneFast, Planners: []pipeline.Planner{empty}}))
		ExpectSingletonReconciled(ctx, c)
		Expect(queue.HasAny(w.providerIDs()[0], w.providerIDs()[1])).To(BeTrue())
		Expect(queue.HasAny(w.providerIDs()[2])).To(BeFalse())
		Expect(lo.Map(queue.GetCommands(), func(cmd *disruption.Command, _ int) disruption.Decision { return cmd.Decision() })).To(Equal([]disruption.Decision{disruption.DeleteDecision}))
	})
})

// admitThen admits with the inner Admitter, then calls then (for example to cancel the step's context).
type admitThen struct {
	pipeline.Admitter
	then func()
}

func (a admitThen) Admit(ctx context.Context, group []*pipeline.Pending, step *pipeline.CommitStep) pipeline.AdmitResult {
	defer a.then()
	return a.Admitter.Admit(ctx, group, step)
}

type admitFunc func(context.Context, []*pipeline.Pending, *pipeline.CommitStep) pipeline.AdmitResult

func (f admitFunc) Admit(ctx context.Context, group []*pipeline.Pending, step *pipeline.CommitStep) pipeline.AdmitResult {
	return f(ctx, group, step)
}
