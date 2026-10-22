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

package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

// commitLoop is the Executor: the single serialized commit loop. Submit may be called from any lane; Step runs one
// commit step at a time. Every write the pipeline makes happens in a step: starts, and the sweep of leftover
// disruption taints and conditions, which never runs while a start does.
type commitLoop struct {
	ledger   Ledger
	admitter Admitter
	starter  Starter
	sweeper  *disruption.MarkSweeper
	queue    *disruption.Queue
	cluster  Synced
	clock    clock.Clock
	scorer   Scorer
	observer Observer

	// step serializes commit steps.
	step sync.Mutex
	// mu guards the fields below; Submit takes only mu.
	mu      sync.Mutex
	waiting []*waitingPlan
	// unresolved counts the unresolved plans per planner, for back-pressure.
	unresolved map[string]int
	// nextSweep is when the next sweep runs; zero means at the next step.
	nextSweep time.Time
}

type waitingPlan struct {
	*Pending
	sub *submission
}

// submission is one Submit's resolution channel.
type submission struct {
	ch        chan Resolution
	remaining int
}

func (s *submission) resolve(r Resolution) {
	s.ch <- r
	if s.remaining--; s.remaining == 0 {
		close(s.ch)
	}
}

// ExecutorDeps are what the commit loop needs. Observer may be nil.
type ExecutorDeps struct {
	Ledger   Ledger
	Admitter Admitter
	Starter  Starter
	Sweeper  *disruption.MarkSweeper
	Queue    *disruption.Queue
	Cluster  Synced
	Clock    clock.Clock
	Scorer   Scorer
	Observer Observer
}

func NewExecutor(deps ExecutorDeps) Executor {
	return &commitLoop{
		ledger:     deps.Ledger,
		admitter:   deps.Admitter,
		starter:    deps.Starter,
		sweeper:    deps.Sweeper,
		queue:      deps.Queue,
		cluster:    deps.Cluster,
		clock:      deps.Clock,
		scorer:     lo.Ternary[Scorer](deps.Scorer != nil, deps.Scorer, NewScorer()),
		observer:   lo.Ternary[Observer](deps.Observer != nil, deps.Observer, NopObserver{}),
		unresolved: map[string]int{},
	}
}

// Submit reserves the plans in the Ledger and queues the reserved ones for admission. A plan that doesn't fit is
// resolved as rejected right away. A planner with unresolved plans gets all of them back rejected (back-pressure).
func (e *commitLoop) Submit(ctx context.Context, plans ...*Plan) <-chan Resolution {
	sub := &submission{ch: make(chan Resolution, len(plans)), remaining: len(plans)}
	if len(plans) == 0 {
		close(sub.ch)
		return sub.ch
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if lo.SomeBy(plans, func(p *Plan) bool { return e.unresolved[p.Planner] > 0 }) {
		for _, p := range plans {
			e.resolveLocked(sub, p, Resolution{PlanID: p.ID, Kind: Rejected, Reason: types.ReasonBackpressure}, false)
		}
		return sub.ch
	}
	errs := e.ledger.Reserve(ctx, plans...)
	rejectedBatch := map[uuid.UUID]bool{}
	for i, p := range plans {
		if errs[i] != nil {
			rejectedBatch[p.BatchID] = true
		}
	}
	now := e.clock.Now()
	for i, p := range plans {
		e.unresolved[p.Planner]++
		if errs[i] != nil {
			e.resolveLocked(sub, p, Resolution{PlanID: p.ID, Kind: Rejected, Reason: rejectReason(ctx, errs[i])}, true)
			continue
		}
		e.waiting = append(e.waiting, &waitingPlan{Pending: &Pending{Plan: p, SubmittedAt: now, SiblingRejected: rejectedBatch[p.BatchID]}, sub: sub})
	}
	return sub.ch
}

// resolveLocked delivers a plan's resolution. counted is whether the plan counts as unresolved for its planner.
func (e *commitLoop) resolveLocked(sub *submission, p *Plan, r Resolution, counted bool) {
	if counted {
		e.unresolved[p.Planner]--
	}
	sub.resolve(r)
}

// Step runs one commit step: the sweep if it is due, then admission of the ready plans in Scorer order, then the
// starts of the admitted plans, in parallel. If ctx is done, every waiting plan is dropped instead: the reservations
// are in memory only, so dropping them leaves nothing behind.
func (e *commitLoop) Step(ctx context.Context) (res CommitStepResult) {
	e.step.Lock()
	defer e.step.Unlock()
	now := e.clock.Now()
	if ctx.Err() != nil {
		e.dropAll(types.ReasonCanceled)
		return res
	}
	if e.sweepDue(now) {
		res.Swept = e.sweep(ctx)
	}
	step := &CommitStep{Now: now, Synced: e.cluster.Synced(ctx)}
	var toStart []*Plan
	// owners maps a plan to start to the submitted plans whose resolution it decides.
	owners := map[uuid.UUID][]*owner{}
	for _, group := range e.ready(now) {
		r := e.admitter.Admit(ctx, group, step)
		e.mu.Lock()
		for _, p := range group {
			wp := e.find(p.Plan.ID)
			switch {
			case r.Rejected[p.Plan.ID] != "":
				e.release(wp, Resolution{PlanID: p.Plan.ID, Kind: Rejected, Reason: r.Rejected[p.Plan.ID]})
				res.Rejected++
			case r.Dropped[p.Plan.ID] != "":
				e.release(wp, Resolution{PlanID: p.Plan.ID, Kind: Dropped, Reason: r.Dropped[p.Plan.ID]})
				res.Rejected++
			case lo.ContainsBy(r.Wait, func(w *Pending) bool { return w.Plan.ID == p.Plan.ID }):
			default:
				e.remove(wp)
				// A plan that admission replaced resolves once all the plans that replace it have started or failed.
				ids := []uuid.UUID{p.Plan.ID}
				if replans, ok := r.Replaced[p.Plan.ID]; ok && len(replans) > 0 {
					ids = replans
				}
				o := &owner{waitingPlan: wp, remaining: len(ids), replanned: !lo.Contains(ids, p.Plan.ID)}
				for _, id := range ids {
					owners[id] = append(owners[id], o)
				}
			}
		}
		e.mu.Unlock()
		step.Admitted = append(step.Admitted, r.Start...)
		toStart = append(toStart, r.Start...)
	}
	if ctx.Err() != nil {
		// The lease was lost during the step: start nothing more.
		e.cancel(toStart, owners)
		return res
	}
	started, failed := e.start(ctx, toStart, owners)
	res.Admitted = started
	if failed > 0 {
		// Sweep a failed start's marks right away; no start runs now.
		res.Swept = e.sweep(ctx) || res.Swept
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	res.Waiting = len(e.waiting)
	for _, wp := range e.waiting {
		if at := wp.readyAt(); at.After(now) && (res.NextReady.IsZero() || at.Before(res.NextReady)) {
			res.NextReady = at
		}
	}
	return res
}

// start starts the admitted plans in parallel. They are pairwise unrelated: admission makes a plan related to one
// admitted earlier in the step wait.
// owner is a submitted plan that admitted plans start for (itself, or the plans that replaced it).
type owner struct {
	*waitingPlan
	remaining int
	replanned bool
	started   bool
	reason    types.RejectReason
}

// done records how one of the owner's plans ended, and resolves the owner after the last one: Started if any of them
// started (nodes are being disrupted), otherwise Rejected with the first failure's reason.
func (e *commitLoop) done(o *owner, started bool, reason types.RejectReason) {
	o.started = o.started || started
	if !started && o.reason == "" {
		o.reason = reason
	}
	if o.remaining--; o.remaining > 0 {
		return
	}
	r := Resolution{PlanID: o.Plan.ID, Kind: Started, Replanned: o.replanned}
	if !o.started {
		r.Kind, r.Reason = Rejected, o.reason
	}
	e.resolveLocked(o.sub, o.Plan, r, true)
}

// cancel drops admitted plans that won't start because the lease was lost, and every waiting plan.
func (e *commitLoop) cancel(plans []*Plan, owners map[uuid.UUID][]*owner) {
	e.mu.Lock()
	for _, p := range plans {
		e.ledger.Release(p.ID, types.ReasonCanceled)
		for _, o := range owners[p.ID] {
			if o.remaining--; o.remaining == 0 {
				e.resolveLocked(o.sub, o.Plan, Resolution{PlanID: o.Plan.ID, Kind: Dropped, Reason: types.ReasonCanceled, Replanned: o.replanned}, true)
			}
		}
	}
	e.mu.Unlock()
	e.dropAll(types.ReasonCanceled)
}

func (e *commitLoop) start(ctx context.Context, plans []*Plan, owners map[uuid.UUID][]*owner) (started, failed int) {
	if len(plans) == 0 {
		return 0, 0
	}
	// A start that fails after marking its candidates leaves marks the next sweep must read from the API server.
	e.sweeper.Recheck(lo.FlatMap(plans, func(p *Plan, _ int) []string { return providerIDsOfCandidates(p.Candidates) })...)
	// A start runs to completion even if the lease is lost meanwhile: a half-started command is worse than one more.
	startCtx := context.WithoutCancel(ctx)
	errs := make([]error, len(plans))
	workqueue.ParallelizeUntil(startCtx, len(plans), len(plans), func(i int) {
		e.ledger.MarkStarting(plans[i].ID)
		errs[i] = e.starter.Start(startCtx, plans[i])
	})
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, p := range plans {
		reason := types.RejectReason("")
		if errs[i] != nil {
			log.FromContext(ctx).Error(errs[i], "failed starting disruption plan", "planner", p.Planner, "plan", p.ID)
			e.ledger.Release(p.ID, types.ReasonStartFailed)
			reason = lo.Ternary(RejectReasonOf(errs[i]) != "", RejectReasonOf(errs[i]), types.ReasonStartFailed)
			failed++
		} else {
			e.ledger.MarkStarted(p.ID)
			started++
		}
		for _, o := range owners[p.ID] {
			e.done(o, errs[i] == nil, reason)
		}
	}
	return started, failed
}

// ready returns the waiting plans whose time has come, grouped by batch, in Scorer order.
func (e *commitLoop) ready(now time.Time) [][]*Pending {
	e.mu.Lock()
	defer e.mu.Unlock()
	var order []uuid.UUID
	groups := map[uuid.UUID][]*Pending{}
	for _, wp := range e.waiting {
		if wp.readyAt().After(now) {
			continue
		}
		if _, ok := groups[wp.Plan.BatchID]; !ok {
			order = append(order, wp.Plan.BatchID)
		}
		groups[wp.Plan.BatchID] = append(groups[wp.Plan.BatchID], wp.Pending)
	}
	out := lo.Map(order, func(id uuid.UUID, _ int) []*Pending { return groups[id] })
	sortGroups(out, e.scorer)
	return out
}

func (e *commitLoop) find(id uuid.UUID) *waitingPlan {
	wp, _ := lo.Find(e.waiting, func(wp *waitingPlan) bool { return wp.Plan.ID == id })
	return wp
}

func (e *commitLoop) remove(wp *waitingPlan) {
	e.waiting = lo.Without(e.waiting, wp)
}

// release releases a waiting plan's reservation and resolves it.
func (e *commitLoop) release(wp *waitingPlan, r Resolution) {
	if wp == nil {
		return
	}
	e.remove(wp)
	e.ledger.Release(wp.Plan.ID, r.Reason)
	e.resolveLocked(wp.sub, wp.Plan, r, true)
}

func (e *commitLoop) dropAll(reason types.RejectReason) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, wp := range append([]*waitingPlan(nil), e.waiting...) {
		e.release(wp, Resolution{PlanID: wp.Plan.ID, Kind: Dropped, Reason: reason})
	}
}

func (e *commitLoop) sweepDue(now time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !now.Before(e.nextSweep)
}

// sweep removes the disruption taint and condition from nodes no action owns: not queued, not MarkedForDeletion, not
// rebooting, and not disrupted by a reserved plan. It runs at the first step, then every SweepTick, and at the next
// step after a start failed.
func (e *commitLoop) sweep(ctx context.Context) bool {
	reserved := e.ledger.View()
	err := e.sweeper.Sweep(ctx, func(n *state.StateNode) bool {
		return e.queue.HasAny(n.ProviderID()) || n.MarkedForDeletion() || n.RebootInProgress() || reserved.IsReserved(n.ProviderID())
	})
	e.mu.Lock()
	defer e.mu.Unlock()
	if err != nil {
		log.FromContext(ctx).Error(err, "failed removing leftover disruption taints and conditions")
		return false
	}
	e.nextSweep = e.clock.Now().Add(SweepTick)
	return true
}
