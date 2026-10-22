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
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"k8s.io/utils/clock"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
)

// LaneSpec is a lane's configuration. Lanes are configuration, not a planner property.
type LaneSpec struct {
	ID types.LaneID
	// Planners run in this order every round.
	Planners []Planner
	// SkipWhileUnsynced skips the whole lane while cluster state is unsynced. Otherwise, while unsynced, planners
	// whose Shape is not ShapePodFree get only pod-free candidates.
	SkipWhileUnsynced bool
	// Trigger paces the lane's rounds when the pipeline runs asynchronously; nil is PollTrigger{}.
	Trigger Trigger
	// FirstWins ends a round after the first planner that submitted a plan that isn't a plain delete, as a pass of the
	// disruption controller ends after the first method with a command that isn't delete-only (ReferenceLanes).
	FirstWins bool
}

// Lane plans for its planners, one round per Step. Step never sleeps.
type Lane interface {
	Spec() LaneSpec
	Step(ctx context.Context) StepResult
}

// StepResult is what one lane round did.
type StepResult struct {
	Lane types.LaneID
	// Plans is the number of plans submitted this round.
	Plans int
	// Admitted is the number of resolutions this round collected for earlier rounds' plans that started.
	Admitted int
	Took     time.Duration
	EndedAt  time.Time
	// SnapshotTook and PlanTook split Took: the snapshot build, and the planners (candidates, Plan and Submit).
	SnapshotTook, PlanTook time.Duration
	// Gated is why the whole lane didn't plan, if it didn't.
	Gated types.GateReason
	// NextTimer is the earliest time a planner reported something becomes eligible; zero if none.
	NextTimer time.Time
	// Err is the error that ended the round early, if any.
	Err error
}

// Synced reports whether cluster state is synced (state.Cluster.Synced).
type Synced interface {
	Synced(ctx context.Context) bool
}

// LaneDeps are what a lane plans with. Budgets, Reserved, Hydrated and Observer may be nil.
type LaneDeps struct {
	Clock     clock.Clock
	Cluster   Synced
	Snapshots SnapshotBuilder
	Executor  Executor
	Budgets   BudgetView
	Reserved  ReservationView
	// Hydrated reports whether the prediction store is hydrated; planning waits for it as the disruption controller
	// does.
	Hydrated func(ctx context.Context) bool
	Observer Observer
}

type lane struct {
	spec  LaneSpec
	scope Scope
	deps  LaneDeps
	// unresolved holds, per planner, the resolutions of its last Submit until all have arrived. A planner with an
	// unresolved Submit doesn't plan (per-planner back-pressure).
	unresolved map[string]<-chan Resolution
	// started counts, per planner, the plans of its unresolved Submit that started so far.
	started map[string]int
	// backoff holds, per planner, until when it doesn't plan because none of its last Submit's plans started.
	backoff map[string]time.Time
}

func NewLane(spec LaneSpec, deps LaneDeps) Lane {
	if deps.Observer == nil {
		deps.Observer = NopObserver{}
	}
	return &lane{spec: spec, scope: ScopeFor(spec.ID, spec.Planners...), deps: deps, unresolved: map[string]<-chan Resolution{},
		started: map[string]int{}, backoff: map[string]time.Time{}}
}

func (l *lane) Spec() LaneSpec { return l.spec }

// Step runs one round: the lane gates, one snapshot, then every planner in order. A planner's plans are submitted
// (and so reserved) before the next planner plans, so the next one plans around them.
func (l *lane) Step(ctx context.Context) (r StepResult) {
	start := l.deps.Clock.Now()
	r.Lane = l.spec.ID
	var ev *RoundEvents
	l.deps.Observer.RoundStarted(l.spec.ID)
	defer func() {
		r.EndedAt = l.deps.Clock.Now()
		r.Took = r.EndedAt.Sub(start)
		l.deps.Observer.RoundFinished(ctx, l.spec.ID, r, ev)
	}()
	for _, p := range l.spec.Planners {
		r.Admitted += l.collect(p.Name())
	}
	if gate := l.gate(ctx); gate != types.GateNone {
		r.Gated = gate
		l.deps.Observer.Gated(l.spec.ID, "", gate)
		return r
	}
	synced := l.deps.Cluster.Synced(ctx)
	snapshot, err := l.deps.Snapshots.Build(ctx, l.scope)
	r.SnapshotTook = l.deps.Clock.Since(start)
	if err != nil {
		r.Err = err
		return r
	}
	ev = snapshot.Events
	defer func() { r.PlanTook = l.deps.Clock.Since(start) - r.SnapshotTook }()
	for _, p := range l.spec.Planners {
		if _, ok := l.unresolved[p.Name()]; ok {
			l.deps.Observer.Gated(l.spec.ID, p.Name(), types.GateUnresolved)
			continue
		}
		if until, ok := l.backoff[p.Name()]; ok {
			if snapshot.BuiltAt.Before(until) {
				l.deps.Observer.Gated(l.spec.ID, p.Name(), types.GatePaced)
				if r.NextTimer.IsZero() || until.Before(r.NextTimer) {
					r.NextTimer = until
				}
				continue
			}
			delete(l.backoff, p.Name())
		}
		if !l.hasBudget(ctx, p) {
			l.deps.Observer.Gated(l.spec.ID, p.Name(), types.GateBudgetZero)
			continue
		}
		out, candidates, err := l.plan(ctx, p, snapshot, synced)
		if err != nil {
			r.Err = err
			return r
		}
		snapshot.Events.Record(out.Verdicts...)
		if !out.Next.IsZero() && (r.NextTimer.IsZero() || out.Next.Before(r.NextTimer)) {
			r.NextTimer = out.Next
		}
		if candidates == 0 || len(out.Plans) == 0 {
			continue
		}
		for _, plan := range out.Plans {
			l.complete(p, plan, snapshot.BuiltAt)
		}
		l.unresolved[p.Name()] = l.deps.Executor.Submit(ctx, out.Plans...)
		r.Plans += len(out.Plans)
		if l.spec.FirstWins && lo.SomeBy(out.Plans, func(plan *Plan) bool { return plan.Action != types.ActionDelete }) {
			return r
		}
	}
	return r
}

// gate is why the lane doesn't plan at all this round, if it doesn't.
func (l *lane) gate(ctx context.Context) types.GateReason {
	if l.deps.Hydrated != nil && !l.deps.Hydrated(ctx) {
		return types.GateHydrating
	}
	if !lo.SomeBy(l.spec.Planners, func(p Planner) bool { return l.hasBudget(ctx, p) }) {
		return types.GateBudgetZero
	}
	if l.spec.SkipWhileUnsynced && !l.deps.Cluster.Synced(ctx) {
		return types.GateSynced
	}
	return types.GateNone
}

// hasBudget reports whether the planner may plan: some NodePool has budget left for its reason, or the planner can plan
// disruptions that no budget counts (BudgetFree).
func (l *lane) hasBudget(ctx context.Context, p Planner) bool {
	if l.deps.Budgets == nil || l.deps.Budgets.AnyRemaining(ctx, p.Reason()) {
		return true
	}
	bf, ok := p.(BudgetFree)
	return ok && bf.BudgetFree()
}

// plan runs one planner on its candidates. A planner with no candidates doesn't plan.
func (l *lane) plan(ctx context.Context, p Planner, snapshot *Snapshot, synced bool) (PlanOutput, int, error) {
	start := l.deps.Clock.Now()
	candidates := snapshot.CandidatesFor(ctx, p)
	if !synced && p.Shape() != types.ShapePodFree {
		// Only a pod-free plan may be admitted while unsynced, so don't plan anything else.
		candidates = lo.Filter(candidates, func(c *disruption.Candidate, _ int) bool { return len(c.ReschedulablePods()) == 0 })
	}
	if len(candidates) == 0 {
		l.deps.Observer.Planned(l.spec.ID, p, 0, PlanOutput{}, l.deps.Clock.Since(start))
		return PlanOutput{}, 0, nil
	}
	in := PlanInput{
		Candidates: candidates,
		Budgets:    l.deps.Budgets,
		Reserved:   l.deps.Reserved,
		Snapshot:   snapshot,
		Now:        snapshot.BuiltAt,
		Synced:     synced,
	}
	if tc, ok := p.(TotalsConsumer); ok && tc.NeedsTotals() {
		in.Evaluator = disruption.NewBalancedEvaluator(snapshot.Totals(ctx, p.Class()), snapshot.Events)
	}
	out, err := p.Plan(ctx, in)
	l.deps.Observer.Planned(l.spec.ID, p, len(candidates), out, l.deps.Clock.Since(start))
	return out, len(candidates), err
}

// complete fills in the fields of a plan that the lane owns.
func (l *lane) complete(p Planner, plan *Plan, now time.Time) {
	if plan.ID == uuid.Nil {
		plan.ID = uuid.New()
	}
	if plan.BatchID == uuid.Nil {
		plan.BatchID = plan.ID
		plan.Siblings = 1
	}
	plan.Planner = p.Name()
	if plan.Reason == "" {
		plan.Reason = p.Reason()
	}
	if plan.ComputedAt.IsZero() {
		plan.ComputedAt = now
	}
}

// collect takes the resolutions that have arrived for the planner's last Submit, and returns how many of them started.
// A planner none of whose plans started (rejected at Submit or admission, failed to start, dropped) backs off for the
// polling period, as the disruption controller retries such a command at its next pass. Without it, the wake-up the
// resolution sends (or the 1 s poll) would re-plan the same candidates at once, in a loop.
func (l *lane) collect(planner string) int {
	ch, ok := l.unresolved[planner]
	if !ok {
		return 0
	}
	started := 0
	for {
		select {
		case res, open := <-ch:
			if !open {
				delete(l.unresolved, planner)
				if l.started[planner] == 0 {
					l.backoff[planner] = l.deps.Clock.Now().Add(failureBackoff)
				}
				delete(l.started, planner)
				return started
			}
			if res.Kind == Started {
				started++
				l.started[planner]++
			}
		default:
			return started
		}
	}
}
