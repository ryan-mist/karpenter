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

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
)

// Revalidator is a planner's acceptance test at admission. Every planner should implement it; a planner that doesn't
// gets defaultRevalidator.
type Revalidator interface {
	// AlwaysResimulate reports that every plan is re-simulated at admission (consolidation, as its validator today).
	AlwaysResimulate() bool
	// Revalidate accepts a plan on its rebuilt candidates, re-simulating if in.Causes is non-empty. It returns the plans
	// to start in its place (usually the plan, possibly with fewer candidates or adopted replacements, and an updated
	// Footprint) or a *RejectError.
	Revalidate(ctx context.Context, in RevalidateInput) ([]*Plan, error)
	// Replan re-plans the surviving siblings of a batch whose other plans were rejected, with one joint simulation.
	// It returns the plans to start instead, or a *RejectError if none should start.
	Replan(ctx context.Context, survivors []*Plan, in RevalidateInput) ([]*Plan, error)
}

// RevalidateInput is what a planner accepts a plan on.
type RevalidateInput struct {
	Plan *Plan
	// Fresh are the plan's candidates rebuilt from live state that passed re-validation, in the plan's order.
	Fresh []*disruption.Candidate
	// Rejected are the plan's candidates that failed re-validation, by providerID.
	Rejected map[string]types.RejectReason
	// Causes are why the plan must be re-simulated; empty if it needn't be.
	Causes   []types.ResimCause
	Reserved ReservationView
	Now      time.Time
}

// Pending is a submitted plan waiting for admission.
type Pending struct {
	Plan        *Plan
	SubmittedAt time.Time
	// SiblingRejected: a plan of the same batch was rejected at Reserve.
	SiblingRejected bool
}

// readyAt is when the plan may be admitted.
func (p *Pending) readyAt() time.Time {
	return lo.Ternary(p.Plan.ValidateAt.After(p.SubmittedAt), p.Plan.ValidateAt, p.SubmittedAt)
}

// CommitStep is the state of one commit step that admission reads.
type CommitStep struct {
	Now time.Time
	// Synced is whether cluster state was synced when the step started.
	Synced bool
	// Admitted are the plans admitted earlier in this step.
	Admitted []*Plan
}

// AdmitResult is the outcome of admitting one group.
type AdmitResult struct {
	Start []*Plan
	Wait  []*Pending
	// Rejected and Dropped are the plans that won't start, with why.
	Rejected map[uuid.UUID]types.RejectReason
	Dropped  map[uuid.UUID]types.RejectReason
	// Replaced maps a plan to the plans in Start that replace it (a planner rewrote or re-planned it).
	Replaced map[uuid.UUID][]uuid.UUID
	Resims   []types.ResimCause
	Events   []*RoundEvents
}

// Admitter is the serial point: it decides, against live state, whether a plan starts. It is called only from the
// commit loop, starts no goroutines and writes nothing.
type Admitter interface {
	// Admit decides for one group: a plan alone, or the ready plans of one batch.
	Admit(ctx context.Context, group []*Pending, step *CommitStep) AdmitResult
}

type admitter struct {
	ledger    Ledger
	rebuilder CandidateRebuilder
	planners  map[string]Planner
}

func NewAdmitter(ledger Ledger, rebuilder CandidateRebuilder, planners ...Planner) Admitter {
	return &admitter{ledger: ledger, rebuilder: rebuilder, planners: lo.SliceToMap(planners, func(p Planner) (string, Planner) { return p.Name(), p })}
}

// Admit runs, for each plan of the group: the admission deadline; the Synced gate (only pod-free plans are admitted
// while cluster state is unsynced); re-validation of its candidates from live state; the Ledger's live recheck; the hard
// relations to commands started since the plan's simulation; the re-simulation causes; the planner's acceptance; and
// coupling to plans admitted earlier in this step. A batch starts as a unit: if one of its plans is rejected, the
// survivors are re-planned once, or none of them starts.
//
//nolint:gocyclo
func (a *admitter) Admit(ctx context.Context, group []*Pending, step *CommitStep) AdmitResult {
	res := AdmitResult{Rejected: map[uuid.UUID]types.RejectReason{}, Dropped: map[uuid.UUID]types.RejectReason{}, Replaced: map[uuid.UUID][]uuid.UUID{}}
	// The deadline and the gate apply to the whole group, so a batch waits or is dropped together.
	if lo.SomeBy(group, func(p *Pending) bool { return step.Now.After(p.readyAt().Add(MaxAdmissionWait)) }) {
		for _, p := range group {
			res.Dropped[p.Plan.ID] = types.ReasonDeadline
		}
		return res
	}
	if !step.Synced && lo.SomeBy(group, func(p *Pending) bool { return !p.Plan.PodFree() }) {
		res.Wait = group
		return res
	}
	accepted := map[uuid.UUID][]*Plan{}
	var survivors []*Pending
	var resims []types.ResimCause
	for _, p := range group {
		plans, causes, wait, err := a.admitOne(ctx, p, step, &res)
		switch {
		case err != nil:
			res.Rejected[p.Plan.ID] = rejectReason(ctx, err)
			resims = append(resims, causes...)
		case wait:
			res.Wait = append(res.Wait, p)
		default:
			accepted[p.Plan.ID] = plans
			survivors = append(survivors, p)
			resims = append(resims, causes...)
		}
	}
	if len(res.Wait) > 0 && len(group) > 1 {
		// A batch starts together: if part of it must wait, all of it waits.
		return waitAll(res, group)
	}
	if len(res.Rejected) > 0 && len(survivors) > 0 && len(group) > 1 {
		return a.replan(ctx, group, survivors, step, res, resims)
	}
	res.Resims = append(res.Resims, resims...)
	if len(survivors) == 0 {
		return res
	}
	// Only now does the Ledger learn what starts: the accepted footprints, or the plans that replace them. A batch is
	// committed with one all-or-nothing Swap, so a member that can't be committed leaves no other member's change
	// behind.
	all := lo.FlatMap(survivors, func(p *Pending, _ int) []*Plan { return accepted[p.Plan.ID] })
	var err error
	if len(group) == 1 {
		err = a.commit(ctx, survivors[0].Plan, all)
	} else {
		err = a.ledger.Swap(ctx, lo.Map(survivors, func(p *Pending, _ int) uuid.UUID { return p.Plan.ID }), all)
	}
	if err != nil {
		for _, p := range survivors {
			res.Rejected[p.Plan.ID] = rejectReason(ctx, err)
		}
		return res
	}
	for _, p := range survivors {
		plans := accepted[p.Plan.ID]
		if len(plans) != 1 || plans[0].ID != p.Plan.ID {
			res.Replaced[p.Plan.ID] = lo.Map(plans, func(q *Plan, _ int) uuid.UUID { return q.ID })
		}
	}
	res.Start = all
	return res
}

// waitAll makes the whole group wait, with no rejection: nothing in the Ledger changed.
func waitAll(res AdmitResult, group []*Pending) AdmitResult {
	res.Wait = group
	res.Rejected = map[uuid.UUID]types.RejectReason{}
	res.Replaced = map[uuid.UUID][]uuid.UUID{}
	res.Start = nil
	return res
}

func (a *admitter) commit(ctx context.Context, plan *Plan, plans []*Plan) error {
	if len(plans) == 1 && plans[0].ID == plan.ID {
		return a.ledger.Amend(ctx, plan.ID, plans[0].Footprint)
	}
	return a.ledger.Swap(ctx, []uuid.UUID{plan.ID}, plans)
}

// rejectReason is the reason a plan is rejected for err: its RejectReason; canceled if the step's context is done (the
// lease was lost); otherwise an error that isn't a verdict on the plan.
func rejectReason(ctx context.Context, err error) types.RejectReason {
	switch {
	case RejectReasonOf(err) != "":
		return RejectReasonOf(err)
	case ctx.Err() != nil:
		return types.ReasonCanceled
	default:
		return types.ReasonError
	}
}

// coupledToAdmitted reports whether a plan relates to one admitted earlier in this step (rule 9): it then waits for the
// next step, whose re-simulation sees that plan started.
func coupledToAdmitted(step *CommitStep, plans []*Plan) bool {
	return lo.SomeBy(plans, func(q *Plan) bool {
		return lo.SomeBy(step.Admitted, func(o *Plan) bool { return o.BatchID != q.BatchID && Relate(q.Footprint, o.Footprint).Related() })
	})
}

// admitOne decides for one plan, returning the plans to start in its place and why they were re-simulated, or that it
// waits.
func (a *admitter) admitOne(ctx context.Context, p *Pending, step *CommitStep, res *AdmitResult) ([]*Plan, []types.ResimCause, bool, error) {
	plan := p.Plan
	planner := a.planners[plan.Planner]
	rv := revalidatorFor(planner)
	ids := providerIDsOfCandidates(plan.Candidates)
	class := disruption.GracefulDisruptionClass
	if planner != nil {
		class = planner.Class()
	}
	rebuilt, err := a.rebuilder.Rebuild(ctx, class, ids...)
	if err != nil {
		return nil, nil, false, err
	}
	res.Events = append(res.Events, rebuilt.Events)
	in := RevalidateInput{Plan: plan, Rejected: rebuilt.Rejected, Reserved: a.ledger.View(), Now: step.Now}
	for _, c := range plan.Candidates {
		fresh, ok := rebuilt.Candidates[c.ProviderID()]
		if !ok {
			continue
		}
		if planner != nil {
			if eligible, _ := planner.Filter(ctx, fresh, step.Now); !eligible {
				in.Rejected[c.ProviderID()] = types.ReasonCandidateIneligible
				continue
			}
		}
		if want, ok := plan.PodSetHash[c.ProviderID()]; ok && want != PodSetHash(fresh) {
			if rv.AlwaysResimulate() {
				// Consolidation's validator rejects a candidate whose pods changed; it doesn't re-place them.
				in.Rejected[c.ProviderID()] = types.ReasonPodsChanged
				continue
			}
			in.Causes = appendCause(in.Causes, types.ResimPodsChanged)
		}
		in.Fresh = append(in.Fresh, fresh)
	}
	// A plan that was pod-free when planned may not be on its rebuilt candidates (a pod bound meanwhile).
	if !step.Synced && !plan.PodFreeOn(in.Fresh) {
		return nil, nil, true, nil
	}
	if err := a.ledger.Recheck(ctx, plan.ID); err != nil {
		return nil, nil, false, err
	}
	if err := a.causes(plan, p, step, rebuilt, rv, &in); err != nil {
		return nil, nil, false, err
	}
	plans, err := rv.Revalidate(ctx, in)
	if err != nil {
		return nil, in.Causes, false, err
	}
	if coupledToAdmitted(step, plans) {
		return nil, nil, true, nil
	}
	return plans, in.Causes, false, nil
}

// causes rejects a plan with a hard relation to a command started since it was simulated, and collects why it must
// be re-simulated.
func (a *admitter) causes(plan *Plan, p *Pending, step *CommitStep, rebuilt *Rebuilt, rv Revalidator, in *RevalidateInput) error {
	if rv.AlwaysResimulate() {
		in.Causes = appendCause(in.Causes, types.ResimAlways)
	}
	inFlight := a.ledger.View().InFlightDestinations()
	if intersects(plan.Footprint.Removed, inFlight) {
		return reject(types.ReasonConflict, "a started command is moving pods onto a candidate")
	}
	// A batch starts as a unit, so no sibling of the plan is among the started plans.
	for _, q := range a.ledger.StartedSince(plan.Footprint.ComputedAt) {
		rel := Relate(plan.Footprint, q.Footprint)
		if len(rel.Hard) > 0 {
			return reject(types.ReasonConflict, "conflicts with started plan %s: %v", q.PlanID, rel.Hard)
		}
		if len(rel.Coupled) > 0 {
			in.Causes = appendCause(in.Causes, types.ResimCoupled)
		}
	}
	if step.Now.Sub(plan.ComputedAt) > StaleAfter {
		in.Causes = appendCause(in.Causes, types.ResimStale)
	}
	for np, v := range plan.CatalogVersions {
		if rebuilt.CatalogVersion(np) != v {
			in.Causes = appendCause(in.Causes, types.ResimCatalog)
		}
	}
	if p.SiblingRejected {
		in.Causes = appendCause(in.Causes, types.ResimSibling)
	}
	return nil
}

// replan re-plans a batch's surviving plans once, all or nothing.
func (a *admitter) replan(ctx context.Context, group, survivors []*Pending, step *CommitStep, res AdmitResult, resims []types.ResimCause) AdmitResult {
	res.Start = nil
	res.Replaced = map[uuid.UUID][]uuid.UUID{}
	plans := lo.Map(survivors, func(p *Pending, _ int) *Plan { return p.Plan })
	rejectAll := func(reason types.RejectReason) AdmitResult {
		for _, p := range plans {
			res.Rejected[p.ID] = reason
		}
		res.Resims = append(res.Resims, resims...)
		return res
	}
	rv := revalidatorFor(a.planners[plans[0].Planner])
	in := RevalidateInput{Plan: plans[0], Causes: []types.ResimCause{types.ResimSibling}, Reserved: a.ledger.View(), Now: step.Now}
	replans, err := rv.Replan(ctx, plans, in)
	if err != nil || len(replans) == 0 {
		return rejectAll(types.ReasonSiblingRejected)
	}
	// The re-planned batch obeys rule 9 like any other plan: related to a plan admitted earlier in this step, the whole
	// group waits for the next step, with nothing changed in the Ledger.
	if coupledToAdmitted(step, replans) {
		return waitAll(res, group)
	}
	if err := a.ledger.Swap(ctx, lo.Map(plans, func(p *Plan, _ int) uuid.UUID { return p.ID }), replans); err != nil {
		return rejectAll(lo.Ternary(RejectReasonOf(err) != "", RejectReasonOf(err), types.ReasonSiblingRejected))
	}
	res.Resims = append(append(res.Resims, resims...), types.ResimSibling)
	ids := lo.Map(replans, func(q *Plan, _ int) uuid.UUID { return q.ID })
	for _, p := range plans {
		res.Replaced[p.ID] = ids
	}
	res.Start = replans
	return res
}

func appendCause(causes []types.ResimCause, c types.ResimCause) []types.ResimCause {
	if lo.Contains(causes, c) {
		return causes
	}
	return append(causes, c)
}

func providerIDsOfCandidates(cs []*disruption.Candidate) []string {
	return lo.Map(cs, func(c *disruption.Candidate, _ int) string { return c.ProviderID() })
}

func revalidatorFor(p Planner) Revalidator {
	if rv, ok := p.(Revalidator); ok {
		return rv
	}
	return defaultRevalidator{}
}

// defaultRevalidator accepts a plan only as planned: every candidate still valid and no re-simulation needed.
type defaultRevalidator struct{}

func (defaultRevalidator) AlwaysResimulate() bool { return false }

func (defaultRevalidator) Revalidate(_ context.Context, in RevalidateInput) ([]*Plan, error) {
	for _, c := range in.Plan.Candidates {
		if reason, ok := in.Rejected[c.ProviderID()]; ok {
			return nil, &RejectError{Reason: reason}
		}
	}
	if len(in.Causes) > 0 {
		return nil, reject(types.ReasonResimChanged, "the planner can't re-simulate (%v)", in.Causes)
	}
	in.Plan.Candidates = in.Fresh
	return []*Plan{in.Plan}, nil
}

func (defaultRevalidator) Replan(context.Context, []*Plan, RevalidateInput) ([]*Plan, error) {
	return nil, &RejectError{Reason: types.ReasonSiblingRejected}
}
