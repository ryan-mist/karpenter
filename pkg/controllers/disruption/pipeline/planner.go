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

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
)

// Planner is a disruption method in the pipeline: a thin adapter over the method's shared core. It is pure: it writes
// nothing to the API server, marks nothing in cluster state and publishes nothing. Its events and rejections go to the
// round's buffer (Snapshot.Events) and its plans to the Executor.
type Planner interface {
	// Name is the planner's metric label value, e.g. "drift".
	Name() string
	// Reason is the disruption reason whose budget the planner's plans charge.
	Reason() v1.DisruptionReason
	// Class is the disruption class whose candidate rules apply (disruption.GracefulDisruptionClass, ...).
	Class() string
	// Shape is the most invasive plan the planner emits. While cluster state is unsynced, a lane gives planners with a
	// Shape other than ShapePodFree only pod-free candidates.
	Shape() types.Shape
	// Filter reports whether a candidate is eligible, as the method's ShouldDisrupt. It may write to the Candidate,
	// which the planner owns. A rejection that should be explained returns a Verdict.
	Filter(ctx context.Context, c *disruption.Candidate, now time.Time) (bool, *types.Verdict)
	// Plan proposes plans for the eligible candidates.
	Plan(ctx context.Context, in PlanInput) (PlanOutput, error)
}

// TotalsConsumer is implemented by planners that score with NodePool totals (balanced consolidation). The lane builds
// their PlanInput.Evaluator from Snapshot.Totals.
type TotalsConsumer interface {
	NeedsTotals() bool
}

// BudgetFree is implemented by planners that can plan disruptions no budget counts, so they plan even when no NodePool
// has budget left for their reason. Repair is one: it repairs nodes that never initialized, which count against no
// budget, as the disruption controller does.
type BudgetFree interface {
	BudgetFree() bool
}

// PlanInput is everything a planner may read. The Candidates are the planner's own; everything else is shared and
// read-only.
type PlanInput struct {
	// Candidates are the eligible candidates, from Snapshot.CandidatesFor.
	Candidates []*disruption.Candidate
	// Budgets is the live budget view, net of reservations. It is a hint: the Ledger is the authority.
	Budgets BudgetView
	// Reserved is the live view of the plans that are reserved but not started, in every lane.
	Reserved ReservationView
	// Evaluator scores consolidation moves. It is built from Snapshot.Totals for a TotalsConsumer.
	Evaluator disruption.Evaluator
	// Snapshot is the round's snapshot.
	Snapshot *Snapshot
	// Now is the round's fixed now (Snapshot.BuiltAt).
	Now time.Time
	// Synced is whether cluster state was synced when the round started.
	Synced bool
}

// PlanOutput is what a planner returns for one round.
type PlanOutput struct {
	Plans    []*Plan
	Verdicts []types.Verdict
	// Next is the earliest time something in the planner's slice becomes eligible; zero if none is known.
	Next time.Time
}

// Plan is a proposed command. Nothing has happened when it exists. From Submit on it is shared by pointer, because the
// Admitter may rewrite it (a drift plan adopts its admission re-simulation).
type Plan struct {
	ID uuid.UUID
	// BatchID is shared by the plans split from one joint simulation; it equals ID for a plan on its own.
	BatchID uuid.UUID
	// Siblings is the number of plans with this BatchID.
	Siblings int

	Planner string
	// Method is the shared core the plan came from, for disruption.Command.Method.
	Method disruption.Method
	// Reason is the disruption reason whose budget the plan charges.
	Reason v1.DisruptionReason
	Action types.Action

	TerminateFirst      disruption.TerminateFirstReason
	Candidates          []*disruption.Candidate
	Replacements        []*disruption.Replacement
	Results             pscheduling.Results
	PoolDisruptionCosts map[string]float64

	Footprint types.Footprint
	// PodSetHash maps each candidate's providerID to a hash of its reschedulable pods when the plan was computed.
	PodSetHash map[string]uint64
	// CatalogVersions maps each NodePool the plan touches to its catalog version when the plan was computed.
	CatalogVersions map[string]string

	// ValidateAt is when the plan may be admitted: the round's now plus the consolidation validation period for
	// emptiness and consolidation, zero otherwise.
	ValidateAt time.Time
	// ComputedAt is the wall time the plan was computed at.
	ComputedAt time.Time
}

// PodFree reports whether starting the plan moves no pods and launches nothing: no replacements, not a reboot, not
// static drift, and no candidate has a reschedulable pod. Such a plan doesn't depend on capacity that cluster state may
// not show yet.
func (p *Plan) PodFree() bool {
	return p.PodFreeOn(p.Candidates)
}

// PodFreeOn is PodFree with the plan's candidates replaced by cs (for example rebuilt from live state). Static drift is
// never pod-free, not even a terminate-first delete of an empty node: its NodePool's static provisioning launches the
// node back (RFC C.4).
func (p *Plan) PodFreeOn(cs []*disruption.Candidate) bool {
	if p.Action == types.ActionReboot || len(p.Replacements) > 0 || p.TerminateFirst == disruption.TerminateFirstStaticAtLimit ||
		len(p.Footprint.StaticNodeCount) > 0 {
		return false
	}
	if _, ok := p.Method.(*disruption.StaticDrift); ok {
		return false
	}
	return lo.EveryBy(cs, func(c *disruption.Candidate) bool { return len(c.ReschedulablePods()) == 0 })
}

// ToCommand is the disruption.Command that starts the plan. Its CreationTimestamp is now, so the time a plan waited for
// admission doesn't count against the queue's retry window.
func (p *Plan) ToCommand(now time.Time) disruption.Command {
	return disruption.Command{
		Method:               p.Method,
		CreationTimestamp:    now,
		ID:                   p.ID,
		Results:              p.Results,
		Candidates:           p.Candidates,
		Replacements:         p.Replacements,
		PoolDisruptionCosts:  p.PoolDisruptionCosts,
		TerminateFirstReason: p.TerminateFirst,
		Reboot:               p.Action == types.ActionReboot,
	}
}
