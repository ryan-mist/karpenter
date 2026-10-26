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
	"math"
	"sort"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
)

// The planners are thin adapters over the disruption methods' shared cores (Compute). They plan with the pipeline's
// options: events go to a buffer the Observer publishes, a static NodePool's node-count slots are only looked at (the
// Ledger takes them at Reserve), simulations treat reserved plans' candidates as unavailable destinations, nothing waits
// for validation (the Admitter validates), and drift and repair plan many candidates per round from one joint
// simulation.

// effects are the method cores' side effects in the pipeline.
type effects struct {
	recorder events.Recorder
	cluster  *state.Cluster
	reserved ReservationView
}

func (e effects) Publish(evts ...events.Event) { e.recorder.Publish(evts...) }

// ReserveNodeCount grants what the Ledger would grant at Reserve without taking it: the NodePool's headroom minus the
// slots reserved, unstarted plans hold.
func (e effects) ReserveNodeCount(nodePool string, limit, n int64) int64 {
	held := int64(0)
	if e.reserved != nil {
		held = e.reserved.StaticHeld(nodePool)
	}
	return min(n, max(e.cluster.NodePoolState.Headroom(nodePool, limit)-held, 0))
}

// planner is what every adapter shares.
type planner struct {
	name    string
	method  disruption.Method
	shape   types.Shape
	effects effects
	sim     disruption.Simulator
	// validateAt is how long after planning a plan may be admitted.
	validateAt time.Duration
	// nominates makes every existing node a plan places pods on (pending pods too) one of its Destinations, as the
	// disruption controller nominates them when the command starts (reference mode).
	nominates bool
	// withoutMetrics records none of the disruption controller's metrics (the shadow mode).
	withoutMetrics bool
}

// methodOf is the disruption method the planner plans with.
func (p *planner) methodOf() disruption.Method { return p.method }

func (p *planner) Name() string                { return p.name }
func (p *planner) Reason() v1.DisruptionReason { return p.method.Reason() }
func (p *planner) Class() string               { return p.method.Class() }
func (p *planner) Shape() types.Shape          { return p.shape }
func (p *planner) ConsolidationType() string   { return p.method.ConsolidationType() }

func (p *planner) Filter(ctx context.Context, c *disruption.Candidate, _ time.Time) (bool, *types.Verdict) {
	return p.method.ShouldDisrupt(ctx, c), nil
}

// budgets are the remaining budgets of the candidates' NodePools for the planner's reason.
func (p *planner) budgets(ctx context.Context, in PlanInput, cs []*disruption.Candidate) map[string]int {
	out := map[string]int{}
	for _, c := range cs {
		if _, ok := out[c.NodePool.Name]; ok {
			continue
		}
		out[c.NodePool.Name] = math.MaxInt32
		if in.Budgets != nil {
			out[c.NodePool.Name] = in.Budgets.Remaining(ctx, c.NodePool.Name, p.Reason())
		}
	}
	return out
}

// plans turns a core's commands into plans. Commands from one joint simulation share a batch.
func (p *planner) plans(cmds []disruption.Command, version uint64, in PlanInput) []*Plan {
	catalog := func(np string) string { return "" }
	if in.Snapshot != nil {
		catalog = in.Snapshot.CatalogVersion
	}
	batches := map[uuid.UUID][]*Plan{}
	plans := lo.Map(cmds, func(cmd disruption.Command, _ int) *Plan {
		cmd.Method = p.method
		plan := PlanFrom(cmd, version, in.Now, catalog)
		plan.Planner, plan.Reason = p.name, p.Reason()
		if p.validateAt > 0 {
			plan.ValidateAt = in.Now.Add(p.validateAt)
		}
		if p.nominates {
			for _, n := range cmd.Results.ExistingNodes {
				if len(n.Pods) > 0 {
					plan.Footprint.Destinations.Insert(n.ProviderID())
				}
			}
		}
		if cmd.BatchID != uuid.Nil {
			batches[cmd.BatchID] = append(batches[cmd.BatchID], plan)
		}
		return plan
	})
	for _, batch := range batches {
		for _, plan := range batch {
			plan.BatchID, plan.Siblings = batch[0].ID, len(batch)
		}
	}
	return plans
}

// accept starts the plan as planned on its rebuilt candidates, which keep what the planner stamped on the planned
// ones (repair's drain bound).
func accept(plan *Plan, fresh []*disruption.Candidate) []*Plan {
	planned := lo.SliceToMap(plan.Candidates, func(c *disruption.Candidate) (string, *disruption.Candidate) { return c.ProviderID(), c })
	for _, c := range fresh {
		if old, ok := planned[c.ProviderID()]; ok {
			c.TerminationGracePeriod, c.RebootEscalated = old.TerminationGracePeriod, old.RebootEscalated
		}
	}
	plan.Candidates = fresh
	return []*Plan{plan}
}

// firstRejection is the reason of the plan's first rejected candidate, if any.
func firstRejection(in RevalidateInput) error {
	for _, c := range in.Plan.Candidates {
		if reason, ok := in.Rejected[c.ProviderID()]; ok {
			return &RejectError{Reason: reason}
		}
	}
	return nil
}

func (p *planner) AlwaysResimulate() bool { return false }

func (p *planner) Replan(context.Context, []*Plan, RevalidateInput) ([]*Plan, error) {
	return nil, &RejectError{Reason: types.ReasonSiblingRejected}
}

// validated re-validates a consolidation plan with the method's own validator, as the disruption controller does after
// the validation delay, and returns the plan the validator accepts.
func validated(ctx context.Context, v disruption.Validator, in RevalidateInput) ([]*Plan, error) {
	cmd := in.Plan.ToCommand(in.Now)
	cmd.Candidates = in.Fresh
	out, err := v.Validate(ctx, cmd, 0)
	if err != nil {
		if disruption.IsValidationError(err) {
			return nil, &RejectError{Reason: types.ReasonResimChanged, Err: err}
		}
		return nil, err
	}
	plan := in.Plan
	plan.Candidates, plan.Replacements, plan.Results = out.Candidates, out.Replacements, out.Results
	plan.Footprint = FootprintOf(out, plan.Footprint.ComputedAt)
	return []*Plan{plan}, nil
}

// emptinessPlanner deletes every empty candidate the budgets allow, in one plan that is admitted after the validation
// delay, without waiting in the lane. Pod-free plans may be admitted while cluster state is unsynced.
type emptinessPlanner struct {
	planner
	core  *disruption.Emptiness
	dedup disruption.Dedup
}

func (p *emptinessPlanner) Plan(ctx context.Context, in PlanInput) (PlanOutput, error) {
	version := in.Reserved.Version()
	res, err := p.core.Compute(ctx, p.budgets(ctx, in, in.Candidates), disruption.CoreOptions{Effects: p.effects, Dedup: p.dedup}, in.Candidates...)
	return PlanOutput{Plans: p.plans(res.Commands, version, in)}, err
}

// Revalidate keeps the candidates that are still empty and valid, as the emptiness validator does. The candidates
// admission already rejected count as failed validations as the emptiness validator counts them: all of them if none
// is left, else the nominated ones (the validator counts those and the over-budget ones it drops itself).
func (p *emptinessPlanner) Revalidate(ctx context.Context, in RevalidateInput) ([]*Plan, error) {
	if len(in.Fresh) == 0 {
		failedValidations(in.Plan, len(in.Plan.Candidates))
		return nil, firstRejection(in)
	}
	failedValidations(in.Plan, lo.CountBy(in.Plan.Candidates, func(c *disruption.Candidate) bool {
		return in.Rejected[c.ProviderID()] == types.ReasonCandidateNominated
	}))
	return validated(ctx, p.core.Validator(), in)
}

// failedValidations adds n to the consolidation type's failed validations, as the method's validator would have.
func failedValidations(p *Plan, n int) {
	if n > 0 && p.Method != nil && p.Method.ConsolidationType() != "" {
		disruption.FailedValidationsTotal.Add(float64(n), map[string]string{disruption.ConsolidationTypeLabel: p.Method.ConsolidationType()})
	}
}

// staticDriftPlanner replaces drifted nodes of static NodePools one per plan; the Ledger takes the node-count slots.
type staticDriftPlanner struct {
	planner
	core *disruption.StaticDrift
}

func (p *staticDriftPlanner) Plan(ctx context.Context, in PlanInput) (PlanOutput, error) {
	version := in.Reserved.Version()
	res, err := p.core.Compute(ctx, p.budgets(ctx, in, in.Candidates), disruption.CoreOptions{Effects: p.effects}, in.Candidates...)
	return PlanOutput{Plans: p.plans(res.Commands, version, in)}, err
}

func (p *staticDriftPlanner) Revalidate(_ context.Context, in RevalidateInput) ([]*Plan, error) {
	if err := firstRejection(in); err != nil {
		return nil, err
	}
	return accept(in.Plan, in.Fresh), nil
}

// batchPlanner is drift and repair: many candidates from one joint simulation, sized to a deadline, split into plans of
// at most disruption.ComponentCap candidates. At admission a plan is re-simulated when a cause says so, and adopts the
// re-simulation's replacements if every candidate's pods still fit.
type batchPlanner struct {
	planner
	compute func(ctx context.Context, budgets map[string]int, opts disruption.CoreOptions, cs ...*disruption.Candidate) (disruption.CoreResult, error)
	sizer   *disruption.BatchSizer
	// cachedReboot plans reboots from the cached NodeClaim; the Starter re-reads it.
	cachedReboot bool
	// budgetFree is repair's: an uninitialized node is repaired without budget, so the planner plans at zero budget.
	budgetFree bool
	// fair, if set, orders candidates round-robin across NodePools and backs off NodePools whose commands failed
	// (drift). Without it the core's own order applies (repair's score order).
	fair *poolFairness
	// reference plans one candidate at a time and keeps pending-only NodeClaims on the first command, as the
	// disruption controller does (PlannerOptions.Reference).
	reference bool
}

func (p *batchPlanner) BudgetFree() bool { return p.budgetFree }

func (p *batchPlanner) options(batch int) disruption.CoreOptions {
	opts := disruption.CoreOptions{
		Batch:        max(batch, 1),
		Sizer:        p.sizer,
		Deadline:     disruption.BatchSimDeadline,
		Effects:      p.effects,
		Sim:          p.sim,
		Pending:      disruption.DropPendingOnly,
		CachedReboot: p.cachedReboot,
	}
	if p.reference {
		opts.Batch, opts.Sizer, opts.Deadline, opts.Pending = 1, nil, 0, disruption.KeepOnFirst
	}
	if p.fair != nil {
		opts.Order = p.fair.order(byDisruptionTime(p.Reason()))
	}
	return opts
}

func (p *batchPlanner) Plan(ctx context.Context, in PlanInput) (PlanOutput, error) {
	version := in.Reserved.Version()
	candidates := in.Candidates
	if p.fair != nil {
		candidates = p.fair.eligible(candidates)
	}
	res, err := p.compute(ctx, p.budgets(ctx, in, candidates), p.options(len(candidates)), candidates...)
	out := PlanOutput{Plans: p.plans(res.Commands, version, in)}
	for _, id := range res.Deferred {
		out.Verdicts = append(out.Verdicts, types.Verdict{Planner: p.name, ProviderID: id, Reason: types.ReasonDeferred})
	}
	return out, err
}

func (p *batchPlanner) Revalidate(ctx context.Context, in RevalidateInput) ([]*Plan, error) {
	if err := firstRejection(in); err != nil {
		return nil, err
	}
	if len(in.Causes) == 0 || in.Plan.Action == types.ActionReboot || in.Plan.TerminateFirst != "" {
		return accept(in.Plan, in.Fresh), nil
	}
	return p.resimulate(ctx, in, in.Fresh)
}

// Resolved records that a NodePool was served when one of its plans started, and backs its NodePools off when the
// start failed (a replacement NodeClaim was rejected, say): the command never reached the queue, so its completion
// hook won't.
func (p *batchPlanner) Resolved(plan *Plan, r Resolution) {
	if p.fair == nil {
		return
	}
	switch {
	case r.Kind == Started:
		p.fair.started(nodePoolsOf(plan.Candidates)...)
	case r.Reason == types.ReasonStartFailed:
		p.fair.finished(false, nodePoolsOf(plan.Candidates)...)
	}
}

// finished backs off the NodePools of a failed command, and resets them when one succeeds.
func (p *batchPlanner) finished(cmd *disruption.Command) {
	if p.fair != nil && cmd.Method == p.method {
		p.fair.finished(cmd.Succeeded, nodePoolsOf(cmd.Candidates)...)
	}
}

// Replan plans the surviving siblings of a batch again, jointly.
func (p *batchPlanner) Replan(ctx context.Context, survivors []*Plan, in RevalidateInput) ([]*Plan, error) {
	return p.resimulate(ctx, in, lo.FlatMap(survivors, func(s *Plan, _ int) []*disruption.Candidate { return s.Candidates }))
}

// resimulate plans the candidates again now, against started state. It fails unless every candidate is planned.
func (p *batchPlanner) resimulate(ctx context.Context, in RevalidateInput, cs []*disruption.Candidate) ([]*Plan, error) {
	version := in.Reserved.Version()
	budgets := lo.CountValuesBy(cs, func(c *disruption.Candidate) string { return c.NodePool.Name })
	res, err := p.compute(ctx, budgets, p.options(len(cs)), cs...)
	if err != nil {
		return nil, err
	}
	planned := lo.FlatMap(res.Commands, func(cmd disruption.Command, _ int) []*disruption.Candidate { return cmd.Candidates })
	if len(planned) != len(cs) {
		return nil, reject(types.ReasonResimUnschedulable, "%d of %d candidates can be disrupted now", len(planned), len(cs))
	}
	plans := p.plans(res.Commands, version, PlanInput{Now: in.Now})
	if len(plans) == 1 && len(res.Commands) == 1 {
		// One plan replaces one plan: keep its identity, so the Ledger amends it.
		plans[0].ID, plans[0].BatchID, plans[0].Siblings = in.Plan.ID, in.Plan.BatchID, in.Plan.Siblings
		plans[0].CatalogVersions, plans[0].ComputedAt = in.Plan.CatalogVersions, in.Now
	}
	return plans, nil
}

// byDisruptionTime orders candidates by when their reason's condition became true, then by providerID, so nodes of a
// herd with identical times are ordered deterministically.
func byDisruptionTime(reason v1.DisruptionReason) func([]*disruption.Candidate) {
	return func(cs []*disruption.Candidate) {
		sort.SliceStable(cs, func(i, j int) bool {
			ti := cs[i].NodeClaim.StatusConditions().Get(string(reason)).LastTransitionTime.Time
			tj := cs[j].NodeClaim.StatusConditions().Get(string(reason)).LastTransitionTime.Time
			if !ti.Equal(tj) {
				return ti.Before(tj)
			}
			return cs[i].ProviderID() < cs[j].ProviderID()
		})
	}
}

// consolidationPlanner is multi- or single-node consolidation: one plan per round, scored with the round's evaluator,
// admitted after the validation delay and always re-validated by the method's validator (a re-simulation).
type consolidationPlanner struct {
	planner
	compute   func(ctx context.Context, budgets map[string]int, opts disruption.CoreOptions, cs ...*disruption.Candidate) (disruption.CoreResult, error)
	validator disruption.Validator
	dedup     disruption.Dedup
}

func (p *consolidationPlanner) NeedsTotals() bool      { return true }
func (p *consolidationPlanner) AlwaysResimulate() bool { return true }

func (p *consolidationPlanner) Plan(ctx context.Context, in PlanInput) (PlanOutput, error) {
	version := in.Reserved.Version()
	res, err := p.compute(ctx, p.budgets(ctx, in, in.Candidates), disruption.CoreOptions{Effects: p.effects, Sim: p.sim, Evaluator: in.Evaluator, Dedup: p.dedup, WithoutMetrics: p.withoutMetrics}, in.Candidates...)
	return PlanOutput{Plans: p.plans(res.Commands, version, in)}, err
}

// Revalidate is all or nothing, as consolidation's validator is; a candidate admission rejected fails the validation of
// all of them, as the validator counts it.
func (p *consolidationPlanner) Revalidate(ctx context.Context, in RevalidateInput) ([]*Plan, error) {
	if err := firstRejection(in); err != nil {
		failedValidations(in.Plan, len(in.Plan.Candidates))
		return nil, err
	}
	return validated(ctx, p.validator, in)
}

// PlannerOptions configure DefaultPlanners.
type PlannerOptions struct {
	// Reference plans as the disruption controller does, for the batch=1 differential (G9) and the shadow mode's
	// reference level: drift and repair plan one candidate at a time without a deadline, keep pending-only NodeClaims
	// on the first command (R71 off), and walk candidates in the core's own order without NodePool round-robin or
	// backoff (R73 off); simulations see the candidates of reserved, unstarted plans as deleting, as a pass of the
	// disruption controller sees the commands earlier methods started (decision #9 off).
	Reference bool
	// shadow makes the consolidation planners search every time, without skipping an unchanged cluster state (the
	// shadow mode plans only on sampled passes, so it can't tell what changed), and record none of the disruption
	// controller's metrics.
	shadow bool
}

// WithReference makes DefaultPlanners plan as the disruption controller does (PlannerOptions.Reference).
func WithReference() option.Function[PlannerOptions] {
	return func(o *PlannerOptions) { o.Reference = true }
}

// dedupFor is a consolidation planner's dedup: nil without dedup.
func dedupFor(o *PlannerOptions, cluster *state.Cluster) disruption.Dedup {
	if o.shadow {
		return nil
	}
	return &stateDedup{cluster: cluster}
}

// DefaultPlanners are the pipeline's planners over the disruption methods (disruption.NewMethods), built with the
// pipeline's effects and simulator. recorder receives what the methods publish; the Observer publishes it.
func DefaultPlanners(ctx context.Context, clk clock.Clock, cluster *state.Cluster, kubeClient client.Client, provisioner *provisioning.Provisioner,
	cp cloudprovider.CloudProvider, queue *disruption.Queue, recorder events.Recorder, reserved ReservationView, opts ...option.Function[PlannerOptions],
) map[string]Planner {
	o := option.Resolve(opts...)
	eff := effects{recorder: recorder, cluster: cluster, reserved: reserved}
	sim := lo.Ternary(o.Reference, newReferenceSimulator, NewSimulator)(disruption.NewLegacySimulator(kubeClient, cluster, provisioner, clk, recorder), reserved)
	base := func(name string, m disruption.Method, shape types.Shape) planner {
		return planner{name: name, method: m, shape: shape, effects: eff, sim: sim, nominates: o.Reference, withoutMetrics: o.shadow}
	}
	out := map[string]Planner{}
	for _, m := range disruption.NewMethods(ctx, clk, cluster, kubeClient, provisioner, cp, recorder, queue) {
		switch m := m.(type) {
		case *disruption.Repair:
			out[RepairPlanner] = &batchPlanner{planner: base(RepairPlanner, m, types.ShapeReplacing), compute: m.Compute,
				sizer: disruption.NewBatchSizer(disruption.BatchSimDeadline), cachedReboot: true, budgetFree: true, reference: o.Reference}
		case *disruption.Emptiness:
			p := &emptinessPlanner{planner: base(EmptinessPlanner, m, types.ShapePodFree), core: m, dedup: dedupFor(o, cluster)}
			p.validateAt = disruption.CommandValidationDelay
			out[EmptinessPlanner] = p
		case *disruption.StaticDrift:
			out[StaticDriftPlanner] = &staticDriftPlanner{planner: base(StaticDriftPlanner, m, types.ShapeReplacing), core: m}
		case *disruption.Drift:
			p := &batchPlanner{planner: base(DriftPlanner, m, types.ShapeReplacing), compute: m.Compute,
				sizer: disruption.NewBatchSizer(disruption.BatchSimDeadline), reference: o.Reference}
			if !o.Reference {
				p.fair = newPoolFairness(clk)
				queue.WithCompletionHook(p.finished)
			}
			out[DriftPlanner] = p
		case *disruption.MultiNodeConsolidation:
			p := &consolidationPlanner{planner: base(MultiNodePlanner, m, types.ShapeReplacing), compute: m.Compute, validator: m.Validator(), dedup: dedupFor(o, cluster)}
			p.validateAt = disruption.CommandValidationDelay
			out[MultiNodePlanner] = p
		case *disruption.SingleNodeConsolidation:
			p := &consolidationPlanner{planner: base(SingleNodePlanner, m, types.ShapeReplacing), compute: m.Compute, validator: m.Validator(), dedup: dedupFor(o, cluster)}
			p.validateAt = disruption.CommandValidationDelay
			out[SingleNodePlanner] = p
		}
	}
	return out
}
