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

package disruption

import (
	"context"
	"errors"
	"slices"
	"sort"

	"github.com/samber/lo"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/utils/pretty"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

// Drift is a subreconciler that deletes drifted candidates.
type Drift struct {
	kubeClient  client.Client
	cluster     *state.Cluster
	provisioner *provisioning.Provisioner
	recorder    events.Recorder
	clock       clock.Clock
}

func NewDrift(kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner, recorder events.Recorder, clk clock.Clock) *Drift {
	return &Drift{
		kubeClient:  kubeClient,
		cluster:     cluster,
		provisioner: provisioner,
		recorder:    recorder,
		clock:       clk,
	}
}

// ShouldDisrupt is a predicate used to filter candidates
func (d *Drift) ShouldDisrupt(ctx context.Context, c *Candidate) bool {
	return !c.OwnedByStaticNodePool() && c.NodeClaim.StatusConditions().Get(string(d.Reason())).IsTrue()
}

// ComputeCommands generates a disruption command given candidates: the drift core with the controller's options.
func (d *Drift) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	res, err := d.Compute(ctx, disruptionBudgetMapping, d.LegacyOptions(), candidates...)
	return orEmpty(res.Commands), err
}

// LegacyOptions are the options the disruption controller computes drift with: one candidate per pass, simulated
// and reported exactly as before the drift core existed.
func (d *Drift) LegacyOptions() CoreOptions {
	return CoreOptions{
		Batch:   1,
		Effects: InlineEffects{Recorder: d.recorder},
		Sim:     NewLegacySimulator(d.kubeClient, d.cluster, d.provisioner, d.clock, d.recorder),
		Pending: KeepOnFirst,
	}
}

// Compute is the drift core. It orders the candidates (opts.Order, or by Drifted transition time), empty candidates
// first, and walks them one at a time, returning the first command. With opts.Batch above 1, the candidates that can
// be planned jointly are planned from one joint simulation instead (planBatch), in order: candidates that only the
// single-candidate path plans (terminate-first) are planned first when they rank ahead of the batch, and end the
// batch where they appear, so a batch never jumps ahead of a candidate it leaves out. When the batch plans nothing,
// the walk resumes at the batch's first candidate. Batched commands decrement the budgets of their candidates; a
// single command doesn't, as before.
func (d *Drift) Compute(ctx context.Context, disruptionBudgetMapping map[string]int, opts CoreOptions, candidates ...*Candidate) (CoreResult, error) {
	if opts.Order != nil {
		opts.Order(candidates)
	} else {
		sort.Slice(candidates, func(i int, j int) bool {
			return candidates[i].NodeClaim.StatusConditions().Get(string(d.Reason())).LastTransitionTime.Time.Before(
				candidates[j].NodeClaim.StatusConditions().Get(string(d.Reason())).LastTransitionTime.Time)
		})
	}

	emptyCandidates, nonEmptyCandidates := lo.FilterReject(candidates, func(c *Candidate, _ int) bool {
		return len(c.reschedulablePods) == 0
	})
	// Prioritize empty candidates since we want them to get priority over non-empty candidates if the budget is constrained.
	// Disrupting empty candidates first also helps reduce the overall churn because if a non-empty candidate is disrupted first,
	// the pods from that node can reschedule on the empty nodes and will need to move again when those nodes get disrupted.
	ordered := slices.Concat(emptyCandidates, nonEmptyCandidates)

	var res CoreResult
	if opts.Batch > 1 || opts.batchMinSize() == 1 {
		// Reserved candidates may need terminate-first, which is a per-candidate decision.
		terminateFirst := options.FromContext(ctx).FeatureGates.TerminateFirstDrift
		first, window := batchWindow(ordered, disruptionBudgetMapping, nil, func(c *Candidate) bool {
			return terminateFirst && c.capacityType == v1.CapacityTypeReserved
		}, func(*Candidate) bool { return true })
		if cmd, err := d.computeSingle(ctx, disruptionBudgetMapping, opts, ordered[:first]); err != nil || cmd != nil {
			return lo.Ternary(cmd != nil, CoreResult{Commands: []Command{lo.FromPtr(cmd)}}, CoreResult{}), err
		}
		var err error
		if res, err = planBatch(ctx, disruptionBudgetMapping, opts, SimOptions{}, window, func(*Candidate) bool { return true }); err != nil || len(res.Commands) > 0 {
			return res, err
		}
		ordered = ordered[first:]
	}
	cmd, err := d.computeSingle(ctx, disruptionBudgetMapping, opts, ordered)
	if err != nil {
		return res, err
	}
	if cmd != nil {
		res.Commands = []Command{*cmd}
		res.Deferred = nil
	}
	return res, nil
}

// computeSingle returns a command for the first candidate, in order, whose pods can be rescheduled.
func (d *Drift) computeSingle(ctx context.Context, disruptionBudgetMapping map[string]int, opts CoreOptions, ordered []*Candidate) (*Command, error) {
	for _, candidate := range ordered {
		// If the disruption budget doesn't allow this candidate to be disrupted,
		// continue to the next candidate. We don't need to decrement any budget
		// counter since drift commands can only have one candidate.
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			continue
		}
		// Simulate rescheduling the candidate's pods. When they can't be replaced-first and the candidate holds a full
		// reservation, this reports terminate-first (RFC #3203): delete the candidate and let reactive provisioning
		// refill the freed slot. See SimulateSchedulingWithReservedFallback.
		out, err := opts.Sim.Simulate(ctx, SimOptions{ReservedFallback: true, TerminateFirst: options.FromContext(ctx).FeatureGates.TerminateFirstDrift}, candidate)
		if err != nil {
			// if a candidate is now deleting, just retry
			if errors.Is(err, errCandidateDeleting) {
				continue
			}
			return nil, err
		}
		results := out.Results
		if out.TerminateFirst {
			// Delete-only (no Replacements): carry the Results so existing nodes that can absorb the freed pods get
			// nominated. Reactive provisioning handles the rest.
			return &Command{
				Candidates:           []*Candidate{candidate},
				Results:              results,
				PoolDisruptionCosts:  computePoolDisruptionCosts([]*Candidate{candidate}),
				TerminateFirstReason: TerminateFirstNoReservedCapacity,
			}, nil
		}
		// Emit an event that we couldn't reschedule the pods on the node.
		if !results.AllNonPendingPodsScheduled() {
			opts.Effects.Publish(disruptionevents.Blocked(candidate.Node, candidate.NodeClaim, pretty.Sentence(results.NonPendingPodSchedulingErrors()))...)
			continue
		}
		if opts.Pending == DropPendingOnly {
			results = Split([]*Candidate{candidate}, results, DropPendingOnly)[0].Results
		}
		return &Command{
			Candidates:          []*Candidate{candidate},
			Replacements:        replacementsFromNodeClaims(results.NewNodeClaims...),
			Results:             results,
			PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{candidate}),
		}, nil
	}
	return nil, nil
}

func (d *Drift) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonDrifted
}

func (d *Drift) Class() string {
	return EventualDisruptionClass
}

func (d *Drift) ConsolidationType() string {
	return ""
}
