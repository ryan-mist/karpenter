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

// Test-only hooks for the method-core tests (core_differential_test.go, drift_core_test.go).

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// SetBatchMinSize sets the smallest batch planned jointly (production: BatchMinSize).
func SetBatchMinSize(o *CoreOptions, n int) { o.minBatch = n }

// NewBareCandidate returns a Candidate of NodePool np with the given providerID, 8 CPUs of capacity and reschedulable
// pods, for tests of code that only reads those.
func NewBareCandidate(providerID string, np *v1.NodePool, pods ...*corev1.Pod) *Candidate {
	sn := state.NewNode()
	sn.NodeClaim = &v1.NodeClaim{Status: v1.NodeClaimStatus{ProviderID: providerID, Capacity: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")}}}
	return &Candidate{StateNode: sn, NodePool: np, reschedulablePods: pods}
}

// TopologyCoupled exposes topologyCoupled.
func TopologyCoupled(a, b []*corev1.Pod) bool { return topologyCoupled(a, b) }

// ReschedulablePodCount returns how many reschedulable pods a candidate carries.
func ReschedulablePodCount(c *Candidate) int { return len(c.reschedulablePods) }

// LegacyDriftComputeCommands is a verbatim copy of Drift.ComputeCommands at the parent of the drift core
// (disr/99-integration, unchanged from upstream 6eac93ba). It is the reference of the batch=1 differential test.
func LegacyDriftComputeCommands(ctx context.Context, d *Drift, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	sort.Slice(candidates, func(i int, j int) bool {
		return candidates[i].NodeClaim.StatusConditions().Get(string(d.Reason())).LastTransitionTime.Time.Before(
			candidates[j].NodeClaim.StatusConditions().Get(string(d.Reason())).LastTransitionTime.Time)
	})

	emptyCandidates, nonEmptyCandidates := lo.FilterReject(candidates, func(c *Candidate, _ int) bool {
		return len(c.reschedulablePods) == 0
	})

	// Prioritize empty candidates since we want them to get priority over non-empty candidates if the budget is constrained.
	// Disrupting empty candidates first also helps reduce the overall churn because if a non-empty candidate is disrupted first,
	// the pods from that node can reschedule on the empty nodes and will need to move again when those nodes get disrupted.
	for _, candidate := range slices.Concat(emptyCandidates, nonEmptyCandidates) {
		// If the disruption budget doesn't allow this candidate to be disrupted,
		// continue to the next candidate. We don't need to decrement any budget
		// counter since drift commands can only have one candidate.
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			continue
		}
		// Simulate rescheduling the candidate's pods. When they can't be replaced-first and the candidate holds a full
		// reservation, this reports terminate-first (RFC #3203): delete the candidate and let reactive provisioning
		// refill the freed slot. See SimulateSchedulingWithReservedFallback.
		results, terminateFirst, err := SimulateSchedulingWithReservedFallback(ctx, d.kubeClient, d.cluster, d.provisioner, d.clock, d.recorder, candidate, options.FromContext(ctx).FeatureGates.TerminateFirstDrift, SimulationOptions{})
		if err != nil {
			// if a candidate is now deleting, just retry
			if errors.Is(err, errCandidateDeleting) {
				continue
			}
			return []Command{}, err
		}
		if terminateFirst {
			// Delete-only (no Replacements): carry the Results so existing nodes that can absorb the freed pods get
			// nominated. Reactive provisioning handles the rest.
			return []Command{{
				Candidates:           []*Candidate{candidate},
				Results:              results,
				PoolDisruptionCosts:  computePoolDisruptionCosts([]*Candidate{candidate}),
				TerminateFirstReason: TerminateFirstNoReservedCapacity,
			}}, nil
		}
		// Emit an event that we couldn't reschedule the pods on the node.
		if !results.AllNonPendingPodsScheduled() {
			d.recorder.Publish(disruptionevents.Blocked(candidate.Node, candidate.NodeClaim, pretty.Sentence(results.NonPendingPodSchedulingErrors()))...)
			continue
		}

		cmd := Command{
			Candidates:          []*Candidate{candidate},
			Replacements:        replacementsFromNodeClaims(results.NewNodeClaims...),
			Results:             results,
			PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{candidate}),
		}
		return []Command{cmd}, nil

	}
	return []Command{}, nil
}

// BatchWindow exposes batchWindow.
func BatchWindow(ordered []*Candidate, budgets map[string]int, skip, single, charge func(*Candidate) bool) (int, []*Candidate) {
	return batchWindow(ordered, budgets, skip, single, charge)
}

// ResetConsolidated forgets that the consolidation method m last found nothing to do.
func ResetConsolidated(m Method) {
	switch t := m.(type) {
	case *Emptiness:
		t.lastConsolidationState = time.Time{}
	case *MultiNodeConsolidation:
		t.lastConsolidationState = time.Time{}
	case *SingleNodeConsolidation:
		t.lastConsolidationState = time.Time{}
	}
}

// MarkedConsolidated returns the consolidation state the method m last found nothing to do in (zero: never).
func MarkedConsolidated(m Method) time.Time {
	switch t := m.(type) {
	case *Emptiness:
		return t.lastConsolidationState
	case *MultiNodeConsolidation:
		return t.lastConsolidationState
	case *SingleNodeConsolidation:
		return t.lastConsolidationState
	}
	return time.Time{}
}

// LegacyEmptinessComputeCommands is a verbatim copy of Emptiness.ComputeCommands at the parent of the emptiness core
// (disr/a01-drift-core; emptiness.go as on disr/99-integration). It is the reference of the emptiness differential.
//
//nolint:gocyclo
func LegacyEmptinessComputeCommands(ctx context.Context, e *Emptiness, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	if e.IsConsolidated() {
		return []Command{}, nil
	}
	candidates = e.sortCandidates(ctx, candidates)

	empty := make([]*Candidate, 0, len(candidates))
	constrainedByBudgets := false
	for _, candidate := range candidates {
		if !candidate.IsEmpty() {
			continue
		}
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			// set constrainedByBudgets to true if any node was a candidate but was constrained by a budget
			constrainedByBudgets = true
			continue
		}
		// If there's disruptions allowed for the candidate's nodepool,
		// add it to the list of candidates, and decrement the budget.
		empty = append(empty, candidate)
		disruptionBudgetMapping[candidate.NodePool.Name]--
	}
	// none empty, so do nothing
	if len(empty) == 0 {
		// if there are no candidates, but a nodepool had a fully blocking budget,
		// don't mark the cluster as consolidated, as it's possible this nodepool
		// should be consolidated the next time we try to disrupt.
		if !constrainedByBudgets {
			e.markConsolidated()
		}
		return []Command{}, nil
	}

	cmd := Command{
		Candidates:          empty,
		PoolDisruptionCosts: computePoolDisruptionCosts(empty),
	}
	validCmd, err := validate(ctx, e.validator, cmd, commandValidationDelay)
	if err != nil {
		if IsValidationError(err) {
			log.FromContext(ctx).V(1).WithValues(cmd.LogValues()...).Info("abandoning empty node consolidation attempt due to pod churn, command is no longer valid")
			return []Command{}, nil
		}
		return []Command{}, err
	}
	return []Command{validCmd}, nil
}

// LegacyStaticDriftComputeCommands and legacyUnreplaceableCommands are verbatim copies of StaticDrift.ComputeCommands
// and StaticDrift.unreplaceableCommands at the parent of the static drift core (disr/a02-emptiness-core; staticdrift.go
// as on disr/99-integration), with the receiver made a parameter. They are the reference of the static drift
// differential.
func LegacyStaticDriftComputeCommands(ctx context.Context, d *StaticDrift, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	// Group candidates by nodepool name
	candidatesByNodePool := lo.GroupBy(candidates, func(candidate *Candidate) string {
		return candidate.NodePool.Name
	})

	var cmds []Command
	for npName, npCandidates := range candidatesByNodePool {
		np := npCandidates[0].NodePool

		if disruptionBudgetMapping[npName] == 0 {
			continue
		}

		limit, ok := np.Spec.Limits[resources.Node]
		nodeLimit := lo.Ternary(ok, limit.Value(), int64(math.MaxInt64))
		// Current nodes (includes in‑flight per your cluster state)
		runningNodes, _, nodesPendingDisruptionCount := d.cluster.NodePoolState.GetNodeCount(npName)

		// We dont want to disrupt nodes until scale down is complete
		if int64(runningNodes+nodesPendingDisruptionCount) > lo.FromPtr(np.Spec.Replicas) {
			continue
		}

		maxDrifts := lo.Min([]int64{
			int64(disruptionBudgetMapping[np.Name]),
			int64(len(npCandidates)),
		})

		// Full reservations are checked before reserving limits.nodes so the delete-only path never takes a slot. Checked
		// only with terminate-first enabled, since without it there's nothing to do but replace first.
		nct := scheduling.NewNodeClaimTemplate(np)
		if options.FromContext(ctx).FeatureGates.TerminateFirstDrift {
			reservationsFull, offered, err := staticReservations(ctx, d.cloudprovider, np, nct)
			// Other NodePools don't depend on this one, and returning here would discard their commands while keeping the
			// limits.nodes slots they reserved.
			if err != nil {
				log.FromContext(ctx).Error(err, "skipping static nodepool, failed checking capacity reservations")
				continue
			}
			if reservationsFull {
				// Only a candidate holding a slot in a still-offered reservation frees a slot the refill can use, so it goes
				// first and is the only one that can terminate first; the rest can't be replaced either way.
				holdsSlot := func(c *Candidate) bool { return holdsOfferedReservation(c, offered) }
				eligible, rest := lo.FilterReject(npCandidates, func(c *Candidate, _ int) bool { return holdsSlot(c) })
				cmds = append(cmds, legacyUnreplaceableCommands(ctx, d, np, append(eligible, rest...)[:maxDrifts], holdsSlot, TerminateFirstNoReservedCapacity,
					"static NodePool's capacity reservations are full and cannot stage a replacement")...)
				continue
			}
		}

		// Acquire limits from cluster state without bursting over. maxAllowedDrifts is how many candidates we can drift
		// while staging a replacement for each without exceeding the NodePool's node limit; 0 means the pool is at its
		// limit and can't stage any replacement.
		maxAllowedDrifts := d.cluster.NodePoolState.ReserveNodeCount(npName, nodeLimit, maxDrifts)

		// Terminate-first (RFC #3203): when the NodePool is at its node limit it can't stage a replacement first — a
		// pre-spun replacement would be an (N+1)th node the operator capped out. Issue budget-paced delete-only commands;
		// once the freed slot is released the static.provisioning controller refills the pool back to Spec.Replicas. The
		// drain still honors PDBs and is bounded by TGP. When the pool has room under its limit, fall through to the
		// normal replace-first path below. No replacement is reserved for terminate-first, so the reservation above is a
		// no-op in that case (it reserved nothing).
		// We will not get a negative value here
		if maxAllowedDrifts == 0 {
			cmds = append(cmds, legacyUnreplaceableCommands(ctx, d, np, npCandidates[:maxDrifts], func(*Candidate) bool { return true }, TerminateFirstStaticAtLimit,
				"static NodePool is at its node limit and cannot stage a replacement")...)
			continue
		}

		// Select candidates up to maxAllowedDrifts
		for _, c := range npCandidates[:maxAllowedDrifts] {
			result := scheduling.Results{
				NewNodeClaims: []*scheduling.NodeClaim{{NodeClaimTemplate: *nct}},
			}
			cmds = append(cmds, Command{
				Candidates:          []*Candidate{c},
				Replacements:        replacementsFromNodeClaims(result.NewNodeClaims...),
				Results:             result,
				PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{c}),
			})
		}
	}
	return cmds, nil
}

// unreplaceableCommands handles a NodePool's candidates when it can't stage a replacement: with TerminateFirstDrift, each
// candidate that canTerminateFirst gets a delete-only command; every other candidate is reported blocked.
func legacyUnreplaceableCommands(ctx context.Context, d *StaticDrift, np *v1.NodePool, candidates []*Candidate, canTerminateFirst func(*Candidate) bool,
	reason TerminateFirstReason, blockedReason string) []Command {
	// Static provisioning refuses NotReady or deleting NodePools, so terminating first there would strand the workload
	// with no replacement.
	terminateFirst := options.FromContext(ctx).FeatureGates.TerminateFirstDrift && np.StatusConditions().Root().IsTrue() && np.DeletionTimestamp.IsZero()
	var cmds []Command
	for _, c := range candidates {
		if terminateFirst && canTerminateFirst(c) {
			cmds = append(cmds, Command{
				Candidates:           []*Candidate{c},
				PoolDisruptionCosts:  computePoolDisruptionCosts([]*Candidate{c}),
				TerminateFirstReason: reason,
			})
			continue
		}
		d.recorder.Publish(disruptionevents.Blocked(c.Node, c.NodeClaim, blockedReason)...)
	}
	return cmds
}

// LegacyRepairComputeCommands and legacyRepairComputeCommands are verbatim copies of Repair.ComputeCommands and
// Repair.computeCommands at the parent of the repair core (disr/a03-staticdrift-core; repair.go as on
// disr/99-integration), with the receiver made a parameter. They are the reference of the repair differential.
// ComputeCommands orders eligible candidates by the repair score and returns one command for the highest-scoring
// candidate whose NodePool has budget. Workload-bearing candidates verify rescheduling capacity and pre-spin any
// required replacement; empty candidates may produce a delete-only command. Reboot candidates produce a Reboot command
// whose handoff to the reboot controller runs when the command starts. Only one command per pass, mirroring drift.
//
// It logs the resolved repair decision for each candidate it acts on, which records why that policy and drain bound won.
func LegacyRepairComputeCommands(ctx context.Context, r *Repair, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	cmds, err := legacyRepairComputeCommands(ctx, r, disruptionBudgetMapping, candidates...)
	for _, cmd := range cmds {
		for _, c := range cmd.Candidates {
			log.FromContext(ctx).WithValues(append([]any{"Node", klog.KObj(c.Node)}, c.RepairPolicyResult.LogValues()...)...).Info("selected repair policy")
		}
	}
	return cmds, err
}

//nolint:gocyclo // Static and dynamic replacement flows are intentionally kept inline.
func legacyRepairComputeCommands(ctx context.Context, r *Repair, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	// We use a fixed value for now so all candidates get the same value
	// This is important for repair since it scores candidates based on their current age (calculated as now - eligibleAt).
	// We don't want the order in which candidates are evaluated to affect their age.
	now := r.clock.Now()
	sort.SliceStable(candidates, func(i, j int) bool {
		si, sj := candidates[i].RepairPolicyResult.ScoreAt(now), candidates[j].RepairPolicyResult.ScoreAt(now)
		if si != sj {
			return si > sj // higher score repairs first
		}
		// Deterministic tie-break: lower disruption cost, then node name, so the same set always yields the same order.
		if candidates[i].DisruptionCost != candidates[j].DisruptionCost {
			return candidates[i].DisruptionCost < candidates[j].DisruptionCost
		}
		return candidates[i].Name() < candidates[j].Name()
	})
	trippedPools, err := r.breakerTrippedPools(ctx)
	if err != nil {
		return []Command{}, err
	}
	for _, candidate := range candidates {
		if trippedPools[candidate.NodePool.Name] {
			r.recorder.Publish(disruptionevents.NodeRepairBlocked(candidate.Node, candidate.NodeClaim, candidate.NodePool,
				fmt.Sprintf("more than %s of nodes in nodepool %q are unhealthy", repairUnhealthyThreshold, candidate.NodePool.Name))...)
			continue
		}
		// Budgets only count initialized nodes, since an uninitialized node runs no workload to protect, so repairing one
		// neither needs nor consumes budget.
		if candidate.Initialized() && disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			continue
		}
		// Repair admits nodes with blocking (PDB / do-not-disrupt) pods only on the promise of this drain bound, so it
		// must be stamped before either replacement path returns a command.
		candidate.TerminationGracePeriod = effectiveDrainBound(candidate, candidate.RepairPolicyResult)
		// Reboot is in-place, so skip replacement and termination paths.
		if candidate.RepairPolicyResult.Action == cloudprovider.RebootNode {
			nodeClaim, err := r.rebootTarget(ctx, candidate)
			if err != nil {
				return []Command{}, err
			}
			if nodeClaim == nil {
				continue
			}
			return []Command{{
				Candidates:          []*Candidate{candidate},
				PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{candidate}),
				Reboot:              true,
				commitReboot: func(ctx context.Context) error {
					return r.commitReboot(ctx, candidate, nodeClaim)
				},
			}}, nil
		}
		terminateFirstEnabled := options.FromContext(ctx).FeatureGates.TerminateFirstRepair

		// Static NodePools aren't reactively scheduled, so repair can't simulate a replacement — it mirrors StaticDrift.
		if candidate.OwnedByStaticNodePool() {
			np := candidate.NodePool
			active, _, pendingDisruption := r.cluster.NodePoolState.GetNodeCount(np.Name)

			// Skip until scale-down completes: a replacement would just be surplus the deprovisioner deletes.
			if int64(active+pendingDisruption) > lo.FromPtr(np.Spec.Replicas) {
				continue
			}

			nct := pscheduling.NewNodeClaimTemplate(np)
			// Only a candidate holding a slot in a still-offered reservation frees a slot the refill can use, so only it can
			// terminate first on full reservations; any other candidate can't be replaced either way, so it's blocked.
			// Checked only with terminate-first enabled, since without it there's nothing to do but replace first.
			var reservationsFull bool
			if terminateFirstEnabled {
				full, offered, err := staticReservations(ctx, r.cloudProvider, np, nct)
				// Other candidates don't depend on this one, so one NodePool's failure doesn't stop repair for the rest.
				if err != nil {
					log.FromContext(ctx).Error(err, "skipping repair candidate, failed checking capacity reservations")
					continue
				}
				if full && !holdsOfferedReservation(candidate, offered) {
					r.recorder.Publish(disruptionevents.Blocked(candidate.Node, candidate.NodeClaim, "static NodePool's capacity reservations are full and cannot stage a replacement")...)
					continue
				}
				reservationsFull = full
			}
			limit, ok := np.Spec.Limits[resources.Node]
			nodeLimit := lo.Ternary(ok, limit.Value(), int64(math.MaxInt64))
			// Atomic accounting, not a naive count: deleting/already-reserved nodes would otherwise let repair burst
			// past limits.nodes when commands race. A zero result reserves nothing, so the delete-only path leaks no
			// reservation; a non-zero result's slot is consumed by the replacement staged below. Full reservations are
			// checked first so the delete-only path never takes a slot.
			if reservationsFull || r.cluster.NodePoolState.ReserveNodeCount(np.Name, nodeLimit, 1) == 0 {
				// Static provisioning refuses NotReady or deleting NodePools, so terminating first there would strand
				// the workload with no replacement.
				refillable := np.StatusConditions().Root().IsTrue() && np.DeletionTimestamp.IsZero()
				if !terminateFirstEnabled || !refillable {
					r.recorder.Publish(disruptionevents.Blocked(candidate.Node, candidate.NodeClaim, lo.Ternary(reservationsFull,
						"static NodePool's capacity reservations are full and cannot stage a replacement",
						"static NodePool is at its node limit and cannot stage a replacement"))...)
					continue
				}
				return []Command{{
					Candidates:           []*Candidate{candidate},
					PoolDisruptionCosts:  computePoolDisruptionCosts([]*Candidate{candidate}),
					TerminateFirstReason: lo.Ternary(reservationsFull, TerminateFirstNoReservedCapacity, TerminateFirstStaticAtLimit),
				}}, nil
			}
			result := pscheduling.Results{NewNodeClaims: []*pscheduling.NodeClaim{{NodeClaimTemplate: *nct}}}
			return []Command{{
				Candidates:          []*Candidate{candidate},
				Replacements:        replacementsFromNodeClaims(result.NewNodeClaims...),
				Results:             result,
				PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{candidate}),
			}}, nil
		}
		// Repair pre-spins for all reschedulable workload, including pods whose eviction is currently blocked.
		results, terminateFirst, err := SimulateSchedulingWithReservedFallback(ctx, r.kubeClient, r.cluster, r.provisioner, r.clock, r.recorder, candidate,
			terminateFirstEnabled, SimulationOptions{IncludeBlockedCandidatePods: true})
		if err != nil {
			if errors.Is(err, errCandidateDeleting) {
				continue
			}
			return []Command{}, err
		}
		if terminateFirst {
			// Carry the Results (no Replacements) so nodes that can absorb the freed pods get nominated.
			return []Command{{
				Candidates:           []*Candidate{candidate},
				Results:              results,
				PoolDisruptionCosts:  computePoolDisruptionCosts([]*Candidate{candidate}),
				TerminateFirstReason: TerminateFirstNoReservedCapacity,
			}}, nil
		}
		if !results.AllNonPendingPodsScheduled() {
			r.recorder.Publish(disruptionevents.Blocked(candidate.Node, candidate.NodeClaim, pretty.Sentence(results.NonPendingPodSchedulingErrors()))...)
			continue
		}
		return []Command{{
			Candidates:          []*Candidate{candidate},
			Replacements:        replacementsFromNodeClaims(results.NewNodeClaims...),
			Results:             results,
			PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{candidate}),
		}}, nil
	}
	return []Command{}, nil
}
