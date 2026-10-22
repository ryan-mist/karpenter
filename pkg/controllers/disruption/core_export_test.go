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
	"slices"
	"sort"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
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
