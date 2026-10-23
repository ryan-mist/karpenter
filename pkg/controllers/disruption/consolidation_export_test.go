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

// Verbatim copies of the consolidation methods at the parent of the consolidation cores (disr/a04-repair-core;
// consolidation.go, multinodeconsolidation.go and singlenodeconsolidation.go as on disr/99-integration), with receivers
// made parameters and calls between them pointed at the copies. They are the reference of the consolidation
// differential (consolidation_core_test.go).

import (
	"context"
	"errors"
	"fmt"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
)

// nolint:gocyclo
func LegacyMultiNodeComputeCommands(ctx context.Context, m *MultiNodeConsolidation, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	if m.IsConsolidated() {
		return []Command{}, nil
	}
	candidates = m.sortCandidates(ctx, candidates)

	// In order, filter out all candidates that would violate the budget.
	// Since multi-node consolidation relies on the ordering of
	// these candidates, and does computation in batches of these nodes by
	// simulateScheduling(nodes[0, n]), doing a binary search on n to find
	// the optimal consolidation command, this pre-filters out nodes that
	// would have violated the budget anyway, preserving the ordering
	// and only considering a number of nodes that can be disrupted.
	disruptableCandidates := make([]*Candidate, 0, len(candidates))
	constrainedByBudgets := false
	for _, candidate := range candidates {
		// If there's disruptions allowed for the candidate's nodepool,
		// add it to the list of candidates, and decrement the budget.
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			constrainedByBudgets = true
			continue
		}
		// set constrainedByBudgets to true if any node was a candidate but was constrained by a budget
		disruptableCandidates = append(disruptableCandidates, candidate)
		disruptionBudgetMapping[candidate.NodePool.Name]--
	}

	// Only consider a maximum batch of 100 NodeClaims to save on computation.
	// This could be further configurable in the future.
	maxParallel := lo.Clamp(len(disruptableCandidates), 0, 100)

	cmd, perPoolResults, err := legacyFirstNConsolidationOption(ctx, m, disruptableCandidates, maxParallel)
	if err != nil {
		return []Command{}, err
	}

	if cmd.Decision() == NoOpDecision {
		// if there are no candidates because of a budget, don't mark
		// as consolidated, as it's possible it should be consolidatable
		// the next time we try to disrupt.
		if !constrainedByBudgets {
			m.markConsolidated()
		}
		return []Command{}, nil
	}

	// Emit balanced scoring events per NodePool
	if perPoolResults != nil {
		m.evaluator.EmitMultiNodeEvents(ctx, cmd, perPoolResults, true)
	}

	if cmd, err = validate(ctx, m.validator, cmd, commandValidationDelay); err != nil {
		if IsValidationError(err) {
			reason := getValidationFailureReason(err)
			cmd.EmitRejectedEvents(m.recorder, reason)
			return []Command{}, nil
		}
		return []Command{}, fmt.Errorf("validating consolidation, %w", err)
	}
	return []Command{cmd}, nil
}

// firstNConsolidationOption looks at the first N NodeClaims to determine if they can all be consolidated at once.  The
// NodeClaims are sorted by increasing disruption order which correlates to likelihood of being able to consolidate the node
// nolint:gocyclo
func legacyFirstNConsolidationOption(ctx context.Context, m *MultiNodeConsolidation, candidates []*Candidate, max int) (Command, map[string]ScoreResult, error) {
	// we always operate on at least two NodeClaims at once, for single NodeClaims standard consolidation will find all solutions
	if len(candidates) < 2 {
		return Command{}, nil, nil
	}
	min := 1
	if len(candidates) <= max {
		max = len(candidates) - 1
	}

	lastSavedCommand := Command{}
	var lastSavedPerPool map[string]ScoreResult
	// Defer rejection events until search completes to avoid log2(N) * pools
	// duplicate emissions.
	var lastRejectedCmd Command
	var lastRejectedPerPool map[string]ScoreResult
	// Set a timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, MultiNodeConsolidationTimeoutDuration)
	defer cancel()
	for min <= max {
		mid := (min + max) / 2
		candidatesToConsolidate := candidates[0 : mid+1]

		// Pass the timeout context to ensure sub-operations can be canceled
		cmd, err := legacyComputeConsolidation(timeoutCtx, &m.consolidation, candidatesToConsolidate...)
		// context deadline exceeded will return to the top of the loop and either return nothing or the last saved command
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				ConsolidationTimeoutsTotal.Inc(map[string]string{ConsolidationTypeLabel: m.ConsolidationType()})
				if lastSavedCommand.Candidates == nil {
					log.FromContext(ctx).V(1).Info("failed to find a multi-node consolidation after timeout", "last_batch_size", (min+max)/2)
					return Command{}, nil, nil
				}
				log.FromContext(ctx).V(1).WithValues(lastSavedCommand.LogValues()...).Info("stopping multi-node consolidation after timeout, returning last valid command")
				return lastSavedCommand, lastSavedPerPool, nil

			}
			return Command{}, nil, err
		}
		// ensure that the action is sensical for replacements, see explanation on filterOutSameType for why this is
		// required
		validDecision := cmd.Decision() == DeleteDecision
		if cmd.Decision() == ReplaceDecision {
			cmd.Replacements[0], err = filterOutSameInstanceType(cmd.Replacements[0], candidatesToConsolidate)
			// we check the error before the replacement instanceTypeOptions since we return nil for the replacement if we get an error
			if err == nil && len(cmd.Replacements[0].InstanceTypeOptions) > 0 {
				validDecision = true
			}
		}
		// Score the move: Balanced pools may reject; other policies pass through.
		if validDecision {
			if approved, perPool := m.evaluator.ApproveCommand(ctx, cmd); !approved {
				validDecision = false
				lastRejectedCmd = cmd
				lastRejectedPerPool = perPool
			} else if perPool != nil {
				lastSavedPerPool = perPool
			}
		}
		if validDecision {
			// We can consolidate NodeClaims [0,mid]
			lastSavedCommand = cmd
			min = mid + 1
		} else {
			max = mid - 1
		}
	}
	// If binary search found no valid command and balanced scoring rejected at
	// least one iteration, emit rejection metrics once using the final (smallest
	// failing window) results rather than at every iteration.
	if lastSavedCommand.Candidates == nil && lastRejectedPerPool != nil {
		m.evaluator.EmitMultiNodeEvents(ctx, lastRejectedCmd, lastRejectedPerPool, false)
	}
	return lastSavedCommand, lastSavedPerPool, nil
}

// ComputeCommand generates a disruption command given candidates
// nolint:gocyclo
func LegacySingleNodeComputeCommands(ctx context.Context, s *SingleNodeConsolidation, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	if s.IsConsolidated() {
		return []Command{}, nil
	}
	candidates = s.SortCandidates(ctx, candidates)

	// Set a timeout
	timeout := s.clock.Now().Add(SingleNodeConsolidationTimeoutDuration)
	constrainedByBudgets := false

	unseenNodePools := sets.New(lo.Map(candidates, func(c *Candidate, _ int) string { return c.NodePool.Name })...)

	for i, candidate := range candidates {
		if s.clock.Now().After(timeout) {
			ConsolidationTimeoutsTotal.Inc(map[string]string{ConsolidationTypeLabel: s.ConsolidationType()})
			log.FromContext(ctx).V(1).Info("abandoning single-node consolidation due to timeout", "candidates_evaluated", i)

			s.PreviouslyUnseenNodePools = unseenNodePools

			return []Command{}, nil
		}
		// Track that we've seen this nodepool
		unseenNodePools.Delete(candidate.NodePool.Name)

		// If the disruption budget doesn't allow this candidate to be disrupted,
		// continue to the next candidate. We don't need to decrement any budget
		// counter since single node consolidation commands can only have one candidate.
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			constrainedByBudgets = true
			continue
		}
		// Skip candidates whose best-case score (delete ratio) cannot pass the
		// threshold. A DELETE is the upper bound; if it fails, no REPLACE will pass.
		if !s.evaluator.CanPassThreshold(candidate) {
			continue
		}

		// compute a possible consolidation option
		cmd, err := legacyComputeConsolidation(ctx, &s.consolidation, candidate)
		if err != nil {
			log.FromContext(ctx).Error(err, "failed computing consolidation")
			continue
		}
		if cmd.Decision() == NoOpDecision {
			continue
		}
		// Score the move: Balanced pools may reject; other policies pass through.
		if approved, _ := s.evaluator.ApproveCommand(ctx, cmd); !approved {
			continue
		}
		if _, err = validate(ctx, s.validator, cmd, commandValidationDelay); err != nil {
			if IsValidationError(err) {
				reason := getValidationFailureReason(err)
				cmd.EmitRejectedEvents(s.recorder, reason)
				return []Command{}, nil
			}
			return []Command{}, fmt.Errorf("validating consolidation, %w", err)
		}
		return []Command{cmd}, nil
	}

	if !constrainedByBudgets {
		// if there are no candidates because of a budget, don't mark
		// as consolidated, as it's possible it should be consolidatable
		// the next time we try to disrupt.
		s.markConsolidated()
	}

	s.PreviouslyUnseenNodePools = unseenNodePools

	return []Command{}, nil
}

// computeConsolidation computes a consolidation action to take
//
// nolint:gocyclo
func legacyComputeConsolidation(ctx context.Context, c *consolidation, candidates ...*Candidate) (Command, error) {
	var err error
	// Run scheduling simulation to compute consolidation option
	results, err := SimulateScheduling(ctx, c.kubeClient, c.cluster, c.provisioner, c.clock, c.recorder, []pscheduling.Options{pscheduling.IsConsolidationSimulation}, SimulationOptions{}, candidates...)
	if err != nil {
		// if a candidate node is now deleting, just retry
		if errors.Is(err, errCandidateDeleting) {
			return Command{}, nil
		}
		return Command{}, err
	}

	// if not all of the pods were scheduled, we can't do anything
	if !results.AllNonPendingPodsScheduled() {
		// This method is used by multi-node consolidation as well, so we'll only report in the single node case
		if len(candidates) == 1 {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, pretty.Sentence(results.NonPendingPodSchedulingErrors()))...)
		}
		return Command{}, nil
	}

	// were we able to schedule all the pods on the inflight candidates?
	if len(results.NewNodeClaims) == 0 {
		return Command{
			Candidates:          candidates,
			Results:             results,
			PoolDisruptionCosts: computePoolDisruptionCosts(candidates),
		}, nil
	}

	// we're not going to turn a single node into multiple candidates
	if len(results.NewNodeClaims) != 1 {
		if len(candidates) == 1 {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, fmt.Sprintf("Can't remove without creating %d candidates", len(results.NewNodeClaims)))...)
		}
		return Command{}, nil
	}

	// get the current node price based on the offering
	// fallback if we can't find the specific zonal pricing data
	candidatePrice := sumCandidatePrices(candidates)

	allExistingAreSpot := true
	for _, cn := range candidates {
		if cn.capacityType != v1.CapacityTypeSpot {
			allExistingAreSpot = false
		}
	}

	// sort the instanceTypes by price before we take any actions like truncation for spot-to-spot consolidation or finding the nodeclaim
	// that meets the minimum requirement after filteringByPrice
	results.NewNodeClaims[0].InstanceTypeOptions = results.NewNodeClaims[0].InstanceTypeOptions.OrderByPrice(results.NewNodeClaims[0].Requirements)

	if allExistingAreSpot &&
		results.NewNodeClaims[0].Requirements.Get(v1.CapacityTypeLabelKey).Has(v1.CapacityTypeSpot) {
		return legacyComputeSpotToSpotConsolidation(ctx, c, candidates, results, candidatePrice)
	}

	// filterByPrice returns the instanceTypes that are lower priced than the current candidate and any error that indicates the input couldn't be filtered.
	// If we use this directly for spot-to-spot consolidation, we are bound to get repeated consolidations because the strategy that chooses to launch the spot instance from the list does
	// it based on availability and price which could result in selection/launch of non-lowest priced instance in the list. So, we would keep repeating this loop till we get to lowest priced instance
	// causing churns and landing onto lower available spot instance ultimately resulting in higher interruptions.
	results.NewNodeClaims[0], err = results.NewNodeClaims[0].RemoveInstanceTypeOptionsByPriceAndMinValues(results.NewNodeClaims[0].Requirements, candidatePrice)
	if err != nil {
		if len(candidates) == 1 {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, fmt.Sprintf("Filtering by price: %v", err))...)
		}
		return Command{}, nil
	}
	if len(results.NewNodeClaims[0].InstanceTypeOptions) == 0 {
		if len(candidates) == 1 {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, "Can't replace with a cheaper node")...)
		}
		return Command{}, nil
	}

	// We are consolidating a node from OD -> [OD,Spot] but have filtered the instance types by cost based on the
	// assumption, that the spot variant will launch. We also need to add a requirement to the node to ensure that if
	// spot capacity is insufficient we don't replace the node with a more expensive on-demand node.  Instead the launch
	// should fail and we'll just leave the node alone. We don't need to do the same for reserved since the requirements
	// are injected on by the scheduler.
	ctReq := results.NewNodeClaims[0].Requirements.Get(v1.CapacityTypeLabelKey)
	if ctReq.Has(v1.CapacityTypeSpot) && ctReq.Has(v1.CapacityTypeOnDemand) {
		results.NewNodeClaims[0].Requirements.Add(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot))
	}

	cmd := Command{
		Candidates:          candidates,
		Replacements:        replacementsFromNodeClaims(results.NewNodeClaims...),
		Results:             results,
		PoolDisruptionCosts: computePoolDisruptionCosts(candidates),
	}
	cmd.EmitCandidateEvents(c.recorder)

	return cmd, nil
}

// Compute command to execute spot-to-spot consolidation if:
//  1. The SpotToSpotConsolidation feature flag is set to true.
//  2. For single-node consolidation:
//     a. There are at least 15 cheapest instance type replacement options to consolidate.
//     b. The current candidate is NOT part of the first 15 cheapest instance types inorder to avoid repeated consolidation.
func legacyComputeSpotToSpotConsolidation(ctx context.Context, c *consolidation, candidates []*Candidate, results pscheduling.Results, candidatePrice float64) (Command, error) {

	// Spot consolidation is turned off.
	if !options.FromContext(ctx).FeatureGates.SpotToSpotConsolidation {
		if len(candidates) == 1 {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, "SpotToSpotConsolidation is disabled, can't replace a spot node with a spot node")...)
		}
		return Command{}, nil
	}

	// Since we are sure that the replacement nodeclaim considered for the spot candidates are spot, we will enforce it through the requirements.
	results.NewNodeClaims[0].Requirements.Add(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot))
	// All possible replacements for the current candidate compatible with spot offerings
	results.NewNodeClaims[0].InstanceTypeOptions = results.NewNodeClaims[0].InstanceTypeOptions.Compatible(results.NewNodeClaims[0].Requirements)

	// filterByPrice returns the instanceTypes that are lower priced than the current candidate and any error that indicates the input couldn't be filtered.
	var err error
	results.NewNodeClaims[0], err = results.NewNodeClaims[0].RemoveInstanceTypeOptionsByPriceAndMinValues(results.NewNodeClaims[0].Requirements, candidatePrice)
	if err != nil {
		if len(candidates) == 1 {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, fmt.Sprintf("Filtering by price: %v", err))...)
		}
		return Command{}, nil
	}
	if len(results.NewNodeClaims[0].InstanceTypeOptions) == 0 {
		if len(candidates) == 1 {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, "Can't replace with a cheaper node")...)
		}
		return Command{}, nil
	}

	// For multi-node consolidation:
	// We don't have any requirement to check the remaining instance type flexibility, so exit early in this case.
	if len(candidates) > 1 {
		cmd := Command{
			Candidates:          candidates,
			Replacements:        replacementsFromNodeClaims(results.NewNodeClaims...),
			Results:             results,
			PoolDisruptionCosts: computePoolDisruptionCosts(candidates),
		}
		cmd.EmitCandidateEvents(c.recorder)

		return cmd, nil
	}

	// For single-node consolidation:

	// We check whether we have 15 cheaper instances than the current candidate instance. If this is the case, we know the following things:
	//   1) The current candidate is not in the set of the 15 cheapest instance types and
	//   2) There were at least 15 options cheaper than the current candidate.
	if len(results.NewNodeClaims[0].InstanceTypeOptions) < MinInstanceTypesForSpotToSpotConsolidation {
		c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, fmt.Sprintf("SpotToSpotConsolidation requires %d cheaper instance type options than the current candidate to consolidate, got %d",
			MinInstanceTypesForSpotToSpotConsolidation, len(results.NewNodeClaims[0].InstanceTypeOptions)))...)
		return Command{}, nil
	}

	// If a user has minValues set in their NodePool requirements, then we cap the number of instancetypes at 100 which would be the actual number of instancetypes sent for launch to enable spot-to-spot consolidation.
	// If no minValues in the NodePool requirement, then we follow the default 15 to cap the instance types for launch to enable a spot-to-spot consolidation.
	// Restrict the InstanceTypeOptions for launch to 15(if default) so we don't get into a continual consolidation situation.
	// For example:
	// 1) Suppose we have 5 instance types, (A, B, C, D, E) in order of price with the minimum flexibility 3 and they’ll all work for our pod.  We send CreateInstanceFromTypes(A,B,C,D,E) and it gives us a E type based on price and availability of spot.
	// 2) We check if E is part of (A,B,C,D) and it isn't, so we will immediately have consolidation send a CreateInstanceFromTypes(A,B,C,D), since they’re cheaper than E.
	// 3) Assuming CreateInstanceFromTypes(A,B,C,D) returned D, we check if D is part of (A,B,C) and it isn't, so will have another consolidation send a CreateInstanceFromTypes(A,B,C), since they’re cheaper than D resulting in continual consolidation.
	// If we had restricted instance types to min flexibility at launch at step (1) i.e CreateInstanceFromTypes(A,B,C), we would have received the instance type part of the list preventing immediate consolidation.
	// Taking this to 15 types, we need to only send the 15 cheapest types in the CreateInstanceFromTypes call so that the resulting instance is always in that set of 15 and we won’t immediately consolidate.
	if results.NewNodeClaims[0].Requirements.HasMinValues() {
		// Here we are trying to get the max of the minimum instances required to satisfy the minimum requirement and the default 15 to cap the instances for spot-to-spot consolidation.
		minInstanceTypes, _, _ := results.NewNodeClaims[0].InstanceTypeOptions.SatisfiesMinValues(results.NewNodeClaims[0].Requirements)
		results.NewNodeClaims[0].InstanceTypeOptions = lo.Slice(results.NewNodeClaims[0].InstanceTypeOptions, 0, lo.Max([]int{MinInstanceTypesForSpotToSpotConsolidation, minInstanceTypes}))
	} else {
		results.NewNodeClaims[0].InstanceTypeOptions = lo.Slice(results.NewNodeClaims[0].InstanceTypeOptions, 0, MinInstanceTypesForSpotToSpotConsolidation)
	}

	cmd := Command{
		Candidates:          candidates,
		Replacements:        replacementsFromNodeClaims(results.NewNodeClaims...),
		Results:             results,
		PoolDisruptionCosts: computePoolDisruptionCosts(candidates),
	}
	cmd.EmitCandidateEvents(c.recorder)

	return cmd, nil
}
