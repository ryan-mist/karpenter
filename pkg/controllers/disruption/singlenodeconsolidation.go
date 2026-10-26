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
	"fmt"
	"strings"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

var SingleNodeConsolidationTimeoutDuration = 3 * time.Minute

// SingleNodeConsolidation evaluates one node at a time for consolidation.
type SingleNodeConsolidation struct {
	consolidation
	PreviouslyUnseenNodePools sets.Set[string]
	validator                 Validator
}

func NewSingleNodeConsolidation(c consolidation, opts ...option.Function[MethodOptions]) *SingleNodeConsolidation {
	o := option.Resolve(append([]option.Function[MethodOptions]{WithValidator(NewSingleConsolidationValidator(c))}, opts...)...)
	return &SingleNodeConsolidation{
		consolidation:             c,
		PreviouslyUnseenNodePools: sets.New[string](),
		validator:                 o.validator,
	}
}

// ComputeCommands generates a disruption command given candidates: the single-node consolidation core with the
// controller's options.
func (s *SingleNodeConsolidation) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	res, err := s.Compute(ctx, disruptionBudgetMapping, s.LegacyOptions(), candidates...)
	return orEmpty(res.Commands), err
}

// LegacyOptions are the options the disruption controller computes single-node consolidation with: the method's own
// consolidation state, evaluator and 15 s validator, run inline.
func (s *SingleNodeConsolidation) LegacyOptions() CoreOptions { return s.legacyOptions(s.validator) }

// Validator is the validator single-node consolidation commands are checked with before they start.
func (s *SingleNodeConsolidation) Validator() Validator { return s.validator }

// Compute is the single-node consolidation core. It walks the candidates (ordered by opts.Order, or by savings ratio
// interleaved by NodePool, NodePools left unseen by an earlier timeout first) until SingleNodeConsolidationTimeoutDuration
// and returns the first command opts.Evaluator approves. With opts.Validator the command is validated before it is
// returned; if validation fails, nothing is returned.
//
// nolint:gocyclo
func (s *SingleNodeConsolidation) Compute(ctx context.Context, disruptionBudgetMapping map[string]int, opts CoreOptions, candidates ...*Candidate) (CoreResult, error) {
	if opts.Dedup != nil && opts.Dedup.IsConsolidated() {
		return CoreResult{}, nil
	}
	if opts.Order != nil {
		opts.Order(candidates)
	} else {
		candidates = s.SortCandidates(ctx, candidates)
	}
	evaluator := evaluatorOf(opts)

	// Set a timeout
	timeout := s.clock.Now().Add(SingleNodeConsolidationTimeoutDuration)
	constrainedByBudgets := false

	unseenNodePools := sets.New(lo.Map(candidates, func(c *Candidate, _ int) string { return c.NodePool.Name })...)

	for i, candidate := range candidates {
		if s.clock.Now().After(timeout) {
			if !opts.WithoutMetrics {
				ConsolidationTimeoutsTotal.Inc(map[string]string{ConsolidationTypeLabel: s.ConsolidationType()})
			}
			log.FromContext(ctx).V(1).Info("abandoning single-node consolidation due to timeout", "candidates_evaluated", i)

			s.PreviouslyUnseenNodePools = unseenNodePools

			return CoreResult{}, nil
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
		if !evaluator.CanPassThreshold(candidate) {
			continue
		}

		// compute a possible consolidation option
		cmd, err := s.computeConsolidation(ctx, opts, candidate)
		if err != nil {
			log.FromContext(ctx).Error(err, "failed computing consolidation")
			continue
		}
		if cmd.Decision() == NoOpDecision {
			continue
		}
		// Score the move: Balanced pools may reject; other policies pass through.
		if approved, _ := evaluator.ApproveCommand(ctx, cmd); !approved {
			continue
		}
		if opts.Validator == nil {
			return CoreResult{Commands: []Command{cmd}}, nil
		}
		if _, err = validate(ctx, opts.Validator, cmd, commandValidationDelay); err != nil {
			if IsValidationError(err) {
				reason := getValidationFailureReason(err)
				cmd.EmitRejectedEvents(opts.Effects, reason)
				return CoreResult{}, nil
			}
			return CoreResult{}, fmt.Errorf("validating consolidation, %w", err)
		}
		return CoreResult{Commands: []Command{cmd}}, nil
	}

	if !constrainedByBudgets && opts.Dedup != nil {
		// if there are no candidates because of a budget, don't mark
		// as consolidated, as it's possible it should be consolidatable
		// the next time we try to disrupt.
		opts.Dedup.MarkConsolidated()
	}

	s.PreviouslyUnseenNodePools = unseenNodePools

	return CoreResult{}, nil
}

func (s *SingleNodeConsolidation) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonUnderutilized
}

func (s *SingleNodeConsolidation) Class() string {
	return GracefulDisruptionClass
}

func (s *SingleNodeConsolidation) ConsolidationType() string {
	return SingleNodeConsolidationType.Name
}

// SortCandidates applies the consolidation sort, then interweaves by NodePool.
func (s *SingleNodeConsolidation) SortCandidates(ctx context.Context, candidates []*Candidate) []*Candidate {
	candidates = s.sortCandidates(ctx, candidates)
	return s.shuffleCandidates(ctx, lo.GroupBy(candidates, func(c *Candidate) string { return c.NodePool.Name }))
}

func (s *SingleNodeConsolidation) shuffleCandidates(ctx context.Context, nodePoolCandidates map[string][]*Candidate) []*Candidate {
	var result []*Candidate
	// Log any timed out nodepools that we're prioritizing
	if s.PreviouslyUnseenNodePools.Len() != 0 {
		log.FromContext(ctx).V(1).Info("prioritizing nodepools that have not yet been considered due to timeouts in previous runs", "nodepools", strings.Join(s.PreviouslyUnseenNodePools.UnsortedList(), ", "))
	}
	sortedNodePools := s.PreviouslyUnseenNodePools.UnsortedList()
	sortedNodePools = append(sortedNodePools, lo.Filter(lo.Keys(nodePoolCandidates), func(nodePoolName string, _ int) bool {
		return !s.PreviouslyUnseenNodePools.Has(nodePoolName)
	})...)

	// Find the maximum number of candidates in any nodepool
	maxCandidatesPerNodePool := lo.MaxBy(lo.Values(nodePoolCandidates), func(a, b []*Candidate) bool {
		return len(a) > len(b)
	})

	// Interweave candidates from different nodepools
	for i := range maxCandidatesPerNodePool {
		for _, nodePoolName := range sortedNodePools {
			if i < len(nodePoolCandidates[nodePoolName]) {
				result = append(result, nodePoolCandidates[nodePoolName][i])
			}
		}
	}

	return result
}
