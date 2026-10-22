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
	"sync"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/utils/pod"
)

// Internal constants of batched planning. None of them is a user setting.
const (
	// BatchMinSize is the smallest batch planned from one joint simulation. Below it, methods plan one candidate at a
	// time.
	BatchMinSize = 2
	// MaxBatchRounds bounds how often a joint simulation is re-run after dropping candidates whose pods didn't fit.
	MaxBatchRounds = 3
	// FirstBatchMax caps the first batch a BatchSizer plans, before it has measured anything.
	FirstBatchMax = 50
	// ComponentCap is the most candidates one command of a split batch may hold, which bounds how many nodes one
	// failed launch can stall. Larger components are planned again in chunks of at most ComponentCap.
	ComponentCap = 10
	// BatchSimDeadline is the time a joint simulation is sized to take. A simulation is abandoned after twice that.
	BatchSimDeadline = 2 * time.Second
)

// Simulator runs the scheduling simulations of the method cores.
type Simulator interface {
	// Simulate removes the candidates from the cluster and schedules their pods, along with the pending pods and the
	// pods of nodes that are already being disrupted.
	Simulate(ctx context.Context, opts SimOptions, candidates ...*Candidate) (Outcome, error)
}

// SimOptions configures one simulation.
type SimOptions struct {
	// IncludeBlockedCandidatePods schedules candidate pods even when they can't be evicted right now (repair).
	IncludeBlockedCandidatePods bool
	// SchedulerOptions are passed to the scheduler.
	SchedulerOptions []scheduling.Options
	// ReservedFallback simulates one candidate with SimulateSchedulingWithReservedFallback, which can find that the
	// candidate must be terminated before its replacement launches. TerminateFirst says whether that is allowed.
	ReservedFallback bool
	TerminateFirst   bool
	// ExcludeDestinations are providerIDs of nodes that may not receive pods.
	ExcludeDestinations sets.Set[string]
	// LimitsUsed counts resources against NodePool limits on top of the simulated nodes, by NodePool name.
	LimitsUsed map[string]corev1.ResourceList
	// SkipDeletingNodePods leaves out the pods of nodes that are already being disrupted.
	SkipDeletingNodePods bool
}

// Outcome is the result of one simulation.
type Outcome struct {
	Results scheduling.Results
	// TerminateFirst reports that the candidate must be deleted before a replacement can launch (ReservedFallback).
	TerminateFirst bool
	// Took is how long the simulation ran, and Pods how many pods it scheduled or failed to schedule.
	Took time.Duration
	Pods int
}

// LegacySimulator simulates exactly as the disruption controller always has, with SimulateScheduling and
// SimulateSchedulingWithReservedFallback.
type LegacySimulator struct {
	kubeClient  client.Client
	cluster     *state.Cluster
	provisioner *provisioning.Provisioner
	clock       clock.Clock
	recorder    events.Recorder
}

func NewLegacySimulator(kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner, clk clock.Clock, recorder events.Recorder) *LegacySimulator {
	return &LegacySimulator{kubeClient: kubeClient, cluster: cluster, provisioner: provisioner, clock: clk, recorder: recorder}
}

func (s *LegacySimulator) Simulate(ctx context.Context, opts SimOptions, candidates ...*Candidate) (Outcome, error) {
	start := time.Now()
	simOpts := SimulationOptions{IncludeBlockedCandidatePods: opts.IncludeBlockedCandidatePods, ExcludeDestinations: opts.ExcludeDestinations,
		LimitsUsed: opts.LimitsUsed, SkipDeletingNodePods: opts.SkipDeletingNodePods}
	var out Outcome
	var err error
	if opts.ReservedFallback && len(candidates) == 1 && len(opts.SchedulerOptions) == 0 {
		out.Results, out.TerminateFirst, err = SimulateSchedulingWithReservedFallback(ctx, s.kubeClient, s.cluster, s.provisioner, s.clock, s.recorder,
			candidates[0], opts.TerminateFirst, simOpts)
	} else {
		out.Results, err = SimulateScheduling(ctx, s.kubeClient, s.cluster, s.provisioner, s.clock, s.recorder, opts.SchedulerOptions, simOpts, candidates...)
	}
	out.Took = time.Since(start)
	out.Pods = scheduledPods(out.Results)
	return out, err
}

func scheduledPods(r scheduling.Results) int {
	return len(r.PodErrors) + lo.SumBy(r.NewNodeClaims, func(nc *scheduling.NodeClaim) int { return len(nc.Pods) }) +
		lo.SumBy(r.ExistingNodes, func(en *scheduling.ExistingNode) int { return len(en.Pods) })
}

// BatchOptions configures SimulateBatch.
type BatchOptions struct {
	SimOptions
	// K is the size of the first joint simulation. The candidates after the first K top up the batch once, after
	// blocked candidates are dropped.
	K int
	// MinSize is the smallest batch simulated jointly (0: BatchMinSize).
	MinSize int
	// MaxRounds bounds the simulations (0: MaxBatchRounds).
	MaxRounds int
	// Deadline is the time the joint simulation is sized to take; it is abandoned after 2*Deadline. 0: no deadline.
	Deadline time.Duration
}

// BatchOutcome is the result of SimulateBatch.
type BatchOutcome struct {
	// Size is the size of the first joint simulation.
	Size int
	// Accepted are the candidates of the last simulation, in which every one of their pods was scheduled. Empty when
	// no batch of at least MinSize candidates worked out: the caller then plans one candidate at a time.
	Accepted []*Candidate
	// Outcome is the simulation that accepted them.
	Outcome Outcome
	// Dropped are providerIDs of candidates taken out of the batch, and why.
	Dropped          map[string]DropReason
	Rounds, Halvings int
	TimedOut         bool
}

// DropReason says why SimulateBatch took a candidate out of a batch.
type DropReason string

const (
	// DropPodsUnschedulable: one of the candidate's pods failed to schedule.
	DropPodsUnschedulable DropReason = "pods_unschedulable"
	// DropUninitializedDestination: one of the candidate's pods was scheduled to a node that isn't initialized.
	DropUninitializedDestination DropReason = "uninitialized_destination"
	// DropHalved: the batch was halved after a pod that belongs to no candidate failed to schedule.
	DropHalved DropReason = "halved"
)

// SimulateBatch plans candidates jointly: one simulation removes all of them at once, so their pods compete for the
// same capacity. Candidates must be in priority order and must all fit the disruption budget together.
//
//   - Candidates whose own pods fail to schedule are dropped and the simulation re-run, at most MaxRounds times. The
//     first re-run is topped up with the candidates after the first K.
//   - When a pod that belongs to no candidate fails (a pod of a node that is already being disrupted, crowded out by the
//     batch), the batch is halved.
//   - A simulation that runs past 2*Deadline is abandoned; its partial result is never used.
//
// It returns no Accepted candidates when no batch of at least MinSize worked out.
//
//nolint:gocyclo
func SimulateBatch(ctx context.Context, sim Simulator, opts BatchOptions, candidates ...*Candidate) (BatchOutcome, error) {
	minSize := lo.Ternary(opts.MinSize > 0, opts.MinSize, BatchMinSize)
	maxRounds := lo.Ternary(opts.MaxRounds > 0, opts.MaxRounds, MaxBatchRounds)
	batch := candidates[:min(max(opts.K, 0), len(candidates))]
	rest := candidates[len(batch):]
	out := BatchOutcome{Size: len(batch), Dropped: map[string]DropReason{}}
	simCtx := ctx
	if opts.Deadline > 0 {
		var cancel context.CancelFunc
		simCtx, cancel = context.WithTimeout(ctx, 2*opts.Deadline)
		defer cancel()
	}
	toppedUp := false
	for ; out.Rounds < maxRounds && len(batch) >= minSize; out.Rounds++ {
		o, err := sim.Simulate(simCtx, opts.SimOptions, batch...)
		if err != nil {
			if simCtx.Err() != nil && ctx.Err() == nil {
				out.TimedOut = true
				return out, nil
			}
			// A candidate started deleting since it was listed. The single-candidate path skips it.
			if errors.Is(err, errCandidateDeleting) {
				return out, nil
			}
			return out, err
		}
		blocked, attributable := attributeBatchFailures(o.Results, batch)
		if !attributable {
			if len(batch)/2 < minSize {
				return out, nil
			}
			for _, c := range batch[len(batch)/2:] {
				out.Dropped[c.ProviderID()] = DropHalved
			}
			batch = batch[:len(batch)/2]
			out.Halvings++
			continue
		}
		if len(blocked) == 0 {
			out.Rounds++
			out.Accepted, out.Outcome = batch, o
			return out, nil
		}
		for id, reason := range blocked {
			out.Dropped[id] = reason
		}
		batch = lo.Reject(batch, func(c *Candidate, _ int) bool { _, ok := blocked[c.ProviderID()]; return ok })
		if !toppedUp {
			toppedUp = true
			n := min(len(blocked), len(rest))
			batch = append(batch[:len(batch):len(batch)], rest[:n]...)
			rest = rest[n:]
		}
	}
	return out, nil
}

// attributeBatchFailures returns the candidates owning a pod that failed to schedule (pending pods aside), keyed by
// providerID. attributable is false when a failed pod belongs to no candidate in the batch.
func attributeBatchFailures(results scheduling.Results, batch []*Candidate) (map[string]DropReason, bool) {
	owner := map[types.UID]*Candidate{}
	for _, c := range batch {
		for _, p := range c.reschedulablePods {
			owner[p.UID] = c
		}
	}
	blocked := map[string]DropReason{}
	for p, err := range results.PodErrors {
		if pod.IsProvisionable(p) {
			continue
		}
		c, ok := owner[p.UID]
		if !ok {
			return nil, false
		}
		reason := DropPodsUnschedulable
		if uninitialized := (*UninitializedNodeError)(nil); errors.As(err, &uninitialized) {
			reason = DropUninitializedDestination
		}
		if blocked[c.ProviderID()] != DropPodsUnschedulable {
			blocked[c.ProviderID()] = reason
		}
	}
	return blocked, true
}

// BatchSizer sizes batches so that a joint simulation takes about a target duration. It starts at
// min(limit, FirstBatchMax), then follows the measured cost per candidate within [K/2, 2K] of the last batch, and
// halves after a batch that timed out or was halved. It is safe for concurrent use.
type BatchSizer struct {
	mu           sync.Mutex
	target       time.Duration
	last         int
	perCandidate time.Duration
}

func NewBatchSizer(target time.Duration) *BatchSizer {
	return &BatchSizer{target: target}
}

// Size returns the next batch size, at most limit (the candidates the budget allows) and at least 1.
func (s *BatchSizer) Size(limit int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := min(limit, FirstBatchMax)
	if s.last > 0 {
		k = s.last
		if s.perCandidate > 0 {
			k = min(max(int(s.target/s.perCandidate), s.last/2), 2*s.last)
		}
	}
	return max(1, min(k, limit))
}

// Observe records a batch's outcome.
func (s *BatchSizer) Observe(o BatchOutcome) {
	if o.Size == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if o.TimedOut || o.Halvings > 0 {
		s.last, s.perCandidate = max(1, o.Size>>max(1, o.Halvings)), 0
		return
	}
	s.last = o.Size
	if len(o.Accepted) > 0 && o.Outcome.Took > 0 {
		s.perCandidate = o.Outcome.Took / time.Duration(len(o.Accepted))
	}
}
