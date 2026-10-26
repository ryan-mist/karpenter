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
	// BatchSizerCoolDown is how many batches a BatchSizer must see accepted after it shrank (a timeout or a halving)
	// before it grows again.
	BatchSizerCoolDown = 2
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
	// TrackUninitialized lets the candidates' pods use nodes that aren't initialized, as Dependencies
	// (SimulationOptions).
	TrackUninitialized bool
	// CandidateGroupSize splits the candidates into groups whose pods never share a node (SimulationOptions).
	CandidateGroupSize int
	// WithoutPendingPodSideEffects lists the pending pods without recording scheduling decisions or publishing events
	// about them (the disruption pipeline).
	WithoutPendingPodSideEffects bool
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
		LimitsUsed: opts.LimitsUsed, SkipDeletingNodePods: opts.SkipDeletingNodePods,
		TrackUninitialized: opts.TrackUninitialized, CandidateGroupSize: opts.CandidateGroupSize,
		WithoutPendingPodSideEffects: opts.WithoutPendingPodSideEffects}
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
	// Budgets, when set, bound the batch: candidates for which Charge is true (nil: every candidate) take one
	// disruption from their NodePool's budget. The batch and its top-up are the first candidates, in order, that fit;
	// a dropped candidate frees its budget for a later candidate of its NodePool. Nil: every candidate fits.
	Budgets map[string]int
	Charge  func(*Candidate) bool
}

// takeWithin returns the first n candidates, in order, that fit the budgets on top of the candidates in batch (budgets
// nil: every candidate fits; charge nil: every candidate is charged), and the others, in order.
func takeWithin(candidates []*Candidate, n int, batch []*Candidate, budgets map[string]int, charge func(*Candidate) bool) (taken, rest []*Candidate) {
	charged := func(c *Candidate) bool { return budgets != nil && (charge == nil || charge(c)) }
	used := map[string]int{}
	for _, c := range batch {
		if charged(c) {
			used[c.NodePool.Name]++
		}
	}
	for _, c := range candidates {
		if len(taken) == n || (charged(c) && used[c.NodePool.Name] >= budgets[c.NodePool.Name]) {
			rest = append(rest, c)
			continue
		}
		taken = append(taken, c)
		if charged(c) {
			used[c.NodePool.Name]++
		}
	}
	return taken, rest
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
	// Sims are the batch size and duration of each simulation that finished, in order (for the BatchSizer).
	Sims []SimSample
}

// SimSample is one finished simulation of a batch: how many candidates it removed and how long it took.
type SimSample struct {
	Size int
	Took time.Duration
}

// DropReason says why SimulateBatch took a candidate out of a batch.
type DropReason string

const (
	// DropPodsUnschedulable: one of the candidate's pods failed to schedule.
	DropPodsUnschedulable DropReason = "pods_unschedulable"
	// DropUninitializedDestination: one of the candidate's pods was scheduled to a node that isn't initialized (only
	// without SimOptions.TrackUninitialized).
	DropUninitializedDestination DropReason = "uninitialized_destination"
	// DropHalved: the batch was halved after a pod that belongs to no candidate failed to schedule.
	DropHalved DropReason = "halved"
)

// SimulateBatch plans candidates jointly: one simulation removes all of them at once, so their pods compete for the
// same capacity. Candidates must be in priority order. Without Budgets they must all fit the disruption budget
// together; with Budgets the batch is the first K that fit.
//
//   - Candidates whose own pods fail to schedule are dropped and the simulation re-run, at most MaxRounds times. The
//     first re-run is topped up, in order, with the candidates left out of the first simulation that now fit the
//     budget, and keeps the candidates' order.
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
	batch, rest := takeWithin(candidates, max(opts.K, 0), nil, opts.Budgets, opts.Charge)
	rank := make(map[*Candidate]int, len(candidates))
	for i, c := range candidates {
		rank[c] = i
	}
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
		if err == nil {
			out.Sims = append(out.Sims, SimSample{Size: len(batch), Took: o.Took})
		}
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
			var top []*Candidate
			top, rest = takeWithin(rest, len(blocked), batch, opts.Budgets, opts.Charge)
			batch = append(batch[:len(batch):len(batch)], top...)
			slices.SortStableFunc(batch, func(a, b *Candidate) int { return rank[a] - rank[b] })
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

// BatchSizer sizes batches so that a joint simulation takes about a target duration on top of its fixed cost. A
// simulation's cost is fixed (building the scheduler and topology over every node: about 90% of a drift simulation at
// 2000 nodes) plus a part that grows with the candidates removed. The sizer estimates the fixed part as the cost of the
// cheapest simulation seen (smoothed) and sizes K so that the growing part takes about target: it starts at
// min(limit, FirstBatchMax), then follows the measured marginal cost per candidate within [K/2, 2K] of the last batch.
// A batch that timed out or was halved halves K (multiplicative decrease), and K doesn't grow again until
// BatchSizerCoolDown batches were accepted, so a size that times out isn't retried at once. Deadline grows with the
// fixed part, so a cluster on which one simulation already takes longer than target still plans in batches. It is safe
// for concurrent use.
type BatchSizer struct {
	mu           sync.Mutex
	target       time.Duration
	last         int
	perCandidate time.Duration
	// fixed is the estimated fixed cost of one simulation; 0 until a simulation was observed.
	fixed time.Duration
	// coolDown counts the accepted batches still to see before K may grow again; set when K shrank.
	coolDown int
}

func NewBatchSizer(target time.Duration) *BatchSizer {
	return &BatchSizer{target: target}
}

// Deadline is the deadline to pass to SimulateBatch (which abandons a batch after twice that): the target plus, per
// possible simulation round, half the estimated fixed cost, so that every round's fixed cost fits.
func (s *BatchSizer) Deadline() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.target + time.Duration(MaxBatchRounds)*s.fixed/2
}

// Size returns the next batch size, at most limit (the candidates the budget allows) and at least 1. It is at least
// BatchMinSize when the limit allows it: below that no joint simulation runs, so no outcome is observed and a size
// that collapsed to 1 (a timeout or halving at 2 or 3) would never grow again.
func (s *BatchSizer) Size(limit int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := min(limit, FirstBatchMax)
	if s.last > 0 {
		k = s.last
		switch {
		case s.perCandidate > 0:
			k = min(max(int(s.target/s.perCandidate), s.last/2), 2*s.last)
		case s.fixed > 0:
			k = 2 * s.last // the marginal cost is below what can be measured: grow
		}
		if s.coolDown > 0 {
			k = min(k, s.last) // after a shrink: may shrink further, but doesn't grow yet
		}
	}
	return max(1, min(max(k, BatchMinSize), limit))
}

// Observe records a batch's outcome.
func (s *BatchSizer) Observe(o BatchOutcome) {
	if o.Size == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(o.Sims) > 0 {
		cheapest := lo.MinBy(o.Sims, func(a, b SimSample) bool { return a.Took < b.Took }).Took
		s.fixed = lo.Ternary(s.fixed == 0 || cheapest < s.fixed, cheapest, (3*s.fixed+cheapest)/4)
	}
	if o.TimedOut || o.Halvings > 0 {
		s.last, s.perCandidate, s.coolDown = max(1, o.Size>>max(1, o.Halvings)), 0, BatchSizerCoolDown
		return
	}
	s.last = o.Size
	if s.coolDown > 0 && len(o.Accepted) > 0 {
		s.coolDown--
	}
	if len(o.Accepted) > 0 && o.Outcome.Took > 0 {
		s.perCandidate = max(o.Outcome.Took-s.fixed, 0) / time.Duration(len(o.Accepted))
	}
}
