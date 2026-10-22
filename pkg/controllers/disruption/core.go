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
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
)

// CoreOptions parameterizes the one shared core of a disruption method (for example Drift.Compute). The disruption
// controller calls each core with the method's legacy options: Batch 1, inline effects, a LegacySimulator and today's
// candidate order, which is exactly what ComputeCommands did before the core existed. Other callers (the disruption
// pipeline) choose their own simulator, effects, order and batch size.
type CoreOptions struct {
	// Batch is the largest number of candidates planned from one joint simulation. 1 plans one candidate at a time, as
	// the disruption controller always has. Methods that don't batch ignore it.
	Batch int
	// Sizer, if set, sizes each batch to a simulation deadline (at most Batch) and learns from every batch.
	Sizer *BatchSizer
	// Deadline bounds one batch: a joint simulation that takes longer than 2*Deadline is abandoned and the batch is
	// halved next time. 0 means no deadline beyond the caller's context.
	Deadline time.Duration
	// Effects receives the side effects the core would otherwise perform itself.
	Effects Effects
	// Sim runs the scheduling simulations.
	Sim Simulator
	// Order sorts candidates in place before the core walks them. nil keeps the method's own order.
	Order func([]*Candidate)
	// Pending says what to do with new NodeClaims that hold none of the candidates' pods.
	Pending PendingClaims
	// Dedup, if set, skips consolidation methods while nothing has changed since they last found nothing to do.
	Dedup Dedup
	// Validator, if set, validates consolidation commands before they are returned, waiting out the validation delay
	// (the disruption controller). Without it, commands are returned unvalidated, for a caller that validates them
	// later.
	Validator Validator

	// minBatch is the smallest batch planned jointly; 0 means 2. Tests lower it to 1 to drive the batched path with
	// one candidate.
	minBatch int
}

func (o CoreOptions) batchMinSize() int {
	if o.minBatch > 0 {
		return o.minBatch
	}
	return BatchMinSize
}

// PendingClaims decides the fate of new NodeClaims that a simulation opens only for pods that aren't the candidates'
// own (pending pods, and pods of nodes that are already being disrupted).
type PendingClaims int

const (
	// KeepOnFirst launches them as replacements of the first command, as the disruption controller always has.
	KeepOnFirst PendingClaims = iota
	// DropPendingOnly drops the ones that hold only pending pods and leaves those pods to the provisioner. Claims for
	// pods of nodes that are already being disrupted stay on the first command.
	DropPendingOnly
	// dropUnowned drops every new NodeClaim that holds none of the candidates' pods: the later chunks of a batch,
	// whose other pods the first chunk already planned for.
	dropUnowned
)

// Effects receives the side effects a method core produces while it plans.
type Effects interface {
	// Publish publishes events about candidates (for example DisruptionBlocked).
	Publish(evts ...events.Event)
	// ReserveNodeCount reserves up to n of a static NodePool's limits.nodes slots for replacements and returns how many
	// it got (state.NodePoolState.ReserveNodeCount).
	ReserveNodeCount(nodePool string, limit, n int64) int64
}

// Dedup remembers that a consolidation method found nothing to do in the current cluster state.
type Dedup interface {
	// IsConsolidated reports whether nothing has changed since MarkConsolidated.
	IsConsolidated() bool
	MarkConsolidated()
}

// InlineEffects performs every effect immediately, as the disruption controller always has.
type InlineEffects struct {
	Recorder events.Recorder
	Cluster  *state.Cluster
}

func (e InlineEffects) Publish(evts ...events.Event) { e.Recorder.Publish(evts...) }

func (e InlineEffects) ReserveNodeCount(nodePool string, limit, n int64) int64 {
	return e.Cluster.NodePoolState.ReserveNodeCount(nodePool, limit, n)
}

// CoreResult is what a method core planned.
type CoreResult struct {
	// Commands are the planned commands. Commands planned from one joint simulation share a non-zero BatchID.
	Commands []Command
	// Batch describes the joint simulation, if one ran.
	Batch *BatchOutcome
	// Chunked reports that the joint simulation produced a command above ComponentCap candidates, so the batch was
	// planned again in chunks (CapPieces).
	Chunked bool
	// Deferred are providerIDs of candidates that a batch accepted but couldn't emit this round: chunks after a chunk
	// that failed, timed out or shares a topology domain with an earlier chunk. They are planned again next round.
	Deferred []string
}

// orEmpty returns cmds, or an empty slice when it is nil: the ComputeCommands wrappers return what the methods
// returned before the cores existed.
func orEmpty(cmds []Command) []Command {
	if cmds == nil {
		return []Command{}
	}
	return cmds
}

// batchWindow picks the candidates of one batch from ordered, in order and within budgets: candidates for which
// charge is true take one disruption from their NodePool's budget (a copy; the caller's budgets don't change), and
// candidates whose NodePool has none left, or for which skip is true, are passed over. A candidate for which single
// is true can only be planned on its own: before the batch's first candidate it is left to the caller, and after it it
// ends the batch, so the batch never plans past a candidate that ranks ahead of the rest. first is the index in ordered
// of the batch's first candidate (len(ordered) when there is none).
func batchWindow(ordered []*Candidate, budgets map[string]int, skip, single, charge func(*Candidate) bool) (first int, window []*Candidate) {
	remaining := lo.Assign(budgets)
	first = len(ordered)
	for i, c := range ordered {
		if (skip != nil && skip(c)) || (charge(c) && remaining[c.NodePool.Name] <= 0) {
			continue
		}
		if single(c) {
			if len(window) > 0 {
				break
			}
			continue
		}
		if len(window) == 0 {
			first = i
		}
		window = append(window, c)
		if charge(c) {
			remaining[c.NodePool.Name]--
		}
	}
	return first, window
}

// planBatch plans commands for eligible candidates, in priority order and all within budget, from one joint simulation
// (SimulateBatch, sized by opts.Batch and opts.Sizer). The joint result is split into independent commands (Split),
// each capped at ComponentCap candidates (CapPieces). Commands of one batch share a BatchID. Candidates for which charge
// is true are taken from the budgets. It plans nothing when no batch of at least two candidates works out (one, in
// tests): the caller then plans one candidate at a time.
func planBatch(ctx context.Context, budgets map[string]int, opts CoreOptions, simOpts SimOptions, eligible []*Candidate, charge func(*Candidate) bool) (CoreResult, error) {
	k := opts.Batch
	if opts.Sizer != nil {
		k = min(k, opts.Sizer.Size(len(eligible)))
	}
	if min(k, len(eligible)) < opts.batchMinSize() {
		return CoreResult{}, nil
	}
	bo, err := SimulateBatch(ctx, opts.Sim, BatchOptions{SimOptions: simOpts, K: k, MinSize: opts.batchMinSize(), Deadline: opts.Deadline}, eligible...)
	if opts.Sizer != nil {
		opts.Sizer.Observe(bo)
	}
	res := CoreResult{Batch: &bo}
	if err != nil || len(bo.Accepted) == 0 {
		return res, err
	}
	pieces := Split(bo.Accepted, bo.Outcome.Results, opts.Pending)
	chunkCtx := ctx
	if opts.Deadline > 0 {
		var cancel context.CancelFunc
		chunkCtx, cancel = context.WithTimeout(ctx, 2*opts.Deadline)
		defer cancel()
	}
	pieces, deferred, chunked := CapPieces(chunkCtx, opts.Sim, simOpts, bo.Accepted, pieces, opts.Pending)
	res.Chunked = chunked
	res.Deferred = lo.Map(deferred, func(c *Candidate, _ int) string { return c.ProviderID() })
	batchID := uuid.New()
	for _, p := range pieces {
		res.Commands = append(res.Commands, Command{
			BatchID:             batchID,
			Candidates:          p.Candidates,
			Replacements:        replacementsFromNodeClaims(p.Results.NewNodeClaims...),
			Results:             p.Results,
			PoolDisruptionCosts: computePoolDisruptionCosts(p.Candidates),
		})
		for _, c := range p.Candidates {
			if charge(c) {
				budgets[c.NodePool.Name]--
			}
		}
	}
	return res, nil
}
