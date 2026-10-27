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
	"sort"

	"github.com/samber/lo"
	"k8s.io/utils/clock"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
)

// Starter performs a plan's side effects.
type Starter interface {
	Start(ctx context.Context, p *Plan) error
}

type queueStarter struct {
	queue *disruption.Queue
	clock clock.Clock
}

// NewQueueStarter starts plans through Queue.StartCommand, unchanged: it taints the candidates, creates the
// replacements and marks the candidates for deletion, as for the disruption controller's commands. A reboot plan's
// handoff is built on its NodeClaim re-read now.
func NewQueueStarter(queue *disruption.Queue, clk clock.Clock) Starter {
	return &queueStarter{queue: queue, clock: clk}
}

func (s *queueStarter) Start(ctx context.Context, p *Plan) error {
	cmd := p.ToCommand(s.clock.Now())
	if p.Action == types.ActionReboot {
		ok, err := disruption.PrepareReboot(ctx, &cmd)
		if err != nil {
			return err
		}
		if !ok {
			return reject(types.ReasonCandidateInvalid, "node is already rebooting, deleting or gone")
		}
	}
	return s.queue.StartCommand(ctx, &cmd)
}

// Scorer orders the ready plans of one commit step. It is a tie-breaker, not fairness.
type Scorer interface {
	Less(a, b *Plan) bool
}

type defaultScorer struct {
	order map[string]int
}

// NewScorer orders plans by their planner's position in the lanes (lane order, then planner order, as the
// disruption controller orders its methods), then ValidateAt, then ComputedAt, then ID.
func NewScorer(lanes ...LaneSpec) Scorer {
	order := map[string]int{}
	for _, l := range lanes {
		for _, p := range l.Planners {
			if _, ok := order[p.Name()]; !ok {
				order[p.Name()] = len(order)
			}
		}
	}
	return &defaultScorer{order: order}
}

func (s *defaultScorer) Less(a, b *Plan) bool {
	oa, ok := s.order[a.Planner]
	oa = lo.Ternary(ok, oa, len(s.order))
	ob, ok := s.order[b.Planner]
	ob = lo.Ternary(ok, ob, len(s.order))
	switch {
	case oa != ob:
		return oa < ob
	case !a.ValidateAt.Equal(b.ValidateAt):
		return a.ValidateAt.Before(b.ValidateAt)
	case !a.ComputedAt.Equal(b.ComputedAt):
		return a.ComputedAt.Before(b.ComputedAt)
	default:
		return a.ID.String() < b.ID.String()
	}
}

func sortGroups(groups [][]*Pending, s Scorer) {
	sort.SliceStable(groups, func(i, j int) bool { return s.Less(groups[i][0].Plan, groups[j][0].Plan) })
}
