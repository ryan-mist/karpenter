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
	"time"

	"github.com/samber/lo"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
)

// Trigger decides when a lane runs its next round. Pace is pure; the runner owns the clock.
type Trigger interface {
	// Wake delivers reasons to run a round early. A nil channel never fires.
	Wake() <-chan types.WakeSet
	// Pace is how long after the end of the last round the next one runs, given what woke the lane since.
	Pace(last StepResult, woke types.WakeSet) time.Duration
}

// Internal constants of the triggers. None of them is a user setting.
const (
	// minPace is the shortest gap between two rounds of a lane that didn't just start a plan.
	minPace = time.Second
	// dutyFactor makes a lane wait dutyFactor times as long as its last round took (a 25% duty cycle).
	dutyFactor = 3
	// gatePoll is how often a lane, or the commit loop, polls a gate that has no event yet (cluster state sync).
	gatePoll = time.Second
	// failureBackoff is how long a planner doesn't plan after none of its last plans started.
	failureBackoff = pollingPeriod
)

// PollTrigger is the alpha trigger: it never wakes a lane early, and paces it like the disruption controller polls.
// A lane runs again right away after one of its plans started (Fast) or after its duty-cycle gap (Background), after a
// second while it waits for a gate or for its plans to resolve, at a planner's next timer, and after the polling period
// otherwise.
type PollTrigger struct {
	// Background paces every round by its duty cycle, even right after a start.
	Background bool
}

func (t PollTrigger) Wake() <-chan types.WakeSet { return nil }

func (t PollTrigger) Pace(last StepResult, _ types.WakeSet) time.Duration {
	duty := max(minPace, dutyFactor*last.Took)
	var pace time.Duration
	switch {
	case last.Err != nil:
		pace = pollingPeriod
	case last.Admitted > 0:
		pace = lo.Ternary(t.Background, duty, 0)
	case last.Plans > 0, last.Gated == types.GateSynced, last.Gated == types.GateHydrating:
		pace = max(gatePoll, lo.Ternary(t.Background, duty, 0))
	default:
		pace = pollingPeriod
	}
	if !last.NextTimer.IsZero() {
		if until := last.NextTimer.Sub(last.EndedAt); until < pace {
			pace = max(until, lo.Ternary(t.Background, duty, 0))
		}
	}
	return pace
}

// DefaultLanes are the default lanes over the given planners (by Name): Fast runs repair, emptiness, static drift and
// drift; Background runs multi- and single-node consolidation, and skips its rounds while cluster state is unsynced.
// Missing planners are left out; a lane without planners is left out.
func DefaultLanes(planners map[string]Planner) []LaneSpec {
	pick := func(names ...string) []Planner {
		return lo.FilterMap(names, func(n string, _ int) (Planner, bool) {
			p, ok := planners[n]
			return p, ok
		})
	}
	lanes := []LaneSpec{
		{ID: types.LaneFast, Planners: pick(RepairPlanner, EmptinessPlanner, StaticDriftPlanner, DriftPlanner), Trigger: PollTrigger{}},
		{ID: types.LaneBackground, Planners: pick(MultiNodePlanner, SingleNodePlanner), SkipWhileUnsynced: true, Trigger: PollTrigger{Background: true}},
	}
	return lo.Filter(lanes, func(l LaneSpec, _ int) bool { return len(l.Planners) > 0 })
}
