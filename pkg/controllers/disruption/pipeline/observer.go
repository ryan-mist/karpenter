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

	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
)

// Observer receives the pipeline's lifecycle events. It is the only publisher of the pipeline's events and metrics.
// Calls are synchronous and must be cheap.
type Observer interface {
	RoundStarted(lane types.LaneID)
	// RoundFinished receives the round's buffered events and verdicts (nil if the round built no snapshot).
	RoundFinished(lane types.LaneID, r StepResult, ev *RoundEvents)
	// Gated reports that a lane (planner "") or one planner did not plan this round.
	Gated(lane types.LaneID, planner string, gate types.GateReason)
	// Planned reports that a planner planned on its candidates.
	Planned(lane types.LaneID, planner Planner, candidates int, out PlanOutput, took time.Duration)
}

// NopObserver observes nothing. Embed it to implement part of Observer.
type NopObserver struct{}

func (NopObserver) RoundStarted(types.LaneID)                                     {}
func (NopObserver) RoundFinished(types.LaneID, StepResult, *RoundEvents)          {}
func (NopObserver) Gated(types.LaneID, string, types.GateReason)                  {}
func (NopObserver) Planned(types.LaneID, Planner, int, PlanOutput, time.Duration) {}

// observers fans each call out to every observer, in order.
type observers []Observer

func (o observers) RoundStarted(lane types.LaneID) {
	for _, x := range o {
		x.RoundStarted(lane)
	}
}

func (o observers) RoundFinished(lane types.LaneID, r StepResult, ev *RoundEvents) {
	for _, x := range o {
		x.RoundFinished(lane, r, ev)
	}
}

func (o observers) Gated(lane types.LaneID, planner string, gate types.GateReason) {
	for _, x := range o {
		x.Gated(lane, planner, gate)
	}
}

func (o observers) Planned(lane types.LaneID, planner Planner, candidates int, out PlanOutput, took time.Duration) {
	for _, x := range o {
		x.Planned(lane, planner, candidates, out, took)
	}
}
