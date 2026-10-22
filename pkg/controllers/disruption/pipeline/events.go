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
	"sync"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/events"
)

// RoundEvents buffers the events and verdicts of one lane round: candidate rejections from the snapshot, planner
// filters and method cores. The pipeline publishes nothing while it plans; the Observer publishes the buffer when the
// round ends. It is an events.Recorder.
type RoundEvents struct {
	mu       sync.Mutex
	events   []events.Event
	verdicts []types.Verdict
	noAction []NoAction
}

// NoAction explains why a planner did nothing for a NodePool this round, e.g. its budget is exhausted. The Observer
// publishes it as a NodePool event when the explanation for that planner and NodePool changes.
type NoAction struct {
	Planner  string
	NodePool string
	// Reason is the event reason, e.g. events.DisruptionBudgetExhausted.
	Reason  string
	Message string
}

func (r *RoundEvents) Publish(evts ...events.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, evts...)
}

// Record adds verdicts to the round.
func (r *RoundEvents) Record(vs ...types.Verdict) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.verdicts = append(r.verdicts, vs...)
}

// Drain returns the events published since the last Drain and forgets them.
func (r *RoundEvents) Drain() []events.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.events
	r.events = nil
	return out
}

// Events returns the round's events in the order they were published.
func (r *RoundEvents) Events() []events.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]events.Event(nil), r.events...)
}

// Verdicts returns the round's verdicts in the order they were recorded.
func (r *RoundEvents) Verdicts() []types.Verdict {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]types.Verdict(nil), r.verdicts...)
}

// ExplainNoAction adds an explanation of why a planner did nothing for a NodePool.
func (r *RoundEvents) ExplainNoAction(n NoAction) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.noAction = append(r.noAction, n)
}

// NoActions returns the round's explanations.
func (r *RoundEvents) NoActions() []NoAction {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]NoAction(nil), r.noAction...)
}
