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
	"github.com/awslabs/operatorpkg/option"
	"github.com/go-logr/logr"
	"k8s.io/utils/clock"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

// TriggerFor is the trigger the controller runs the lane with.
func TriggerFor(c *Controller, lane types.LaneID) Trigger {
	for _, l := range c.lanes {
		if l.Spec().ID == lane {
			return c.trigger(l.Spec())
		}
	}
	return nil
}

// NewWakeObserver is the observer that publishes the pipeline's own wake-ups.
func NewWakeObserver(hub *WakeHub, lanes ...LaneSpec) Observer { return newWakeObserver(hub, lanes...) }

// PoolFairness exposes drift's NodePool fairness for tests.
type PoolFairness = poolFairness

func NewPoolFairness(clk clock.Clock, jitter func() float64) *PoolFairness {
	f := newPoolFairness(clk)
	f.jitter = jitter
	return f
}

func (f *PoolFairness) Eligible(cs []*disruption.Candidate) []*disruption.Candidate {
	return f.eligible(cs)
}
func (f *PoolFairness) Order(cs []*disruption.Candidate) {
	f.order(func([]*disruption.Candidate) {})(cs)
}
func (f *PoolFairness) Started(pools ...string)           { f.started(pools...) }
func (f *PoolFairness) Finished(ok bool, pools ...string) { f.finished(ok, pools...) }

func NewStateDedup(cluster *state.Cluster) disruption.Dedup { return &stateDedup{cluster: cluster} }

// WithReservedCredit is the reference mode's Ledger option.
func WithReservedCredit() option.Function[LedgerOptions] { return withReservedCredit() }

// NewReferenceSimulator is the reference mode's Simulator.
func NewReferenceSimulator(inner disruption.Simulator, reserved ReservationView) disruption.Simulator {
	return newReferenceSimulator(inner, reserved)
}

// SetShadowPlanner replaces one of the shadow's planners.
func (s *Shadow) SetShadowPlanner(name string, p Planner) { s.planners[name] = p }

// ShadowPlanner is one of the shadow's planners.
func (s *Shadow) ShadowPlanner(name string) Planner { return s.planners[name] }

// SetShadowLogger replaces the shadow's root logger.
func (s *Shadow) SetShadowLogger(l logr.Logger) { s.logger = l }

// WaitIdle waits for the shadow's sample goroutines.
func (s *Shadow) WaitIdle() { s.jobs.Wait() }
