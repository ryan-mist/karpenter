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

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
)

// reservingSimulator simulates as the disruption controller does, except that the candidates of reserved, unstarted
// plans of every lane are unavailable as destinations. Their pods are not re-placed (they aren't deleting yet) and
// still count at their nodes for topology; whichever simulation runs after such a plan starts re-places them. So no
// lane plans capacity for another lane's pods before that lane's plan launched its own (decision #9, F26). Nodes of
// started commands are MarkedForDeletion, and simulations re-place their pods as today.
type reservingSimulator struct {
	inner    disruption.Simulator
	reserved ReservationView
}

// NewSimulator wraps inner (normally disruption.NewLegacySimulator) with the reserved-plans rule.
func NewSimulator(inner disruption.Simulator, reserved ReservationView) disruption.Simulator {
	return &reservingSimulator{inner: inner, reserved: reserved}
}

func (s *reservingSimulator) Simulate(ctx context.Context, opts disruption.SimOptions, candidates ...*disruption.Candidate) (disruption.Outcome, error) {
	if s.reserved != nil {
		exclude := s.reserved.Unstarted()
		if len(exclude) > 0 {
			opts.ExcludeDestinations = exclude.Union(opts.ExcludeDestinations)
		}
	}
	return s.inner.Simulate(ctx, opts, candidates...)
}
