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
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

// reservingSimulator simulates as the disruption controller does, except that the candidates of reserved, unstarted
// plans of every lane are unavailable as destinations. Their pods are not re-placed (they aren't deleting yet) and
// still count at their nodes for topology; whichever simulation runs after such a plan starts re-places them. So no
// lane plans capacity for another lane's pods before that lane's plan launched its own (decision #9, F26). Nodes of
// started commands are MarkedForDeletion, and simulations re-place their pods as today. Its simulations list pending pods
// without side effects (no scheduling decisions recorded, no events), so planning writes nothing (F29).
//
// In reference mode it simulates the candidates of reserved, unstarted plans as already deleting instead (their pods
// re-placed), as the disruption controller's pass sees the commands of earlier methods, which it has started by then.
type reservingSimulator struct {
	inner     disruption.Simulator
	reserved  ReservationView
	reference bool
}

// NewSimulator wraps inner (normally disruption.NewLegacySimulator) with the reserved-plans rule.
func NewSimulator(inner disruption.Simulator, reserved ReservationView) disruption.Simulator {
	return &reservingSimulator{inner: inner, reserved: reserved}
}

// newReferenceSimulator wraps inner for reference mode (PlannerOptions.Reference).
func newReferenceSimulator(inner disruption.Simulator, reserved ReservationView) disruption.Simulator {
	return &reservingSimulator{inner: inner, reserved: reserved, reference: true}
}

// simNodesKey carries the state nodes a shadow-mode plan simulates on (withSimNodes).
type simNodesKey struct{}

// withSimNodes makes the pipeline's simulations under ctx run on nodes, a copy of the state its caller planned on, in
// place of cluster state's current nodes (the shadow mode, which plans beside the disruption controller).
func withSimNodes(ctx context.Context, nodes state.StateNodes) context.Context {
	return context.WithValue(ctx, simNodesKey{}, nodes)
}

func (s *reservingSimulator) Simulate(ctx context.Context, opts disruption.SimOptions, candidates ...*disruption.Candidate) (disruption.Outcome, error) {
	if nodes, ok := ctx.Value(simNodesKey{}).(state.StateNodes); ok && opts.Nodes == nil {
		opts.Nodes = nodes
	}
	// Planning records nothing about pending pods; the provisioner does on its own loop.
	opts.WithoutPendingPodSideEffects = true
	if s.reserved != nil {
		exclude := s.reserved.Unstarted()
		switch {
		case len(exclude) == 0:
		case s.reference:
			opts.AlsoDeleting = exclude.Union(opts.AlsoDeleting)
		default:
			opts.ExcludeDestinations = exclude.Union(opts.ExcludeDestinations)
		}
	}
	return s.inner.Simulate(ctx, opts, candidates...)
}
