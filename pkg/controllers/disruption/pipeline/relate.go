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
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
)

// Conflict is a relation between two plans that no re-simulation can fix: one of them must not start.
type Conflict string

const (
	// SameCandidate: both plans disrupt a node.
	SameCandidate Conflict = "same_candidate"
	// RemovesDestination: a disrupts a node b moves pods onto. A re-simulation of a can't see the pods b has yet to move.
	RemovesDestination Conflict = "removes_destination"
	// RemovesDependency: a disrupts a node that b waits on before deleting its candidates (decision #16).
	RemovesDependency Conflict = "removes_dependency"
)

// Coupling is a relation between two plans under which one plan's simulation may be invalid once the other started.
type Coupling string

const (
	// DestinationRemoved: b disrupts a node a moves pods onto.
	DestinationRemoved Coupling = "destination_removed"
	// SharedDestination: both plans move pods onto a node (capacity may be double-booked).
	SharedDestination Coupling = "shared_destination"
	SharedDevice      Coupling = "shared_device"
	// SharedTopology: both plans move pods of a non-hostname topology group (zone spread or affinity).
	SharedTopology Coupling = "shared_topology"
	// SharedPending: both plans size a new NodeClaim for the same pending pod.
	SharedPending Coupling = "shared_pending"
)

// Relation is how plan a relates to plan b.
type Relation struct {
	Hard    []Conflict
	Coupled []Coupling
}

// Related reports whether the plans relate at all.
func (r Relation) Related() bool { return len(r.Hard) > 0 || len(r.Coupled) > 0 }

// Relate classifies how plan a relates to plan b, from their footprints. It is a pre-filter: disjoint footprints don't
// prove two plans independent (zone topology couples plans with disjoint footprints), so the Admitter re-simulates a
// plan when a related plan started after its simulation. NodePool membership and the Ledger's sums (budgets, limits,
// static counts, capacity reservations) are not relations. Relate doesn't know about batches: plans from one joint
// simulation (siblings) don't relate on destinations, and callers skip them.
func Relate(a, b types.Footprint) Relation {
	var r Relation
	if intersects(a.Removed, b.Removed) {
		r.Hard = append(r.Hard, SameCandidate)
	}
	if intersects(a.Removed, b.Destinations) {
		r.Hard = append(r.Hard, RemovesDestination)
	}
	if intersects(a.Removed, b.Dependencies) {
		r.Hard = append(r.Hard, RemovesDependency)
	}
	if intersects(a.Destinations, b.Removed) {
		r.Coupled = append(r.Coupled, DestinationRemoved)
	}
	if intersects(a.Destinations, b.Destinations) {
		r.Coupled = append(r.Coupled, SharedDestination)
	}
	if intersects(a.Devices, b.Devices) {
		r.Coupled = append(r.Coupled, SharedDevice)
	}
	if intersects(a.TopologyGroups, b.TopologyGroups) || anyNamespace(a.TopologyGroups, b.TopologyGroups) || anyNamespace(b.TopologyGroups, a.TopologyGroups) {
		r.Coupled = append(r.Coupled, SharedTopology)
	}
	if intersects(a.PendingPods, b.PendingPods) {
		r.Coupled = append(r.Coupled, SharedPending)
	}
	return r
}

// anyNamespace reports whether a has a pod whose term selects any namespace and b moves pods.
func anyNamespace(a, b sets.Set[uint64]) bool {
	return a.Has(AnyNamespace) && b.Len() > 0
}

func intersects[T comparable](a, b sets.Set[T]) bool {
	if len(a) > len(b) {
		a, b = b, a
	}
	for k := range a {
		if b.Has(k) {
			return true
		}
	}
	return false
}
