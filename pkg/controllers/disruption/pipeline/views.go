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
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
)

// BudgetView is the live budget view that lanes and planners size their work with: allowed disruptions at the live
// node count and clock, minus every node that is disrupting or reserved. The Ledger implements it and is the
// authority; a view is only a hint.
type BudgetView interface {
	// Remaining is how many more nodes of nodePool may be disrupted for reason.
	Remaining(ctx context.Context, nodePool string, reason v1.DisruptionReason) int
	// AnyRemaining reports whether any NodePool may disrupt another node for reason.
	AnyRemaining(ctx context.Context, reason v1.DisruptionReason) bool
}

// ReservationView is the live view of the plans that are reserved but not started, in every lane, and of the started
// commands' destinations. It is safe for concurrent use. The Ledger implements it.
type ReservationView interface {
	// Version increases whenever the reservations change.
	Version() uint64
	// IsReserved reports whether a reserved, unstarted plan disrupts the node.
	IsReserved(providerID string) bool
	// Unstarted returns the providerIDs that reserved, unstarted plans disrupt.
	Unstarted() sets.Set[string]
	// Footprints returns the footprints of the reserved, unstarted plans.
	Footprints() []types.Footprint
	// InFlightDestinations returns the existing nodes that started commands are moving pods onto.
	InFlightDestinations() sets.Set[string]
	// StaticHeld is the static node-count slots that reserved, unstarted plans hold in nodePool.
	StaticHeld(nodePool string) int64
	// ReservationsHeld is the capacity-reservation slots that reserved, unstarted plans hold in reservationID.
	ReservationsHeld(reservationID string) int
}

// referenceView is the reference mode's ReservationView: the nodes reserved plans move pods onto count as in-flight
// destinations, so they aren't candidates (WithReferenceMode).
type referenceView struct{ ReservationView }

func (v referenceView) InFlightDestinations() sets.Set[string] {
	out := v.ReservationView.InFlightDestinations()
	for _, fp := range v.Footprints() {
		out = out.Union(fp.Destinations)
	}
	return out
}

// Internal constants. None of them is a user setting.
const (
	// StaleAfter is how long after its submission a plan may wait before the Admitter re-simulates it.
	StaleAfter = 10 * time.Second
	// MaxPlanTime bounds how long a round's planning may take before its plans are stale anyway (StaleAfter counts from
	// submission); the Ledger keeps started footprints for StaleAfter+MaxPlanTime.
	MaxPlanTime = 30 * time.Second
	// MaxAdmissionWait is how long after its ValidateAt a plan may wait for admission before it is dropped.
	MaxAdmissionWait = time.Minute
	// SweepTick is how often the commit loop removes leftover disruption taints and conditions.
	SweepTick = time.Minute
)
