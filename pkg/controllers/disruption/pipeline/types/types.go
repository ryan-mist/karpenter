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

// Package types holds the value types that the disruption method cores and the disruption pipeline share. It imports
// neither package, so both can import it.
package types

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
)

// Footprint is everything a plan touches, derived from the plan's own candidates' pods. It is a cheap pre-filter for
// how two plans relate, and the input to the Ledger's sums. It is not a proof that two plans are independent: zone
// topology spread and zone (anti-)affinity couple plans with disjoint footprints, so the Admitter re-simulates a plan
// when a coupled plan started after it was simulated.
type Footprint struct {
	// Removed is the set of providerIDs the plan disrupts (deletes, replaces or reboots).
	Removed sets.Set[string]
	// Destinations is the set of existing nodes (providerIDs) that the plan moves at least one pod onto.
	Destinations sets.Set[string]
	// Dependencies are the Destinations that weren't initialized when the plan was simulated (decision #16): the queue
	// deletes the plan's candidates only once they are initialized. Like any destination, no plan may disrupt them
	// while the plan is reserved or in flight.
	Dependencies sets.Set[string]
	// NewNodeClaims are the simulation-local IDs of new NodeClaims hosting at least one of the plan's pods. They are
	// meaningful only within one simulation.
	NewNodeClaims []string
	// TopologyGroups are the hashes of the non-hostname topology groups that own, select or inversely select a pod the
	// plan moves.
	TopologyGroups sets.Set[uint64]
	// Devices are the cluster-scoped DRA devices allocated to the plan's pods' claims.
	Devices sets.Set[string]
	// Reservations maps a capacity-reservation ID to the slots the plan launches into it (positive) or frees (negative).
	Reservations map[string]int
	// BudgetNodes maps a NodePool to the providerIDs in Removed that count against its disruption budget.
	BudgetNodes map[string]sets.Set[string]
	// LimitCost maps a NodePool to the resources the plan's new NodeClaims may use: for each, the maximum capacity over
	// its instance type options, plus one node.
	LimitCost map[string]corev1.ResourceList
	// StaticNodeCount maps a static NodePool to the node-count slots the plan's replacements need.
	StaticNodeCount map[string]int64
	// PendingPods are the pending pods that share one of the plan's new NodeClaims with the plan's own pods.
	PendingPods sets.Set[k8stypes.UID]
	// ComputedAt is the Ledger version read just before the simulation read cluster state. Anything reserved or
	// started at a later version may not have been seen by the simulation.
	ComputedAt uint64
}

// Action is what a plan does when it starts.
type Action int

const (
	ActionDelete Action = iota
	ActionReplace
	ActionReboot
)

// Shape is the most invasive plan a planner can emit. A lane uses it to decide which candidates a planner gets while
// cluster state is unsynced; the Admitter decides per plan (Plan.PodFree) and doesn't trust it.
type Shape int

const (
	ShapePodFree   Shape = iota // only plans whose candidates have no reschedulable pods (emptiness)
	ShapePodMoving              // deletes whose pods move to existing nodes (consolidation delete)
	ShapeReplacing              // plans that launch replacements or reboot (drift, repair, static drift, consolidation replace)
)

// RejectReason is a typed reason for a verdict, a rejection or a dropped plan. The values are metric label values.
type RejectReason string

const (
	// Verdicts from planning.
	ReasonNoCheaperReplacement RejectReason = "no_cheaper_replacement"
	ReasonSpotToSpotDisabled   RejectReason = "spot_to_spot_disabled"
	ReasonPriceNotLower        RejectReason = "price_not_lower"
	ReasonMinValues            RejectReason = "min_values"
	ReasonPodsDoNotFit         RejectReason = "pods_do_not_fit"
	ReasonPDBBlocked           RejectReason = "pdb_blocked"
	ReasonDoNotDisrupt         RejectReason = "do_not_disrupt"
	ReasonPodsUnschedulable    RejectReason = "pods_unschedulable"
	ReasonBatchBlocked         RejectReason = "batch_blocked"
	// Rejections at Reserve and admission, and drops.
	ReasonConflict            RejectReason = "conflict"
	ReasonBudget              RejectReason = "budget"
	ReasonNodePoolLimit       RejectReason = "nodepool_limit"
	ReasonStaticNodeLimit     RejectReason = "static_node_limit"
	ReasonCapacityReservation RejectReason = "capacity_reservation"
	ReasonCandidateInvalid    RejectReason = "candidate_invalid"
	ReasonCandidateBlocked    RejectReason = "candidate_blocked"
	ReasonCandidateNominated  RejectReason = "candidate_nominated"
	ReasonCandidateIneligible RejectReason = "candidate_ineligible"
	ReasonPodsChanged         RejectReason = "pods_changed"
	ReasonResimUnschedulable  RejectReason = "resim_unschedulable"
	ReasonResimChanged        RejectReason = "resim_changed"
	ReasonSiblingRejected     RejectReason = "sibling_rejected"
	ReasonDeferred            RejectReason = "deferred"
	ReasonStartFailed         RejectReason = "start_failed"
	ReasonBackpressure        RejectReason = "backpressure"
	ReasonDeadline            RejectReason = "deadline"
	ReasonCanceled            RejectReason = "canceled"
	// ReasonError: a check failed with an API or provider error, not a verdict on the plan.
	ReasonError RejectReason = "error"
)

// Verdict is a typed rejection of one candidate by one planner. Verdicts replace the events that filters and method
// cores publish today: they are buffered for the round and published by the Observer, and they feed the memo.
type Verdict struct {
	Planner    string
	ProviderID string
	Reason     RejectReason
	// Message is the text of the event that the disruption controller publishes for the same rejection.
	Message string
	// AtLocal, AtCapacity and AtCatalog are the snapshot versions the verdict was reached at, for memo invalidation.
	AtLocal    uint64
	AtCapacity uint64
	AtCatalog  string
	// NotBefore is when the candidate may be planned again; zero means the next round.
	NotBefore time.Time
}

// LaneID names a lane. The values are metric label values.
type LaneID string

const (
	LaneFast       LaneID = "fast"
	LaneBackground LaneID = "background"
)

// GateReason is why a lane or a planner did not plan. The values are metric label values.
type GateReason string

const (
	GateNone       GateReason = ""
	GateSynced     GateReason = "synced"
	GateBudgetZero GateReason = "budget_zero"
	GateBreaker    GateReason = "breaker"
	GateUnresolved GateReason = "unresolved"
	GatePaced      GateReason = "paced"
	GateHydrating  GateReason = "hydrating"
)

// WakeSet is a set of reasons to wake a lane.
type WakeSet uint32

const (
	WakeDrifted WakeSet = 1 << iota
	WakeConsolidatable
	WakeCapacityUp
	WakeHealth
	WakeLedgerRelease
	WakeSynced
	WakeResolution
	WakeTimer
	WakeCatalog
)

// ResimCause is why the Admitter re-simulates a plan. The values are metric label values.
type ResimCause string

const (
	ResimNone        ResimCause = ""
	ResimAlways      ResimCause = "always"       // consolidation always re-simulates, as its validator does today
	ResimCoupled     ResimCause = "coupled"      // a plan coupled to this one started after this one was simulated
	ResimStale       ResimCause = "stale"        // the plan is older than StaleAfter
	ResimCatalog     ResimCause = "catalog"      // the catalog of a NodePool the plan touches changed
	ResimPodsChanged ResimCause = "pods_changed" // a candidate's pods changed since planning
	ResimSibling     ResimCause = "sibling"      // a plan from the same joint simulation was rejected
	ResimPending     ResimCause = "pending"      // a pending pod the plan accounted for was placed meanwhile
)
