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
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/google/uuid"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	nodepoolutils "sigs.k8s.io/karpenter/pkg/utils/nodepool"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// RejectError is a typed rejection of a plan.
type RejectError struct {
	Reason types.RejectReason
	Err    error
}

func (e *RejectError) Error() string {
	if e.Err == nil {
		return string(e.Reason)
	}
	return fmt.Sprintf("%s, %s", e.Reason, e.Err)
}

func (e *RejectError) Unwrap() error { return e.Err }

func reject(reason types.RejectReason, format string, args ...any) *RejectError {
	return &RejectError{Reason: reason, Err: fmt.Errorf(format, args...)}
}

// RejectReasonOf is the RejectReason of err, or "" if err isn't a RejectError.
func RejectReasonOf(err error) types.RejectReason {
	var re *RejectError
	if errors.As(err, &re) {
		return re.Reason
	}
	return ""
}

// AllocationPolicy decides whether a NodePool's budget can take a plan's charge. It is the seam for a future per-reason
// fair share. It must be pure: the Ledger calls it under its lock.
type AllocationPolicy interface {
	Name() string
	Allow(pool PoolUsage, reason v1.DisruptionReason, charge int) bool
}

// PoolUsage is one NodePool's budget use, as the Ledger sees it: what a policy needs to decide.
type PoolUsage struct {
	NodePool string
	// Allowed is the allowed disruptions per reason, for every reason, at the live node count and clock.
	Allowed map[v1.DisruptionReason]int
	// Used is the number of nodes disrupting or reserved, for any reason, as the reason being checked counts them:
	// |D_live ∪ R|, plus once more for every NotReady node a reserved plan disrupts (it was charged twice when its plan
	// was reserved, as the disruption controller decrements its budget for every candidate, NotReady or not).
	Used int
	// ByReason splits the nodes being disrupted by reason: reserved plans by their reason, nodes MarkedForDeletion by
	// their NodeClaim's DisruptionReason condition. A fair-share policy would use it; first come first served doesn't.
	ByReason map[v1.DisruptionReason]int
	// NumNodes is the number of nodes the budget's percentages apply to.
	NumNodes int
	// Reserved is the number of nodes reserved by unstarted plans.
	Reserved int
}

// FirstComeFirstServed is today's allocation: a plan fits if the nodes disrupting or reserved plus its charge fit the
// allowed disruptions of its reason.
type FirstComeFirstServed struct{}

func (FirstComeFirstServed) Name() string { return "first_come_first_served" }

func (FirstComeFirstServed) Allow(pool PoolUsage, reason v1.DisruptionReason, charge int) bool {
	return pool.Used+charge <= pool.Allowed[reason]
}

// StartedFootprint is the footprint of a plan that started, at the Ledger version it started at.
type StartedFootprint struct {
	Version   uint64
	PlanID    uuid.UUID
	BatchID   uuid.UUID
	Footprint types.Footprint
	At        time.Time
}

// Ledger is the bookkeeping for the sums that no footprint overlap can see: the disruption budget cap, NodePool
// limits, static NodePool node counts and capacity reservations. It holds plans from Reserve until they start or are
// released, across all lanes, under one mutex. It doesn't change budget semantics: budgets still cap the total number
// of nodes disrupting in a NodePool, for any reason; the Ledger adds the nodes of reserved, unstarted plans to that
// total.
type Ledger interface {
	// Version increases on every change to the reserved plans or the started log.
	Version() uint64
	Policy() AllocationPolicy
	// Reserve reserves plans in order, returning one error per plan (a *RejectError, or nil if reserved). Within one
	// BatchID, once a plan doesn't fit, the later plans of the batch are rejected with the same reason.
	Reserve(ctx context.Context, plans ...*Plan) []error
	// Recheck checks a reserved plan against live state, excluding its own reservation.
	Recheck(ctx context.Context, id uuid.UUID) error
	// Amend replaces a reserved plan's footprint (a re-simulation changed it), if the new footprint fits.
	Amend(ctx context.Context, id uuid.UUID, fp types.Footprint) error
	// Swap replaces reserved plans with re-planned ones, all or nothing.
	Swap(ctx context.Context, old []uuid.UUID, replans []*Plan) error
	// MarkStarting hands a plan's static node-count slots to the Queue, which releases them from now on.
	MarkStarting(id uuid.UUID)
	// MarkStarted logs a started plan's footprint and releases its reservation. Call it after StartCommand returned,
	// when its candidates are MarkedForDeletion.
	MarkStarted(id uuid.UUID)
	// StartedSince returns the plans that started after version v.
	StartedSince(v uint64) []StartedFootprint
	// Release releases a rejected or dropped plan's reservation.
	Release(id uuid.UUID, reason types.RejectReason)
	// Usage is every NodePool's budget use for reason, for metrics.
	Usage(ctx context.Context, reason v1.DisruptionReason) []PoolUsage

	View() ReservationView
	Budgets() BudgetView
	// Released is signalled (coalesced) whenever a reservation is released or a plan starts.
	Released() <-chan struct{}
}

type LedgerOptions struct {
	policy AllocationPolicy
}

// WithAllocationPolicy replaces FirstComeFirstServed.
func WithAllocationPolicy(p AllocationPolicy) option.Function[LedgerOptions] {
	return func(o *LedgerOptions) { o.policy = p }
}

type ledger struct {
	kubeClient    client.Client
	cluster       *state.Cluster
	cloudProvider cloudprovider.CloudProvider
	clock         clock.Clock
	queue         *disruption.Queue
	policy        AllocationPolicy

	mu       sync.Mutex
	version  atomic.Uint64
	reserved map[uuid.UUID]*reservation
	// removed indexes the Removed nodes of the reserved plans, so IsReserved is a map lookup.
	removed  map[string]uuid.UUID
	started  []StartedFootprint
	released chan struct{}
}

type reservation struct {
	plan      *Plan
	footprint types.Footprint
	// static is the node-count slots this reservation holds per NodePool; nil once the Queue owns them.
	static map[string]int64
}

func NewLedger(kubeClient client.Client, cluster *state.Cluster, cp cloudprovider.CloudProvider, clk clock.Clock, queue *disruption.Queue,
	opts ...option.Function[LedgerOptions],
) Ledger {
	o := option.Resolve(opts...)
	return &ledger{
		kubeClient:    kubeClient,
		cluster:       cluster,
		cloudProvider: cp,
		clock:         clk,
		queue:         queue,
		policy:        lo.Ternary[AllocationPolicy](o.policy != nil, o.policy, FirstComeFirstServed{}),
		reserved:      map[uuid.UUID]*reservation{},
		removed:       map[string]uuid.UUID{},
		released:      make(chan struct{}, 1),
	}
}

func (l *ledger) Version() uint64           { return l.version.Load() }
func (l *ledger) Policy() AllocationPolicy  { return l.policy }
func (l *ledger) Released() <-chan struct{} { return l.released }
func (l *ledger) View() ReservationView     { return (*ledgerView)(l) }
func (l *ledger) Budgets() BudgetView       { return (*ledgerBudgets)(l) }

func (l *ledger) bump() { l.version.Add(1) }

func (l *ledger) signal() {
	select {
	case l.released <- struct{}{}:
	default:
	}
}

// add and drop keep reserved and its removed index in step.
func (l *ledger) add(r *reservation) {
	l.reserved[r.plan.ID] = r
	for id := range r.footprint.Removed {
		l.removed[id] = r.plan.ID
	}
}

func (l *ledger) drop(id uuid.UUID) *reservation {
	r, ok := l.reserved[id]
	if !ok {
		return nil
	}
	delete(l.reserved, id)
	for n := range r.footprint.Removed {
		if l.removed[n] == id {
			delete(l.removed, n)
		}
	}
	return r
}

// inputs are what a check reads that doesn't need the Ledger's lock: NodePools from the informer cache and, if any
// plan launches into capacity reservations, their capacities from the provider's (cached) offerings. Live cluster
// state is read under the lock.
type inputs struct {
	nodePools map[string]*v1.NodePool
	capacity  map[string]int
}

func (l *ledger) inputs(ctx context.Context, plans ...types.Footprint) (inputs, error) {
	nps, err := nodepoolutils.ListManaged(ctx, l.kubeClient, l.cloudProvider)
	if err != nil {
		return inputs{}, fmt.Errorf("listing nodepools, %w", err)
	}
	in := inputs{nodePools: lo.SliceToMap(nps, func(np *v1.NodePool) (string, *v1.NodePool) { return np.Name, np })}
	if lo.SomeBy(plans, func(fp types.Footprint) bool { return len(fp.Reservations) > 0 }) {
		in.capacity = l.reservationCapacity(ctx, nps)
	}
	return in, nil
}

func (l *ledger) Reserve(ctx context.Context, plans ...*Plan) []error {
	errs := make([]error, len(plans))
	in, err := l.inputs(ctx, lo.Map(plans, func(p *Plan, _ int) types.Footprint { return p.Footprint })...)
	if err != nil {
		for i := range errs {
			errs[i] = err
		}
		return errs
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	live := l.live()
	inFlight := l.queue.InFlightDestinations()
	failedBatch := map[uuid.UUID]error{}
	for i, p := range plans {
		if err, ok := failedBatch[p.BatchID]; ok {
			errs[i] = err
			continue
		}
		if err := l.reserve(in, live, inFlight, p, nil); err != nil {
			errs[i] = err
			failedBatch[p.BatchID] = err
		}
	}
	return errs
}

// reserve checks and reserves one plan. staticCredit are node-count slots that plans being swapped out hold and give
// to this one; reserve takes what it uses from it.
func (l *ledger) reserve(in inputs, live liveState, inFlight sets.Set[string], p *Plan, staticCredit map[string]int64) error {
	if _, ok := l.reserved[p.ID]; ok {
		return reject(types.ReasonConflict, "plan %s is already reserved", p.ID)
	}
	if err := l.exclusive(live, inFlight, p.Footprint, uuid.Nil); err != nil {
		return err
	}
	if err := l.check(in, live, p.Footprint, p.Reason, uuid.Nil); err != nil {
		return err
	}
	static, err := l.reserveStatic(in.nodePools, p.Footprint.StaticNodeCount, staticCredit)
	if err != nil {
		return err
	}
	l.add(&reservation{plan: p, footprint: p.Footprint, static: static})
	l.bump()
	return nil
}

// check runs the sums for a footprint: the budget cap, NodePool limits and capacity reservations. exclude is the plan
// whose reservation fp stands in for (Recheck, Amend).
func (l *ledger) check(in inputs, live liveState, fp types.Footprint, reason v1.DisruptionReason, exclude uuid.UUID) error {
	if err := l.budget(live, in.nodePools, fp, reason, exclude); err != nil {
		return err
	}
	if err := l.limits(in.nodePools, live, fp, exclude); err != nil {
		return err
	}
	return l.capacityReservations(in.capacity, fp, exclude)
}

// exclusive rejects a plan that would share a node with another action: it disrupts a node that a reserved plan or a
// started command disrupts, or that a reserved plan or a started command moves pods onto (a re-simulation can't see
// the pods those still have to move); or it moves pods onto a node that a reserved plan disrupts or that is
// MarkedForDeletion. exclude is the plan whose reservation fp replaces (Amend).
func (l *ledger) exclusive(live liveState, inFlight sets.Set[string], fp types.Footprint, exclude uuid.UUID) error {
	for id := range fp.Removed {
		if n, ok := live.nodes[id]; ok && n.marked {
			return reject(types.ReasonConflict, "node %s is already being disrupted", id)
		}
		if l.queue.HasAny(id) {
			return reject(types.ReasonConflict, "node %s is in the orchestration queue", id)
		}
		if inFlight.Has(id) {
			return reject(types.ReasonConflict, "a started command is moving pods onto node %s", id)
		}
		if owner, ok := l.removed[id]; ok && owner != exclude {
			return reject(types.ReasonConflict, "a reserved plan disrupts node %s", id)
		}
	}
	for qid, r := range l.reserved {
		if qid == exclude {
			continue
		}
		if fp.Removed.HasAny(r.footprint.Destinations.UnsortedList()...) {
			return reject(types.ReasonConflict, "a reserved plan moves pods onto a node the plan disrupts")
		}
	}
	for id := range fp.Destinations {
		if n, ok := live.nodes[id]; ok && n.marked {
			return reject(types.ReasonConflict, "the plan moves pods onto node %s, which is being disrupted", id)
		}
		if owner, ok := l.removed[id]; ok && owner != exclude {
			return reject(types.ReasonConflict, "the plan moves pods onto node %s, which a reserved plan disrupts", id)
		}
	}
	return nil
}

// budget checks the budget cap of every NodePool the footprint charges:
//
//	Used(np, reason) + |BudgetNodes[np] \ (R(np) ∪ M_live(np))| ≤ allowed(np, reason)
//
// where Used = |D_live(np, reason) ∪ R(np)| plus the reserved NotReady candidates once more (see PoolUsage.Used),
// M_live is the nodes MarkedForDeletion or rebooting, D_live adds the NotReady nodes (except for the Unhealthy
// reason), and R is the budget nodes of every reserved plan except exclude. A NotReady candidate is charged even
// though it is in D_live, as the disruption controller decrements its budget for every candidate; a plan's NotReady
// candidates therefore count twice both before and after it is reserved, so Reserve and Recheck agree.
func (l *ledger) budget(live liveState, nodePools map[string]*v1.NodePool, fp types.Footprint, reason v1.DisruptionReason, exclude uuid.UUID) error {
	for name, ids := range fp.BudgetNodes {
		np, ok := nodePools[name]
		if !ok {
			return reject(types.ReasonBudget, "nodepool %s not found", name)
		}
		usage := l.usage(live, np, reason, exclude)
		charge := ids.Difference(l.reservedBudgetNodes(name, exclude)).Difference(live.marked[name]).Len()
		if !l.policy.Allow(usage, reason, charge) {
			return reject(types.ReasonBudget, "nodepool %s allows %d disruptions for %s, %d are disrupting or reserved, the plan needs %d more",
				name, usage.Allowed[reason], reason, usage.Used, charge)
		}
	}
	return nil
}

// budgetReasons are the reasons PoolUsage.Allowed covers.
var budgetReasons = []v1.DisruptionReason{v1.DisruptionReasonEmpty, v1.DisruptionReasonUnderutilized, v1.DisruptionReasonDrifted, v1.DisruptionReasonUnhealthy}

func (l *ledger) usage(live liveState, np *v1.NodePool, reason v1.DisruptionReason, exclude uuid.UUID) PoolUsage {
	r := l.reservedBudgetNodes(np.Name, exclude)
	d := live.marked[np.Name].Union(r)
	used := 0
	if reason != v1.DisruptionReasonUnhealthy {
		d = d.Union(live.notReady[np.Name])
		// A reserved NotReady candidate was charged twice when its plan was reserved; keep counting it twice.
		used = r.Intersection(live.notReady[np.Name]).Difference(live.marked[np.Name]).Len()
	}
	byReason := lo.Assign(live.markedByReason[np.Name])
	for id, q := range l.reserved {
		if id != exclude {
			byReason[q.plan.Reason] += q.footprint.BudgetNodes[np.Name].Difference(live.marked[np.Name]).Len()
		}
	}
	return PoolUsage{
		NodePool: np.Name,
		Allowed: lo.SliceToMap(lo.Uniq(append(budgetReasons, reason)), func(rr v1.DisruptionReason) (v1.DisruptionReason, int) {
			return rr, np.MustGetAllowedDisruptions(l.clock, live.numNodes[np.Name], rr)
		}),
		Used:     d.Len() + used,
		ByReason: byReason,
		NumNodes: live.numNodes[np.Name],
		Reserved: r.Len(),
	}
}

func (l *ledger) reservedBudgetNodes(nodePool string, exclude uuid.UUID) sets.Set[string] {
	out := sets.New[string]()
	for id, r := range l.reserved {
		if id != exclude {
			out = out.Union(r.footprint.BudgetNodes[nodePool])
		}
	}
	return out
}

// limits checks every limited NodePool the footprint launches into. The sum is live usage (which excludes nodes
// MarkedForDeletion), plus the plan's own new NodeClaims minus the capacity of its own candidates in that NodePool
// that aren't MarkedForDeletion yet, plus for every other reserved plan with new NodeClaims there the same net, but
// never less than zero per resource: another reserved plan may still be rejected, and then its candidates stay. A
// plan's candidates are credited only for the resources its LimitCost names (resources.Subtract keeps the left
// side's keys), which is what FootprintOf puts there (the largest instance type's resources and nodes); that is
// conservative. The sum must not exceed the limits. exclude is the plan whose reservation fp replaces (Recheck,
// Amend).
func (l *ledger) limits(nodePools map[string]*v1.NodePool, live liveState, fp types.Footprint, exclude uuid.UUID) error {
	for name := range fp.LimitCost {
		np, ok := nodePools[name]
		if !ok || len(np.Spec.Limits) == 0 {
			continue
		}
		total := l.cluster.NodePoolResourcesFor(name)
		net := func(f types.Footprint) corev1.ResourceList {
			out := lo.Assign(f.LimitCost[name])
			for id := range f.Removed {
				if n, ok := live.nodes[id]; ok && n.nodePool == name && !n.marked {
					out = resources.Subtract(out, n.capacity)
				}
			}
			return out
		}
		for id, r := range l.reserved {
			if id == exclude || len(r.footprint.LimitCost[name]) == 0 {
				continue
			}
			total = resources.Merge(total, lo.OmitBy(net(r.footprint), func(_ corev1.ResourceName, q resource.Quantity) bool { return q.Sign() <= 0 }))
		}
		total = resources.Merge(total, net(fp))
		if err := np.Spec.Limits.ExceededBy(total); err != nil {
			return &RejectError{Reason: types.ReasonNodePoolLimit, Err: fmt.Errorf("nodepool %s, %w", name, err)}
		}
	}
	return nil
}

// capacityReservations checks that the reserved plans and this one launch no more into a capacity reservation than it
// has capacity for, as its offerings currently report (the minimum across NodePools).
func (l *ledger) capacityReservations(capacity map[string]int, fp types.Footprint, exclude uuid.UUID) error {
	for id, n := range fp.Reservations {
		if n <= 0 {
			continue
		}
		held := 0
		for pid, r := range l.reserved {
			if pid != exclude {
				held += r.footprint.Reservations[id]
			}
		}
		if c, ok := capacity[id]; !ok || held+n > c {
			return reject(types.ReasonCapacityReservation, "capacity reservation %s has capacity for %d, reserved plans hold %d, the plan needs %d", id, c, held, n)
		}
	}
	return nil
}

func (l *ledger) reservationCapacity(ctx context.Context, nps []*v1.NodePool) map[string]int {
	capacity := map[string]int{}
	for _, np := range nps {
		its, err := l.cloudProvider.GetInstanceTypes(ctx, np)
		if err != nil {
			log.FromContext(ctx).V(1).Info("skipping nodepool for capacity reservations, failed listing instance types", "NodePool", klog.KObj(np), "error", err.Error())
			continue
		}
		for _, it := range its {
			for _, o := range it.Offerings {
				if o.CapacityType() != v1.CapacityTypeReserved {
					continue
				}
				id := o.ReservationID()
				if c, ok := capacity[id]; !ok || o.ReservationCapacity < c {
					capacity[id] = o.ReservationCapacity
				}
			}
		}
	}
	return capacity
}

// reserveStatic takes the static node-count slots a footprint needs, all or none. It first takes them from credit
// (slots that plans being swapped out hold), then from the NodePool's headroom.
func (l *ledger) reserveStatic(nodePools map[string]*v1.NodePool, want map[string]int64, credit map[string]int64) (map[string]int64, error) {
	if credit == nil {
		credit = map[string]int64{}
	}
	held := map[string]int64{}
	taken := map[string]int64{}
	undo := func() {
		l.releaseStatic(taken)
		for name, n := range held {
			credit[name] += n - taken[name]
		}
	}
	for name, n := range want {
		fromCredit := min(n, credit[name])
		if fromCredit > 0 {
			credit[name] -= fromCredit
		}
		limit := int64(math.MaxInt64)
		if np, ok := nodePools[name]; ok {
			if q, ok := np.Spec.Limits[resources.Node]; ok {
				limit = q.Value()
			}
		}
		granted := int64(0)
		if n > fromCredit {
			granted = l.cluster.NodePoolState.ReserveNodeCount(name, limit, n-fromCredit)
		}
		taken[name] = granted
		held[name] = fromCredit + granted
		if held[name] < n {
			undo()
			return nil, reject(types.ReasonStaticNodeLimit, "static nodepool %s has room for %d of %d nodes", name, held[name], n)
		}
	}
	return held, nil
}

func (l *ledger) releaseStatic(held map[string]int64) {
	for name, n := range held {
		if n > 0 {
			l.cluster.NodePoolState.ReleaseNodeCount(name, n)
		}
	}
}

func (l *ledger) Recheck(ctx context.Context, id uuid.UUID) error {
	l.mu.Lock()
	r, ok := l.reserved[id]
	l.mu.Unlock()
	if !ok {
		return reject(types.ReasonCanceled, "plan %s is not reserved", id)
	}
	in, err := l.inputs(ctx, r.footprint)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if r, ok = l.reserved[id]; !ok {
		return reject(types.ReasonCanceled, "plan %s is not reserved", id)
	}
	return l.check(in, l.live(), r.footprint, r.plan.Reason, id)
}

func (l *ledger) Amend(ctx context.Context, id uuid.UUID, fp types.Footprint) error {
	in, err := l.inputs(ctx, fp)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.reserved[id]
	if !ok {
		return reject(types.ReasonCanceled, "plan %s is not reserved", id)
	}
	live := l.live()
	if err := l.exclusive(live, l.queue.InFlightDestinations(), fp, id); err != nil {
		return err
	}
	if err := l.check(in, live, fp, r.plan.Reason, id); err != nil {
		return err
	}
	if r.static != nil {
		// The plan's own slots are its credit; take or return only the difference.
		credit := lo.Assign(r.static)
		held, err := l.reserveStatic(in.nodePools, fp.StaticNodeCount, credit)
		if err != nil {
			return err
		}
		l.releaseStatic(credit)
		r.static = held
	}
	l.drop(id)
	r.footprint = fp
	l.add(r)
	l.bump()
	return nil
}

func (l *ledger) Swap(ctx context.Context, old []uuid.UUID, replans []*Plan) error {
	in, err := l.inputs(ctx, lo.Map(replans, func(p *Plan, _ int) types.Footprint { return p.Footprint })...)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	removed := map[uuid.UUID]*reservation{}
	credit := map[string]int64{}
	for _, id := range old {
		if r := l.drop(id); r != nil {
			removed[id] = r
			for name, n := range r.static {
				credit[name] += n
			}
		}
	}
	// The old plans' static slots are the replans' credit, so a same-size re-plan at the limit fits.
	initial := lo.Assign(credit)
	restore := func() {
		for _, p := range replans {
			if r := l.drop(p.ID); r != nil {
				// Slots beyond the credit came from the NodePool; return those, and put the credit back.
				for name, n := range r.static {
					fromCredit := min(n, initial[name]-credit[name])
					credit[name] += fromCredit
					if n-fromCredit > 0 {
						l.cluster.NodePoolState.ReleaseNodeCount(name, n-fromCredit)
					}
				}
			}
		}
		for _, r := range removed {
			l.add(r)
		}
	}
	live := l.live()
	inFlight := l.queue.InFlightDestinations()
	for _, p := range replans {
		if err := l.reserve(in, live, inFlight, p, credit); err != nil {
			restore()
			return err
		}
	}
	// Credit the replans didn't use goes back to the NodePools.
	l.releaseStatic(credit)
	l.bump()
	l.signal()
	return nil
}

func (l *ledger) MarkStarting(id uuid.UUID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if r, ok := l.reserved[id]; ok {
		r.static = nil
	}
}

func (l *ledger) MarkStarted(id uuid.UUID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := l.drop(id)
	if r == nil {
		return
	}
	l.bump()
	l.started = append(l.started, StartedFootprint{Version: l.version.Load(), PlanID: id, BatchID: r.plan.BatchID, Footprint: r.footprint, At: l.clock.Now()})
	l.prune()
	l.signal()
}

// prune drops the started footprints that no reserved plan can need: older than every reserved plan's ComputedAt, and
// older than StaleAfter (a plan computed but not yet submitted that is older is re-simulated anyway).
func (l *ledger) prune() {
	oldest := uint64(math.MaxUint64)
	for _, r := range l.reserved {
		oldest = min(oldest, r.footprint.ComputedAt)
	}
	cutoff := l.clock.Now().Add(-StaleAfter)
	l.started = lo.Filter(l.started, func(s StartedFootprint, _ int) bool { return s.Version > oldest || s.At.After(cutoff) })
}

func (l *ledger) StartedSince(v uint64) []StartedFootprint {
	l.mu.Lock()
	defer l.mu.Unlock()
	return lo.Filter(l.started, func(s StartedFootprint, _ int) bool { return s.Version > v })
}

func (l *ledger) Release(id uuid.UUID, _ types.RejectReason) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := l.drop(id)
	if r == nil {
		return
	}
	l.releaseStatic(r.static)
	l.bump()
	l.prune()
	l.signal()
}

func (l *ledger) Usage(ctx context.Context, reason v1.DisruptionReason) []PoolUsage {
	in, err := l.inputs(ctx)
	if err != nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	live := l.live()
	return lo.Map(lo.Values(in.nodePools), func(np *v1.NodePool, _ int) PoolUsage { return l.usage(live, np, reason, uuid.Nil) })
}

// liveState is what the Ledger reads from cluster state, in one pass under its read lock.
type liveState struct {
	// numNodes, marked and notReady count as disruption.NodePoolStatsFromNodes does: managed nodes that are initialized
	// or rebooting and not terminating.
	numNodes map[string]int
	marked   map[string]sets.Set[string]
	notReady map[string]sets.Set[string]
	// markedByReason counts the marked nodes by their NodeClaim's DisruptionReason condition.
	markedByReason map[string]map[v1.DisruptionReason]int
	// nodes are all managed nodes.
	nodes map[string]liveNode
}

type liveNode struct {
	nodePool string
	marked   bool
	capacity corev1.ResourceList
}

func (l *ledger) live() liveState {
	s := liveState{numNodes: map[string]int{}, marked: map[string]sets.Set[string]{}, notReady: map[string]sets.Set[string]{},
		markedByReason: map[string]map[v1.DisruptionReason]int{}, nodes: map[string]liveNode{}}
	for n := range l.cluster.Nodes() {
		if !n.Managed() {
			continue
		}
		name := n.Labels()[v1.NodePoolLabelKey]
		marked := n.MarkedForDeletion() || n.RebootInProgress()
		s.nodes[n.ProviderID()] = liveNode{nodePool: name, marked: n.MarkedForDeletion(), capacity: n.Capacity()}
		if !n.Initialized() && !n.RebootInProgress() {
			continue
		}
		if n.NodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() {
			continue
		}
		s.numNodes[name]++
		if marked {
			s.marked[name] = s.marked[name].Union(sets.New(n.ProviderID()))
			if s.markedByReason[name] == nil {
				s.markedByReason[name] = map[v1.DisruptionReason]int{}
			}
			reason := ""
			if c := n.NodeClaim.StatusConditions().Get(v1.ConditionTypeDisruptionReason); c != nil {
				reason = c.Reason
			}
			s.markedByReason[name][v1.DisruptionReason(reason)]++
		}
		if nodeutils.GetCondition(n.Node, corev1.NodeReady).Status != corev1.ConditionTrue {
			s.notReady[name] = s.notReady[name].Union(sets.New(n.ProviderID()))
		}
	}
	return s
}

// ledgerView is the Ledger's ReservationView.
type ledgerView ledger

func (v *ledgerView) Version() uint64 { return (*ledger)(v).Version() }

func (v *ledgerView) IsReserved(providerID string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	_, ok := v.removed[providerID]
	return ok
}

func (v *ledgerView) Unstarted() sets.Set[string] {
	v.mu.Lock()
	defer v.mu.Unlock()
	return sets.KeySet(v.removed)
}

func (v *ledgerView) Footprints() []types.Footprint {
	v.mu.Lock()
	defer v.mu.Unlock()
	return lo.Map(lo.Values(v.reserved), func(r *reservation, _ int) types.Footprint { return r.footprint })
}

func (v *ledgerView) InFlightDestinations() sets.Set[string] {
	return v.queue.InFlightDestinations()
}

func (v *ledgerView) StaticHeld(nodePool string) int64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return lo.SumBy(lo.Values(v.reserved), func(r *reservation) int64 { return r.static[nodePool] })
}

func (v *ledgerView) ReservationsHeld(reservationID string) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return lo.SumBy(lo.Values(v.reserved), func(r *reservation) int { return r.footprint.Reservations[reservationID] })
}

// ledgerBudgets is the Ledger's BudgetView.
type ledgerBudgets ledger

func (b *ledgerBudgets) Remaining(ctx context.Context, nodePool string, reason v1.DisruptionReason) int {
	l := (*ledger)(b)
	np := &v1.NodePool{}
	if err := l.kubeClient.Get(ctx, client.ObjectKey{Name: nodePool}, np); err != nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	u := l.usage(l.live(), np, reason, uuid.Nil)
	return max(u.Allowed[reason]-u.Used, 0)
}

func (b *ledgerBudgets) AnyRemaining(ctx context.Context, reason v1.DisruptionReason) bool {
	return lo.SomeBy((*ledger)(b).Usage(ctx, reason), func(u PoolUsage) bool { return u.Allowed[reason] > u.Used })
}
