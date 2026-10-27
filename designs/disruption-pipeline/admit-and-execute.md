# Admit and Execute: lifecycle, Ledger, Admitter, commit loop, Queue dependencies, Observer

Appendix B of [Disruption Pipeline](../disruption-pipeline.md). Scope: everything from `Executor.Submit` to the
command finishing in the `disruption.Queue`, plus the Observer. [Appendix A](observe-and-plan.md) covers how plans are
made.

Code references are `pkg/...` paths at the tip of the implementation stack; `mainline:` marks kubernetes-sigs/karpenter
@ `6eac93ba`. `pipeline/` and `disruption/` mean `pkg/controllers/disruption/{pipeline,}/`.

## 1. Plan lifecycle

```
            Submit                ValidateAt             Admit                 Start                Queue
 computed ──────────► reserved ─────────────► ready ───────────► admitted ──────────► started ──────────► done
  (lane)    Ledger.     (slot held,   (commit loop)  (rebuild, recheck,  (Queue.StartCommand,  (replacements and
            Reserve     waiting)                      relations, re-sim) MarkStarted releases)  dependencies initialized
               │                         │                   │                    │              → delete candidates;
               └──► rejected ◄───────────┴──── rejected ◄────┘   start_failed ◄───┘                else roll back)
                    (released at once)        dropped (deadline, lease lost)
```

| state | held in the Ledger | visible to other lanes' simulations as | who moves it on |
|---|---|---|---|
| computed | nothing | nothing | the lane calls `Submit` |
| reserved, waiting | `BudgetNodes`, `LimitCost`, static slots, `Removed` in `IsReserved`, `Destinations` | candidates are unavailable destinations; their pods are **not** re-placed | the commit loop at `ValidateAt`, or when `Synced` |
| admitted | same, possibly amended or swapped | same | the Starter, in the same step |
| started | released after `StartCommand` returned (`MarkStarted`); footprint kept in the started log | MarkedForDeletion: pods re-placed as today | the Queue |
| rejected, dropped | released at once; static slots returned | gone | terminal; one `Resolution` delivered |

Every submitted plan gets exactly one `Resolution` (`Started`, `Rejected`, `Dropped`, with a typed reason), then its
channel closes (`pipeline/executor.go:28-58`). A plan that admission replaced (adopted re-simulation, sibling re-plan)
resolves `Started` if any plan that replaced it started, else with the first failure's reason.

**Invariants** (test targets):

| # | invariant | where it is enforced | pinned by |
|---|---|---|---|
| I1 | for every NodePool and reason, disrupting ∪ reserved plus charges in flight never exceeds `allowed` at the last Reserve or recheck | `ledger.budget` under one mutex | 10 concurrent Reserves for 5 slots → exactly 5; both lanes and the commit loop running concurrently under `-race` with the budget asserted after every step |
| I2 | a node is in at most one reserved or started plan's `Removed`, never also queued or MarkedForDeletion | `ledger.exclusive` | the concurrent spec with an intruder planner that plans started commands' nodes; mutating `exclusive` to return nil fails it |
| I3 | no Start runs concurrently with the taint sweep | the sweep is a step of the commit loop | a sweep that comes due while a start is blocked between taint and queue insert waits for it |
| I4 | a reservation is released only after MarkForDeletion is visible, or after rejection or drop | `MarkStarted` after `StartCommand` returns | executor specs |
| I5 | a plan that isn't pod-free is admitted only while `cluster.Synced()` | Admitter step 2 | admitter specs |
| I6 | each planner has at most one unresolved Submit | lane back-pressure, plus `backpressure` rejection in Submit | lane and executor specs |

### 1.1 Timeline example

2,000 nodes, one NodePool, budget 10% = 200, an AMI rollout plus consolidation demand. Timings are the shape KWOK
showed at this size on 4 CPU (a joint simulation ~6 s), not a guarantee.

```
t=0     Fast round: snapshot; repair, emptiness, static drift have nothing; drift: K=50 (first batch) →
        one joint simulation, groups of 10 → Split → ≥ 5 plans of ≤ 10 candidates, one BatchID
t≈6     Submit → Ledger.Reserve 50 slots (first come, first served); drift is now unresolved
        commit loop: ValidateAt = 0, Synced, nothing started since ComputedAt → no re-simulation
        → admit the batch (one rebuild, one RecheckAll) → StartCommand per plan, in parallel → MarkStarted
t≈7     Synced=false (replacements have no providerID yet). Fast runs again: drift holds non-empty candidates
        (only pod-free plans while unsynced), polls every 1 s. Background is gated `synced`.
t≈9     Synced=true → wake → Fast round: the sizer grew K (marginal cost small) → 150 candidates → ≥ 15 plans
t≈16    Background: no Underutilized budget left in any NodePool (200 of 200 disrupting or reserved) → the lane is
        gated `budget_zero` before it builds a snapshot; `DisruptionBudgetExhausted` is published once per planner
t≈300   first commands finish → nodes leave the disrupting count → Fast, polling every 1 s while budget-bound, refills
t≈...   while slots free up one by one, consolidation competes for them first come, first served; with
        {5%, [Drifted]} + {10%}, drift stops at 5% and the other 5% stays open to it
```

## 2. Ledger

The Ledger (`pipeline/ledger.go`) is bookkeeping for the sums no footprint overlap can see: the budget cap, NodePool
limits, static node counts and capacity reservations. It holds plans from Reserve until they start or are released,
across lanes, under one mutex. **No budget semantics change**: budgets still cap the total number of nodes disrupting
in a NodePool for any reason; reserved, unstarted plans are added to that total. It is disruption-local; the
provisioner never sees a reservation.

```go
// pipeline/ledger.go:126-158
type Ledger interface {
	Version() uint64
	Policy() AllocationPolicy
	Reserve(ctx context.Context, plans ...*Plan) []error     // in order; within a batch, after the first misfit the rest get its reason
	Recheck(ctx context.Context, id uuid.UUID) error         // live, excluding the plan's own reservation
	RecheckAll(ctx context.Context, ids ...uuid.UUID) []error // several plans against one reading of live state
	Amend(ctx context.Context, id uuid.UUID, fp types.Footprint) error // a re-simulation changed the footprint
	Swap(ctx context.Context, old []uuid.UUID, replans []*Plan) error  // all or nothing
	MarkStarting(id uuid.UUID)  // hand static slots to the Queue
	MarkStarted(id uuid.UUID)   // log the footprint, release the reservation
	StartedSince(v uint64) []StartedFootprint
	Release(id uuid.UUID, reason types.RejectReason)
	Usage(ctx context.Context, reason v1.DisruptionReason) []PoolUsage
	View() ReservationView
	Budgets() BudgetView
	Released() <-chan struct{}
}
```

### 2.1 Budget cap

Today (`mainline: helpers.go:370-413`, `NodePoolStatsFromNodes`): `numNodes` counts managed nodes that are initialized
or rebooting and not `InstanceTerminating`; disrupting = MarkedForDeletion ∪ RebootInProgress ∪ (NotReady ∧ reason ≠
Unhealthy); `remaining = allowed − disrupting`, and each method then decrements it once per candidate. The Ledger keeps
exactly this, as node-ID sets (`ledger.go:378-430`):

```
M_live(np)     = { n ∈ np : MarkedForDeletion(n) ∨ RebootInProgress(n) }
D_live(np, r)  = M_live(np) ∪ { n ∈ np : NotReady(n) }   if r ≠ Unhealthy, else M_live(np)
R(np)          = ⋃ BudgetNodes[np] over reserved, unstarted plans (all reasons), except the plan being rechecked
Used(np, r)    = |D_live(np, r) ∪ R(np)|  +  |R(np) ∩ NotReady(np) \ M_live(np)|        (second term: r ≠ Unhealthy)
charge(p, np)  = |BudgetNodes_p[np] \ (R(np) ∪ M_live(np))|
allowed(np, r) = np.MustGetAllowedDisruptions(clock, numNodes_live(np), r)
Reserve ok ⇔ ∀ np ∈ BudgetNodes_p: Policy.Allow(PoolUsage{Used, Allowed, ...}, p.Reason, charge) 
FirstComeFirstServed.Allow ⇔ Used + charge ≤ Allowed[reason]
```

- **One count across the hand-off.** A node is in `R` while reserved and in `M_live` once marked; the union counts it
  once, so the count can only drop at the hand-off.
- **NotReady candidates are charged**, as today's per-candidate decrement charges them even though they are already
  disrupting. A reserved NotReady candidate keeps that second unit in `Used`, so Reserve, Recheck and later plans agree.
- **Live.** `numNodes`, `M_live` and `D_live` come from one pass over `cluster.Nodes()` (`ledger.live`,
  `ledger.go:782-816`); `allowed` uses the live clock, so cron windows and the growing node count during a rollout are
  current.
- **Own reservation excluded at recheck.** Today's validation checks `remaining == 0` (`mainline: validation.go:243,287`),
  which would always fail for a plan that holds its own slot.
- **Parity.** With nothing reserved, `Remaining` and `AnyRemaining` equal `disruption.BuildDisruptionBudgetMapping` for
  four reasons across marked, NotReady and `InstanceTerminating` states (`ledger_test.go`).

Repair's exception stays with the planner: uninitialized repair candidates carry no `BudgetNodes`, as today.

### 2.2 NodePool limits

`ledger.limits` (`ledger.go:450-490`) checks every limited NodePool the footprint launches into:

```
total = NodePoolResourcesFor(np)                       live usage, excludes MarkedForDeletion
      + Σ over other reserved plans q with LimitCost[np]: max(0, LimitCost_q[np] − capacity of q's unmarked candidates in np)
      + LimitCost_p[np] − capacity of p's own unmarked candidates in np
reject nodepool_limit if np.Spec.Limits.ExceededBy(total)
```

A plan's candidates are credited for its own replacements: otherwise a same-size drift at the limit would be blocked,
and today it is allowed. Another reserved plan's freed capacity is **never** a credit: that plan may still be rejected,
and then its candidates stay. Crediting it let a pool exceed its limit (reproduced: 11 nodes against
`limits.nodes=10`). The binding check is the admission recheck, which runs only while synced, because another
unlaunched NodeClaim may be missing from live usage at Reserve. `LimitCost` charges each new NodeClaim at its largest
instance-type option plus one node: conservative.

### 2.3 Static node counts

Today `ReserveNodeCount` is taken in compute, and its grant picks the shape (replace first, or terminate first on a
grant of 0). In the pipeline:
1. **Planning reads.** The planner's effects return `min(n, NodePoolState.Headroom(np, limit) − StaticHeld(np))`
   without writing (`pipeline/planners.go:57-63`; `state/statenodepool.go:161`).
2. **Reserve takes the slots** (`ledger.reserveStatic`, `ledger.go:535-570`). A short grant is returned and the plan
   rejected `static_node_limit`. Plans are per candidate, so there is no partial trim.
3. **Ownership moves at Start.** `MarkStarting` hands the slots to the Queue, which releases them on its own paths.
   `Swap` and `Amend` credit the replaced plan's own slots, so a same-size static re-plan at the limit succeeds and a
   failed Swap restores them.

### 2.4 Capacity reservations

`ledger.capacityReservations` (`ledger.go:492-508`) sums signed reservation counts of reserved plans against the
latest offering capacity (minimum across NodePools), so two plans can't both count on the last slot. **`FootprintOf`
doesn't derive `Reservations` yet** (it needs a `ReservationManager` export), so this sum sees nothing and behaves as
today's loop: no cross-simulation accounting, and a simulation that loses the last slot falls back to on-demand.

### 2.5 Ownership

`ledger.exclusive` (`ledger.go:343-384`) rejects `conflict` when a plan:
- disrupts a node that is MarkedForDeletion, in the orchestration queue, a destination or dependency of a started
  command (`Queue.InFlightDestinations`), disrupted by another reserved plan, or a destination of another reserved plan;
- moves pods onto a node that is MarkedForDeletion or disrupted by a reserved plan.

It runs at Reserve, Swap and Amend, both ways. This replaces today's non-atomic `HasAny`-then-insert in the Queue for
pipeline plans.

### 2.6 Batches, Amend, Swap, the started log

- **Prefix fit.** Within one `BatchID`, once a plan doesn't fit, the later plans of the batch get its reason, and the
  batch's survivors are flagged `SiblingRejected` (which adds the `sibling` re-simulation cause at admission).
- **Amend** replaces a reserved plan's footprint after an adopted re-simulation, if the new footprint fits.
- **Swap** replaces reserved plans with re-planned ones, all or nothing: it drops the old reservations, reserves the new
  ones with the old static slots as credit, and restores everything if any fails.
- **Started log.** `MarkStarted` appends the footprint at the new Ledger version. `StartedSince(v)` feeds the `coupled`
  re-simulation cause. Entries are pruned when older than every reserved plan's `ComputedAt` **and** older than
  `StaleAfter + MaxPlanTime` (40 s), which covers a plan computed but not yet submitted when its coupling partner
  started.
- **Lock scope.** NodePools, ODCR capacities and cluster lists are read before the mutex; `IsReserved` uses a node-ID
  index.

### 2.7 Tests

`pipeline/ledger_test.go` (envtest, `-race`): the cap across reasons, first come first served; the per-reason pattern
`{2, [Drifted]}` + `{4}`; NotReady counted except for Unhealthy, and a NotReady candidate still charged; one count
across the hand-off; recheck without the own reservation and with a budget that shrank, including a scheduled window
closing on the fake clock; batch prefix fit; 10 concurrent Reserves for 5 slots give exactly 5; parity with
`BuildDisruptionBudgetMapping`; every ownership conflict, both ways and at Amend; release, started log and pruning;
Swap all or nothing, at a static limit, and restoring on failure; limits that credit own candidates and add other
reserved plans but never borrow from them; static slots taken, a short grant returned, not released once the Queue
owns them; the last ODCR slot; dependencies of reserved plans and started commands protected. Each review regression is
mutation-checked: the spec fails without its fix.

### 2.8 Allocation policy

```go
// pipeline/ledger.go:79-111
type AllocationPolicy interface {
	Name() string
	Allow(pool PoolUsage, reason v1.DisruptionReason, charge int) bool // pure, under the Ledger lock
}
type PoolUsage struct {
	NodePool string
	Allowed  map[v1.DisruptionReason]int // every budget reason, at the live node count and clock
	Used     int                         // as in 2.1, reason-agnostic
	ByReason map[v1.DisruptionReason]int // reserved plans by reason + marked nodes by their DisruptionReason condition
	NumNodes int
	Reserved int
}
```

`FirstComeFirstServed` is today's behaviour and the only policy. A fair-share policy would also need a demand signal
(which planners have eligible work), which the Ledger doesn't collect yet, so adding one is not free of Ledger
changes. `GetAllowedDisruptionsByReason` takes the minimum over the budgets that apply; with `{5%, [Drifted]}` +
`{10%}`, drift sees 5% and consolidation sees 10%, and because the disrupting count is shared, at least 5% stays usable
by other reasons while drift is saturated. At 1,000 nodes: Drifted remaining = min(50, 100) − disrupting;
Underutilized and Empty remaining = 100 − disrupting.

## 3. Admitter

The Admitter (`pipeline/admitter.go`) is the serial point where a plan becomes a command. It is called only from the
commit loop, starts no goroutines, and writes only to the Ledger, after the whole group is decided, so a waiting batch
leaves no partial state.

### 3.1 Steps

`Admit(ctx, group []*Pending, step *CommitStep)` (`admitter.go:125-243`), for one plan or the ready plans of one batch:

1. **Deadline** (whole group). Any member more than `MaxAdmissionWait` (1 min) past its ready time → every member
   dropped `deadline`, budget released.
2. **Synced gate** (whole group). While unsynced, if any member isn't pod-free, the group waits.
3. **One rebuild for the group** (`CandidateRebuilder.Rebuild` over every member's providerIDs, `rebuilder.go:81-111`):
   the disruption controller's class rules on live state, copying only those state nodes. A rebuild error rejects
   every member with it.
4. **One `Ledger.RecheckAll`** for the group, against one reading of live state.
5. **Per plan** (`admitOne`, `admitter.go:283-341`), on the rebuild:
   - its own candidates' rebuild rejections (below);
   - planner `Filter` on the fresh candidate → `candidate_ineligible`;
   - pod-set hash: changed → consolidation rejects the candidate `pods_changed`; drift and repair add the
     `pods_changed` cause;
   - while unsynced, a plan no longer pod-free on its fresh candidates (a pod bound meanwhile) waits;
   - the recheck result (`budget`, `nodepool_limit`, `capacity_reservation`);
   - hard relations and re-simulation causes (3.2);
   - a planner that always re-simulates waits if any other batch was admitted earlier in this step (its validator sees
     only live state, and those plans haven't started);
   - drift and repair members that need a re-simulation are collected for one joint re-simulation (3.4); others call
     the planner's `Revalidate`;
   - a result related to a plan admitted earlier in this step waits for the next step (rule 9).
6. **Group decision**: if any member waits, the whole batch waits; joint re-simulation of the collected members; if a
   member was rejected, sibling re-plan of the survivors (3.4).
7. **Commit**: one plan → `Amend` (same identity) or `Swap`; a batch → one `Swap` of all survivors. If it fails, the
   first survivor carries the reason and the others are `sibling_rejected`.

Rebuild rejections (`rebuilder.go:113-122`):

| check | source today (mainline) | reason |
|---|---|---|
| exists; not MarkedForDeletion; not queued; not rebooting | `types.go:154` | `candidate_invalid` |
| nominated for a pending pod | `validation.go:239,283` | `candidate_nominated` |
| node or pod do-not-disrupt, PDB (per class) | `statenode.go:286,328` | `candidate_blocked` |

Errors that aren't verdicts map to `canceled` (the step's context is done) or `error` (API or provider error), not
`candidate_invalid`.

### 3.2 Hard relations and re-simulation causes

`Relate(a, b)` (`pipeline/relate.go:66-94`) classifies two footprints. NodePool membership and the Ledger's sums are
**not** relations; otherwise every plan in a one-NodePool cluster would re-simulate.

| relation | kind | meaning |
|---|---|---|
| `same_candidate` | hard | both disrupt a node |
| `removes_destination` | hard | a disrupts a node b moves pods onto: a re-simulation of a can't see the pods b has yet to move |
| `removes_dependency` | hard | a disrupts a node b waits on before deleting its candidates |
| `destination_removed` | coupled | b disrupts a node a moves pods onto |
| `shared_destination` | coupled | both move pods onto a node |
| `shared_device` | coupled | both use a cluster-scoped DRA device (not derived yet) |
| `shared_topology` | coupled | both move pods of a coupling key (namespace over-approximation) |
| `shared_pending` | coupled | both size a new NodeClaim for the same pending pod |

`causes` (`admitter.go:344-379`): a plan that disrupts an in-flight destination is rejected `conflict`; for every plan
started since `Footprint.ComputedAt` (siblings can't be among them, a batch starts as a unit), a hard relation rejects
`conflict` and a coupling adds `coupled`. Plans computed after q started need nothing for q: their simulation already
saw q as deleting.

| cause | test | why |
|---|---|---|
| `always` | multi, single | parity with today's validation re-simulation |
| `coupled` | related to a plan started since this plan's simulation | that start changed something this simulation read |
| `stale` | > `StaleAfter` (10 s) since **submission**, or > `StaleAfter + MaxPlanTime` (40 s) since the round started | pods bound and the provisioner launched since; the second bound matches the started log's retention |
| `catalog` | a touched NodePool's catalog version changed | prices, offerings or reservations changed with no core event |
| `pods_changed` | a candidate's pod-set hash changed (drift, repair) | the plan's own pods moved |
| `sibling` | a batch member was rejected at Reserve | Split hazard |
| `pending` | declared, not produced yet | needs pending-pod tracking |

Staleness counts from submission because a joint simulation at 2,000 nodes takes 5-7 s: stamping plans with the
round's start made every plan of a batch stale on arrival. On KWOK that was 141 of 151 drift admission re-simulations,
each run alone, serializing the batch.

### 3.3 Per-planner acceptance

```go
// pipeline/admitter.go:31-56
type Revalidator interface {
	AlwaysResimulate() bool
	Revalidate(ctx context.Context, in RevalidateInput) ([]*Plan, error)
	Replan(ctx context.Context, survivors []*Plan, in RevalidateInput) ([]*Plan, error)
}
type JointRevalidator interface {
	RevalidateJointly(ctx context.Context, ins []RevalidateInput) ([]*Plan, error)
}
```

| planner | re-simulates | accepts if |
|---|---|---|
| emptiness | through its validator | still empty and valid; may shrink; failed validations counted as today's validator counts them |
| static drift | never | no candidate rejected |
| drift, repair (replace) | on any cause; jointly for a batch | the re-simulation places the candidates; the plan **adopts** its replacements, which is what serial planning would launch at that instant, and the Ledger is amended |
| repair (reboot), terminate-first | never | no candidate rejected; the Starter re-reads the NodeClaim |
| multi, single | always, with the method's validator | today's command shape check (`validation.go` semantics), all or nothing; balanced approval is not re-applied, as today |
| planner without a `Revalidator` | never | unchanged plans only; a needed re-simulation rejects `resim_changed` |

Accepted plans keep what the planner stamped on the planned candidates (repair's drain bound) on the fresh ones
(`planners.go:145-154`).

**Re-simulation options.** A re-simulation plans exactly the given candidates, so the batch sizer doesn't size it (with
the sizer, a stale batch of 12 was re-run at the sizer's current K of 2 and rejected). Its deadline is still the
sizer's (`resimOptions`, `planners.go:349-356`), which grows with the measured fixed cost: with the 2 s constant,
re-plans at 2,000 nodes timed out, fell back to one candidate, and rejected the survivors (414 `sibling_rejected`
against 1,694 admitted in one KWOK rollout). With both fixes: 0 `sibling_rejected` and 0 `catalog` re-simulations in
the next two runs.

### 3.4 Siblings, joint re-simulation and partial acceptance

Commands of one joint simulation are correct only together ([A §7](observe-and-plan.md#7-candidate-groups-and-split)).
In the zone-spread counter-example, admitting one piece after rejecting the other leaves skew 2. So:

- **A batch is admitted as a unit.** If any member waits, all wait.
- **Joint re-simulation.** Members that need a re-simulation and have no rejected candidate (and aren't reboots or
  terminate-first) are re-simulated **once, together**, on their fresh candidates (`RevalidateJointly`). The first
  such member carries the resulting plans, so the batch's `Swap` starts each once.
- **Sibling re-plan.** If a member was rejected, the survivors (including members collected for the joint
  re-simulation) are re-planned once with one joint simulation (`Replan` + `Ledger.Swap`), or none starts
  (`sibling_rejected`). The prototype's fix took skew 2 → 0 and 1 → 0 extra NodeClaims on the hazard specs.
- **Partial acceptance.** A joint re-simulation or re-plan may accept only some candidates. Their plans are consistent
  on their own (the accepting simulation removed exactly them), so they start and the rest return next round. Each
  member maps only to the started plans holding one of its candidates (`replaceOrDefer`, `admitter.go:418-428`); a
  member with none is rejected `deferred`, doesn't resolve `Started`, doesn't count as admitted, and doesn't mark its
  NodePool served. A plan re-simulated alone stays all or nothing; a re-simulation that plans nothing is
  `resim_unschedulable`.
- **Rule 9 on the joint path.** A joint or re-planned result related to a plan admitted earlier in the step makes the
  whole group wait, with nothing changed in the Ledger.

### 3.5 One rebuild per batch

The first Admitter rebuilt each plan's candidates on its own. On KWOK (the stack through #88, ~2,215 nodes, 4 CPU, 180 s drift
CPU profile) `Admit` took 73.6 s:

| per plan, before | CPU in the profile |
|---|---|
| `Rebuild` → `NewCandidateSnapshot` → `BuildNodePoolMap` → AWS `GetInstanceTypes` (NodePools, instance types, PDBs) | 42.7 s |
| `Rebuilt.CatalogVersion` (order-independent fingerprint: a sort and format per offering) | 12.6 s |
| `Ledger.Recheck` → `live()` (a pass over every node) | 11.2 s |

Admission took 9.8 s per batch (440 s over 45 batches); re-simulations were 7 s of the 73.6 s. Now `Admit` rebuilds
the group's candidates once (each plan sees only its own rejections), memoizes the catalog version per NodePool on
that rebuild, and rechecks the group with one `RecheckAll`. Microbenchmark (`GetInstanceTypes` slowed to 50 ms, one
batch of 20 single-node plans): 1.12 s with 20 `GetInstanceTypes` calls → 79 ms with 1. On KWOK round 3 (~2,100-2,240
nodes): admission per batch p50 0.27-0.38 s, p99 8.5-13.1 s, sum 80-95 s over 68-95 batches per rollout.

## 4. Commit loop

`commitLoop` (`pipeline/commitloop.go`) implements `Executor` and is the one serialized writer.

**Submit** (`commitloop.go:125-169`), in the lane's goroutine: after lease loss, rejects `canceled` (under the lock the
final drop takes, so a plan is either rejected here or dropped there); a planner with unresolved plans gets them all
back `backpressure`; otherwise `Ledger.Reserve` in order. A plan that doesn't fit resolves `Rejected` at once with
`AtSubmit` set (it wakes nothing) and flags its batch's other plans `SiblingRejected`. Reserved plans join the waiting
set; `Ready()` signals the commit runner.

**Step** (`commitloop.go:185-258`):
1. on a cancelled context, drop every waiting plan `canceled` and return;
2. the sweep, if due (section 5);
3. ready plans (ready time = max(`ValidateAt`, submission)) grouped by batch, ordered by the `Scorer`: lane order,
   then planner order, then `ValidateAt`, `ComputedAt` and ID (`starter.go:61-102`; a tie-breaker, not fairness);
4. `Admit` per group; released, waiting or admitted per plan; admitted plans join `step.Admitted`;
5. if the context was cancelled during admission, admitted plans are dropped, not started;
6. start the admitted plans **in parallel** (`commitloop.go:298-330`); they are pairwise unrelated by rule 9. Each start
   calls `MarkStarting`, then `Starter.Start`, then `MarkStarted` (or `Release` with `start_failed`);
7. a failed start triggers the sweep at once (its candidates may carry a taint).

The step returns admitted, rejected and waiting counts and the earliest future ready time, which the runners use to
pace. A start that began runs to completion under `context.WithoutCancel`: a half-started command is worse than one
more. There is no start timeout below the lease renew deadline (an open question).

## 5. Starter, reboots and the taint sweep

**Starter** (`pipeline/starter.go:43-59`) is `Queue.StartCommand`, unchanged: it taints the candidates, creates
replacements before MarkForDeletion, and inserts the command into the queue, exactly as for the disruption
controller's commands. `Plan.ToCommand` stamps `CreationTimestamp = now`, so the time a plan waited for admission
doesn't count against the queue's retry window.

**Reboots.** A pipeline plan can start seconds to minutes after planning, so its handoff must not patch the NodeClaim
read at planning. `disruption.PrepareReboot(ctx, &cmd)` (`disruption/pipeline_seams.go:114`) re-reads the NodeClaim and
builds today's handoff on it (two Patches, `UpdateNodeClaim`, history on success); a node that started rebooting or
deleting meanwhile rejects the plan `candidate_invalid`. A stale object would be safe (optimistic lock conflict) but
would waste the slot.

**Taint sweep.** Today the loop untaints and clears the `DisruptionReason` condition of nodes not in the queue at the
top of every pass; `StartCommand` taints before it inserts into the queue, and both run on one goroutine. A tier-1 PR
made the sweep read only nodes that carry the taint or condition in cluster state (`disruption/sweep.go`, 0 Gets in
steady state; −92% of sweep time per drift iteration on the scale benchmark) and takes the keep predicate as a
parameter. In the pipeline the sweep is a step of the commit loop (`commitloop.go:378-402`), so it never runs
concurrently with a start, and its keep predicate also keeps nodes of reserved plans:

```go
keep := func(n *state.StateNode) bool {
	return queue.HasAny(n.ProviderID()) || n.MarkedForDeletion() || n.RebootInProgress() || reserved.IsReserved(n.ProviderID())
}
```

It runs at the first step, every `SweepTick` (1 min) and right after a failed start. Candidates of every start are
registered with `Sweeper.Recheck` first, so marks written through the API server but not yet seen by cluster state
are re-read until a sweep succeeds.

**Synced gate scope.**

| plan | admitted while unsynced |
|---|---|
| replace (drift, repair, static drift, consolidation replace) | no |
| delete with any reschedulable pod (multi/single delete, drift delete-only, terminate-first) | no |
| reboot | no (it drains pods) |
| **pod-free delete**: no replacements, no reschedulable pods on the rebuilt candidates (emptiness, empty drifted nodes) | **yes** |

A pod-free delete's validity doesn't depend on unlaunched capacity. Its only coupling is a pending pod nominated onto
the node, which the rebuild's nomination check catches. Exempting pod-moving deletes would need the virtual-node
overlay. `Synced` stays global, so provisioner bursts also gate replacing plans.

## 6. Queue dependencies

The `disruption.Queue` is shared by both controllers. `Command.Dependencies []string` (`disruption/types.go:263-266`)
holds the providerIDs of uninitialized nodes the command's own candidate pods were simulated onto; it is empty for
every gate-off command.

**Waiting** (`queue.go:203-260`, `waitForDependencies` at `queue.go:308-354`). `waitOrTerminate` deletes the candidates
only once every replacement **and** every dependency is initialized. A dependency is looked up as a replacement is:
cluster-state providerID → NodeClaim name, then a Get; NotFound and absent from cluster state means deleted.
Initialization isn't cached; every dependency is rechecked until the delete.

**Failure.** A dependency that is gone, being deleted (launch failure, registration or initialization TTL), or not
initialized when the command times out (`GetMaxRetryDuration`, the replacement timeout) fails the command through the
existing unrecoverable path: candidates untainted, `DisruptionReason` cleared, `UnmarkForDeletion`, completion hooks
(in the pipeline: a failed resolution and the drift NodePool back-off). A deleted dependency fails the command even
while its own replacements are still initializing. The timeout error is now `CommandTimeoutError` inside the same
unrecoverable error, with the same message.

**Protection.** Dependencies are in-flight destinations (`Queue.InFlightDestinations`, `pipeline_seams.go:94-107`), so
the snapshot drops them as candidates for every planner and the Ledger and Admitter reject plans that disrupt them.
`Footprint.Dependencies` (also merged into `Destinations`) protects them while the depending plan is only reserved;
`Relate` reports `removes_dependency`.

**Metrics** (alpha, `disruption/metrics.go:266-290`):
- `karpenter_voluntary_disruption_queue_commands_waiting_on_dependencies` (gauge), exported only once the queue has
  seen a command with dependencies;
- `karpenter_voluntary_disruption_queue_dependency_failures_total{decision,reason,consolidation_type,cause}` with
  `cause=deleted` (a dependency is gone or being deleted) or `timeout` (the command timed out while a dependency was
  still uninitialized, with no other unrecoverable failure). A command that failed for another reason isn't counted.

**Gate-off identity.** Legacy commands have no dependencies: `waitForDependencies` returns at once,
`InFlightDestinations` is unchanged, and neither series is exported. A trace of 4 Queue scenarios (events, NodeClaim,
taint, cluster-state and queue state, every `karpenter_*` series) is byte-identical, 134 lines, before and after the
change. Specs (`disruption/queue_dependencies_test.go`, 8): waits then deletes; waits for replacements and
dependencies; dependency deleted → rollback, candidate untainted, `cause=deleted`; deleted while the replacement
initializes → rollback; being deleted (finalizer held) → immediate rollback; replacement deleted while a dependency
waits → no dependency failure counted; not initialized by the timeout → rollback, `cause=timeout`; legacy command → no
series. Mutations: a no-op `waitForDependencies` fails 5 of the first 6.

**Known gap.** The pipeline's Starter builds the command with `Plan.ToCommand` (`pipeline/planner.go:150-161`), which
doesn't copy the core command's `Dependencies` (the plan keeps them only in `Footprint.Dependencies`). So a started
pipeline command doesn't wait for its dependencies at the Queue and isn't rolled back by them; the planning-side
protection still applies, and the Queue side is only tested directly. Until fixed, a candidate can be deleted while
the node its pods were planned onto is still initializing: the pods go pending until it is ready (no
double-provisioning, since the provisioner counts the in-flight node), and a dependency that fails doesn't roll the
command back. The fix is to carry `Dependencies` on the plan into `ToCommand`, plus an end-to-end gate-on spec through
the Starter. KWOK round 3's zero dependency waits and failures are a consequence of this gap, not evidence that
dependencies resolve quickly.

## 7. Cancel, restart and leadership

- **Lease loss.** Lanes stop; the commit runner's deferred final step drops every waiting plan `canceled`. Reservations
  are memory only, so nothing is left behind. Submit after lease loss reserves nothing.
- **Restart.** Started commands are recovered as today: MarkedForDeletion from deletion timestamps, leftover taints via
  the first sweep, orphan replacements consolidated later. Sizer state, fairness and dedup restart from scratch.
- **2 CPU.** On KWOK both builds lost the leader lease during 12,500-pod provisioning bursts at 2 CPU
  ([Appendix C §6.4](rollout-validation-alternatives.md#64-two-cpu-controllers-and-the-leader-lease)); the pipeline's
  disruption work was a smaller CPU share than mainline's in both windows measured.

## 8. Observer: metrics, events and logs

The Observer (`pipeline/observe.go`) is the only publisher of the pipeline's metrics and events. All series below are
`karpenter_voluntary_disruption_pipeline_<name>`, alpha, gate-on only, with label values declared as
`opmetrics.Value`s (`pipeline/metrics.go`).

| name | type | labels | meaning |
|---|---|---|---|
| `round_duration_seconds` | histogram | `lane`, `phase={snapshot,plan,admit}` | where a round's time goes |
| `rounds_total` | counter | `lane`, `result` | rounds that planned, were no-ops or were gated |
| `gated_seconds_total` | counter | `lane`, `gate={synced,budget_zero,unresolved,paced,hydrating}` | why a lane or planner didn't plan; a round's interval counted once per (lane, gate) |
| `planner_duration_seconds` | histogram | `planner` | candidates + `Plan` |
| `admission_duration_seconds` | histogram | `planner` | one `Admit` of a plan or batch |
| `plan_wait_seconds` | histogram | `lane`, `planner` | submission → start |
| `admission_resims_total` | counter | `planner`, `cause` | counted only for plans that leave admission |
| `plans_total` | counter | `planner`, `result`, `reason` | admitted, rejected, dropped, by reject reason |
| `unresolved_plans` | gauge | `lane` | submitted, not yet resolved |
| `ledger_reserved_nodes` | gauge | `nodepool` | budget held by unstarted plans |
| `last_admitted_timestamp_seconds` | gauge | `planner` | starvation detector |
| `taint_sweep_duration_seconds` | histogram | | sweep cost |
| `planned_candidates` | histogram | `planner`, `stage={eligible,planned,deferred}` | how far a round's candidates got |
| `plans_per_round` | histogram | `planner` | plans of a round that planned |
| `admitted_batches_total` | counter | `planner`, `result={started,rejected,waited,dropped}` | admission per batch (a lone plan is a batch of one) |
| `joint_simulations_total` | counter | `planner`, `result={accepted,none,timed_out}` | how each joint simulation ended |
| `joint_simulation_candidates` | histogram | `planner`, `stage={first,accepted,deferred}` | first K, accepted, deferred |
| `joint_simulation_dropped_candidates_total` | counter | `planner`, `drop_reason={pods_unschedulable,halved,uninitialized_destination}` | the last value is reserved, no longer produced |
| `joint_simulations_chunked_total` | counter | `planner` | the defensive chunk path ran |
| `joint_simulation_duration_seconds` | histogram | `planner` | the accepting simulation |
| `shadow_comparisons_total`, `shadow_mismatches_total`, `shadow_agreement_ratio` | counter, gauge | `level`, `planner`, `kind`, `cause` | shadow mode only |

Label values: `lane` = `fast`, `background`; `planner` = `repair`, `emptiness`, `static_drift`, `drift`, `multi`,
`single`; `cause` = `always`, `coupled`, `stale`, `catalog`, `pods_changed`, `sibling`, `pending`; `reason` =
`conflict`, `budget`, `nodepool_limit`, `static_node_limit`, `capacity_reservation`, `candidate_invalid`,
`candidate_blocked`, `candidate_nominated`, `candidate_ineligible`, `pods_changed`, `resim_unschedulable`,
`resim_changed`, `sibling_rejected`, `deferred`, `start_failed`, `backpressure`, `deadline`, `canceled`, `error`, and
`""` for started.

**Existing metrics with the gate on.**

| metric | gate on | why |
|---|---|---|
| `decisions_total`, `decisions_by_nodepool_total`, `queue_failures_total`, `queue_commands` | from the shared Queue, unchanged | |
| `eligible_nodes{reason}`, `last_evaluation_timestamp_seconds` | set when a planner plans | **gap**: a planner skipped for budget doesn't update them, so during a long budget-bound rollout they freeze, and a "disruption hasn't evaluated" alert can fire. Accepted for alpha; fix: count candidates for budget-gated planners every round |
| `nodepools_allowed_disruptions`, `nodes_consuming_budgets` | from the Ledger, so reserved plans count | |
| `failed_validations_total` | the methods' validators at admission, plus the planner counting candidates admission rejected before the validator ran, as the validator would | |
| `cluster_state_gate_duration_seconds` | each unsynced period, observed by the commit loop when it ends | pod-free deletes may still be admitted during it |
| `decision_evaluation_duration_seconds` | **not emitted** | covers a whole serial `disrupt()` including the 15 s wait; replaced by `round_duration_seconds` + `plan_wait_seconds` |
| `decision_evaluation_phase_duration_seconds`, `decision_evaluation_scheduling_simulations`, `skipped_evaluations_total`, `cluster_state_gated_passes_total` | **not emitted** | their meaning is tied to the serial pass; replaced by the round, planner, re-simulation and gate series |

A silently changed meaning breaks dashboards worse than an absent series, so a name is reused only where its meaning
carries over unchanged.

**Events.** A round's buffered events and the admission rebuild's events go to the recorder exactly as the disruption
controller publishes them, so the recorder's dedupe gives today's `kubectl describe`. The budget-blocked NodePool event
(`allowed == 0`) is kept. Six NodePool reasons for "why nothing happened" are declared in `pkg/events`
(`DisruptionBudgetExhausted`, `DisruptionWaitingForLaunch`, `DisruptionCandidatesBlocked`,
`ConsolidationNoCheaperOption`, `ConsolidationScanIncomplete`, `RepairBreakerTripped`), published per (planner,
NodePool) only when the explanation changes. Only `DisruptionBudgetExhausted` is emitted so far, including while the
lane is budget-gated: "drift is waiting for budget: 100 of 100 allowed nodes are disrupting (reason Drifted; in-flight
nodes count against every reason)". On KWOK mainline logged 317 "too many queued events, dropped event" in one
rollout; the gate-on build logged 0.

**Logs.** V(1), structured with constant messages: "disruption pipeline planned candidates" per planning round
(planner, reason, planned, eligible, plans, deferred, duration) and "disruption pipeline admitted batch" per batch
outcome; logger `disruption.pipeline`. Shadow mismatches go to the root logger at ERROR, once per (planner, level,
kind) per 10 min.

## 9. Edge cases and tests

| case | behaviour | test |
|---|---|---|
| two lanes Reserve the last slot at once | the mutex serializes them; the second is rejected `budget` | concurrent Reserve, `-race` |
| a NotReady drifted node in a pool whose budget it already consumes | charged, rejected `budget` (today: skipped; same outcome) | Ledger spec, batch=1 differential |
| a cron window closes during a 15 s wait | `RecheckAll` with the live clock rejects `budget` | fake clock |
| a drift plan is computed, then a coupled consolidation starts | the drift plan re-simulates (`coupled`) and adopts a zone-correct replacement | admitter specs |
| a Split sibling is rejected | survivors re-planned jointly, or nothing starts | Split-hazard specs |
| a plan deletes a node a reserved or in-flight plan moves pods onto or depends on | rejected `conflict` | Ledger and Relate specs |
| `limits.nodes=3`, two drift plans, one pending pod | the second recheck fails `nodepool_limit` | Ledger spec |
| a static drift grant is short at Reserve | release, reject; the next round may pick terminate-first | counter back to 0 |
| start fails after the taint | the sweep untaints in the same step; reservation released | injected `CreateNodeClaims` error |
| sweep vs start | never concurrent | a hook blocks start between taint and insert while forcing a sweep |
| lease lost during the 15 s wait, or during a step | dropped; nothing written; nothing admitted starts | cancel under the fake clock |
| emptiness while unsynced | admitted (pod-free); a nominated node is rejected | admitter spec |
| emptiness (Fast) and consolidation (Background) ready in the same step | consolidation waits for the next step, after emptiness started | admitter spec (found by the pipeline-at-batch-1 differential) |
| a batch's joint re-simulation accepts one of two NodePools' candidates | the other NodePool's plan is rejected `deferred` and comes first next round | planners and admitter specs |

## 10. Alternatives and risks

| alternative | why not |
|---|---|
| Reserve and start atomically at admission | consolidation would find its slot taken after 15 s (drift refills within a second), and the Admitter would re-simulate every plan |
| Concurrent starts with a queue `TryReserve` | fixes the `HasAny`/insert race but not the sweep race, coupling or sibling re-plan |
| Topology conflicts instead of re-simulation | serializes nearly everything; a topology ledger is a later optimization if `admission_resims_total` shows a bottleneck |
| Re-simulate every plan at admission (coupled by NodePool) | a serial joint re-simulation per batch in a one-NodePool cluster; the sums are already exact in the Ledger |
| Per-plan rebuild and recheck | 9.8 s per batch at ~2,200 nodes (section 3.5) |
| Sweep on its own goroutine with a re-checked predicate | correct but subtle, and buys nothing |
| Taint at Reserve ("cordon early") | consolidation is often rejected after 15 s, leaving tainted-then-rejected nodes, and it hides destinations from the provisioner |
| A start timeout below the lease renew deadline | a timeout leaves half-started commands |
| Edge filter for candidate events keyed by blocker kind | publishes more than today; the recorder's dedupe already matches today |

**Risks.**
1. **Serial admission holds the commit loop during a joint re-simulation.** A coupled drift batch re-simulates in the
   commit loop (6-15 s at 2,000 nodes). Mitigations: coupling is tested only against plans started since the plan was
   simulated, back-pressure bounds that window, and staleness counts from submission. The two round-3 A/B runs had
   22-60 `stale`, 5-8 `pods_changed` and 8-9 `sibling` drift re-simulations per rollout against 1,581-1,720 admitted
   drift plans.
2. **Partial start of a sibling group** (a sibling's launch fails after start). Started members may rely on it for
   topology: a transient pending pod and a re-launch, not a hard violation, bounded by `ComponentCap`.
3. **First come, first served reads as a regression risk.** With turn starvation fixed, users will expect
   consolidation during rollouts; the RFC states that it runs only when budget remains.
4. **Dependencies tie commands together** (and the Queue gap in section 6 must close before alpha).
