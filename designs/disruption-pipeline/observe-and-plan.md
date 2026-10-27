# Observe and Plan: snapshot, planners, cores, batching, lanes

Appendix A of [Disruption Pipeline](../disruption-pipeline.md). Scope: everything that happens before a plan reaches
the commit loop. That is the snapshot a lane round plans on, the planner contract, the shared method cores, the
simulator and joint simulation, the batch sizer, candidate groups and `Split`, dependency tracking on the core side,
footprints, lanes, triggers and fairness. [Appendix B](admit-and-execute.md) covers what happens after `Submit`.

Code references are `pkg/...` paths at the tip of the implementation stack. `mainline:` marks
kubernetes-sigs/karpenter @ `6eac93ba`. Bare `disruption/` and `pipeline/` mean `pkg/controllers/disruption/` and
`pkg/controllers/disruption/pipeline/`.

Package layout and import direction:

| package | contents | imports |
|---|---|---|
| `pipeline/types` (leaf) | `Footprint`, `Action`, `Shape`, `RejectReason`, `Verdict`, `LaneID`, `GateReason`, `WakeSet`, `ResimCause` | neither `disruption` nor `pipeline` |
| `disruption` | method cores (`Compute`, `LegacyOptions`), `CoreOptions`, `Effects`, `Simulator`, `SimulateBatch`, `BatchSizer`, `Split`, `CapPieces`, `CandidateSnapshot`, `Queue`, `MarkSweeper`, read-only accessors in `pipeline_seams.go` | `types` |
| `pipeline` | `SnapshotBuilder`, `Planner`, planners, `Lane`, triggers, runners, `Ledger`, `Admitter`, `Executor`, `Starter`, `Observer`, shadow mode | `disruption`, `types` |

`disruption` never imports `pipeline`. With the gate off, `pipeline` is imported by `controllers.go` (its metric
families register at init) but never constructed, unless the shadow environment variable is set.

## 1. Snapshot

### 1.1 What today's build costs

`mainline: GetCandidatesWithTotals` (`helpers.go:273-299`) runs once per method, 3-6 times per `Reconcile`. Each call
lists NodePools and instance types (`BuildNodePoolMap`), lists and parses every PDB, takes one `DeepCopyNodes` under
the cluster-state read lock, lists each node's pods with a deep copy (managedFields included), checks do-not-disrupt
and PDBs per pod, publishes `DisruptionBlocked` events, and only then runs the method's `ShouldDisrupt`.

Split of one drift-filtered build at N=2,000 (envtest 1.37, fake provider with 1,344 instance types, 10 pods/node,
no PDBs; 9 runs, median of 7 calls each; shared machine at load 5-46):

| sub-step | ms (median, range) | share |
|---|---:|---:|
| pod list + `Pod.DeepCopy` | 48.7 (37-89) | 55% |
| `DeepCopyNodes` | 32.2 (25-59) | 36% |
| PDB `CanEvictPods` (0 PDBs) | 4.9 | 6% |
| everything else | < 9 | < 10% |
| **total**, 172 MB allocated (~86 KB/node) | 89 (81-177) | |

### 1.2 The build, step by step

`SnapshotBuilder.Build(ctx, Scope)` (`pipeline/snapshot.go:112-132`) wraps `disruption.CandidateSnapshot`, which the
tier-1 PRs introduced for today's controller:

1. **Fixed now.** One `BuiltAt` for the round, through a fixed-now clock (`disruption/helpers.go:426-430`), so
   disruption costs and repair tie-breaks don't depend on evaluation order (#3406).
2. **Round inputs, once.** `BuildNodePoolMap`, one `pdb.NewLimits` (indexed by namespace and by each selector's first
   `Equals`/`In` requirement, with parsed PDBs reused while their UID and resourceVersion hold), one
   `DeepCopyNodes`.
3. **Pods without a deep copy.** `StateNode.ValidatePodsDisruptable` lists with `nodeutils.GetPodsReadOnly`
   (`client.UnsafeDisableDeepCopy`). With a cache-backed client the result is a shallow struct copy of each cached
   pod. The informer never mutates a stored object (an update replaces it), so a held pointer keeps a stale but
   self-consistent pod. The only writer is the scheduler (`Inject` assigns `TopologySpreadConstraints`), so
   `SimulateScheduling` deep-copies candidate pods before handing them over.
4. **Per-node pod checks, memoized.** The class-independent results of `ValidatePodsDisruptable` and the events it
   published are stored per node and replayed per class, so each class publishes the same events in the same order as
   its own build would.
5. **Class rules per call.** `CandidatesFor(ctx, planner)` (`snapshot.go:163-187`) applies `NewCandidate`'s class
   rules to the stored *first* error of its ordered chain (rebooting, deleting, uninitialized, nominated, node
   do-not-disrupt, NodePool label). An unordered blocker set would change repair eligibility for nodes that are both
   uninitialized and nominated.
6. **Exclusions.** Nodes that a reserved, unstarted plan disrupts (`ReservationView.IsReserved`) and nodes that a
   started command moves pods onto or depends on (`InFlightDestinations`) are dropped.
7. **Filter.** The planner's `Filter` (the method's `ShouldDisrupt`). A rejection with an explanation becomes a
   `Verdict` in the round's buffer.
8. **Lazy totals.** `Totals(class)` (`snapshot.go:191`) computes NodePool totals from all of the class's candidates the
   first time a totals consumer (balanced consolidation) asks, as `mainline: controller.go:211-217` does.

`Scope` (`snapshot.go:44-65`) is computed once per lane from its planners: the classes they use and the classes whose
totals they consume.

**Invariant.** For every planner and the same cluster state and `now`, `CandidatesFor` returns the same set, fields,
rejection reasons and visiting order as one `GetCandidatesWithTotals(planner.Filter, planner.Class())`, and `Totals`
equals its totals. `pipeline_snapshot_test.go` checks this for all six methods over seeded churn (4 seeds × 15 steps
in CI), and scribbles on each planner's Candidates after use to prove isolation. A sensitivity check fails if the
fixed clock is removed.

A stated timing change: today each method's build happens at a different instant inside one `Reconcile`. A lane round
evaluates all of its planners at `BuiltAt`. A pod that binds between two methods' builds today is seen by the second
method; in the pipeline it is seen by the next round or by the Admitter, the same staleness every plan has by the
time it is admitted.

### 1.3 Who may write what

| object | shared across planners and lanes? | who may write |
|---|---|---|
| per-node core (pod check results, first error) | yes, within one snapshot | nobody after the build |
| `Candidate` | **no**: new per `CandidatesFor` call (~200 B per node per planner) | the planner that received it; the Starter and Queue once it is in a plan |
| `StateNode` shell (`Node`, `NodeClaim` pointers, flags) | **no**: shallow-copied per Candidate | same as the Candidate (covers the queue's `cmd.Candidates[i].NodeClaim` write) |
| `StateNode` maps, host-port and volume usage | yes, read-only | nobody; a simulation takes its own `DeepCopyNodes` |
| `*corev1.Node`, `*v1.NodeClaim`, pods | yes: informer-cache objects | nobody |
| `NodePool`, `InstanceType` | yes: informer and provider pointers | nobody |

This is why repair's drain bound (`TerminationGracePeriod` stamped on every candidate it walks), `RepairPolicyResult`
and `RebootEscalated` can't leak into a drift plan built from the same snapshot.

### 1.4 Events

The build publishes nothing. Rejections and verdicts go to `RoundEvents` (`pipeline/events.go`), which the Observer
publishes through the existing recorder after the round. The recorder's dedupe (keyed by node UID, 2 min) already
gives today's `kubectl describe` output; an extra edge filter keyed by blocker kind would publish *more* than today,
so there is none.

### 1.5 Measured

Prototype of the same build (`A+`) against today's three builds per drift round, N=2,000 (shared machine at load
9-28; 6 runs alternating size order, median of each run's p50 [range]):

| | today | one build, deep copies | **one build, no pod copies (shipped)** |
|---|---:|---:|---:|
| drift round (3 methods), wall | 289 ms [236-448] | 90 [71-134] | **62 [50-81]** |
| drift round, allocated | 514 MB | 172 MB | **92 MB** |
| all 6 methods (quiet poll), wall | 505 [407-742] | 100 [74-120] | **68 [55-93]** |
| drift round with 20 PDBs, wall | 307 [256-455] | 100 [78-145] | **75 [65-107]** |

In today's controller the tier-1 PRs that build this snapshot measured, on the scale benchmark (3 alternating pairs):
drift `Reconcile` iteration −26% to −29% (shared snapshot, N=500/1,000/2,000), cached candidate build −28% to −49%
(no pod copies), `pdb.Limits` −70% time at 200 PDBs (go bench, 2,000 pods). On KWOK the drift profile went from
`GetCandidatesWithTotals` 19.5% and `DeepCopy` 21.9% of samples (mainline) to 0% and 7.5% (gate on).

### 1.6 Catalog version

Instance types, offerings, prices and capacity reservations change inside the provider with no core event (AWS:
instance types and prices every 12 h, ICE'd offerings for 3 min, ODCRs polled every minute). The pipeline still calls
`GetInstanceTypes` every round, so eligibility and price are as fresh as today. `CatalogVersion`
(`snapshot.go:216-241`) is a per-NodePool fingerprint over instance type names and each offering's zone, capacity type,
reservation ID and capacity, availability and price. The Admitter compares it at admission (`catalog` re-simulation
cause, [B §3](admit-and-execute.md#3-admitter)).

It must not depend on offering order. The AWS provider builds offerings by ranging over its zone set and the
capacity-type requirement's values, so every rebuild of its offering cache (1 min TTL) lists the same offerings in a
new order. The first version hashed slice order: on KWOK, 95 of 98 admission re-simulations in a drift rollout were
`catalog`. Offerings are now sorted before hashing, and requirement values are read with `Requirement.String()`
because `Offering.Zone()` uses `Any()`, which picks a random value from a multi-valued requirement.

### 1.7 What Observe does not do

- It doesn't make simulations consistent: topology counts, pending-pod nominations, DRA and `cluster.Nodes()` stay live
  inside each simulation, as today.
- It doesn't replace the `Synced` gate: an unlaunched NodeClaim is not a StateNode (`state/cluster.go:389-392`).
- It is never proof. The Admitter re-validates every plan from live state.

**Later versions.** A per-node memo keyed by (StateNode version, PDB structural generation, NodePool generation,
catalog version) needs copy-on-write StateNodes (#3240, closed unmerged: `UpdatePod` 1.6× slower,
`SimulateScheduling` 15-29% faster). A persistent candidate pool measured 6.4 vs 62 ms per drift round at N=2,000 and
6.2 vs 92 MB, but after batching that is ~4-5% of a round; it ships only with event-driven lanes. Both plug in behind
`SnapshotBuilder` and must pass the same differential.

## 2. Planner contract

```go
// pipeline/planner.go:35-50
type Planner interface {
	Name() string                // metric label value, e.g. "drift"
	Reason() v1.DisruptionReason // whose budget its plans charge
	Class() string               // disruption class whose candidate rules apply
	Shape() types.Shape          // most invasive plan: ShapePodFree | ShapePodMoving | ShapeReplacing
	Filter(ctx context.Context, c *disruption.Candidate, now time.Time) (bool, *types.Verdict)
	Plan(ctx context.Context, in PlanInput) (PlanOutput, error)
}

type PlanInput struct {
	Candidates []*disruption.Candidate // the planner's own
	Budgets    BudgetView              // live, net of reservations; a hint, the Ledger decides
	Reserved   ReservationView         // live view of reserved, unstarted plans in every lane
	Evaluator  disruption.Evaluator    // balanced scoring, built from Snapshot.Totals
	Snapshot   *Snapshot
	Now        time.Time               // Snapshot.BuiltAt
	Synced     bool
}

type PlanOutput struct {
	Plans    []*Plan
	Verdicts []types.Verdict
	Next     time.Time // when something in the planner's slice becomes eligible
}
```

A `Plan` (`planner.go:94-125`) carries the method, reason and action (delete, replace, reboot), the candidates,
replacements and simulation results, a `Footprint`, a pod-set hash per candidate, the catalog version of every
NodePool it touches, `BatchID`/`Siblings` for plans from one joint simulation, `ValidateAt` (now + 15 s for emptiness
and consolidation) and `ComputedAt`. `Plan.PodFree()` is true only for a plan with no replacements, not a reboot, not
static drift (its NodePool's static provisioning launches the node back) and no reschedulable pod on its candidates.

Optional interfaces a planner may implement:

| interface | who | used by |
|---|---|---|
| `TotalsConsumer` | multi, single | the lane builds `PlanInput.Evaluator` from snapshot totals |
| `BudgetFree` | repair | lane and planner budget gates (an uninitialized node is repaired without budget) |
| `Revalidator` | every planner | the Admitter's acceptance test, [B §3](admit-and-execute.md#3-admitter) |
| `JointRevalidator` | drift, repair | one joint admission re-simulation per batch |
| `Resolver` | drift | NodePool fairness from resolutions |

**Purity.** A full `Lane.Step` against a counting client issues zero API writes, publishes no event and leaves
cluster state unchanged (`pipeline/lane_test.go`). Each compute-time side effect of today's methods has an owner:

| side effect today (mainline) | where | owner in the pipeline | legacy mode |
|---|---|---|---|
| `ReserveNodeCount` during compute | `staticdrift.go:114`, `repair.go:211` | `effects.ReserveNodeCount` returns `min(n, Headroom(np, limit) − StaticHeld(np))` without writing (`pipeline/planners.go:57-63`); the Ledger takes the slots at Reserve | `InlineEffects` = today's CAS |
| reboot handoff (Get, two Patches, history) | `repair.go:268-309` | `Plan.Action = Reboot`, planned from the cached NodeClaim (`CoreOptions.CachedReboot`); the Starter re-reads it with `disruption.PrepareReboot` | a tier-1 PR moved it into `Queue.StartCommand`, on the NodeClaim re-read in compute |
| Blocked/Unconsolidatable events | filters, cores, validators | buffered; Observer publishes | `Effects.Publish` = recorder |
| `GetPendingPods` side effects per simulation | `provisioner.go:213-240` | `provisioning.WithoutSideEffects` in every pipeline planning simulation | unchanged |
| evaluator swap `SetNodePoolTotals` | `consolidation.go:71-73` | `CoreOptions.Evaluator` per call | unchanged |
| consolidated-state bit | `consolidation.go:90-97` | `CoreOptions.Dedup` = `stateDedup` | method's own bit |

## 3. Shared method cores

Each method keeps its file in package `disruption`. Its body is `Compute`, and `ComputeCommands` is
`Compute(ctx, budgets, m.LegacyOptions(), candidates...)` (`disruption/drift.go:64-78`).

```go
// disruption/core.go:34-69
type CoreOptions struct {
	Batch          int           // largest number of candidates from one joint simulation; 1 = one at a time
	Sizer          *BatchSizer   // sizes each batch to a deadline and learns from every batch
	Deadline       time.Duration // a joint simulation past 2*Deadline is abandoned; 0 = none
	Effects        Effects       // Publish, ReserveNodeCount
	Sim            Simulator
	Order          func([]*Candidate) // nil = the method's own order
	Pending        PendingClaims      // KeepOnFirst | DropPendingOnly
	Dedup          Dedup              // consolidation "nothing changed" bit
	CachedReboot   bool               // plan reboots from the cached NodeClaim
	Evaluator      Evaluator          // balanced consolidation; nil approves all
	Validator      Validator          // inline 15 s validation; nil returns commands unvalidated
	WithoutMetrics bool               // shadow mode: record none of the controller's metrics
}

type CoreResult struct {
	Commands []Command     // commands from one joint simulation share a non-zero BatchID
	Batch    *BatchOutcome // the joint simulation, if one ran
	Chunked  bool          // the defensive chunk path ran (section 7)
	Deferred []string      // accepted but not emitted this round
}
```

| method | legacy options (gate off) | pipeline planner (gate on) |
|---|---|---|
| Emptiness (`emptiness.go:69-138`) | inline effects, method's dedup bit, `EmptinessValidator` inline (15 s) | `emptinessPlanner`: all empties in one plan, `ValidateAt = now + 15s`, re-validated at admission with the method's validator (may shrink) |
| StaticDrift (`staticdrift.go:68-80`) | `InlineEffects{Recorder, Cluster}` (today's CAS in compute) | `staticDriftPlanner`: read-only headroom, one plan per candidate, slots taken at Reserve; NodePools in the order of their first candidate |
| Drift (`drift.go:71-130`) | Batch 1, `LegacySimulator`, Drifted-time sort, empty first, `KeepOnFirst` | `batchPlanner`: Batch = all candidates, `Sizer`, `DropPendingOnly`, NodePool round-robin order, pipeline simulator |
| Repair (`repair.go:122-138`) | Batch 1, score order, breaker, budgets on initialized nodes only, NodeClaim re-read in compute | `batchPlanner`: batched replacement with `IncludeBlockedCandidatePods`, `CachedReboot`, budget-free, score order |
| MultiNode, SingleNode (`multinodeconsolidation.go:59-70`, `singlenodeconsolidation.go:60-71`) | method's evaluator, dedup and validator; search unchanged | `consolidationPlanner`: one plan per round, round's evaluator, `stateDedup`, `ValidateAt = now + 15s`, always re-validated |

Static drift, reboots, static-pool repair and terminate-first candidates always take the single-candidate path.
Consolidation doesn't batch-and-split: Multi emits one command with at most one replacement, Single one candidate, and
the command-level price rules (≤ 1 replacement, price on the sum, spot-to-spot exemption for multi-candidate
commands) stay untouched.

**Differentials** (each against a verbatim copy of the method on the PR's parent commit, kept in a `_test.go` file
until GA; upstream nondeterminism resampled, up to 64× for drift):

| core | generator | result |
|---|---|---|
| drift | 10-200 nodes, 1-3 NodePools and zones, budgets 1/10%/100%, zone spread, hostname anti-affinity, blocking and `maxUnavailable=1` PDBs, do-not-disrupt, CPU limits, pending pods, reserved-capacity pools, terminate-first on/off, pods pinned to their node; up to 3 started commands per seed | 100 seeds: 297/297 states at 10-50 nodes and 308/308 at 10-200, for `ComputeCommands` and for the batched path forced onto single candidates |
| emptiness | 0-60% empty, 4/8/16-CPU types, budgets 1/10%/50%/100% | 40 seeds, 165 states: 160 exact, 5 equal up to savings-ratio ties |
| static drift | 1-3 static pools, 2-8 replicas, `limits.nodes` none/at/above, scaling down, NotReady, reserved pools | 100 seeds, 173 states: 168 exact, 5 equal up to the choice within a pool |
| repair | replace and reboot policies, drain bounds, 10-40% unhealthy (trips the breaker), terminate-first on/off | 40 seeds, 88/88 exact, and 88/88 for the forced batched path |
| multi, single | 20-60% utilization, on-demand and spot, balanced pools with the evaluator per pass | 30 + 30 seeds, 160 states: 151 exact, 9 equal up to equal nodes |

Mutations that each fail a differential: dropping the reserved fallback or the Blocked event (drift), removing the
savings-ratio sort (emptiness), reversing Single's candidate order, losing the prefix of top-ranked reboots (repair).

## 4. Simulator

```go
// disruption/simulator.go:60-104
type Simulator interface {
	Simulate(ctx context.Context, opts SimOptions, candidates ...*Candidate) (Outcome, error)
}
type Outcome struct {
	Results        scheduling.Results
	TerminateFirst bool
	Took           time.Duration
	Pods           int
}
```

`LegacySimulator` (`simulator.go:106-138`) is `SimulateScheduling` / `SimulateSchedulingWithReservedFallback` exactly.
`SimOptions` fields map one to one onto new `SimulationOptions` fields, each a no-op at its zero value:

| field | effect | set by |
|---|---|---|
| `IncludeBlockedCandidatePods` | schedule candidate pods that can't be evicted now | repair |
| `ReservedFallback`, `TerminateFirst` | one candidate through the reserved-capacity fallback | drift, repair single path |
| `ExcludeDestinations` | providerIDs that may not receive pods | pipeline simulator; defensive chunk path |
| `TrackUninitialized` | candidate pods may use uninitialized nodes; see section 8 | `planBatch` |
| `CandidateGroupSize` | candidates in groups whose pods never share a node; see section 7 | `planBatch` |
| `WithoutPendingPodSideEffects` | pending pods listed without decisions or events | pipeline simulator |
| `LimitsUsed`, `SkipDeletingNodePods` | limits accounting across chunks | defensive chunk path only |
| `AlsoDeleting` | nodes simulated as already deleting | reference mode only (tests) |
| `Nodes` | state nodes to simulate on instead of cluster state's | shadow mode only |

**The pipeline simulator** (`pipeline/simulator.go:26-77`) wraps the legacy one with one rule:

| node or pod set | treated as |
|---|---|
| candidates being simulated | removed; their reschedulable pods re-placed (as today) |
| **started** work (MarkedForDeletion) | removed; pods re-placed (as today) |
| **reserved, unstarted** plans' candidates, every lane | unavailable as destinations **only**; their pods are **not** re-placed and still count at their nodes for topology |
| reserved plans' and started commands' destinations | usable as destinations; overlap is caught at admission |
| launched-but-uninitialized nodes | unavailable today; usable by batched simulations as dependencies (section 8) |
| unlaunched NodeClaims | invisible, as today; the `Synced` gate covers them |

This is sound without a virtual-node overlay. A reserved plan's pods are re-placed by whichever simulation runs after
it starts. What is lost is the reserved plan's consumption of shared destinations, which is exactly the coupling the
Admitter re-simulates. Test case: 4 nodes against `limits.nodes=3` with one plan reserved; without the rule the second
plan's simulation re-places the first plan's pods and launches a node for them
(`pipeline/invariants_test.go`, "Simulator" spec: with node 0 reserved, node 1's pod goes to a new NodeClaim instead
of node 0, and every pod still schedules).

**Deadlines.** Every joint simulation runs under `context.WithTimeout(2 × deadline)`, where the deadline is
`max(BatchSimDeadline, Sizer.Deadline())`. Solve checks the context per pod and returns partial results, which
`SimulateBatch` discards (`TimedOut`). Multi keeps its 1 min and Single its 3 min timeouts.

**Not shared.** Every simulation still takes its own `DeepCopyNodes` and builds a scheduler and topology over every
node. At 2,000 nodes on KWOK, `Provisioner.NewScheduler` is 88% of a drift simulation, `DeepCopyNodes` 9% and `Solve`
0.3% (30 s profile mid-rollout, 4 CPU). That fixed cost dominates batching and is the cheaper-simulation track's job.
There is no shared simulation slot: each lane and the commit loop simulate on their own goroutines, so a Background
scan can compete with a Fast-lane joint simulation for CPU (an earlier draft proposed one priority slot; it is a
follow-up if the 1-2 CPU measurements call for it).

## 5. SimulateBatch

`SimulateBatch` (`disruption/simulator.go:223-299`) is a function over any `Simulator`, so every simulator shares one
batch algorithm, and the drift core can call it without importing `pipeline`.

```
SimulateBatch(ctx, sim, opts{K, MinSize=2, MaxRounds=3, Deadline, Budgets, Charge}, ordered...):
  batch, rest := takeWithin(ordered, K, budgets)        // the first K that fit their NodePools' budgets
  for round := 1..MaxRounds while len(batch) >= MinSize:
     out := sim.Simulate(ctx with 2*Deadline, batch...)
     if deadline exceeded:          return TimedOut          // never a partial result
     if a candidate started deleting: return no batch        // the single path skips it
     blocked, attributable := attribute(out, batch)       // own non-pending pod failures, by pod UID
     if !attributable:                                     // a deleting node's pod was crowded out
         if len(batch)/2 < MinSize: return no batch
         batch = batch[:len/2]; Halvings++; continue
     if len(blocked) == 0: return Accepted = batch, Outcome = out
     drop blocked
     if not topped up yet: top up from rest with candidates that now fit the budget, re-sort by rank
  return no batch
```

- **Within budget.** The batch and its top-up are the first candidates, in rank order, that fit their NodePools'
  budgets (`BatchOptions.Budgets`, `Charge`). A dropped candidate frees its NodePool's budget for that NodePool's next
  candidate, as the single-candidate walk would. Without this, a forced batch=1 differential seed (budget 1,
  terminate-first, 10%) planned a later candidate of another NodePool than legacy.
- **Rank order.** `batchWindow` (`disruption/core.go:154-183`) picks the batch's candidates in order. A candidate that
  only the single path can plan (terminate-first, reboot, static) is planned first when it ranks ahead of the batch,
  and ends the batch where it appears, so a batch never jumps ahead of a candidate it leaves out.
- **Fallback.** When no batch of at least two works out, the core walks the remaining candidates one at a time, as
  today (`drift.go:121-129`). Batched drift reaches every decision batch=1 reaches.
- **Attribution.** Which candidate's pod fails first depends on the scheduler's queue order, so a drop is not proof
  the candidate is unmovable; it is planned again next round.

## 6. BatchSizer

K sizes itself to a deadline; there is no knob (`disruption/simulator.go:330-406`).

```
fixed     = cheapest finished simulation seen (a new minimum is taken at once, otherwise smoothed 3:1)
marginal  = (Took(accepting sim) − fixed) / |Accepted|
K_0       = min(limit, FirstBatchMax = 50)
K_next    = clamp(target / marginal, K_last / 2, 2 · K_last)        target = BatchSimDeadline = 2 s
            2 · K_last while the marginal cost is below what can be measured
on a timeout or halving: K_last = Size >> max(1, Halvings), coolDown = BatchSizerCoolDown (2)
while coolDown > 0: K ≤ K_last (may shrink further, doesn't grow); each accepted batch decrements it
K         = max(1, min(max(K_next, BatchMinSize = 2), limit))
Deadline  = target + MaxBatchRounds · fixed / 2                      (abandoned at 2 × Deadline)
```

One sizer per planner (drift, repair) is kept across rounds. Each line of this has a KWOK finding behind it (config R,
~2,000 nodes, 10% budget):

| finding | what happened | fix |
|---|---|---|
| collapse to 1 | after a timeout at 2 or a halving at 2-3, `Size` returned 1; `planBatch` then never simulated, `Observe` was never called again, and drift planned one candidate at a time for the planner's life: 1-node commands, ~10 nodes/min, ~2% budget utilization (2 CPU) | floor at `BatchMinSize` while the limit allows it |
| fixed-cost-dominated simulations | the per-candidate model saw ~1.5 s per candidate at K=2 (one simulation ~3-5 s regardless of K), so K stayed at 2 and multi-round batches timed out at 4 s: ~16 nodes/min, 7-9 of 197 slots in flight (4 CPU) | size on the marginal cost above the fixed cost; the deadline grows with the fixed cost |
| retried at the same K | once a simulation had finished, a timeout set K to half and the next `Size` doubled it straight back, so a batch that timed out at K was retried at K every round | multiplicative decrease and a two-batch cool-down |

Specs: "never sizes below `BatchMinSize`", "sizes on the marginal cost above the fixed cost" (3 s fixed + 20 ms per
candidate grows 2 → 4 → 8 → 16; the per-candidate model stays at 2), "shrinks after every timeout or halving and holds
K for a cool-down" (probe 50, 100, 200, 400 then 5 timeouts gives 200, 100, 50, 25, 12), and a 2,000-step property
spec with realistic simulation samples. Each fails on the code before its fix.

KWOK round 3 ran a build from before the last review fixes (multiplicative decrease and cool-down, budget-bounded
top-up). Its joint simulations took p50 5.8-7.3 s
and p99 14.0-14.8 s, accepted a median of 25-42 candidates (p99 98-183), and gave 18-32 plans per round.

## 7. Candidate groups and Split

**Groups.** `planBatch` (`disruption/core.go:211-263`) sets `CandidateGroupSize = ComponentCap` (10). `SimulateScheduling`
assigns candidates, in rank order, to groups of 10 (`disruption/helpers.go:185-192`), and the scheduler option
`PodGroups` keeps pods of different groups off each other's nodes, existing or new
(`provisioning/scheduling/scheduler.go`, `options.podGroups`). Pods without a group (pending pods, pods of deleting
nodes) go anywhere.

**Split** (`disruption/split.go:44-125`) breaks the joint result into pieces by union-find over candidates:

1. Candidates whose pods share a new NodeClaim are one piece.
2. Candidates whose pods share an existing node that **isn't initialized** are one piece (both depend on it).
3. Sharing an initialized existing node does not connect them: the simulation already accounted for all of their pods
   on it.
4. Each piece nominates the existing nodes its own pods go to, so a piece that fails to start doesn't drop
   nominations another relies on.
5. New NodeClaims holding none of the candidates' pods go to the first piece, except that with `DropPendingOnly`
   (the pipeline) a NodeClaim holding only pending pods goes nowhere and those pods are left to the provisioner.
   Claims for pods of nodes already being disrupted stay on the first piece.
6. Each piece's `Dependencies` are the uninitialized, non-deleting existing nodes holding one of its candidates' pods.

Because groups never share a node, every piece has at most 10 candidates, and one simulation yields the whole batch's
commands with limits, reservations and zone topology accounted across all of them. Commands of one batch share a
`BatchID`.

**Why groups, not chunks.** The first design re-solved any component above 10 candidates in chunks of 10, one
simulation per chunk, excluding earlier chunks' nodes as destinations and deferring chunks topology-coupled to an
earlier chunk. On KWOK an accepted batch packed onto shared replacements, `Split` gave one giant component, and each
chunk simulation cost ~4 s at 2,000 nodes (mostly `NewTopology`): 1-2 chunks per round, the rest deferred. Groups
remove the re-planning. The chunk path (`CapPieces`, `split.go:126-196`) is kept as a documented fallback that
`planBatch` doesn't reach (a panic at its entry hits only its own unit specs), because it guarantees the cap for any
simulation options a caller passes and the pipeline reads `CoreResult.Chunked/Deferred`.

**Siblings are correct only together.** Split separates commands by shared nodes, but zone spread and NodePool limits
were accounted across all of them, and nothing defers groups whose pods share a topology domain. A command that starts
without its siblings can leave zone spread skewed (spec "violates zone TSC when one command starts without its
sibling"). Correctness relies on the Admitter re-planning the survivors when a sibling is rejected
([B §3.4](admit-and-execute.md#34-siblings-joint-re-simulation-and-partial-acceptance)). A sibling whose launch fails
after it started is rolled back by the Queue, and the next round plans on the resulting state, as after any failed
command today.

**Packing at NodePool limits.** Groups can't share new NodeClaims, and each NodeClaim is charged at its largest
instance type, so fewer pods fit in the limit: one joint simulation of 50 candidates at an exact limit had 17 pod
errors without groups and 56 with them (19 vs 110-133 with zone spread). Those candidates are dropped and planned in
later rounds or one at a time. Without limits there is no difference (33 vs 32-33 NodeClaims). A follow-up could retry
a batch that fails on limit errors with groups off.

**Invariants at batch=K** (same generator as the drift differential, run to quiescence serially and batched, with an
end-state checker that doesn't trust the scheduler: zone spread skew, anti-affinity, CPU capacity, every moving pod
placed, budget, limits; planned state and provisioner end state):

| 60 seeds, 10-200 nodes | serial | batched (K unbounded) | sized (sizer + deadline + `DropPendingOnly`) |
|---|---|---|---|
| nodes disrupted | 797 | 765 | 765 |
| new NodeClaims | 533 | **261** | 260 |
| drift rounds | 797 | **101** | 104 |
| max candidates per command | 1 | 10 | 10 |
| hard violations | 0 | **0** | 0 |

Fewer nodes are disrupted only where the batched run's percentage budget is smaller at quiescence (packing launches
fewer replacements). At an exact NodePool limit, serial is stuck (disrupts 0 every time) while a batched run's first
joint simulation varies between runs (upstream zone-spread ties in map order); when a batch fits, the deleted
candidates free limit headroom and drift cascades. That is real gate-on behaviour: at an exact limit gate-on drift
progresses with some probability each round where gate-off never does.

Scale benchmark, drift fill of a 10% budget (envtest, batched path forced on, 3 alternating pairs at load 10-20,
before groups replaced chunking):

| N | passes (base → batched) | fill time | replacements | max nodes per command |
|---|---|---|---|---|
| 500 | 56 → 3 | 11.7 s → 2.2 s (5.4×) | 56 → 26 | 10 |
| 1,000 | 112 → 4 | 71.0 s → 5.0 s (14×) | 112 → 52 | 10 |
| 2,000 | 223 → 5 | 375 s → 16.2 s (23×) | 223 → 103 | 10 |

The uncapped prototype was 64× at N=2,000 (one command of 200 candidates); the cap costs 2 passes. Launch latency and
the `Synced` wait are not in these numbers.

## 8. Dependencies, core side

Batched simulations set `TrackUninitialized` (`disruption/core.go:225`). Three rules change, and only there:

1. **No `UninitializedNodeError` for candidate pods.** A candidate's pod may land on a launched-but-uninitialized node
   (typically another command's in-flight replacement) as bin-packing chooses (`helpers.go:221`).
2. **Candidate pods last, when it matters.** When some state node isn't initialized, the scheduler places candidate
   pods after pending pods and pods of deleting nodes (`scheduling.CandidatePodsLast`, a stable partition of the
   size-sorted queue; `helpers.go:182-184`). Pods of nodes already being replaced keep the capacity launched for them,
   and candidates get what is left, so nothing is provisioned twice. Without an uninitialized node the order is
   mainline's: applying it always made the forced batch=1 path differ from legacy on 3 seeds where pending pods compete
   with candidates.
3. **Dependencies.** `UninitializedDependencies(candidates, results)` (`helpers.go:323-346`) returns, sorted, the
   uninitialized, non-deleting nodes holding a pod of the command's own candidates. Pending pods and deleting-node pods
   don't create dependencies. `Split` puts candidates sharing such a node in one piece.

The single-candidate path inside gate-on drift and repair, and every gate-off simulation, keep the legacy rule:
`UninitializedNodeError`, no dependencies, mainline order. The Queue waits for dependencies before deleting candidates
and rolls the command back if one fails ([B §6](admit-and-execute.md#6-queue-dependencies)).

**Why not fence.** The first fix for KWOK's late registrations kept candidate pods off uninitialized nodes. Before it,
joint simulations dropped every candidate whose pod landed on an uninitialized replacement (1,194 drops in one KWOK
run, all `uninitialized_destination`), batches re-ran, timed out (33 of 96) or accepted none (28 of 96). The fence made
every candidate buy capacity of its own next to nodes already coming up. Dependency tracking packs onto them instead.
Envtest repro (replacements never register, batches of N/10): at N=1,000, 10 rounds of 50, budget utilization 1.00
both ways, **141 replacements with dependencies vs 150 with the fence (−6%)**, 24 dependencies, each verified to be an
uninitialized node.

Specs (`disruption/dependencies_test.go`): a batch whose pods fit only on another command's uninitialized replacement
plans one command with no new NodeClaim and that node as its dependency; with the option off, both pods fail with
`UninitializedNodeError` and legacy plans nothing; a candidate pod bigger than a deleting node's pod doesn't take that
node's uninitialized replacement; repair packs the same way. Removing `CandidatePodsLast` fails the
double-provisioning and the late-registration specs.

**Scope (accepted risk).** Any uninitialized, non-deleting node may be a dependency, not only replacements of started
commands. Registration and initialization are expected to be quick; a node that registers but never initializes is
most likely a misconfiguration (a startup taint never removed, a device plugin that never registers; only
registration has a TTL, 15 min). Commands that pack onto it wait until the command timeout, roll back, and the next
round may pack onto it again; while it is a dependency it is protected from other planners, repair included. If this
shows up in practice, the follow-up is to restrict dependencies to replacements of started commands, or to stop using
nodes uninitialized for longer than the replacement timeout.

## 9. Footprint

`FootprintOf(cmd, computedAt)` (`pipeline/footprint.go:42-100`) derives a plan's footprint from its candidates'
**own** pods. A footprint is a pre-filter for how two plans relate and the input to the Ledger's sums, not a proof of
independence: under zone spread and zone (anti-)affinity, plans with disjoint footprints still couple (7 of 7
counter-examples in the conflict research), so the Admitter re-simulates on coupling.

| field | derivation | used by | status |
|---|---|---|---|
| `Removed` | candidates' providerIDs (reboot nodes too) | Relate, Ledger | derived |
| `Destinations` | existing nodes receiving ≥ 1 own pod, plus `Dependencies` | Relate, Ledger ownership | derived |
| `Dependencies` | the command's `Dependencies` | Relate (`removes_dependency`) | derived |
| `BudgetNodes` | NodePool → `Removed` that count (uninitialized repair candidates don't) | Ledger budget | derived |
| `LimitCost` | NodePool → per new NodeClaim, its largest instance-type option's capacity plus `nodes: 1` | Ledger limits | derived |
| `StaticNodeCount` | static NodePool → slots its replacements need | Ledger static counts | derived |
| `PendingPods` | pending pods sharing a new NodeClaim with own pods | Relate (`shared_pending`) | derived |
| `TopologyGroups` | `CouplingKeys` of every own pod | Relate (`shared_topology`) | derived, over-approximated |
| `Devices` | cluster-scoped DRA devices of own claims | Relate | **not derived** (needs a DRA allocator export) |
| `Reservations` | capacity-reservation ID → slots launched (+) or freed (−) | Ledger | **not derived** (needs a `ReservationManager` export) |
| `ComputedAt` | Ledger version read before the simulation read state | Admitter (`coupled`) | derived |

`CouplingKeys(pod)` (`footprint.go:123-149`) over-approximates the scheduler's topology groups by namespace: the pod's
own namespace (its spread constraints and terms select their own namespace by default, and another pod's term may
select it), every namespace its non-hostname (anti-)affinity terms list, and `AnyNamespace` for a term with a
`namespaceSelector`, which couples with every footprint that moves pods. Label selectors and topology keys are
ignored, so plans moving pods of one namespace always couple: more admission re-simulations than needed, never fewer.
A first version keyed (namespace, topology key) and missed `namespaceSelector` terms and cross-namespace custom keys;
three regression specs pin those. Exporting the scheduler's group hashes would make this exact.

## 10. Lanes and triggers

```go
// pipeline/lane.go:32-75
type LaneSpec struct {
	ID                types.LaneID
	Planners          []Planner // in this order every round
	SkipWhileUnsynced bool      // skip the lane while unsynced; otherwise pod-moving planners get pod-free candidates
	Trigger           Trigger   // nil = PollTrigger{}
	FirstWins         bool      // reference mode only: end the round after the first non-delete plan
}

type StepResult struct {
	Lane                   types.LaneID
	Plans, Admitted        int
	Took                   time.Duration
	EndedAt                time.Time
	SnapshotTook, PlanTook time.Duration
	Gated                  types.GateReason // why the whole lane didn't plan
	BudgetBound            bool             // a planner (or the lane) had no budget left
	HeldUnsynced           bool             // candidates were held back because cluster state was unsynced
	NextTimer              time.Time
	Err                    error
}
```

`Lane.Step` (`lane.go:121-191`):

1. Collect resolutions of earlier Submits. A planner none of whose last plans started backs off for the polling period
   (10 s, gated `paced`), as today's loop retries a failed command at its next pass. Without it, a starter that always
   failed re-planned the same candidate 133 times in 2 s, tainting and untainting it each time.
2. Lane gates (`lane.go:194-205`): prediction store not hydrated (`hydrating`); no planner has budget for its reason
   anywhere and none is budget-free (`budget_zero`, checked before the snapshot is built); unsynced and
   `SkipWhileUnsynced` (`synced`).
3. One snapshot.
4. Each planner in order: skip while its last Submit is unresolved (`unresolved`), while it backs off (`paced`), or
   while its reason has no budget anywhere (`budget_zero`); while unsynced, a planner whose `Shape` isn't pod-free gets
   only pod-free candidates; `CandidatesFor`, `Plan`, `Executor.Submit`. Submit reserves in the caller's goroutine, so
   the next planner plans around those reservations.

Back-pressure is per planner, not per lane: emptiness holds a 15 s `ValidateAt`, and a per-lane bound would stall drift
for 15 s whenever an empty node exists. On KWOK at N=300, `gated_seconds{gate="unresolved"}` was 1.9 s of a 6 min
rollout, so back-pressure is not the bottleneck.

**Default lanes** (`pipeline/trigger.go:100-112`):

| lane | planners, in order | flags | notes |
|---|---|---|---|
| fast | repair, emptiness, static drift, drift | | repair first, no preemption; it already waits 10-30 min of toleration in the AWS provider |
| background | multi, single | `SkipWhileUnsynced` | long scans; duty-cycle paced |

**Pacing.** `PollTrigger.Pace(last, woke)` (`trigger.go:58-82`), with duty = `max(1 s, 3 × last.Took)`:

| last round | Fast | Background |
|---|---|---|
| error | 10 s | 10 s |
| one of its plans started | 0 | duty |
| planned, or gated `synced`/`hydrating`, or held candidates for sync | 1 s | max(1 s, duty) |
| budget-bound | max(1 s, duty) | 10 s |
| otherwise | 10 s | 10 s |
| a planner's `NextTimer` is sooner | that time (≥ duty for Background) | same |

The budget-bound row exists because nothing frees budget at an event the lane sees: the queue's completion hook fires
when it issues the candidates' delete, before the nodes leave the disrupting count. Polling at 10 s left freed budget
unused for half the period on average. In the envtest rollout repro (replacements register 10 s after launch, N=300)
the 1 s poll took mean budget utilization from 0.67 to 0.88; with every fix at N=1,000 and a 30 s delay it is 0.90,
against 0.52 without them.

**Wake-ups** (`pipeline/wakes.go`). A `WakeHub` gives each lane a coalescing channel. The pipeline publishes
`LedgerRelease` when a reserved plan is rejected or dropped (not when rejected at Submit: it held nothing, and waking
would re-plan the same rejection in a loop) and when the Queue completes a command; `Synced` on the false → true edge
seen by the commit loop; `Resolution` to the plan's own lane. `wakeTrigger` runs Fast at once on any of these and
Background after its duty cycle on a release or resolution. There are no Drifted, Consolidatable or capacity wake-ups
yet, so the idle pace stays the 10 s polling period rather than a 5 min poll floor. Wake-ups never fire on raw informer
resyncs (~1k pod updates/s at 60k pods with no change).

**Runners** (`pipeline/runner.go`, `pipeline/controller.go:210-229`). One goroutine per lane and one for the commit
loop, registered as `manager.Runnable`s with `NeedLeaderElection=true`. A long Background round (SingleNode at 125 s
on a packed 2,000-node cluster) no longer delays repair, emptiness or drift. The commit runner steps on every Submit,
at a waiting plan's `ValidateAt`, every second while plans wait for a gate, at once after a step that admitted plans
others waited on, and every `SweepTick`; however it ends, it drops the waiting plans.

**Synchronous runner** (`controller.go:234-291`). `Controller.Reconcile` runs one round of every lane, each followed by
commit steps until nothing is ready, waiting on the clock for a plan's `ValidateAt` as today's loop waits for its 15 s
validation. It requeues at once after a start, after 1 s while plans wait for a gate, and after 10 s otherwise: the
legacy contract. The ~200 singleton-reconciled specs that exercise the pipeline in the gate-on twin suite stay
deterministic through it.

**Reference lanes** (tests only). `ReferenceLanes` is one lane with every planner in today's method order, first-wins,
skipped while unsynced, over planners built `WithReference()` (one candidate at a time, pending-only claims kept,
core order, reserved candidates simulated as deleting). It exists for the pipeline-at-batch-1 differential and the
shadow mode's reference level ([Appendix C §3.3](rollout-validation-alternatives.md#33-pipeline-at-batch1)).

## 11. Fairness

**Within drift** (`pipeline/fairness.go`). Candidates are ordered by Drifted transition time then providerID within each
NodePool (`planners.go:385-396`: a herd shares one transition second), then interleaved across NodePools, least
recently *served* first. A NodePool is served when one of its plans starts (the `Resolver` hook). A NodePool whose
drift command failed (Queue completion hook, `Succeeded=false`, or a `start_failed` resolution, e.g. a webhook rejected
the replacement) is skipped for 1 min, doubling per consecutive failure up to 10 min, ±10% jitter, reset by a success.
A command spanning NodePools backs off each. This addresses a failing drift head blocking younger NodePools (#3080)
and NodePools starving each other (#2927). Upstream's per-NodePool drift back-off design (#3178) is not in this tree;
its invariant "a drift command targets exactly one NodePool" does not survive packing, so the pipeline owns the
back-off. The legacy controller keeps today's sort.

**Partial batches.** When admission accepts only part of a batch (B §3.4), a member none of whose candidates started is
rejected `deferred`, so it doesn't mark its NodePool served and that NodePool comes first next round.

**Consolidation dedup.** Emptiness, multi and single skip their search while `state.Cluster.ConsolidationState()` is
unchanged since they last found nothing (`fairness.go:133-151`), as today's consolidated-state bit does (it also flips
at least every 5 min). An idle Background lane costs what today's idle loop costs.

**Across reasons.** Not a planner concern: the Ledger's `FirstComeFirstServed` policy decides who gets a freed slot
([B §2](admit-and-execute.md#2-ledger)). The Fast lane reacts to a freed slot within a second; consolidation arrives
every 20 s to several minutes. During a budget-saturated rollout drift takes nearly every freed slot, and consolidation
runs only when budget remains.

**Not done.** A rejection memo keyed by (planner, providerID) with typed invalidation, and SingleNode resuming from a
cursor after an admission rejection instead of restarting from the top (#3305), both need hooks in the consolidation
core. `pods_unschedulable` after the single-path retry doesn't back off yet.

## 12. Plan-layer alternatives and risks

| alternative | why not |
|---|---|
| Exclude reserved plans' destinations in cross-lane simulations | stricter than needed; shrinks every Background plan's destination set during a rollout |
| Topology groups as hard conflicts | zone spread is near-universal, so this would serialize nearly everything |
| A fixed batch-size knob | maintainers decline knobs; the sizer adapts, and KWOK showed a fixed per-candidate model fails |
| Chunk re-planning with topology deferral | one simulation per chunk at ~4 s each; replaced by candidate groups (section 7) |
| Fence uninitialized destinations | every candidate buys its own capacity; replaced by dependencies (section 8) |
| Keep `IsConsolidated` per method | replaced by the same state-based dedup, shared by both lanes |
| Shared Candidates with the drain bound moved into `Plan` | fixes one of four writers and changes the shared `Command` |
| Lazy pod listing on first `CandidatesFor` | mixes a `BuiltAt` node copy with pod lists taken seconds later |

**Risks.**
1. **Sibling coupling is handled by rules, not a model.** A missed dimension (minDomains, maxSkew > 1 across disjoint
   domains) regresses to "pods transiently pending, then the provisioner launches the missing node": worse than serial,
   not an outage.
2. **The fixed cost dominates.** A joint simulation at 2,000 nodes takes 6-15 s on 4 CPU, and the Fast lane waits for
   it. That is behind the plan-wait p99 miss ([Appendix C §6.5](rollout-validation-alternatives.md#65-round-3-dependency-tracking)).
   Cheaper simulations fix it at the root.
3. **The shared-core refactor touches every method.** "Gate off equals mainline" rests on generator coverage. Known thin
   spots: replace decisions in repair, DRA, ODCR and static pools in the drift generator.
4. **Dependency scope** (section 8): accepted risk.
