# Disruption Pipeline

*Voluntary disruption that keeps up with large clusters: drift, repair and consolidation make progress side by side,
and throughput is bounded by the disruption budget, not by the controller loop.*

_Draft RFC. The detailed designs are linked as appendices under `designs/disruption-pipeline/`. Code references are
`pkg/...` paths at the tip of the implementation stack (Implementation status); `mainline:` marks
kubernetes-sigs/karpenter @ `6eac93ba`. Every measurement is quoted with its conditions._

**Decisions so far:**
1. One alpha feature gate, `DisruptionPipeline` (default off). With it off, behaviour equals mainline at that commit.
   Improvements to today's controller ship as separate PRs on their own merit, and the one that changes behaviour says
   so.
2. Method logic is shared, not copied. Each method has one core; today's controller calls it with batch size 1 and
   inline side effects, and differential tests prove each refactoring step adds no behaviour.
3. Budget semantics, the API, NodePool limits, PDB and do-not-disrupt handling do not change. The budget still caps
   total concurrent disruption across reasons. Allocation stays first come, first served.
4. Lanes are injected configuration. Default: Fast = repair, emptiness, static drift, drift; Background = multi- and
   single-node consolidation. Repair stays first in Fast, with no preemption.
5. Batched drift and repair may pack N candidates onto M replacements. Blast radius is capped: one simulation emits
   commands of at most 10 candidates (`ComponentCap`).
6. Batched simulations may place candidate pods on launched-but-uninitialized nodes. Those nodes become the command's
   dependencies; the Queue waits for them and rolls the command back if one fails. Any uninitialized, non-deleting
   node may be a dependency (an accepted risk, see Open Questions).
7. No user-facing knobs. The batch size sizes itself to a deadline; the shadow mode is switched by a test/soak-only
   environment variable.

## Summary

The disruption controller is a singleton that runs six methods in a fixed order and stops at the first method that
produces a command. Drift and repair emit one command per pass, and every replacing command blocks the next pass until
its replacement has a providerID. At 2,000-5,000 nodes rollouts take hours to days, and consolidation starves while
drift has work.

This RFC replaces that loop, behind `DisruptionPipeline`, with three stages:

1. **Observe.** One read-only snapshot per lane round, shared by the lane's planners.
2. **Plan.** Two lanes of side-effect-free planners over the shared method cores. Drift and repair plan many nodes per
   round from one joint simulation that emits commands of at most 10 candidates.
3. **Admit & Execute.** One serialized commit loop. A *Ledger* keeps the budget cap, NodePool limits and static node
   counts as live sums across lanes; an *Admitter* re-validates each plan against live state and re-simulates when a
   plan that started meanwhile could have invalidated it. Starts go through today's `disruption.Queue`.

On KWOK (AWS provider, ~2,100-2,240 nodes, 10% budget, 4-CPU controller) the gate-on build replaced drifted nodes at
146-166 nodes/min against 6.9-9.5 for mainline, with 0 placement-constraint violations and about 1.3 s instead of
about 24 s of controller CPU per replaced node. Beta is not met yet: budget utilization is 0.56-0.65 (target 0.9), the
Fast-lane plan wait p99 is 13-37 s (target 5 s), and the shadow soak still shows logic mismatches.

## Motivation

The disruption controller (`mainline: pkg/controllers/disruption/controller.go`) runs Repair → Emptiness →
StaticDrift → Drift → MultiNode → SingleNode on every pass (`controller.go:102-123`). The first method that produces a
command ends the pass (`controller.go:179-191`). Three properties of that loop limit every large cluster:

1. **One command per pass for drift and repair.** Drift returns as soon as it has one command (`drift.go:117`). After
   any replacing command, `cluster.Synced()` stays false until the replacement NodeClaim has a providerID
   (`state/cluster.go:189-195`), and the next pass requeues after 1 s (`controller.go:152`). The loop pays launch
   latency once per drifted node.
2. **Redundant work.** Every method rebuilds the full candidate list (`helpers.go:273-299`). Every simulation re-places
   the pods of every node already being deleted (`helpers.go:113-117`). A 15 s validation wait runs inside the loop
   for every consolidation command (`consolidation.go:45`, `validation.go:139,204`).
3. **Head-of-line blocking across methods.** While drift has work, consolidation never computes. While SingleNode scans
   a large cluster (125 s per pass at 2,000 packed nodes, below), drift and repair wait.

The execution side was parallelized long ago (orchestration queue #542, multithreaded queue #2293, eviction queue
#2252). Command **generation** is still a serial loop that rebuilds everything each pass.

### Evidence

**Users and maintainers** (states checked 2026-10-21; full table in
[Appendix C](disruption-pipeline/rollout-validation-alternatives.md#11-upstream-issues)):

| issue | what users saw | relevance |
|---|---|---|
| [kubernetes-sigs/karpenter#3197](https://github.com/kubernetes-sigs/karpenter/issues/3197) (open, `triage/accepted`, `/needs-design`) | Drift rollouts above 4,000 nodes "take multiple days"; raising budgets "had no effect". A maintainer names the invariant to keep: "simulation B shouldn't depend on simulation A" | direct |
| [#3320](https://github.com/kubernetes-sigs/karpenter/pull/3320) (RFC PR) | 7 days at ~5,000 nodes: ~1,000 drifted nodes eligible, ~17 drift decisions/h, ~4.5 of ~2,100 allowed drift slots used; ~2,000 consolidation-eligible nodes, 25 consolidation decisions in 7 days | throughput and turn starvation |
| [#2927](https://github.com/kubernetes-sigs/karpenter/pull/2927) (RFC PR, Stripe) | "consolidations are effectively paused for 20 minutes to an hour" while drift runs | turn starvation |
| [#3080](https://github.com/kubernetes-sigs/karpenter/issues/3080) | A failing (ICE) drift head blocks younger NodePools. A maintainer: "We've been wanting to refactor the disruption controller into more of a pipeline with fairness" | head-of-line blocking |
| [#3421](https://github.com/kubernetes-sigs/karpenter/pull/3421), [#3305](https://github.com/kubernetes-sigs/karpenter/issues/3305) | One SingleNode pass took ~1 h at 300 nodes with 22,500 pending pods; at 300 nodes / 30 NodePools SingleNode abandons after 215 candidates | long scans block everything |
| [#3288](https://github.com/kubernetes-sigs/karpenter/issues/3288), aws#9207, aws#8212, aws#7935 | Users can't tell "decided not to act" from "never reached" | observability |
| [#1750](https://github.com/kubernetes-sigs/karpenter/issues/1750) (122 reactions) | "Drift itself we can't rely on ... so we effectively turned it off" | workaround |

Users work around this by restarting the controller on a cron (aws#3785, aws#3576), deleting nodes by hand (300 in
aws#5733), disabling drift or consolidation, or narrowing instance types (aws#9531). Maintainers have declined tuning
knobs for this (polling period #2853, timeouts #1031/#1754/#1733, max candidates #3186), so the fix must be
self-tuning.

**Measured on today's code** (real `Controller.Reconcile` on envtest 1.37 with a cache-backed client; 1 NodePool,
budget `nodes: 10%`; N spot nodes, 10 pods each at 94% CPU; 1,344 fake instance types; shared 18-core machine with
±30-50% noise):

| finding | number |
|---|---|
| Drift simulations and commands per pass | exactly 1 and 1 on every pass that filled the budget, at every N |
| `Synced()` false after a replacing command | 222 of 222 passes at N=2,000 |
| Filling a 10% budget at N=2,000 | 222 passes, 206-268 s of loop compute (launch latency excluded) |
| Share of drift-loop `Reconcile` CPU in candidate builds (3 per pass) | 62% (pprof); GC 44% of process CPU |
| SingleNode on a packed cluster | 0.77 s at N=100, 10.5 s at 500, 32-46 s at 1,000, 125-127 s at 2,000 |
| Consolidation during a budget-saturated drift rollout | 0 simulations: blocked by the shared disrupting count (`helpers.go:395-412`) |

**Measured on KWOK** (AWS provider in KWOK mode on EKS, ~2,100-2,240 nodes from 12,500 pods, 10 NodePools, 30% of
Deployments with zone spread, 10% with hostname anti-affinity, budget `nodes: 10%`, controller 4 CPU / 8 Gi;
[Appendix C](disruption-pipeline/rollout-validation-alternatives.md#6-kwok-results)):

| | mainline | gate on (latest) |
|---|---|---|
| drift, nodes replaced/min (3 gate-off runs capped at 1 h, 4 gate-on runs) | 6.9-9.5 | **146-166** |
| drift rollout of ~2,100-2,240 nodes | not finished in 1 h (424-575 replaced) | 760-876 s |
| budget utilization during drift (time-weighted gauges) | 0.017-0.018 | 0.56-0.65 |
| controller CPU per replaced node | 19.6-25.8 s | 1.27-1.40 s |
| placement violations after drift (zone spread, anti-affinity, limits) | 0-2 (skew left by the provisioning bind race) | 0 |
| drift heap p95 | 5.2-5.7 GB | 6.1-6.5 GB |

**What can and cannot improve.** By Little's law the ceiling is `slots / slot lifetime`. 200 slots held 5-10 min each
(launch, initialization, drain) is 20-40 nodes/min. KWOK nodes initialize in seconds, so the KWOK rates above are
far above what a real cloud allows; what carries over is that the loop stops being the limit. **The goal is to make
rollouts budget-bound**: the budget the user configured becomes the throughput they get. The larger gain is that
consolidation, repair and drift stop blocking each other.

The budget caps **total** concurrent disruption: every in-flight node counts against every reason
(`mainline: helpers.go:395-412`, #981). A reason's remaining slots are `min(allowed over the budgets that apply) −
disrupting(all reasons)` (`apis/v1/nodepool.go:368-381`). This is intentional and does not change. In #3320 the budget
was nearly empty (4.5 of 2,100), so consolidation's 25 decisions in 7 days were **turn starvation**, which this RFC
removes. Once drift fills the budget, consolidation competes for freed slots first come, first served, as today.

### Use Cases

1. **Fleet-wide AMI rollout at 2,000-5,000 nodes** (#3197, #3320, #1750). The Fast lane plans one joint simulation per
   round for up to the remaining budget and emits commands of at most 10 candidates. Success: budget utilization at or
   above 0.9 for the whole rollout.
2. **Consolidation while drift rolls** (#2927, #3320). The Background lane computes in its own goroutine and its 15 s
   validation is a per-plan timer. Consolidation acts only when budget remains; users who want it guaranteed during
   rollouts use per-reason budgets (Interaction with Existing Features).
3. **A long consolidation scan must not delay drift or repair** (#3421, #3305). A 125 s SingleNode pass runs in the
   Background lane; repair and drift keep planning and are admitted between consolidation plans.
4. **"Why is nothing happening?"** (#3288, aws#9207). Per-lane gate time, per-planner admission outcomes, and a
   NodePool event when a planner has no budget left.

### Goals

- Remove "first method wins". Every planner in a lane runs every round; the two lanes plan independently.
- Batch drift and repair through one joint simulation per round. "Simulation B does not depend on simulation A" holds
  by construction inside a batch, and by serial admission with re-simulation across plans.
- Turn every blocking wait (15 s validation, `Synced`, budget) into a gate or timer of one plan or lane.
- Keep budget semantics, NodePool limits, PDB and do-not-disrupt handling exactly as today.
- Add no user-facing knob.
- Ship behind one gate, with today's controller unchanged when it is off.

### Non-Goals

- **Method algorithms.** MultiNode's prefix binary search and 100-candidate cap, SingleNode's scan order and the
  consolidation price rules are reused as they are.
- **Cheaper simulations.** `NewScheduler` is 88% of a drift simulation at 2,000 nodes and `Solve` 0.3% (KWOK). Reusing
  scheduler or topology state across simulations is a separate track (#2972, #3240, and the topology RFC). This RFC
  changes how many simulations run and where they wait, not what one costs.
- **Simulating onto pending replacements** (a virtual-node overlay for unlaunched NodeClaims). Follow-up RFC.
- **Per-reason fair share of the budget.** v1 is first come, first served; the Ledger keeps a policy seam.
- **Budget API or semantics changes** (#1691, #2930, #2914), and charging empty drifted nodes to the Empty budget
  (aws#8032).
- **Persistent candidate pool, cluster partitioning, early replacements, standby replicas** (Alternatives).
- **Concurrent action by multiple replicas.** The pipeline runs on the leader only.

## Proposal

### Overview

```
  Observe (per lane round)         Plan (lanes, one goroutine each)            Admit & Execute (one goroutine)
 ─────────────────────────   ──────────────────────────────────────────   ─────────────────────────────────────────
 SnapshotBuilder.Build  ──►  Fast:       repair → emptiness →             Executor.Submit ─► Ledger.Reserve
  NodePools, ITs, PDBs,                  static drift → drift               (in the lane's goroutine; slot held)
  one DeepCopyNodes,         Background: multi → single                   commit loop, one Step at a time:
  fixed now, pods without                                                   sweep if due
  a deep copy                planners are pure adapters over the            ready plans (ValidateAt passed),
                             shared method cores; each returns Plans        grouped by batch, Scorer order
 ReservationView (live) ──►  drift/repair: SimulateBatch → groups of       → Admitter: deadline, Synced gate,
 BudgetView (live)      ──►  ComponentCap from one simulation → Split        one rebuild per batch, RecheckAll,
                             reservingSimulator: reserved plans'             hard relations, re-sim causes,
                             candidates are not destinations                 joint re-sim / sibling re-plan
                                                                          → Starter = Queue.StartCommand (parallel)
                                                                          → Ledger.MarkStarted (slot released)
                                                       disruption.Queue: replacements + dependencies initialized
                                                                         → delete candidates, or roll back
```

Three rules make it equivalent to today's loop where it must be:

1. **Planning may be stale; admission is fresh and serial.** Planners run in parallel lanes on snapshots. The
   Admitter, the only place a plan becomes a command, re-checks every plan against live state.
2. **Sums live in the Ledger.** Budgets, NodePool limits, static node counts and capacity reservations are sums. No
   pairwise footprint test can see a sum, so the Ledger evaluates them live, across lanes, under one mutex.
3. **Every write goes through the commit loop.** That covers StartCommand, reboots and the taint sweep. Today's
   single-goroutine invariant, "StartCommand's side effects land before the next decision", holds across lanes.

| layer | what it holds | lifetime | where | appendix |
|---|---|---|---|---|
| Observe | NodePools, instance types, PDB index, one node copy, fixed `BuiltAt`; fresh Candidates per planner | one lane round | `pipeline/snapshot.go`, `disruption/helpers.go` (`CandidateSnapshot`) | [A §1](disruption-pipeline/observe-and-plan.md#1-snapshot) |
| Plan | planners, method cores, Simulator, BatchSizer, Split, Footprint | planner state across rounds (sizer, fairness) | `pipeline/planners.go`, `disruption/{core,simulator,split}.go` | [A §2-§9](disruption-pipeline/observe-and-plan.md#2-planner-contract) |
| Lanes | `LaneSpec`, `Lane.Step`, triggers, runners | process (leader) | `pipeline/{lane,trigger,runner,wakes}.go` | [A §10-§11](disruption-pipeline/observe-and-plan.md#10-lanes-and-triggers) |
| Admit & Execute | Ledger reservations, waiting plans, started log | memory only; dropped on lease loss | `pipeline/{ledger,admitter,commitloop,starter}.go`, `disruption/queue.go` | [B](disruption-pipeline/admit-and-execute.md) |

### How It Works

#### 1. Observe: one snapshot per lane round

`SnapshotBuilder.Build` (`pipeline/snapshot.go:112`) builds the class-independent part of candidate construction
once per round: `BuildNodePoolMap`, one PDB list turned into a namespace/label index, one `DeepCopyNodes`, one fixed
`BuiltAt`, and each node's pods listed without a deep copy (informer-cache objects, read-only by contract).
`Snapshot.CandidatesFor(ctx, planner)` (`snapshot.go:163`) applies the disruption controller's class rules to that
core and returns **fresh Candidates with private StateNode shells**, so a planner that writes to its Candidates
(repair stamps a drain bound; the queue rewrites the NodeClaim pointer) can't affect another. It drops nodes that a
reserved plan disrupts and nodes that a started command moves pods onto or depends on, then runs the planner's
`Filter`. Events and verdicts go to a per-round buffer that the Observer publishes.

The same snapshot shape already runs in today's controller through tier-1 PRs (shared candidate snapshot, no pod deep
copy, PDB index): KWOK's drift profile shows `GetCandidatesWithTotals` at 19.5% of samples on mainline and 0% gate-on,
`DeepCopy` 21.9% → 7.5%. Observe does not make simulations consistent (topology counts, nominations and
`cluster.Nodes()` stay live inside each simulation, as today) and is never proof: the Admitter re-validates.

#### 2. Plan: pure planners over shared method cores

**Planners are pure** (`pipeline/planner.go:35-50`). A planner reads its Candidates, a live `BudgetView` and
`ReservationView`, and returns `Plan`s and typed `Verdict`s. It writes nothing to the API server, marks nothing in
cluster state and publishes nothing; a full lane Step against a counting client issues zero writes (spec in
`pipeline/lane_test.go`). Each compute-time side effect of today's methods has a named owner:

| side effect today | owner in the pipeline |
|---|---|
| `ReserveNodeCount` during compute (static drift, repair) | planner reads `NodePoolState.Headroom` minus slots held by reserved plans; the Ledger takes the slots at Reserve |
| repair's reboot handoff | `Plan.Action = Reboot`; the Starter re-reads the NodeClaim and builds the handoff with `disruption.PrepareReboot` |
| Blocked/Unconsolidatable events | per-round buffer → Observer |
| `GetPendingPods` side effects per simulation | pipeline simulations list pending pods with `provisioning.WithoutSideEffects` |

**One method core, two callers** (`disruption/core.go:34-69`). Each method's body moves into
`Compute(ctx, budgets, CoreOptions, candidates...) (CoreResult, error)`, and `ComputeCommands` becomes a wrapper that
passes the method's `LegacyOptions()` (Batch 1, `InlineEffects`, `LegacySimulator`, today's order, pending-only
NodeClaims kept on the first command, the inline validator and dedup bit). The pipeline passes its own options. Each
core has a differential against a verbatim copy of the method on its parent commit: drift 297/297 states at 10-50
nodes and 308/308 at 10-200 nodes, for both `ComputeCommands` and the batched path forced onto single candidates.

**The Simulator and reserved but unstarted plans** (`pipeline/simulator.go:26-77`). A plan holds its budget slot from
Submit until it starts; consolidation plans hold it through their 15 s `ValidateAt`. If other lanes' simulations
treated a reserved plan's candidates as deleting, they would re-place its pods and launch capacity for them twice. So
the pipeline simulator treats **reserved plans' candidates as unavailable destinations only**: their pods are not
re-placed and still count at their nodes for topology. Whichever simulation runs after the plan starts re-places them
(the Admitter's re-simulation for anything coupled to it, or the next round).

**Batched drift and repair** (`disruption/core.go:211-263`). `planBatch` runs `SimulateBatch`: one joint simulation of
K candidates. Candidates whose own pods fail are dropped and the simulation re-run (at most 3 rounds, topped up once
within the NodePools' budgets); an unattributable failure halves the batch; a simulation past twice its deadline is
abandoned; below 2 candidates the core falls back to today's one-at-a-time walk. Terminate-first, reboot and
static-pool candidates take the single path and end a batch where they rank, so a batch never jumps ahead of a
candidate it leaves out.

**Candidate groups.** The joint simulation puts candidates, in rank order, into groups of `ComponentCap` = 10 whose
pods never share a node, existing or new (scheduler option `PodGroups`). `Split` then yields commands of at most 10
candidates from **one** simulation, with limits, reservations and zone topology accounted across the whole batch. One
failed launch stalls at most one group. Commands of one batch are correct together, not each alone: if one is rejected
at admission, the survivors are re-planned (Admit & Execute).

**The BatchSizer** (`disruption/simulator.go:330-406`). K sizes itself; there is no knob. A simulation's cost is a
fixed part (scheduler and topology over every node: ~88% of a drift simulation at 2,000 nodes on KWOK) plus a marginal
part per candidate. The sizer takes the cheapest simulation seen as the fixed cost, sizes K so the marginal part takes
about 2 s, grows at most 2× per batch, never returns less than 2 while the budget allows it, and on a timeout or
halving halves K and holds it for two accepted batches. The deadline grows with the fixed cost. The first two KWOK
attempts found the sizer collapsing to 1 for good and a per-candidate model stuck at K=2; both are fixed in this design.

**Dependencies.** In batched simulations a candidate's pods may land on launched-but-uninitialized nodes (another
command's in-flight replacement) as bin-packing chooses. When some destination isn't initialized, candidate pods are
scheduled after pending pods and pods of deleting nodes (`CandidatePodsLast`), so those keep the capacity launched for
them and the candidates get what is left: nothing is provisioned twice. The uninitialized nodes holding a candidate's
pod are the command's `Dependencies`; candidates sharing one are one command. The Queue waits for them like
replacements and rolls the command back if one is deleted or doesn't initialize before the command times out.
The legacy path keeps rejecting such placements (`UninitializedNodeError`).

**Fairness within drift.** Drift candidates are ordered round-robin across NodePools, least recently served first,
then Drifted time, then providerID (a herd shares one transition second). A NodePool whose drift command failed backs
off 1 min, doubling to 10 min with ±10% jitter, reset by a success (#3080). A planner none of whose last plans started
doesn't plan for 10 s. Fairness across reasons is the Ledger's policy (first come, first served).

#### 3. Lanes and triggers

A lane is injected configuration (`pipeline/lane.go:32-44`): `LaneSpec{ID, Planners, SkipWhileUnsynced, Trigger}`.
`DefaultLanes` (`pipeline/trigger.go:100-112`) is Fast = repair, emptiness, static drift, drift and Background = multi,
single (skipped while unsynced). `Lane.Step` (`lane.go:121-191`) is the only unit of work and never sleeps:

1. lane gates: prediction store hydrating; no budget for any planner's reason (repair is budget-free, because an
   uninitialized node is repaired without budget); unsynced for a `SkipWhileUnsynced` lane;
2. one snapshot;
3. each planner in order: skipped while its previous Submit is unresolved (back-pressure is per planner, so a 15 s
   emptiness plan doesn't stall drift), while it backs off, or while its reason has no budget; while unsynced, a
   planner that moves pods gets only pod-free candidates; then `CandidatesFor`, `Plan`, `Executor.Submit`. The Ledger
   reserves inside Submit, so the next planner plans around those reservations: "repair before drift" by program order.

One goroutine per lane and one for the commit loop run as leader-elected runnables; `Controller.Reconcile` is a
synchronous runner for tests. The trigger paces rounds: Fast runs again at once after a start or a wake-up (a slot
freed, cluster state synced, one of its plans resolved); while budget-bound or holding candidates back for sync it
polls every second; Background waits `max(1 s, 3 × last round)`, a 25% duty cycle; otherwise the 10 s polling period.
Wake-ups are semantic only, never raw informer events (~1k pod updates/s at 60k pods with no change).

#### 4. Admit & Execute: the serial point

**Plan lifecycle.** `computed → Submit (Ledger.Reserve) → reserved → ValidateAt → admitting → admitted → started
(Ledger.MarkStarted, after StartCommand marked the candidates) → Queue`. A rejected or dropped plan releases at once.
The lane receives exactly one `Resolution` per plan.

**Ledger** (`pipeline/ledger.go:120-158`; bookkeeping for the existing cap). Per NodePool np and reason r, with
`M_live` = nodes MarkedForDeletion or rebooting, `D_live(np, r)` = `M_live` plus NotReady nodes unless r is Unhealthy,
and `R(np)` = budget nodes of reserved, unstarted plans (all reasons):

```
Used(np, r)  = |D_live(np, r) ∪ R(np)|  (+ reserved NotReady candidates once more)
charge(p)    = |BudgetNodes_p[np] \ (R(np) ∪ M_live(np))|
Reserve(p) ok ⇔ ∀ np: Used(np, p.Reason) + charge(p) ≤ allowed(np, p.Reason, numNodes_live, now)
```

A node is counted once across the hand-off from reservation to MarkedForDeletion. A NotReady candidate is charged as
today's per-candidate decrement does. The admission re-check excludes the plan's own reservation. Limits add each
other reserved plan's net (new capacity minus its own unmarked candidates' capacity), never below zero, so a plan never
borrows capacity another plan has not yet freed. Static node counts are taken at Reserve. Allocation goes through an
`AllocationPolicy` seam; `FirstComeFirstServed` is the only policy.

**Admitter** (`pipeline/admitter.go:125-243`). Called only from the commit loop; no goroutines, no writes except to
the Ledger. Per group (one plan, or the ready plans of one batch):

1. **Deadline.** More than 1 min past `ValidateAt` → dropped, budget released.
2. **Synced gate.** While unsynced only pod-free plans (no replacements, not a reboot, not static drift, no
   reschedulable pods) are admitted; a batch waits as a whole. This is the only relaxation from today.
3. **One rebuild for the batch** from live state (O(candidates) node copies, one NodePool/instance-type/PDB listing),
   and **one `Ledger.RecheckAll`** against one reading of live state.
4. Per plan: still a candidate of its class, not nominated, not blocked, planner filter still true, pod-set hash
   unchanged (consolidation rejects on change, drift and repair re-simulate).
5. **Hard relations** reject: the plan disrupts a node a started command moves pods onto or depends on.
6. **Re-simulation causes**: `always` (consolidation), `coupled` (related to a plan started since this one was
   simulated), `stale` (>10 s since submission), `catalog`, `pods_changed`, `sibling`.
7. **Acceptance.** Drift and repair re-simulate a batch's members jointly (one simulation, accepting the subset it
   places) and adopt the result; consolidation re-validates with the method's own validator; emptiness may shrink.
   If a batch member is rejected, the survivors are re-planned once with one joint simulation, or none starts.
8. A plan related to one admitted earlier in the same step waits for the next step.

**Executor and Starter** (`pipeline/commitloop.go:185-330`). Each commit step runs the taint sweep if due (startup,
every minute, after a failed start; it keeps reserved nodes), admits the ready groups in `Scorer` order (lane, planner,
`ValidateAt`, age: a tie-breaker), then starts the admitted plans in parallel. The Starter is `Queue.StartCommand`,
unchanged: taint, create replacements, MarkForDeletion. On lease loss waiting plans are dropped (reservations are in
memory only); a start that began runs to completion.

## Proposed Spec

There is no CRD, API or provider-interface change. The user-facing surface is one feature gate
(`pkg/operator/options/options.go:161-168`):

```go
DisruptionPipelineFeatureGate = FeatureGate{
	Name:    "DisruptionPipeline",
	Default: false,
	Stage:   docs.Alpha,
	Help: "Karpenter plans voluntary disruption in parallel lanes and admits plans one at a time against fresh " +
		"cluster state, instead of running disruption methods one after another. Drift and repair are batched up " +
		"to the disruption budget. Budget semantics are unchanged.",
}
```

The switch is one function in `pkg/controllers/controllers.go:239-251`: `pipeline.NewController` with the gate on,
`disruption.NewController` with it off. Both share the `disruption.Queue`. The KWOK chart exposes
`settings.featureGates.disruptionPipeline` (default false).

**No new tuning knobs.** Lanes are injected in code (`pipeline.WithLanes`); internal constants are not options:

| constant | value | where |
|---|---|---|
| `ComponentCap` | 10 | candidates per command of a batch (blast radius of one launch failure) |
| `BatchSimDeadline` | 2 s (abandoned at 2×, grown by the measured fixed cost) | marginal cost a joint simulation is sized to |
| `FirstBatchMax`, `BatchMinSize`, `MaxBatchRounds`, `BatchSizerCoolDown` | 50, 2, 3, 2 | first batch, smallest joint batch, simulation rounds, batches before K grows after a shrink |
| `StaleAfter`, `MaxPlanTime` | 10 s, 30 s | `stale` re-simulation; started-log retention is their sum |
| `MaxAdmissionWait` | 1 min after `ValidateAt` | plans stuck behind `Synced` release their budget |
| `SweepTick` | 1 min | leftover taint sweep |
| pacing | 1 s minimum, 3× duty factor, 1 s gate poll, 10 s polling period and planner back-off | lanes |
| drift NodePool back-off | 1 → 10 min, ±10% jitter | failed drift commands |

**Shadow mode** is switched on only by `KARPENTER_DISRUPTION_PIPELINE_SHADOW=<rate in (0,1]>`
(`pipeline/shadow.go:56,71`) with the gate off. It is undocumented and unsupported, for tests, KWOK and soak clusters;
there is no flag, option or Helm value, and it is deleted at GA.

## Interaction with Existing Features

| feature | with the gate on |
|---|---|
| **Disruption budgets** | Same semantics. The Ledger reserves at Submit and holds the slot while a plan waits; `nodes_consuming_budgets` counts reserved plans. Consolidation during a saturated rollout runs only when budget remains |
| **Per-reason budget pattern** (documented, no change) | `{nodes: "5%", reasons: [Drifted]}` + `{nodes: "10%"}`: drift stops at 5%, so at least 5% stays usable by consolidation and emptiness |
| **Empty drifted nodes** | Unchanged: emptiness runs before drift in the Fast lane and takes those it is eligible for (Empty budget); drift skips nodes emptiness reserved |
| **NodePool limits** | Live Ledger sum, re-checked at admission; a replacement is charged at its largest instance-type option |
| **PDBs, do-not-disrupt** | Same checks, re-validated at admission from live state |
| **`Synced`** | Still global. Pod-free deletes may be admitted while unsynced; nominated nodes are rejected at re-validation |
| **Terminate-first** | Single-candidate path, unchanged decisions |
| **Static NodePools** | Same per-candidate commands; the node-count slot is taken at Reserve |
| **Balanced consolidation** | Evaluator built per round from the snapshot's totals; approval not re-checked at admission, as today |
| **Node repair** | Fast lane, first, no preemption, batched like drift; reboot is a plan action re-read at start |
| **Drift** | Several commands per round up to the remaining budget, packed N→M, ≤ 10 candidates per command, round-robin across NodePools with per-pool back-off |
| **Pending pods** | Drift and repair plans don't launch NodeClaims that hold only pending pods; the provisioner launches those |
| **In-flight destinations** | Nodes that started commands move pods onto, or depend on, are not candidates until those commands finish |
| **Events** | Same reasons and text through the recorder's dedupe. One new NodePool reason is emitted (`DisruptionBudgetExhausted`), edge-triggered per planner |

## Observability

All new series use `karpenter_voluntary_disruption_pipeline_*`, are emitted only with the gate on, and use bounded
label values (full list in [Appendix B §8](disruption-pipeline/admit-and-execute.md#8-observer-metrics-events-and-logs)):

| metric | labels | answers |
|---|---|---|
| `round_duration_seconds`, `rounds_total` | `lane`, `phase={snapshot,plan,admit}` / `result` | is a lane running, where its time goes |
| `gated_seconds_total` | `lane`, `gate={synced,budget_zero,unresolved,paced,hydrating}` | why a lane or planner is idle |
| `plans_total` | `planner`, `result`, `reason` | admission outcomes and reject reasons |
| `plan_wait_seconds`, `admission_duration_seconds` | `lane`, `planner` / `planner` | submission → start; cost of admission |
| `admission_resims_total` | `planner`, `cause` | re-simulation rate by cause |
| `joint_simulations_total`, `joint_simulation_candidates`, `joint_simulation_duration_seconds`, `joint_simulation_dropped_candidates_total` | `planner`, `result`/`stage`/`drop_reason` | batching |
| `planned_candidates`, `plans_per_round`, `admitted_batches_total` | `planner`, `stage`/`result` | where a batch's candidates were lost |
| `ledger_reserved_nodes`, `unresolved_plans`, `last_admitted_timestamp_seconds` | `nodepool` / `lane` / `planner` | budget held by unstarted plans; starvation |

The Queue, shared by both controllers, adds `karpenter_voluntary_disruption_queue_commands_waiting_on_dependencies`
and `..._queue_dependency_failures_total{cause=deleted|timeout}`; neither series exists until a command with
dependencies was seen, so the gate-off build exports nothing new. `decisions_total`, `eligible_nodes`,
`nodepools_allowed_disruptions`, `failed_validations_total` keep their meaning. `decision_evaluation_duration_seconds`
covers all of a serial `disrupt()` call including the 15 s wait; that meaning doesn't exist in the pipeline, so it is
**not emitted with the gate on**, and dashboards built on it go empty.

## Edge Cases

1. **AMI herd, one NodePool, zone spread on most Deployments.** One joint simulation of K candidates, groups of 10, all
   commands from one simulation. Measured on KWOK: joint simulations accept a median of 25-42 candidates (p99
   98-183), 18-32 plans per round.
2. **Split hazard.** The joint simulation gives siblings {a1 → zone b} and {b1 → zone a}. If b1 is rejected, a1 alone
   would leave zone skew 2; the Admitter re-plans a1 with a fresh joint simulation instead.
3. **Consolidation straddling a drift plan.** SingleNode never re-placed a reserved drift plan's pods, so it can't
   double-provision for them. If it plans to delete a node a started drift command moves pods onto, it is rejected.
4. **Launch failure in a batch.** That command fails (≤ 10 candidates), its NodePools back off, and commands that
   depend on its replacement roll back with it.
5. **A cron budget window closes while a consolidation plan waits 15 s.** `RecheckAll` with the live clock rejects
   it `budget`.
6. **Lease lost during `ValidateAt`.** The plan is dropped; nothing was written.

## Alternatives Considered

Details and data are in [Appendix C §7](disruption-pipeline/rollout-validation-alternatives.md#7-alternatives-considered).

1. **Incremental fixes on the singleton** (#3320's replacement-only drift batching, #2927's time slicing). Kept as the
   tier-1 PRs; they doubled KWOK drift throughput (5.0 → 9.9 nodes/min) but leave one command per pass.
2. **Parallel methods with a conflict arbiter.** Parallel simulations pick the same nodes (~1/K survive on loose
   clusters) and topology is blind across them. Kept: the conflict taxonomy, the single commit point, the joint Ledger.
3. **Pre-partitioning into independent islands.** Common setups collapse to one island; Amdahl caps the gain at 30-60%.
4. **One goroutine per planner.** "Repair before drift" becomes a race. Lanes keep the order in code.
5. **Re-planning chunks of a batch with one simulation each** (the first batching design). At 2,000 nodes each chunk
   simulation costs ~4 s, so 1-2 chunks fit a round. Replaced by candidate groups from one simulation.
6. **Fencing uninitialized destinations** (candidates' pods kept off in-flight replacements). Every candidate bought
   capacity of its own; replaced by dependency tracking (141 vs 150 replacements at N=1,000 in envtest).
7. **Per-plan admission.** At ~2,200 nodes it cost 9.8 s per batch (per-plan rebuilds and live reads); replaced by one
   rebuild and one recheck per batch.
8. **Event-driven validation instead of the 15 s wait; a persistent candidate pool now; simulating onto pending
   replacements in v1; reserving and starting atomically at admission; launching the next batch's replacements early;
   planning on standby replicas; per-reason fair share in v1.** Each is discussed with its data in Appendix C.

## Backward Compatibility

- **Gate off (alpha default):** `disruption.NewController` runs. The pipeline package isn't constructed: no new
  goroutines, pipeline metrics or events. The legacy controller calls the shared cores with legacy options; each
  refactoring PR carries a differential against a verbatim copy of the method on its parent commit, and the existing
  suite runs unmodified (638/639 at the tip under `-race`, the one failure an envtest DRA spec that also fails on the
  base). Shared-code changes are additive and default to today: new `SimulationOptions` fields, scheduler options,
  `Queue.WithCompletionHook`, `NodePoolState.Headroom`, and the Queue's dependency wait, which returns at once for a
  command without dependencies. A 4-scenario Queue trace (events, NodeClaim, taint, state, every `karpenter_*` series)
  is byte-identical before and after the dependency change. One tier-1 PR changes behaviour and is labelled: the loop
  keeps evaluating methods after delete-only commands.
- **Gate on:** same API, CRDs, budget, limits, PDB and do-not-disrupt semantics. The intended, observable changes, each
  pinned by a spec: several commands per round up to the remaining budget; N→M packing with at most 10 candidates per
  command; consolidation computes and validates while drift rolls and acts when budget remains; pod-free deletes may
  be admitted while a launch is pending; drift order is NodePool round-robin with back-off and providerID tie-break;
  pending-only NodeClaims are left to the provisioner; in-flight destinations and dependencies are not candidates;
  commands may wait for dependencies; `decision_evaluation_duration_seconds` and four loop metrics are not emitted.
  The gate-on twin of the disruption suite runs 633/634 with 5 specs skipped, each citing the intended difference.
- **Flipping the gate** needs a restart. Everything new is in memory; started commands are recovered as today.
- **Cloud providers:** no interface change.

## Graduation Criteria

### Alpha (gate default off)

Too risky to default on because admission is a new serial point whose correctness rests on re-simulation rules;
batched drift changes replacement shapes; the Fast lane changes who gets freed slots in practice; and dependency
tracking makes one command's success depend on another's replacement.

Ships when: the stack below is merged; with the gate off the full disruption suite passes unmodified and every
batch=1 differential passes; with the gate on the twin suite passes except the cited intended differences; invariant
specs pass under `-race`; the known gaps in Implementation status are closed.

### Beta (gate default on)

Gate on vs off, same image, KWOK config R (Appendix C §5), n ≥ 10 interleaved pairs for the "improve" rows. Status
from the latest runs (round 3: 4 gate-on and 3 gate-off runs, ~2,100-2,240 nodes, 10% budget, 4 CPU / 8 Gi):

| # | criterion | threshold | latest status |
|---|---|---|---|
| 1 | shadow soak | ≥ 2 weeks at rate ≥ 0.1 on ≥ 3 clusters; logic mismatches 0; race < 0.5% | **not met**: 9 logic + 13 race of 319 comparisons (one cluster, 45 min drift + 20 min consolidation) |
| 2 | budget utilization during drift | mean ≥ 0.9 | **not met**: 0.56-0.65 (0.85-0.98 within each fill in round 2; gaps are registration waits and NodePool tails) |
| 3 | rollout time, 2,000 nodes, 10% | ≤ max(1.1 × ceil(N/B) × T, 0.5 × gate off) | met: 760-876 s; gate off replaced 424-575 nodes in 1 h |
| 4 | consolidation during a rollout | > 0 with the per-reason pattern; not worse than gate off | met (round 2, overlap variant): 11 empty + 7 underutilized vs 9 empty + 0 underutilized |
| 5 | Fast-lane plan wait p99 | ≤ 5 s | **not met**: 13-37 s (p50 1.2-2.3 s); the tail waits behind 6-15 s joint simulations |
| 6 | controller CPU per replaced node (replaces "per simulation", wrong for batches) | ≤ gate off | met: 1.27-1.40 s vs 19.6-25.8 s |
| 7 | provisioner pending→bound p95 during drift | ≤ gate off + 10% | met: 28-38 s vs 34-38 s |
| 8 | heap p95 during drift | ≤ gate off + 20% | met, narrowly: 6.1-6.5 GB vs 5.2-5.7 GB (+18% median) |
| 9 | placement after drift | 0 violations; extra NodeClaims ≤ gate off + 2% | violations met (0 zone spread, 0 anti-affinity, 0 limit); NodeClaim count **open** (packing question) |
| 10 | perf suite `drift/drift` | heap < 250 MB, CPU < 1.05 cores, < 50 min | not run |
| 11 | real cloud | one 1k-node AWS cluster: launch latency, utilization | not run |
| 12 | bugs | no open important+ `area/disruption` bug attributed to the pipeline | - |

### GA (gate removed)

At least 2 minor releases at beta with no mismatch-class or budget-breach reports; the AWS provider and one other
provider ran the perf suite; the legacy controller, the batch=1 call sites, the reference copies and the shadow
environment variable are deleted; the per-reason budget pattern is in the docs.

## Validation

Summary only; [Appendix C](disruption-pipeline/rollout-validation-alternatives.md) has methods, tooling and every
result.

1. **Differentials at batch=1, per method,** against verbatim copies on generated states (zone spread, anti-affinity,
   PDBs, do-not-disrupt, limits, reserved capacity, pending pods), resampling upstream's own nondeterminism.
2. **Pipeline at batch=1.** One lane in the legacy method order, first-wins, batch 1, against the legacy controller:
   100 seeds, 241 passes, 235 exact, 2 equal up to ties, 4 ended at the intended in-flight-destination difference,
   0 mismatches.
3. **Gate-on twin** of the disruption suite (~200 specs reconcile the pipeline).
4. **Invariants for batch=K**: an end-state checker that doesn't trust the scheduler (60/60 seeds, 0 hard violations,
   261 vs 533 new NodeClaims against serial), plus both lanes and the commit loop running concurrently under `-race`
   with budget and ownership asserted after every step.
5. **Shadow mode** with the gate off: snapshot and reference levels, race/logic recheck.
6. **KWOK**: three rounds on two EKS clusters, A/B pairs on one cluster, fixed-window profiles, a packing and
   constraint checker before and after drift.

## Open Questions

1. **Dependency scope.** Any uninitialized, non-deleting node may be a dependency, not only replacements of started
   commands. A node that registers but never initializes (most likely a misconfiguration) can cause repeated wait and
   rollback cycles for commands that pack onto it, and while it is a dependency it is protected from repair. Accepted
   for alpha; the fallback is to restrict dependencies to started commands' replacements or by node age. Do
   maintainers agree?
2. **Budget utilization 0.56-0.65.** Within a fill it reaches 0.85-0.98; the gaps are registration waits and NodePool
   tails. Is 0.9 the right beta bar on KWOK, where slot lifetime is seconds, or should it be measured on a real cloud?
3. **Plan wait p99.** Should a long joint simulation yield the commit loop, or should the 5 s bar apply to the
   commit-loop part only?
4. **Fair share.** Do maintainers want a per-reason share of freed slots as a follow-up, given the `AllocationPolicy`
   seam? Otherwise the per-reason budget pattern is the documented answer.
5. **Packing.** Gate-on drift launched 0.92-1.02 replacements per replaced node, mainline 0.67-0.85 over partial
   rollouts. These are not comparable end states; how should beta measure "extra NodeClaims"?
6. **Leader lease at 2 CPU.** Both builds lost the lease during 12,500-pod provisioning bursts at 2 CPU. Out of scope,
   but it bounds what can be measured at 1-2 CPU.
7. **Start timeout.** Starts run to completion after lease loss instead of under a timeout below the renew deadline.
   Acceptable?
8. **Internal constants** (Proposed Spec): acceptable without knobs?

## Implementation Status

All PRs are open on the fork for review, each one commit on the previous, and none has been proposed upstream.

| stack | PRs | contents |
|---|---|---|
| Tier 1 (today's controller, no gate) | [#49](https://github.com/ryan-mist/karpenter/pull/49) scale benchmark, [#50](https://github.com/ryan-mist/karpenter/pull/50) loop metrics, [#51](https://github.com/ryan-mist/karpenter/pull/51) shared candidate snapshot, [#52](https://github.com/ryan-mist/karpenter/pull/52) reboot handoff out of compute, [#53](https://github.com/ryan-mist/karpenter/pull/53) PDB index, [#54](https://github.com/ryan-mist/karpenter/pull/54) no pod deep copy, [#55](https://github.com/ryan-mist/karpenter/pull/55) continue after delete-only commands (**behaviour change**), [#56](https://github.com/ryan-mist/karpenter/pull/56) taint sweep filter | KWOK A/B (3 pairs): drift 5.0 → 9.9 nodes/min, CPU per replaced node −49% |
| Stack A (shared cores, gate off) | [#67](https://github.com/ryan-mist/karpenter/pull/67) drift + batching, [#68](https://github.com/ryan-mist/karpenter/pull/68) emptiness, [#69](https://github.com/ryan-mist/karpenter/pull/69) static drift, [#70](https://github.com/ryan-mist/karpenter/pull/70) repair, [#71](https://github.com/ryan-mist/karpenter/pull/71) consolidation | one core per method, legacy options, differentials |
| Stack B (pipeline skeleton) | [#57](https://github.com/ryan-mist/karpenter/pull/57) gate and switch, [#65](https://github.com/ryan-mist/karpenter/pull/65) types and snapshot, [#66](https://github.com/ryan-mist/karpenter/pull/66) lanes and sync runner, [#58](https://github.com/ryan-mist/karpenter/pull/58) Ledger, [#59](https://github.com/ryan-mist/karpenter/pull/59) Admitter, [#60](https://github.com/ryan-mist/karpenter/pull/60) commit loop and Starter, [#61](https://github.com/ryan-mist/karpenter/pull/61) Observer | gate on does nothing until Stack C |
| Stack C (planners and lanes) | [#62](https://github.com/ryan-mist/karpenter/pull/62) async runners, [#63](https://github.com/ryan-mist/karpenter/pull/63) wake-ups, [#64](https://github.com/ryan-mist/karpenter/pull/64) footprints, [#76](https://github.com/ryan-mist/karpenter/pull/76) planners (gate on disrupts), [#77](https://github.com/ryan-mist/karpenter/pull/77) drift fairness, [#78](https://github.com/ryan-mist/karpenter/pull/78) invariants, [#79](https://github.com/ryan-mist/karpenter/pull/79) side-effect-free pending pods | |
| Stack D (validation and KWOK fixes) | [#80](https://github.com/ryan-mist/karpenter/pull/80) reference mode, [#81](https://github.com/ryan-mist/karpenter/pull/81) gate-on twin suite, [#82](https://github.com/ryan-mist/karpenter/pull/82) shadow mode, [#83](https://github.com/ryan-mist/karpenter/pull/83) KWOK chart gate, [#84](https://github.com/ryan-mist/karpenter/pull/84) rollout bench, [#85](https://github.com/ryan-mist/karpenter/pull/85) joint admission re-simulation, [#86](https://github.com/ryan-mist/karpenter/pull/86) batch metrics, [#87](https://github.com/ryan-mist/karpenter/pull/87) budget-bound pacing, [#88](https://github.com/ryan-mist/karpenter/pull/88) fewer re-simulations, [#89](https://github.com/ryan-mist/karpenter/pull/89) shadow triage, [#90](https://github.com/ryan-mist/karpenter/pull/90) one-rebuild admission, [#91](https://github.com/ryan-mist/karpenter/pull/91) dependencies | the tip |

**Known gaps at the tip** (to close before alpha):
- **The pipeline doesn't hand dependencies to the Queue.** `Plan.ToCommand` (`pipeline/planner.go:150-161`) copies
  candidates, replacements and results but not `Command.Dependencies`, so a started pipeline command doesn't wait for
  its dependencies or roll back on them; only the planning-side protection applies (dependencies are in
  `Footprint.Destinations` and `Queue.InFlightDestinations` via the command's results). The Queue side is tested
  directly, not end to end. Fix: carry the core's dependencies on the Plan and add a gate-on spec through the Starter.
  KWOK round 3's zero dependency waits are therefore not evidence for the Queue side.
- `Footprint.Devices` and `Footprint.Reservations` aren't derived yet (`pipeline/footprint.go:40-41`), so the Ledger's
  capacity-reservation sum and device coupling see nothing, as today's loop (no cross-simulation accounting).
- Emptiness and consolidation admission re-validation uses the methods' validators, which still list pending pods with
  side effects (once per admitted plan, as today's validation).
- Metric help text and code comments still cite internal design numbers; replace with prose.

Deviations from earlier drafts: candidate groups replace chunk re-planning (the chunk path stays as an unreachable,
documented fallback); dependency tracking replaces fencing; the sizer models a fixed cost; admission rebuilds and
rechecks once per batch; no separate event edge filter (the recorder's dedupe already gives today's output);
`round_duration_seconds` has no `start` phase; five of the six planned NodePool event reasons are declared but not yet
emitted; the snapshot-level and reference-level shadow exist, the `default` level and the reverse shadow don't.

## Appendices

- [A. Observe and Plan: snapshot, planners, cores, batching, groups, dependencies, lanes, fairness](disruption-pipeline/observe-and-plan.md)
- [B. Admit and Execute: lifecycle, Ledger, Admitter, commit loop, Queue dependencies, Observer](disruption-pipeline/admit-and-execute.md)
- [C. Rollout, validation, KWOK results, alternatives, risks and follow-ups](disruption-pipeline/rollout-validation-alternatives.md)
