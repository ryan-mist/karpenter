# Rollout, validation, KWOK results, alternatives

Appendix C of [Disruption Pipeline](../disruption-pipeline.md). Scope: the upstream evidence in full, how the change
rolls out, how it is validated (differential, invariant and twin tests, shadow mode, KWOK), every KWOK result with its
conditions, the alternatives considered with their data, risks and follow-ups.

Code references are `pkg/...` paths at the tip of the implementation stack; `mainline:` marks kubernetes-sigs/karpenter
@ `6eac93ba`. Envtest numbers come from a shared 18-core machine; each is quoted with the load during its runs.

## 1. Evidence

### 1.1 Upstream issues

States checked read-only on 2026-10-21. `k#` is kubernetes-sigs/karpenter, `aws#` is aws/karpenter-provider-aws.

| issue | state | what users saw |
|---|---|---|
| [k#3197](https://github.com/kubernetes-sigs/karpenter/issues/3197) | open, `kind/bug`, `triage/accepted` | Above 4,000 nodes a drift rollout "takes multiple days", and larger clusters "make progress more slowly per node". Raising budgets "had no effect". A maintainer: commands are serial "for correctness", and "simulation B shouldn't depend on simulation A" is the invariant to keep. `/needs-design` |
| [k#3320](https://github.com/kubernetes-sigs/karpenter/pull/3320) (RFC PR) | open | 7 days, ~5,000 nodes: ~1,000 drifted eligible, ~17 drift decisions/h, ~4.5 of ~2,100 allowed drift slots used, mean drift evaluation 26.8 s (max 604 s). Consolidation: ~2,000 eligible, 25 decisions in 7 days, "because successful drift requeues before consolidation" |
| [k#2927](https://github.com/kubernetes-sigs/karpenter/pull/2927) (RFC PR) | open | Stripe: AMIs every few days; "consolidations are effectively paused for 20 minutes to an hour". Maintainer guidance: finish a full round, batch drift, pick up fairness "as part of a larger refactor" |
| [k#3141](https://github.com/kubernetes-sigs/karpenter/issues/3141) | open, `needs-design`, `help wanted` | one scored candidate list; "the score only decides evaluation order" |
| [k#3080](https://github.com/kubernetes-sigs/karpenter/issues/3080) | open, `priority/important-soon` | a failing (ICE) drift head sorts first every pass; younger NodePools make "zero drift progress" |
| [k#2972](https://github.com/kubernetes-sigs/karpenter/issues/2972) | open, `priority/important-longterm` | 0.1 vCPU at 10 nodes, 3.0 vCPU at 500 during consolidation; `NewScheduler` 78.85%, `DeepCopyNodes` 18.95%, `Solve` 1.12% |
| [k#3421](https://github.com/kubernetes-sigs/karpenter/pull/3421) | open PR | one SingleNode pass took ~1 h at 300 nodes with 22,500 pending pods ("I saw 16 hours") |
| [k#3305](https://github.com/kubernetes-sigs/karpenter/issues/3305), [aws#9531](https://github.com/aws/karpenter-provider-aws/issues/9531) | open; closed | ~300 nodes, 30 NodePools: SingleNode abandons after 215 candidates and the tail is never consolidated; node count ~2× at equal capacity |
| [aws#7129](https://github.com/aws/karpenter-provider-aws/issues/7129) → [k#1733](https://github.com/kubernetes-sigs/karpenter/issues/1733) | closed → open | consolidation "can't process fast enough"; "a fairly large uptick in spend" |
| [k#1750](https://github.com/kubernetes-sigs/karpenter/issues/1750) | open, 122 reactions | Stripe: drift "effectively turned off" |
| aws#9207, aws#8212, aws#7935, [k#3288](https://github.com/kubernetes-sigs/karpenter/issues/3288) | closed, closed, open, open | "evaluated hundreds of times, never a single decision"; #3288 asks for pass counts, last-evaluated timestamps, starvation metrics |
| [aws#3561](https://github.com/aws/karpenter-provider-aws/issues/3561) (historical, pre-v1) | closed (2023) | 450 expired nodes not replaced after 12 h at 2,000 nodes. Expiration has been forceful and outside disruption since then, so this shows the same shape (a serial backlog), not current behaviour |

Maintainers have declined knobs for this: polling period (#2853), timeouts (#1031, #1754, #1733), max candidates
(#3186).

### 1.2 Today's code, measured

Real `Controller.Reconcile` against envtest 1.37 with a cache-backed client and the operator's field indexers; 1
NodePool, budget `nodes: 10%`; N spot nodes with 10 pods each at 94% CPU; 1,344 fake instance types; ±30-50% noise.

- Drift: exactly 1 simulation and 1 command on every pass that filled the budget, at every N. `Synced()` false after
  222 of 222 replacing passes at N=2,000. Filling a 10% budget at N=2,000 took 222 passes and 206-268 s of loop
  compute, launch latency excluded.
- 62% of drift-loop `Reconcile` CPU is candidate builds (3 per pass); GC is 44% of process CPU.
- Solve time per drift simulation grows with work in flight: 61-86 ms at 0-44 in flight, 507-721 ms at 177-220.
- SingleNode on a packed cluster is O(N²): 0.77 s at N=100, 10.5 s at 500, 32-46 s at 1,000, 125-127 s at 2,000; 96%
  of each simulation is `DeepCopyNodes` + `NewScheduler`.
- Consolidation during a budget-saturated drift rollout: 0 simulations (blocked by the shared disrupting count).
- ~670 `DisruptionBlocked` publishes per iteration at N=2,000; 149k Blocked and 248k Nominated events in one fill (test
  recorder, no dedupe). The taint sweep was 105 ms of every drift `Reconcile` at N=2,000 (25-29 s of a ~460 s,
  223-iteration rollout). Idle polling while gated costs 16-18% of a core.

The benchmark's own estimate: today's loop is the binding limit once launch latency is ≥ 1 s, at ~1.5-2.2 h for 2,000
nodes at 2-3 s per launch.

## 2. Rollout plan

**Tier 1, on today's controller, no gate.** Each PR stands on its own; the rule is "with the gate off, behaviour equals
mainline at that commit", and the one PR that changes behaviour says so in its title and pins the new behaviour.

| PR | change | evidence |
|---|---|---|
| [#49](https://github.com/ryan-mist/karpenter/pull/49) | opt-in scale benchmark (`test_performance` tag, `KARPENTER_DISRUPTION_BENCH=1`) | default run stays at upstream's spec count |
| [#50](https://github.com/ryan-mist/karpenter/pull/50) | seven loop metrics (phase durations, simulations per evaluation, skipped evaluations, last evaluation, gated passes and duration, queue size) | decisions identical at N=500/1,000/2,000; ~1.6 µs per method evaluation |
| [#51](https://github.com/ryan-mist/karpenter/pull/51) | one candidate snapshot shared across methods until one has candidates; fresh Candidates and StateNode shells per call | 0 mismatches over 100 seeds × 40 steps; drift iteration −26% to −29% |
| [#52](https://github.com/ryan-mist/karpenter/pull/52) | repair's reboot handoff moved from compute into `Queue.StartCommand`; computed-but-unstarted commands release static slots (labelled leak fix) | neutral on the scale benchmark |
| [#53](https://github.com/ryan-mist/karpenter/pull/53) | PDBs indexed by namespace and label, parsed PDBs reused per resourceVersion | differential vs verbatim copy; −70% time at 200 PDBs (go bench) |
| [#54](https://github.com/ryan-mist/karpenter/pull/54) | candidate pods listed without a deep copy; simulations copy explicitly | cached build −28% to −49%; decisions identical |
| [#55](https://github.com/ryan-mist/karpenter/pull/55) | **behaviour change**: keep evaluating methods after delete-only commands while synced | churn scenario: rollout −19% to −20% at 60 s/30 s empty-node churn; at 15 s churn base starves (23 of 200 drifted replaced, 0 consolidation in 1 h), the PR completes |
| [#56](https://github.com/ryan-mist/karpenter/pull/56) | taint sweep reads only marked nodes; recheck set for just-started candidates | sweep time −92%; decisions identical |

KWOK A/B of the tier-1 stack against mainline (3 pairs, section 6.3): drift 5.0 → 9.9 nodes/min, CPU per replaced
node −49%.

**Stacks behind the gate** (PR numbers in the main RFC's Implementation status):
- **Stack A, shared cores** (#67-#71): one PR per method, each with its batch=1 differential. Drift carries batching,
  the sizer, candidate groups, `Split` and the core side of dependencies.
- **Stack B, pipeline skeleton** (#57, #65, #66, #58-#61): gate and switch, types and snapshot, lanes and the
  synchronous runner, Ledger, Admitter, commit loop and Starter, Observer. With the gate on, nothing disrupts until
  Stack C.
- **Stack C, planners and lanes** (#62-#64, #76-#79): asynchronous runners and poll trigger, wake-ups, footprints,
  planners over the cores (gate on disrupts from here), drift fairness, concurrent invariants, side-effect-free
  pending pods.
- **Stack D, validation and fixes from KWOK** (#80-#91): reference mode and the pipeline-at-batch-1 differential, the
  gate-on twin suite, shadow mode, the KWOK chart gate, an envtest rollout benchmark, then the fixes KWOK runs found
  (joint admission re-simulation, batch metrics, budget-bound pacing, fewer re-simulations, shadow triage, one-rebuild
  admission) and the Queue side of dependencies.

**GA PR:** remove the gate, the legacy loop and its batch=1 call sites, the reference copies, reference mode and the
shadow environment variable.

## 3. Differential, invariant and twin tests

### 3.1 Per-method differentials

Each Stack A PR compares the shared core at legacy options against a verbatim copy of the method on its parent commit,
on seeded generated states, resampling upstream's own nondeterminism (zone ties in topology spread, equal
`LastTransitionTime`s, savings-ratio ties, NodePools in map order). Results per method are in
[Appendix A §3](observe-and-plan.md#3-shared-method-cores). Comparable builds need fixed-length node-name prefixes
(the scheduler orders existing nodes by name) and distinct drift times. CI runs 12 seeds per method; nightly runs
100. A drift seed with zone spread failed ~4% of runs under load with 16 resamples because the verbatim legacy method
itself returns the other zone in ~5 of 48 samples; 64 resamples made it 100/100.

### 3.2 Snapshot invariant

One pipeline snapshot per round gives each of the six methods (as a planner whose filter is `ShouldDisrupt`) exactly
what one legacy `GetCandidatesWithTotals` returned: ids, order, every Candidate field, StateNode, events (buffered ones
replayed in order), NodePool totals. The tier-1 snapshot PR's differential ran 100 seeds × 40 churn steps (pod bind,
delete, terminating, terminal; do-not-disrupt values; Ready flips; Drifted, Consolidatable and Rebooting flips; mark,
unmark, nominate; queue add/remove; PDB create, status, spec, delete; NodePool changes; price changes; instance-type
removal; `GetInstanceTypes` errors; clock steps from 1 s to 31 min): 0 mismatches, and four harness mutations are each
detected. The prototype version ran 128,100 comparisons with 0 mismatches; frozen `now` (527), frozen pod checks (196),
frozen NodePool (983) and no queue check (1,966) were each detected.

### 3.3 Pipeline at batch=1

Reference mode (`pipeline.WithReferenceMode()`, tests only) plans as one pass of today's controller: one lane with
every planner in today's method order, first-wins (a round ends after the first planner with a plan that isn't a plain
delete), skipped while unsynced; drift and repair one candidate at a time with no deadline; pending-only NodeClaims
kept on the first command; core order without NodePool round-robin; reserved plans' candidates simulated as already
deleting (as a later method sees earlier methods' started commands); reserved plans' destinations not candidates; and
the Ledger credits what reserved plans free against limits.

Each step runs a plan-only reference pipeline (sync runner, private Ledger and recorder, a Starter that records
commands and marks candidates for the round only; sampled 8 times, up to 32 when samples disagree), then one pass of the
disruption controller, whose commands start and launch. The pass must equal a pipeline sample exactly, up to ties of
equivalent nodes, or up to the replacement's zone.

**Result**: 100 seeds (budgets 1/10%/20%/100%, 10-40 nodes, 1-3 NodePools and zones, zone spread, anti-affinity, PDBs,
do-not-disrupt, limits, pending pods, mixed sizes, consolidation on), 241 compared passes (214 commands): 235 exact,
2 equal up to ties, 4 ended at the one intended difference left in reference mode (a node an in-flight command moves
pods onto isn't a candidate), 0 mismatches. The generator makes no unhealthy nodes or static pools, so repair and static
drift rely on their per-method differentials.

It found two gate-on bugs, both fixed and pinned by admitter specs: a consolidation plan validated before plans admitted
earlier in the same step had started (their nodes looked like destinations, the re-simulation differed, and the plan
was rejected and backed off 10 s; this happens when emptiness and consolidation are both ready at +15 s), and waiting
plans that only re-stepped after the 1 s poll.

### 3.4 Invariants

- **Batch=K end state** ([Appendix A §7](observe-and-plan.md#7-candidate-groups-and-split)): 60 of 60 seeds, 0 hard
  violations, 261 vs 533 new NodeClaims against serial at 10-200 nodes. Three bugs this checker found before the first
  review: chunk simulations double-counted limit headroom (2 of 60 seeds over a limit), `DropPendingOnly` dropped
  claims for pods of deleting nodes (planned overbooking in 13 of 60), and every chunk re-placed all in-flight pods
  (215 replacements for ~100 nodes in one pass at N=2,000).
- **Concurrent lanes** (`pipeline/invariants_test.go`): both lanes and the commit loop run as in production (their
  leader-elected runnables, concurrently, `-race`) on 20 nodes with drift, emptiness and consolidation demand and a
  budget of 5, while the fake clock advances and replacements launch. After every step: queued nodes never exceed the
  budget, and no node is reserved by two plans or while MarkedForDeletion (checked on a Ledger wrapper that records
  every Reserve, Swap, Amend, MarkStarted and Release). A budget-free intruder planner plans started commands' nodes, as
  a stale snapshot would. Mutations: an always-true allocation policy fails with "disrupting 10 nodes with a budget of
  5"; a no-op ownership check fails with "reserved ... while MarkedForDeletion".
- **Simulator rule**: with node 0 reserved by another plan, node 1's pod goes to a new NodeClaim instead of node 0, and
  every pod still schedules.

### 3.5 Gate-on twin suite

`KARPENTER_TEST_DISRUPTION_PIPELINE=1` (test only) runs the existing disruption suite with every spec that reconciles
the suite's controller reconciling `pipeline.NewController(...)` with its defaults, through the synchronous runner.
Real validation runs in every twin spec (the gate-off suite uses no-op validators); the sync runner steps the fake
clock by 15 s whenever it blocks on `ValidateAt`. About 200 specs exercise the pipeline. At the tip: 633/634 passed, 5
skipped, the one failure an envtest DRA spec that also fails gate-off and on the base. Every difference is cited:

| spec | class | handling |
|---|---|---|
| three evaluation-metrics specs | intended | the serial-pass loop metrics aren't emitted with the gate on; skipped |
| drift budgets "only 3 nodes allowed" | intended, adapted | `nodes_consuming_budgets` counts reserved plans; twin expects 3 |
| "drift empty nodes before non-empty", "one non-empty node at a time" | intended | batched drift replaces both in one round, packed 2 → 1; skipped |
| "delete drifted nodes when empty and consolidatable", "delete empty and drifted nodes" | adapted | runs; only the final `eligible_nodes` assertion is skipped (the budget-gated gauge gap) |
| "multiple replace operations successively" | intended, adapted | all 3 budget-allowed replacements in one round |
| multi-node fails validation after TTL, falls to single | intended, adapted | re-planned in the lane's next round, not inside the first `Reconcile` |

Mutations the twin catches: Ledger budget check skipped (8 specs fail), MultiNode without `ValidateAt` (6 fail).

### 3.6 Envtest rollout benchmark

`pipeline_rollout_benchmark_test.go` (tag `test_performance`, `KARPENTER_PIPELINE_ROLLOUT=1`) runs the pipeline with its
production asynchronous runners on a scale-bench cluster: N drifted spot nodes, 10 pods of 3 CPU / 5 Gi each, a 10%
drift budget, consolidation off. A new NodeClaim gets a providerID at once (cluster state stays synced) but its Node
registers only after a delay, as on KWOK. It samples budget utilization every 200 ms. Load 5-17:

| stack | N | registration delay | mean utilization | nodes/min | rollout |
|---|---|---|---|---|---|
| planners only | 300 | 10 s | 0.31 | 40 | > 6 min (timeout) |
| + joint admission re-simulation | 300 | 10 s | 0.39 | 48 | > 6 min |
| + batch metrics, uninitialized fence | 300 | 10 s | 0.67 | 73 | 247 s |
| + budget-bound pacing (#87) | 300 | 10 s | **0.88** | 94 | 192 s |
| through one-rebuild admission, with the fence (#90) | 300 | 10 s | 0.91 | 102 | 176 s |
| through dependency tracking instead of the fence (#91) | 300 | 10 s | **0.93** | 103 | 174 s |
| planners only | 1,000 | 30 s | 0.52 | 65 | 929 s |
| through budget-bound pacing | 1,000 | 30 s | **0.90** | 103 | 585 s |

It found the two 10 s gaps per wave behind the budget-bound pacing fix ([Appendix A §10](observe-and-plan.md#10-lanes-and-triggers)).

## 4. Shadow mode

### 4.1 Design

With the gate **off**, the disruption controller is the reference and the only actor. For a sampled fraction of its
method computations the pipeline plans beside it on the same state and never writes.

- **Switch.** `KARPENTER_DISRUPTION_PIPELINE_SHADOW=<rate>` (`pipeline/shadow.go:56-77`; NaN, infinities and
  non-positive values mean off; capped at 1). Undocumented, unsupported, for tests, KWOK and soak clusters; no flag,
  option or Helm value; every build reads it. `controllers.go` builds the shadow only with the gate off.
- **Hook.** `disruption.WithComputeObserver(o)` (`disruption/pipeline_seams.go:139-153`), nil by default. The
  controller calls `o.Computing(method, snapshot, candidates, budgets)` right before `ComputeCommands` and
  `o.Computed(method, cmds, err)` right after, before any command starts; a method without candidates gets
  `Computing(…, nil, nil)` too. With the variable unset, the two nil checks are the only new code on the path.
- **Snapshot level.** The reference planner's `CandidatesFor` on the same node copies as the method's snapshot,
  evaluated at the method snapshot's fixed now, against the method's candidates (nodes in-flight commands move pods
  onto are not differences); and the Ledger's remaining budget per NodePool, read right after the method's budgets,
  against the method's budgets.
- **Reference level.** One plan of the reference planner (built `WithReference()`, no consolidation dedup) against the
  commands the method computed: decision, candidates, replacements' instance types; or equal up to equivalent nodes. A
  consolidation method that skips an unchanged state isn't sampled.
- **Off the controller's path.** `Computing` builds the snapshot on the method's nodes, compares the snapshot level, and
  copies the live nodes right before the method computes. A goroutine plans on that copy (through
  `SimulationOptions.Nodes`), waits for the method's commands, and compares. No computation is sampled while a sample's
  goroutine runs. A 3 s planner, in the comparison or in the recheck, leaves the pass under 1 s (spec).
- **Recheck → cause.** On a mismatch the goroutine runs the shadow's **own copy** of the method's core (legacy options
  without validator or dedup, no metrics, side-effect-free simulator, discarding effects and recorder, the private
  Ledger's budgets) and then the planner (up to 4 samples) on one fresh node copy. Agreement → `race` (state changed or
  upstream nondeterminism), otherwise `logic`. Snapshot-level differences are rechecked the same way: the method's rule
  against the planner's on the same nodes and now; `NodePoolStats` + `NodePoolBudgetMap` against the Ledger. Static
  drift isn't rechecked, so its mismatches are `logic`.
- **No writes, no legacy side effects.** Methods and planners built with a discarding recorder, simulations without
  pending-pod side effects, a Ledger that is never reserved, read-only static counts, no starter, no queue hook, a
  context outside the controller's evaluation statistics, controller name `disruption.shadow` (its solves show as
  `scheduler_scheduling_duration_seconds{controller="disruption.shadow"}`), and the methods' own logging discarded.
- **Reporting.** `pipeline_shadow_comparisons_total{level,planner}`, `pipeline_shadow_mismatches_total{level,planner,
  kind=candidates|budget|commands,cause=race|logic}`, `pipeline_shadow_agreement_ratio{planner}`; ERROR logs to the root
  logger captured at construction, once per (planner, level, kind) per 10 min.
- **Cost.** On the controller's path a sampled computation costs a snapshot build (NodePools, instance types, PDBs, the
  planner's candidates), a node copy and a Ledger budget read. At rate 1 with no mismatches the total is about 2× the
  controller's planning CPU, one sample at a time.

**Not implemented:** a `default` level (`DefaultLanes`, batch=K, every plan through the invariant checker plus a
typed-explanation agreement ratio; batch=K invariants are checked offline instead) and the reverse shadow for beta
(pipeline acting, legacy cores plan-only).

**Tests.** `shadow_differential_test.go`: the disruption controller runs 4 passes on deterministic state twice without
the shadow (a determinism control) and once with it at rate 1, comparing commands, every event, consolidation state,
node marks and nominations, pending pods' scheduling records, every API write, every `karpenter_*` series (shadow and
timestamp gauges excepted) and the controller's log lines. 10 entries: all methods with forced mismatches (so rechecks
run), with a race, unforced; single only (forced, race, a jumping clock so its core times out); multi (forced, timing
out); emptiness; drift. Mutations each fail it: recheck on the controller's method, recheck with metrics, evaluation
statistics kept, no controller name, methods' logger kept. With 40 generated seeds and the shadow at rate 1: 366
comparisons, 0 mismatches.

### 4.2 Soak results

**First soak** (stack through #87, gate off, rate 0.1, config R, 2,202 nodes, 4 CPU / 8 Gi, 4 h: 3 h drift timed out at
1,452 of 2,202 replaced at 7.9 nodes/min, then 1 h consolidation 1,969 → 1,236; 0 panics, 0 restarts):

| level / planner | comparisons | logic | race |
|---|---|---|---|
| reference / drift | 144 | 20 | 21 |
| reference / single | 3 | 1 | 2 |
| reference / multi, emptiness | 9 | 0 | 0 |
| snapshot / drift | 145 | 10 (9 budget, 1 candidates) | - |
| snapshot / multi | 55 | 1 (candidates) | - |
| snapshot / emptiness, repair, single, static drift | 517 | 0 | - |
| **total** | **877** | **32** | **23** |

Agreement ratio (reference): drift 0.72, multi 1.0, emptiness 1.0, single 0/3. Heap p95 6.06 GB vs 5.12 GB for a
mainline run (+18%; part may be the tier-1 and Stack A code, not only the shadow).

**Triage.** Every pattern had a shadow-side cause; none pointed to a planning difference:

| symptom | cause | fix |
|---|---|---|
| reference: legacy `replace X` vs pipeline `delete X` or `replace Y` | the shadow simulated on the pass snapshot's nodes, the method on live state; earlier methods of the pass may have started commands since, so the shadow saw more room | copy live nodes right before the method computes |
| reference recheck says `logic` | core and planner rechecked on live state at different times while the controller kept starting commands | both on one fresh node copy |
| snapshot: ~30 nodes of one zone only the pipeline had | candidates evaluated at the shadow's now, not the method snapshot's; nominations from a provisioning pass expired in between | evaluate at the method's now |
| snapshot: budget off by one ("21 nodes may be disrupted for Drifted, the pipeline says 22") | the Ledger was read after building the snapshot (slow at 2,200 nodes), so a disruption finished in between | read right after the method's budgets |
| every snapshot-level mismatch `logic` | no recheck at that level | recheck on fresh state |

An envtest shadow rollout (N=300, single NodePool, rate 1) found 0 mismatches before the fixes, so these artefacts need
KWOK's scale and concurrency.

**Second soak** (stack through #91 as run in round 3, gate off, rate 0.1, 2,069 nodes, 4 CPU / 8 Gi; 45 min drift cap
at 516 of 2,069 replaced, 11.4 nodes/min; 20 min consolidation 1,934 → 1,260; 0 panics):
**319 comparisons, 22 mismatches: 9 logic (2.8%), 13 race** (first soak: 3.6% logic).

| level / planner | comparisons | logic | race |
|---|---|---|---|
| reference / drift | 39 | 4 | 10 |
| snapshot / drift | 46 | 1 (candidates) | 0 |
| reference / single | 3 | 2 | 1 |
| snapshot / single | 29 | 1 (candidates) | 1 (budget) |
| reference / multi | 5 | 1 | 0 |
| snapshot / multi | 30 | 0 | 1 (candidates) |
| emptiness, repair, static drift | 50, 62, 55 | 0 | 0 |

The logic cases look like the first soak's: candidate-set differences (the controller has extra candidates) and a
different node choice (single: controller deletes node A, pipeline replaces node B; multi: controller found no command,
pipeline planned a replace). The log stream captured 11 of the 22; the metrics are authoritative. **Beta needs logic =
0: not met.** With a single NodePool hash bump, hundreds of drift candidates share one Drifted transition second, so
legacy's order is a tie (`sort.Slice`) and 4 resamples rarely land on the same pick; tie-aware comparison is the next
step before treating the remainder as real differences.

## 5. KWOK methodology

**Clusters.** Two EKS 1.36 clusters on the tier-4xl control plane, AWS provider in KWOK mode (the provider at a fixed
commit, built against each core build), kwok-controller 4 CPU / 8 Gi, 3 × m6i.2xlarge managed nodes for system pods.
Runs reset their cluster. Gates are set per run and verified on the leader pod's environment and by
`pipeline_rounds_total` (0 with the gate off).

**Config R.** 10 NodePools; c/m/r instance types of generation > 2 with ≤ 8 vCPU; 100 Deployments, 30% with required
zone spread and 10% with hostname anti-affinity; 90 PDBs (none on the anti-affinity Deployments, which made a rollout
PDB-bound); budget `nodes: 10%`; 12,500 pods → ~1,950-2,250 nodes. Controller **4 CPU / 8 Gi**.

**Phases.** (1) provisioning from empty; (2) drift: bump every NodePool's hash, until nothing is drifted; (3)
consolidation: scale Deployments down, until 300 s without change or a timeout. Variants: *overlap* (scale Deployments
to 50% and `consolidateAfter: 0s` at the drift trigger) and *tight limits* (round 3: `limits.cpu` = used × 1.10 at the
drift trigger). Each phase writes metrics at start and end, fixed-window CPU and alloc profiles, and `results.tsv`.

**Measures.**

| measure | source |
|---|---|
| nodes replaced/min, rollout time | NodeClaim timestamps over the drift phase |
| budget utilization | time-weighted in-flight / allowed from the budget gauges (and from cluster state) |
| CPU and alloc per replaced node, per simulation | controller CPU and alloc over the phase / replaced nodes or `karpenter_scheduler_scheduling_duration_seconds` count |
| pending → bound p50/p95 | `karpenter_pods_provisioning_bound_duration_seconds` |
| heap p95 | sampled `go_memstats_heap_inuse_bytes` |
| plan wait, admission, joint simulations, re-simulations, plans | `pipeline_*` series |
| packing and constraints (round 3) | snapshots before and after drift and at the end: NodeClaims, CPU/memory requested over allocatable, fleet $/h from the controller's offering price gauges, instance-type mix, replacements per replaced node; required zone-spread skew, hostname anti-affinity and NodePool limits checked on the snapshot |
| profile shares | merged CPU profiles in fixed windows (0-180 s of drift, and a later window), % of samples with the frame on the stack |

**Normalization.** Node counts vary by ±60-200 between runs, and mainline never finishes a 2,000-node rollout in 1 h,
so compare rates, per-node and per-simulation costs, and fixed-window profile shares, never whole-phase totals. A/B
runs are back to back on one cluster, order rotated per pair, each mainline run the "before" of its pair; drift capped
at 1 h and consolidation at 30 min. With 3 vs 3 runs the exact two-sided Mann-Whitney p is at best 0.10, with 2 vs 2 at
best 0.33: these are direction-of-effect results, not release gates.

**Deviations from the original plan.** 4 CPU / 8 Gi instead of 2 CPU / 4 Gi: at 4 Gi the gate-on heap reached 3.5 GB
of a 3.9 GB GOMEMLIMIT and GC took ~40% of CPU; at 2 CPU both builds lost the leader lease (section 6.4). ~6 pods per
node instead of ~30; the 3 EKS add-on DaemonSets instead of 10 (bench DaemonSet pods lost the bind race on
instantly-ready KWOK nodes); no do-not-disrupt pods (drift could never finish); 90 instead of ~500 PDBs.

**Builds.**

| name | what | gate |
|---|---|---|
| mainline | `6eac93ba` | off |
| tier 1 | tier-1 PRs merged | off |
| R2 | the stack through #87 (budget pacing) | on; off + shadow 0.1 for the first soak |
| R2b | through #88 (order-independent catalog version, re-simulation deadline, partial acceptance) | on |
| R3 | through #91 (one-rebuild admission, shadow triage, dependency tracking), before the last review fixes (sizer multiplicative decrease, budget-bounded top-up, partial-batch resolution bookkeeping, dependency metric attribution) | on; off + shadow 0.1 for the second soak |

## 6. KWOK results

### 6.1 Round 1: smoke

Gate on, X=1,000 pods (212 nodes), 2 CPU / 4 Gi: all phases completed, 0 panics, 0 ERROR logs, 0 restarts. Drift 617 s
vs mainline 1,252 s (2.0×); budget utilization 0.75 vs 0.34; controller CPU in the drift phase 545 s vs 1,752 s.

### 6.2 Round 2: what the first large runs found

At ~2,000 nodes the first gate-on build was ~2× mainline and not budget-bound. Each finding became a fix:

| build | drift nodes/min | budget utilization | what limited it | fix |
|---|---|---|---|---|
| planners only (2 CPU) | ~10 | ~0.02 | sizer collapsed to 1 after a timeout; one candidate at a time for good (`SimulateBatch` in the 0-180 s profile, absent at +600 s) | sizer floor |
| + floor (4 CPU) | ~16 (mainline ~9 at the time) | ~0.04 | K stuck at 2: `NewScheduler` 88% of a simulation, `Solve` 0.3%, ~3-5 s per simulation regardless of K | fixed-cost sizer |
| + fixed cost + batch metrics | 10.2 (stopped at 9,189 s, 1,565 of 2,248) | 0.034 | joint simulations accepted up to ~156 candidates but ~5 plans per Fast round were admitted; 1,194 drops all `uninitialized_destination`; 141 of 151 admission re-simulations `stale`, run one by one | candidate groups, uninitialized fence, joint admission re-simulation, staleness from submission |
| R2 | 95-102 | 0.53-0.57 | 10 s gaps while budget-bound or held for sync (from the envtest repro) | budget-bound pacing (in R2) |

### 6.3 Round 2: runs and A/B

**Single run, R2 vs mainline** (one cluster, 4 CPU / 8 Gi): rollout 1,348 s vs > 3 h (mainline timed out at 801 of 1,935);
94.9 vs 4.45 nodes/min (21×); budget utilization 0.53 vs 0.01 (0.85-0.98 during each fill; gaps are registration
waits and per-NodePool tails); drift CPU 4.4k s vs 32.5k s; 554 vs 11,288 simulations at 7.9 vs 2.9 s CPU each; heap
p95 5.5 vs 5.1 GB; pending → bound p95 37 vs 37 s. Gate-on drift: 1,694 plans admitted, 414 `sibling_rejected`, 15
`candidate_blocked`; 98 admission re-simulations, 95 of them `catalog`; 69 accepted joint simulations averaging 39.5
candidates; Fast plan wait p50 6.8 s, p99 14.8 s. Mainline logged 317 "too many queued events, dropped event"; R2 0.

**R2b, two runs** (one per cluster): 116 and 127 nodes/min; utilization 0.63 / 0.60; `catalog` re-simulations 0 / 0;
`sibling_rejected` 0 / 0; pending → bound p95 29 / 28 s. Fast plan wait got worse: p50 11.8 / 11.5 s, p99 29.7 /
36.7 s; the profile showed per-plan admission rebuilds at 9.8 s per batch, fixed by one-rebuild admission in R3.

**Overlap variant** (R2 vs mainline): gate-on drift replaced 2,248 nodes in 776 s (174 nodes/min, utilization 0.76),
with 11 empty and 7 underutilized consolidation decisions during the rollout (cluster 2,248 → 1,083 by its end).
Mainline replaced 694 of 1,977 in the 1 h cap (11.6/min, utilization 0.014) with 9 empty and **0 underutilized**
decisions: legacy consolidation runs only when drift has no command.

**A/B, mainline vs R2, 3 pairs, 4 CPU / 8 Gi** (medians; per-pair after/before):

| measure | mainline | R2 | per pair |
|---|---|---|---|
| drift nodes replaced/min | 5.0 | **102** (20×) | 13.5, 20.2, 20.4 |
| drift rollout (2,000-2,300 nodes) | not done in 1 h (~300 replaced) | 17-22 min | |
| budget utilization | 0.010 | **0.56** | 35, 65, 57 |
| drift CPU per replaced node | 35.9 s | **1.94 s (−95%)** | 0.08, 0.05, 0.05 |
| drift alloc per replaced node | 11,287 MB | 532 MB (−95%) | 0.07, 0.05, 0.05 |
| drift CPU per simulation | 3,012 ms | 7,903 ms (+162%) | 2.7, 2.3, 2.8 |
| drift controller cores | 3.04 | 3.33 (+9%) | |
| drift pending → bound p95 | 29.0 s | 34.9 s (**+21%**) | 1.24, 1.21, 1.31 |
| drift heap p95 | 5.73 GB | 5.84 GB (+2%) | |
| consolidation decisions (30 min) | 8 | 38 | 4.8, 5.4, 5.2 |
| panics / restarts | 0 / 0 | 0 / 0 | |

CPU per simulation is higher because one joint simulation covers ~40 candidates; per replaced node it is −95%, which is
why the beta row is restated per replaced node. Caveat for the consolidation row: mainline's 1 h drift window ends with
~1,600 drifted nodes left and its loop runs drift first, so its "consolidation" phase is mostly still drift.

**A/B, mainline vs tier 1, 3 pairs:** drift 5.0 → 9.9 nodes/min (+97%; per pair 1.33, 1.76, 1.97); CPU per replaced
node 35.9 → 18.3 s (−49%); alloc per replaced node −48%; CPU per simulation −12%; pending → bound p95 +5%; heap p95
−4%; consolidation decisions after the drift window 8 → 52.

Drift profile window 0-180 s (merged, share of samples), mainline → tier 1 → R2: `GetCandidatesWithTotals` 19.5% → 0%
→ 0%; `DeepCopy` 22.3% → 14.6% → 6.7%; `DeepCopyNodes` 10.8% → - → 3.6%; GC 22.4% → - → 16.4%;
`Provisioner.NewScheduler` 6.3% → - → 13.4% (the joint simulations' fixed cost); `SimulateBatch` 5.8% (R2).

One tier-1 run and one mainline run (round 3) logged recovered nil-pointer panics in the `state.node` controller for a
single node (23 and 322 occurrences); cluster-state code is unchanged from mainline, and no gate-on run logged any.

### 6.4 Two-CPU controllers and the leader lease

**One pair at 2 CPU, chart-default memory** (R2 vs mainline): drift 43.9 vs 5.8 nodes/min (7.5×); whole rollout in 48
min vs 351 of 1,917 in 1 h; utilization 0.32 vs 0.014; CPU per replaced node 2.7 vs 19.9 s; alloc per replaced node
0.7 vs 6.4 GB; pending → bound p95 38 vs 38 s; heap p95 5.3 vs 5.3 GB; consolidation 20.1 vs 14.6 nodes removed/min.

**Lease loss.** At 2 CPU both builds restarted during 12,500-pod provisioning bursts:

| situation | mainline | gate on |
|---|---|---|
| provisioning 12,500 pods from empty (~2,000 nodes) | 2 of 3 runs restarted (3×, 0×, 5×) | 3 of 3 runs restarted (8×, 2×, 1×) |
| rollout restart onto the loaded, quiescent cluster (5 min watched) | 3 of 3 survived | 5 of 5 survived |

Every captured exit is `panic: leader election lost`: the lease is acquired ~15 s after start, a 5 s lease GET times
out ~16 s later, renewal fails. Not OOM (heap ≤ 4.2 GB, no limit), not liveness. The leader-election client has its
own rest config without Karpenter's client QPS limit, so the timeout is CPU starvation of the process (~2.0 of 2 cores,
GC ~13%) or API-server latency. By frequency the builds are not distinguishable (n is small); by CPU the gate-on build
is not worse: in the first 90 s of provisioning the pipeline's lanes take 3-4% of samples vs 4.9% for the legacy
loop, and in a 30 s profile from +15 s after a restart onto a loaded cluster, disruption work is 5.5-6.8% vs 8.7-15.7%.
A restart during provisioning also strands launched NodeClaims whose KWOK Node wasn't created (66 `NodeNotFound` in one
run) until the registration TTL, stretching provisioning to 1,137 s. Not a pipeline issue, but it bounds what can be
measured at 1-2 CPU and 2,000 nodes; options are a higher renew deadline or keeping lease renewal off the starved path.

### 6.5 Round 3: dependency tracking

R3 vs mainline, config R, 10% budget, 4 CPU / 8 Gi: four gate-on runs (a full rollout, two A/B runs, one with tight
limits) and three mainline runs (two A/B runs, one with tight limits). The second shadow soak (gate off) is in
section 4.2.

| run | gate | nodes | drift s | nodes/min | utilization (gauges / state) | drift CPU s | CPU s per replaced node | heap p95 GB | pending → bound p95 s | Fast plan wait p50 / p99 s |
|---|---|---|---|---|---|---|---|---|---|---|
| R3, full rollout | on | 2,128 | 876 | 146 | 0.56 / 0.64 | 2,968 | 1.39 | 6.52 | 32.3 | 1.39 / 13.0 |
| R3, A/B pair 1 | on | 2,193 | 808 | 163 | 0.65 / 0.68 | 2,790 | 1.27 | 6.13 | 33.8 | 1.17 / 32.8 |
| R3, A/B pair 2 | on | 2,239 | 854 | 157 | 0.60 / 0.61 | 2,920 | 1.30 | 6.32 | 37.6 | 2.32 / 37.1 |
| R3, tight limits | on | 2,105 | 760 | 166 | 0.63 / 0.62 | 2,773 | 1.32 | 5.93 | 28.3 | 1.60 / 26.8 |
| mainline, A/B pair 1 | off | 1,826 | 3,611 (cap) | 6.9 | 0.017 / 0.019 | 10,922 | 25.8 | 5.24 | 33.7 | - |
| mainline, A/B pair 2 | off | 2,035 | 3,603 (cap) | 8.4 | 0.017 / 0.019 | 10,970 | 21.4 | 5.32 | 38.1 | - |
| mainline, tight limits | off | 2,108 | 3,616 (cap) | 9.5 | 0.018 / 0.022 | 11,284 | 19.6 | 5.71 | 35.8 | - |

Mainline replaced 424, 513 and 575 nodes in its 1 h cap. Gate-on pipeline detail (the four gate-on runs): admission per
batch p50 0.27-0.38 s, p99 8.5-13.1 s; joint simulation p50 5.8-7.3 s, p99 14.0-14.8 s; candidates accepted per joint
simulation p50 25-42, p99 98-183; plans per round p50 18-32; batches started 51-63, waited 3-33, rejected 0-2. The
two A/B runs: 1,581-1,720 drift plans admitted, 15-17 `candidate_blocked`, 4 `candidate_nominated`; re-simulations
22-60 `stale`, 8-9 `sibling`, 5-8 `pods_changed`, 0 `catalog`. 0 panics, 0 restarts, 0 ERROR logs in every gate-on
run.

**A/B, 2 pairs** (medians; MWU p = 0.33 at best with 2 vs 2):

| measure | mainline | R3 | per pair |
|---|---|---|---|
| drift nodes replaced/min | 7.65 | **160** | 23.7×, 18.7× |
| budget utilization | 0.017 | 0.63 | 38.7, 36.3 |
| drift CPU per replaced node | 24 s | **1.29 s (−95%)** | 0.05, 0.06 |
| drift alloc per replaced node | 7,388 MB | 343 MB (−95%) | |
| drift CPU per simulation | 2.9 s | 9.7 s (joint simulations) | |
| drift pending → bound p95 | 35.9 s | 35.7 s | 1.00, 0.99 |
| drift heap p95 | 5.28 GB | 6.23 GB (**+18%**) | 1.17, 1.19 |
| consolidation decisions (30 min) | 9.5 | 43 | |
| consolidation heap p95 | 3.82 GB | 3.64 GB (−5%) | |

Drift profile, 0-180 s window (mainline → R3): `disruption.(*Controller).Reconcile` 30.2% → 1.6%; pipeline 17.8%;
`GetCandidatesWithTotals` 19.6% → 0%; `DeepCopyNodes` 10.3% → 3.7%; `DeepCopy` 21.9% → 7.5%; GC 23.2% → 14.3%;
`Provisioner.NewScheduler` 6.8% → 13.0% and `NewTopology` 2.6% → 9.8% (the joint simulations' fixed cost);
`SimulateBatch` 8.3%; `Solve` 0.0% → 0.4%.

The heap rise (+18% vs paired mainline, ~+10% vs R2b) is within the 8 Gi limit (~1.5 GB headroom); per-command
dependency state and larger joint simulations are the likely causes, not yet attributed.

**Packing and constraints** (snapshots before and after drift):

| measure | mainline (3 runs) | R3 (4 runs) |
|---|---|---|
| zone-spread violations after drift | 0, 0, 2 (max skew 20, already present before drift from the provisioning bind race) | 0 (max skew 1) |
| anti-affinity / limit violations after drift | 0 / 0 | 0 / 0 |
| CPU requested / allocatable, before → after drift | 0.82-0.86 → 0.89-0.91 | 0.80-0.82 → 0.94-0.95 |
| memory requested / allocatable after drift | 0.53-0.58 | 0.63-0.65 |
| fleet $/h, after / before drift | 0.90-0.93 (only 23-27% replaced) | 0.83-0.85 (all replaced; −15% to −17%) |
| replacements per replaced node | 0.67-0.85 (partial rollouts) | 0.92-1.02 |
| tight limits: max used / limit after drift | 0.90 | 0.84 |

The packing question is open. Gate-on drift launched 0.92-1.02 replacements per replaced node; mainline 0.67-0.85,
but over the first 23-27% of its rollout only, and those are the oldest nodes. At 10% replaced the gate-on fleet cost
0.975-0.989 of its start and replacement over replaced price was 0.73-0.87; mainline's equivalent was 0.92-0.96 and
0.10-0.65 (noisy). The end states are not comparable; a complete mainline rollout (> 4 h at these rates) or price at
equal progress over a whole rollout would answer it. A retro analysis of round 2 dumps showed the same shape: the
gate-on build's mix shifts from 8 to 4 vCPU types.

Consolidation-end snapshots show zone-spread violations for both builds (gate on 15-19 Deployments, mainline 12-18):
the harness's Deployment scale-down ignores zone spread, and kube-scheduler enforces skew only at bind time, so these
come from the scale-down, not from Karpenter placement.

**Dependencies.** Every R3 run recorded 0 commands waiting on dependencies and 0 dependency failures. Because the
pipeline's `Plan.ToCommand` doesn't yet carry dependencies to the Queue ([Appendix B §6](admit-and-execute.md#6-queue-dependencies)),
that is expected whatever the initialization speed; round 3 tests the planning side (packing onto uninitialized
nodes; 141 vs 150 replacements in the envtest repro), not the Queue side.

### 6.6 Beta criteria, status

| criterion | result | verdict |
|---|---|---|
| shadow soak: logic 0, race < 0.5% | 9 logic + 13 race of 319 (R3); 32 + 23 of 877 (R2) | not met |
| budget utilization during drift ≥ 0.9 | 0.56-0.65 (R3); 0.85-0.98 within each fill (R2) | not met |
| rollout ≤ max(1.1 × ceil(N/B) × T, 0.5 × gate off) | 760-876 s vs gate off 6.9-9.5 nodes/min | met (vs 0.5 × gate off) |
| budget fill ≤ 2 Fast rounds + 1 launch (≤ 3 min) | in flight reached 150-190 of ~195 within the first 2-3 samples (≤ ~90 s, 30 s sampling; R2) | met, approximately |
| consolidation during a rollout > 0, not worse than gate off | overlap: 18 vs 9 (0 underutilized for mainline; R2) | met |
| Fast-lane plan wait p99 ≤ 5 s | 13-37 s (R3); p50 1.2-2.3 s | not met |
| CPU per replaced node ≤ gate off (restated from per simulation) | −95% | met |
| pending → bound p95 ≤ gate off + 10% | R3 28-38 vs 34-38 s; R2 +21% | met on R3 |
| heap p95 ≤ gate off + 20% | +18% (R3), +2% (R2) | met, narrowly |
| 0 constraint violations; extra NodeClaims ≤ gate off + 2% | 0 violations; NodeClaims open | partly |
| perf suite `drift/drift` | not run | open |
| real cloud, 1k nodes | not run | open |

## 7. Alternatives considered

1. **Incremental fixes on the singleton** (#3320's replacement-only drift batching, #2927's time slicing). These keep
   one simulation per candidate and give up packing (the joint simulation launched about half the NodeClaims of serial
   in the invariant runs). Time slices share the blocking instead of removing it. *Kept:* the tier-1 PRs, which doubled
   KWOK drift throughput (5.0 → 9.9 nodes/min) and halved CPU per replaced node, but still emit one command per pass.
2. **Parallel methods with a conflict arbiter.** Parallel simulations from one snapshot pick the same nodes (first fit
   in name order), so ~1/K survive on loose clusters; topology is blind across simulations; memory is K × 50-200 MB at
   2,000 nodes on controllers that are already saturated (aws#9733: 99.5% of 4 CPU at 193 nodes); starvation is
   untouched. A prototype (`arbiter.go`, `partition.go`, 10 tests under `-race`) is the source of these numbers.
   *Kept:* the conflict taxonomy (now `Relate`), the single commit point, the joint Ledger.
3. **Pre-partitioning into independent islands.** Common setups collapse to one island: a single default NodePool,
   untainted spot/on-demand fallback pools, Auto Mode `general-purpose` + `system` (add-ons bridge them), GPU pools
   whose pods have no selector. Only multi-tenant clusters with tainted per-team pools split. Amdahl caps the island
   speedup at 30-60%.
4. **One goroutine per planner.** "Repair before drift" becomes a race, and singleton-reconciled specs stop being
   deterministic. Lanes keep the order in code.
5. **Per-candidate work queue.** The wrong unit: batched drift needs K candidates at once, Multi needs the sorted set,
   Single is a deadline-bounded scan with NodePool rotation. Its per-key back-off survives as drift's NodePool back-off.
6. **Unified scored packing across methods** (#3141). No common unit between obligations (drift, repair) and optional
   savings; independently simulated actions double-book; static drift and MultiNode don't map to per-node scores.
   *Kept:* the `Scorer`, as a tie-breaker ("the score only decides evaluation order").
7. **Event-driven validation instead of the 15 s wait.** Dropped. Once validation is a per-plan timer the wait blocks
   nothing else, and it exists to catch pods that just landed (#651, #2724).
8. **A persistent candidate pool now.** It can be exact and is 9.9× cheaper than the shipped snapshot per drift round
   (6.4 vs 62 ms at N=2,000), but after batching it saves ~4-5% of a round and needs copy-on-write StateNodes. Later,
   with event-driven lanes.
9. **Simulate onto pending replacements in v1** (virtual-node overlay). The exact model, but it creates plan-to-plan
   dependencies and cancellation for unlaunched NodeClaims. The reserved-candidates rule avoids double-provisioning
   without it; dependency tracking covers launched-but-uninitialized nodes. Follow-up RFC.
10. **Reserve and start atomically at admission.** Consolidation would usually find its slot taken 15 s later,
    because drift refills within a second, wasting minutes of compute.
11. **Launch the next batch's replacements early.** The only option that raises the budget ceiling, but it is
    unbudgeted surge capacity, a user-facing semantics change. Separate RFC (aws#4811, aws#1738).
12. **Plan on standby replicas.** Needs cross-process snapshot consistency, and every plan would be re-simulated anyway.
13. **Per-reason fair share in v1.** It changes who gets a freed slot, which the budget API doesn't express. The
    starvation #3320 measured was turn starvation, which lanes remove. The seam is kept.
14. **Chunk re-planning of a batch** (the first batching design: re-solve any component above 10 candidates in chunks,
    one simulation each, deferring topology-coupled chunks). At 2,000 nodes a chunk simulation costs ~4 s, so 1-2
    chunks fit a round and the rest were deferred. Replaced by candidate groups from one simulation; the chunk path is
    kept only as an unreachable fallback.
15. **Fence uninitialized destinations** instead of tracking them as dependencies. Every candidate buys capacity of
    its own next to nodes already coming up (150 vs 141 replacements at N=1,000 in envtest, +6%). Replaced; the fence
    remains the legacy rule.
16. **Size batches per candidate** (the first sizer). At 2,000 nodes a simulation's fixed cost is ~88% of it, so the
    per-candidate cost at K=2 already exceeds the target and K never grows (KWOK: stuck at 2, ~16 nodes/min).
17. **Admit plan by plan.** 9.8 s per batch at ~2,200 nodes from per-plan rebuilds, catalog fingerprints and live
    reads; one rebuild and one recheck per batch is 79 ms vs 1.12 s for 20 plans in a microbenchmark.
18. **Restrict dependencies to replacements of started commands.** Safer against nodes that never initialize, but
    packs less. Not chosen for alpha; the fallback if the accepted risk shows up.

## 8. Risks

1. **Dependency scope (accepted).** Any uninitialized, non-deleting node may be a dependency. A node that registers but
   never initializes (most likely a misconfiguration: a startup taint never removed, a device plugin that never
   registers; only registration has a TTL) can cause repeated wait-then-rollback cycles for commands that pack onto it,
   and while it is a dependency it is protected from other planners, repair included. Mainline only blocks those
   candidates. Mitigation if it shows up: restrict dependencies to replacements of started commands, or stop using
   nodes uninitialized for longer than the replacement timeout.
2. **The Queue doesn't see pipeline dependencies yet** (Appendix B §6). Must close before alpha.
3. **Admission is a new serial point** whose correctness rests on re-simulation rules tested on generated clusters,
   the twin suite and KWOK, not on production.
4. **Siblings are correct only together.** A missed coupling dimension degrades to transient pending pods and a
   re-launch, not an outage.
5. **Batched drift changes replacement shapes** (N → M packing, instance-type mix shifts). Cost after a full rollout
   was −15% to −17% on KWOK, but the per-node comparison with mainline is open.
6. **Heap +18%** during drift at ~2,200 nodes, within limits; unattributed.
7. **First come, first served** will read as a regression to users who expect consolidation during rollouts; the RFC
   says plainly that it runs only when budget remains, and documents the per-reason pattern.
8. **Gate-on observability gaps**: `eligible_nodes` and `last_evaluation_timestamp_seconds` freeze for budget-gated
   planners; five NodePool "why nothing happened" reasons aren't emitted yet.
9. **2-CPU controllers at 2,000 nodes** lose the lease on both builds during provisioning bursts.

## 9. Follow-ups

| follow-up | why | blocks |
|---|---|---|
| Carry `Command.Dependencies` from the plan to the Queue, with an end-to-end gate-on spec | Queue side of dependency tracking is not wired for pipeline commands | alpha |
| Tie-aware reference comparison in the shadow mode, then re-soak on ≥ 3 clusters for 2 weeks | remaining logic mismatches look like Drifted-time ties | beta |
| Plan-wait p99: keep long joint re-simulations from holding the commit loop; prioritize Fast-lane simulations over Background ones | 13-37 s vs 5 s | beta |
| Budget utilization: a completion signal when a node actually leaves the disrupting count, and NodePool-tail scheduling | 0.56-0.65 vs 0.9 | beta |
| Packing measurement: a complete mainline rollout, or price at equal progress over a whole rollout | replacements per replaced node not comparable today | beta |
| `eligible_nodes` and `last_evaluation_timestamp_seconds` for budget-gated planners | gauges freeze during budget-bound rollouts | beta |
| The remaining five NodePool "why nothing happened" events | declared, not emitted | beta |
| Virtual-node overlay: simulate onto unlaunched replacements | exact model for reserved plans; separate RFC | - |
| Per-reason fair share through `AllocationPolicy` (needs a demand signal) | maintainers' call | - |
| SingleNode resume cursor after an admission rejection, and a rejection memo | the scan restarts from the top today (#3305) | - |
| Cheaper simulations (shared scheduler/topology state across simulations) | `NewScheduler` is 88% of a simulation at 2,000 nodes; drives plan wait and CPU | - |
| Derive `Footprint.Devices` and `Footprint.Reservations` (DRA allocator and `ReservationManager` exports); exact topology coupling keys from the scheduler | cross-simulation ODCR accounting and fewer coupled re-simulations | - |
| Drifted, Consolidatable and capacity wake-ups; a 5 min poll floor | idle pace is the 10 s polling period | - |
| A pipeline-only validator option so admission re-validation lists pending pods without side effects | consolidation and emptiness admission still record decisions for ignored pods | - |
| Retry a batch that fails on limit errors with candidate groups off | groups pack less at NodePool limits | - |
| Shadow `default` level and reverse shadow | beta soak with the pipeline acting | beta |
| Replace internal design numbers in metric help text and code comments with prose | user-visible strings | alpha |
| Bounding SingleNode's unbounded Solve in the legacy controller (#3421) | separate bug-fix PR, outside this RFC | - |
