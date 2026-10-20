<!-- Section: Motivation, Use Cases, Non-Goals, Alternatives Considered, Backward Compatibility, Graduation Criteria,
     Observability, Validation plan, and the non-topology NewScheduler costs.
     Code refs are to branch topo/integration (branch topo/integration, 1d4cec82) unless
     marked "base" (00-base, mainline 6eac93ba). Binding user decisions (BRIEF.md): parity first, no new Go deps,
     random start offset per Solve for tie-breaks, user reviews/merges PRs. -->

## Motivation

Every scheduling simulation builds a new `Topology` from scratch. `Provisioner.NewScheduler`
(`pkg/controllers/provisioning/provisioner.go:282`) calls `scheduler.NewTopology` (`provisioner.go:350`). That builds
the domain universe from every NodePool and instance type, then calls `countDomains` once per distinct topology group
(`scheduling/topology.go:421`). `countDomains` lists the group's pods from the informer cache for each namespace
(`topology.go:428`), deep-copies them, and looks up each pod's node (`topology.go:516`). Provisioning does this once per
batch. Disruption does it once per candidate simulation (`disruption/helpers.go:64`, `helpers.go:128`). That is about
1000 simulations every 3 minutes in a drift- or consolidation-heavy cluster, and almost all of them produce no command
(1052 simulations for 11 decisions in one NodeRepair drift window, `NodeRepair/perf5-opt/sim.md`). Between two
simulations, only the candidate set and node usage change. The pods, nodes, NodePools and instance types the topology
reads are the same.

### Evidence

**1. Simulation setup dominates, not Solve** (NodeRepair bug bash, AWS provider, about 200 nodes / 5k pods, 1-4 CPU
controller; `NodeRepair/scale-results.md` Finding 3, `perf5-opt/sim.md`):

| phase (profile window) | SimulateScheduling CPU | `Scheduler.Solve` share | NewTopology/buildDomainGroups |
|---|---|---|---|
| repair burst R (180 s) | 36.3 s (148 ms/sim) | 1.7% | 9.25 s |
| drift D (180 s) | 139.0 s (132 ms/sim) | 1.4% | 31.9 s (10.2% of process CPU, 9.9 GB alloc) |

About 98% of each simulation is setup. In D, `NewScheduler` was 101 s. That breaks down as existing nodes 43.4 s
(daemon compatibility 28.6 s), NodeClaimTemplate pre-filter 14.1 s, `NewReservationManager` 4.8 s, NewTopology 31.9 s,
and `GetInstanceTypes`/`InjectOfferings` 9.6 s.

**2. KWOK config T, mainline vs the 8 tier PRs** (`prs/99-integration/comment.md`; `prs/01-taint-dedupe/comment.md`).
Setup: AWS provider in KWOK mode on EKS tier-4xl, controller at 4 CPU / 8 Gi with 1 replica, 10 tainted NodePools, 100
Deployments and 3000 pods. Every Deployment has a required zone spread, every 5th also a hostname spread, and every
10th hostname anti-affinity. Phases are provisioning, drift of all nodes, then consolidation to 50%. Numbers are
cumulative CPU from fixed-length pprof windows (120 s drift), so they don't depend on the node-count variance between
runs (199 vs 137 nodes for two mainline runs).

| | mainline 6eac93ba | #19 only | integration (all 8) |
|---|---|---|---|
| drift: `NewTopology` share of controller CPU | 24.9% (52.5 s) | 27.7% (58.4 s) | **10.5% (18.7 s)** |
| drift: `buildDomainGroups` | 30.3 s | 28.4 s | 0.9 s (lazy `domainGroup`) |
| drift: `countDomains` | 21.5 s | 28.8 s | **16.5 s** (14.1 s of it is `CacheReader.List`) |
| drift: `Provisioner.NewScheduler` share | 39.7% | 43.5% | **37.2%** |
| consolidation: `NewTopology` share | 12.6% | 9.3% | 1.9% |
| consolidation: time per disruption scheduling call | 0.63 s | 0.66 s | 0.46 s (−27%) |
| drift: controller CPU / alloc (whole phase) | 2186 s / 684 GB | 1825 s / 563 GB | 1555 s / 332 GB |

Two conclusions follow from this table:
- The tier PRs remove the universe cost. Lazy per-key domains (#25) take `buildDomainGroups` from 30.3 s to 0.9 s.
- What remains in `NewTopology` is counting, and most of that is the informer `List` plus deep copy per group per
  namespace per simulation (14.1 of 18.7 s). Memoization inside a single `Topology` can't remove it, because each
  simulation builds a new `Topology`. Only a structure that outlives the simulation can, and that is the
  cluster-state `TopologyIndex` (L2) this RFC proposes.

**3. Microbenchmarks at scale** (research prototypes, synthetic fixtures, differential-tested; `research/SYNTHESIS.md`):
- `countDomains` costs 8.81 s and 19.4 GB at 15k nodes / 150k pods / 500 groups. The index answers the same query in
  118 ms / 27 MB (`research/selector-index`).
- On the base code, 95% of that per-group cost was the O(nodes) domain-presence loop. #23 already fixes the loop
  (`prs/05-countdomains-node-cache/PR.md`: 5.45 s → 545 ms at 15k nodes). The List cost scales with matching pods and
  remains.
- Interned label sets plus a warm selector cache count 500 groups in 2.25 ms, vs 720 ms for a naive per-pod scan with
  no API calls (`research/labelset-interning`).

**4. Upstream reports.** These issues show that setup cost and topology construction reach users:

| issue | what users saw | relevance |
|---|---|---|
| [kubernetes-sigs/karpenter#2972](https://github.com/kubernetes-sigs/karpenter/issues/2972) (open, triage/accepted, important-longterm) | 3.0 vCPU mean at 500 nodes during consolidation on ROSA, growing faster than node count. pprof shows `SimulateScheduling` setup "repeated per candidate evaluation". A maintainer notes that "needing to recompute topology after each consolidation candidate is selected is particularly painful" | direct |
| [#3197](https://github.com/kubernetes-sigs/karpenter/issues/3197) (open) | Drift rollouts at more than 4000 nodes take days. Each loop does O(cluster) work, including `SimulateScheduling`, to emit one command. A maintainer suggests "reuse scheduling-simulation state across candidates" | direct (per-simulation cost) |
| [#2954](https://github.com/kubernetes-sigs/karpenter/issues/2954) / [#2958](https://github.com/kubernetes-sigs/karpenter/issues/2958) | v1.11.0 had a 2-20x CPU regression that scaled with NodePool count. It was traced to `buildDomainGroups` after #2671 (topology domain filtering) and reverted in v1.11.1 | the universe build is a CPU hot spot. The domain filtering feature was lost until it can be done cheaply |
| [#2997](https://github.com/kubernetes-sigs/karpenter/issues/2997) | The #2671 regression escaped CI because the consolidation benchmark has a 40-60% CV on kind | why graduation needs multi-run KWOK gates (below) |
| [#2779](https://github.com/kubernetes-sigs/karpenter/issues/2779) | v1.8.4 memory went from 300 MB to 1.6 GB and caused OOMs. The cause was cloning requirements in `topologydomaingroup.go` while building topology domains | topology construction allocations |
| [aws/karpenter-provider-aws#7321](https://github.com/aws/karpenter-provider-aws/issues/7321) (open) | Above 5 GiB and sustained high CPU while provisioning about 2000 pods / 500 nodes across 3 tainted NodePools | general scheduling cost. Not attributed to topology |
| [#3204](https://github.com/kubernetes-sigs/karpenter/issues/3204), [aws#7986](https://github.com/aws/karpenter-provider-aws/issues/7986) | CPU regressions in `ZoneInfo`/`cacheKeyFromInstanceType` and the offerings path | **not topology**. They show that `GetInstanceTypes`, which runs per NodePool per simulation, is the next hot spot (see "Remaining NewScheduler costs") |
| [#370](https://github.com/kubernetes-sigs/karpenter/issues/370) (closed mega issue), [#722](https://github.com/kubernetes-sigs/karpenter/issues/722) | Long history of deprovisioning performance work and high-CPU reports | context |

### Use Cases

1. **Fleet-wide drift on a topology-heavy cluster.** An AMI bump drifts 3000 nodes. The workloads use required zone
   spread plus hostname anti-affinity (config T is a 140-node model of this). Each drift evaluation runs 1-2
   simulations, and each one rebuilds every group's counts from the informer. Today `NewTopology` is 10.5% of drift
   CPU even with all tier PRs, and 25% on mainline. With the index, the per-simulation topology cost becomes
   O(groups touched by pending or candidate pods), not O(pods matched by all groups). Target below 2% (see
   Graduation).
2. **Single-node consolidation sweeps at 500-5000 nodes** (#2972). For N candidates, consolidation runs N
   simulations that differ only in the excluded pods. With the index, excluding one candidate costs
   O(pods on the candidate) as −1 deltas in the L3 overlay: 29 µs for 30 pods at 500 groups
   (`research/incremental-cow` §5). Today it costs a full recount.
3. **Many NodePools with taints and per-team selectors.** Examples are 10-50 NodePools, each a tenant with
   `nodeSelector`, tolerations and its own spread (the #2954 shape: "scales with the number of nodepools"). Today
   each Topology re-derives the domain universe and its taint groups. L1 makes this a lookup keyed by
   (NodePool UID, generation, instance-type requirements identity).
4. **Large provisioning bursts** (30k pending pods, #3204 and aws#7321). Provisioning builds one scheduler per batch,
   so it gains less than disruption. With an all-namespace or `namespaceSelector: {}` anti-affinity group, though,
   today's `countDomains` lists every pod in every namespace on every batch: 6.4 ms per simulation for one such
   group at 150k pods, even in the index's on-demand mode. Registered incremental counts make this O(1) per batch.

### Non-Goals

- **No semantic changes.** Parity first (binding decision 1). The new path reproduces today's results exactly,
  including known quirks. Fixes go into separate follow-up PRs (see "Follow-up semantic fixes" below).
- No user-facing API, CRD field or Helm value beyond the feature gate (and one hidden debug option for differential
  mode).
- No new Go dependencies (binding decision 2). That rules out roaring, bits-and-blooms/bitset, go-immutable-radix,
  benbjohnson/immutable and go-memdb.
- Not changing how often disruption simulates. Memoizing "Blocked" results per candidate keyed on cluster-state
  generation, or reusing simulations across candidates (#3197, #3141), is orthogonal. That would lower the number of
  simulations, and this RFC lowers the cost of each one.
- Not caching the non-topology parts of `NewScheduler` (templates, daemon overhead groups, reservation manager,
  instance types). See "Remaining NewScheduler costs". Those go in a sibling RFC that can share the L1 key.
- Not changing Solve's algorithms (`domainMinCount` heaps, criticalPaths). The owner/selector index (#26) and dense
  counts are enough. The research found criticalPaths wrong in 8-87% of cases (`research/solve-hotpath`).
- No multi-replica or sharded controller support. The index lives in the leader's in-memory cluster state like
  everything else in `state.Cluster`.

### Follow-up semantic fixes (out of scope; one PR each, after GA of the parity path)

Each one is preserved bit-for-bit by the index and pinned by a parity test that the follow-up PR flips:
1. **nil selector.** `countDomains` lists with `TopologyListOptions`, which maps nil to `Everything`
   (`topology.go:647-651`). `TopologyGroup.selector` maps nil to `Nothing` (`LabelSelectorAsSelector`). A nil-selector
   group therefore seeds counts from every pod in the namespace but never `Record`s new placements. The fix is
   `Nothing` everywhere, which is kube-scheduler's behaviour.
2. **Terminating pods.** `countDomains` drops pods that are `IgnoredForTopology` (unscheduled, terminal or
   terminating; `topology.go:685`). The inverse anti-affinity path iterates `cluster.ForPodsWithAntiAffinity`
   (`state/cluster.go:252`, called from `topology.go:373`) and does not apply that filter, so terminating pods still
   block placement there. The index keeps two counted universes, or one flag per pod, to reproduce this.
3. **NotIn becomes a domain.** A NodePool `K NotIn [x]` combined with an instance type that lacks K inserts `x` as a
   domain (kept for parity in #25, `prs/07-lazy-domain-universe/PR.md` "Behaviour changes").
4. `TopologyNodeFilter.Matches` drops compatibilityOptions (SYNTHESIS "Bugs found").
5. `Topology.Update` errors are ignored, so constraints fail open.
6. Pod-affinity bootstrap should filter domains by the *scheduling* pod's tolerations and NodePool compatibility at
   `Get` time, not by the creator's at construction (`prs/04-group-hash-identity/PR.md` "Follow-up").

## Alternatives Considered

### A1. Per-pass topology base cache (memoize the built `Topology` per disruption Reconcile)

*What:* build one `Topology` per disruption pass (or per `ComputeCommands`) and clone it per simulation, with a
staleness bound equal to the pass. This is roughly the hack in mcornea's #2972 experiment.

*Why rejected as the end state:*
- Each candidate needs a different "excluded pods" set, so the cached Topology must be deep-copied and recounted,
  or "un-recorded" per simulation. Today's `map[string]int32` counts plus `emptyDomains`/owners would make that clone
  O(groups × domains) per simulation: 1.2 ms / 206 KB at 500 groups vs 2 ns for an overlay
  (`research/incremental-cow` §5).
- It only helps disruption, and only within a pass. Provisioning batches, Validation (`disruption/validation.go:302`)
  and the next pass all rebuild from scratch.
- Its correctness depends on the pass boundary. Cluster state changes during a pass (`Provisioner.Create` →
  `cluster.UpdateNodeClaim`, `MarkForDeletion`) would make the cache stale in ways that are hard to reason about.
- #2972's own measurement found the win shrinks at 500 nodes, because per-simulation allocations still dominate.

*Kept as a stepping stone:* the non-topology `NewScheduler` inputs *are* per-pass invariant. A per-pass "scheduling
snapshot" is the right shape for them (sibling RFC, below).

### A2. Incremental tier PRs only (stop after #19-#26 and maybe #23's caches)

*What:* land the 8 PRs, which are bug fixes plus within-Topology memoization, and stop there.

*Why not sufficient:*
- They took drift `NewTopology` from 24.9% to 10.5%. What's left (`countDomains` → `List`, 14.1 of 18.7 s) is
  per-simulation by construction.
- Counting cost grows with matched pods × groups × simulations, so the remaining 10% grows with cluster size (Use
  Cases 1 and 2).

*Status:* not an alternative so much as phase 1. The RFC builds on them. #19 (taint dedupe), #25 (per-NodePool,
lazy universe) and #22 (hash before build) become L1. #26 (owner/selector index) becomes L3. #20, #21 and #24 are
correctness prerequisites: a group identity that is safe to key a cache on, a non-mutated selector, and the NotIn
fast path.

### A3. kube-scheduler-style brute force with pre-parsed PodInfo

*What:* keep counting per simulation, but over cluster-state pod records with parsed selectors/terms cached per pod
(kube-scheduler `framework.PodInfo`, `types.go#L1233`), plus a parallel O(nodes + pods) scan per group
(`podtopologyspread/filtering.go#L241`).

*Why rejected:*
- kube-scheduler pays this once per pod scheduling attempt. Karpenter pays it once per group × simulation, and
  disruption runs about 5.5 simulations/s.
- Measured: a naive per-pod count with no API calls is 720 ms for 500 groups at 150k pods, vs 2.25 ms warm for the
  interned index (`research/labelset-interning` §2).
- Parallelism burns cores that the 1-4 CPU controller doesn't have (NodeRepair: CPU-saturated at 1 CPU).
- `research/prior-art` notes that kube-scheduler is not evidence for or against an index, because it never indexes
  inter-pod constraints.

*Adopted piece:* parse selectors and namespace sets once per group hash (prior-art #4).

### A4. Cluster Autoscaler-style fork/revert snapshot of the whole cluster

*What:* adopt CA's `ClusterSnapshot` `Fork/Revert/Commit` over NodeInfos (`clustersnapshot.go`, `DeltaSnapshotStore`
in `store/delta.go`). It is O(1) fork with per-NodeInfo copy-on-write, and simulations run the real filter plugins
against the forked lister.

*Why rejected:*
- It solves a bigger problem than this one. Karpenter's simulation state is `StateNode` + `ExistingNode` +
  `Topology`, not a scheduler-framework lister. Rebasing Solve onto a NodeInfo snapshot is a rewrite of the
  scheduler.
- CA's delta store fully invalidates derived lists (`havePodsWithAffinity`, `nodeInfoList`) on *any* pod add/remove
  (`clearCaches`, with a TODO in its own code). Topology counts are exactly that kind of derived data, so we would
  inherit the weak spot.
- Fork/revert nesting is strictly LIFO. Karpenter runs a provisioner scheduler and a disruption scheduler
  concurrently against the same base, which needs an immutable shared base, not a mutable chain.

*Adopted pieces:* the overlay idea (immutable base + per-simulation deltas, excluded pods as −1) is L3. CA's
`BasicSnapshotStore` pattern (an obviously correct reference kept next to the fast store, with the same tests run
against both) is the differential-check mode below.

### A5. Materialize counts for all selectors eagerly

*What:* on every pod or node event, maintain counts for every selector that any pod in the cluster declares, so
first use never pays a registration scan.

*Why rejected:*
- The set of groups depends on the *pending* pod's constraints. Inverse anti-affinity groups come from existing
  pods. TSC groups come from pending pods, with matchLabelKeys values, default-injected constraints and
  `nodeFilter`. So "all selectors" is not knowable from the pod store alone. Eager mode would have to synthesize
  groups for every Deployment's pod template (most never pending) and recompute on every rollout (new
  `pod-template-hash`).
- Each event's cost grows with matched groups. Measured +150-350 ns/event at 500 registered groups with the
  inverted anchor index, and 1032 ns without it (`research/incremental-cow` §4).
- Eager registration of every historical template would grow without bound under Job churn (one `controller-uid`
  per Job).
- The hybrid costs one on-demand scan per group at first use (≈27 µs typical, 6.4 ms worst case for an
  all-namespace selector), then incremental maintenance with refcount + TTL GC. That gives the same steady state
  without the unbounded set.

### A6. go-memdb / go-immutable-radix (or HAMT / persistent B-tree) as the L2 store

*What:* Nomad's pattern of a single writer plus an atomically swapped `iradix` root with secondary indexes.

*Why rejected* (`research/incremental-cow` §1, 150k pods):
- Reads are 20-60x slower than a chunked COW vector: Get 151 ns (iradix), 125 ns (SortedMap), 55 ns (HAMT), vs
  2.6 ns.
- iradix retains 401 B/pod (60 MB) vs 34 B/pod (5 MB).
- It adds new dependencies (MPL-2.0 iradix, MIT immutable). Karpenter depends on neither, and decision 2 forbids it.

*Adopted pieces:* the single writer, `atomic.Pointer` root and `SnapshotMinIndex`-style `SnapshotAtLeast(gen)`.
The chunked COW vector is about 150 LoC.

### A7. Roaring postings for the selector index

*What:* `RoaringBitmap/roaring/v2` per (ns, key, value) posting.

*Why rejected:*
- Dependency policy (decision 2). Roaring pulls in roaring/v2 (Apache-2.0), `bits-and-blooms/bitset` (BSD-3) and
  `mschoch/smat`, and none of them are in go.mod.
- Memory: roaring is 4.6 MB of posting payload vs **12.3 MB** for sorted `[]uint32` at 150k pods / 14,475 postings.
  The whole index is 82 MB either way, because it is dominated by label copies, and interning fixes that
  (`research/selector-index` "Memory").
- Speed: sorted is equal or faster for per-namespace postings (113 vs 118 ms for 500 groups at 15k/150k).
- Known risk: sorted slices degrade on a "label every pod has" posting in a hot namespace (O(n) memmove on add).
  Mitigations are to iterate interned label sets rather than pods, which shrinks postings 20x, and to route that
  case through the per-namespace `all` list. A bitset tier can be added later without a dependency if KWOK/large-
  cluster data shows dense postings.

### A8. Hold `Cluster.mu.RLock` for the duration of Solve as the "snapshot"

Rejected. A 200 ms hold blocked the informer writer for 191 ms. Because Go's RWMutex prefers writers, it also blocked
the other scheduler's `RLock` for 180 ms (`research/incremental-cow`, `TestRWMutexHeldSnapshotBlocks`). Deep copying
on acquire instead costs 0.9 ms / 8 MB per Solve, which is 2.6 GB/min.

## Backward Compatibility

- **Gate off (alpha default):** no change at all. `NewTopology` takes today's path. The index isn't built and
  informer handlers don't feed it. The only shared code is the tier PRs, which have already merged with their own
  parity tests.
- **Gate on:**
  - Scheduling decisions are identical to the gate-off path, except for tie-breaks among equal-count domains.
    Today those come from Go map iteration order. The new path uses a random start offset per Solve over interned
    domain IDs (decision 3), seeded in tests. The distribution of choices stays uniform, so there is no zone-a
    bias.
  - Metrics and events stay the same, including `PendingPodsByEffectiveZone` via `GetTopologyZoneConstraints`.
  - Memory grows by the index: about 34 B/pod facts + about 146 B/pod with interned label sets, about 43 MB at 150k
    pods (`research/labelset-interning` §3), offset by fewer per-simulation allocations. Heap thresholds below
    track this.
- **Upgrade/downgrade:** the index is in-memory only. It is rebuilt from informers after leader election, gated by
  the existing `cluster.Synced` (`state/cluster.go:152`). There is no persisted state, no CRD change, and nothing to
  migrate. A leader change mid-rollout is safe: the new leader rebuilds the index.
- **Cloud providers:** no interface change. L1 keys on the identity of each `it.Requirements`. That relies on
  providers not mutating `it.Requirements` in place after `GetInstanceTypes`. That is already a de facto contract
  (AWS `InjectOfferings` reuses the cached map; `perf5-opt/sim.md` #3 step 2), but it is now documented on
  `cloudprovider.InstanceType`. A provider that violates it is caught by the differential mode, and the fallback is
  to key on a content hash (see Open Questions).
- **Fallback path:** whenever the index can't answer a group (an unregistered group whose registration failed,
  a snapshot older than `SnapshotAtLeast`, `cluster.Synced` false, or a selector shape the compiler doesn't support),
  `NewTopology` falls back per group to today's `countDomains`. The `topology_index_fallbacks_total` counter records
  it. The fallback never fails the simulation.

## Graduation Criteria

**Feature gate:** `TopologyIndex`, declared as a `FeatureGate` in `pkg/operator/options/options.go:89-160` and added
to `KarpenterFeatureGates` (`options.go:163`):

```go
TopologyIndexFeatureGate = FeatureGate{
    Name:    "TopologyIndex",
    Default: false,
    Stage:   docs.Alpha,
    Help: "Karpenter keeps topology spread and pod (anti-)affinity counts in an incrementally maintained in-memory " +
        "index instead of recounting pods for every scheduling simulation. Scheduling decisions are unchanged.",
}
```

Alternative name: `ClusterStateTopology`. `TopologyIndex` is preferred because it says what is built, not where.

**Hidden debug option** (not a feature gate; `--topology-index-differential-sample-rate`, env
`TOPOLOGY_INDEX_DIFFERENTIAL_SAMPLE_RATE`, float in [0,1], default 0; mirrors how `IgnoreDRARequests` is
documented as temporary in `options.go`). For the sampled fraction of simulations:
- After `NewTopology` the controller *also* runs the legacy `countDomains` path for every group.
- It compares per group: the domain → count map, the domain-presence set (`emptyDomains`), owners, and the inverse
  group set.
- On mismatch it increments `topology_index_differential_mismatches_total{group_type, topology_key, cause}` and logs
  the first mismatch per group hash per 10 min at ERROR, with the group identity and a diff. The simulation always
  uses the **legacy** result while differential mode samples it, so a mismatch is never user-visible.
- The diff is computed against the same `stateNodes` snapshot. Legacy reads the informer at a slightly different
  instant than the index generation, so mismatches whose diff touches only pods with resourceVersion newer than the
  snapshot generation are classed `cause=race` and excluded from the gate.

### Alpha (gate default off)

Ships when:
- All three layers are merged behind the gate. The legacy path stays intact.
- The unit differential suite, the fuzz targets and the envtest suites pass with the gate both on and off (see
  Validation plan).
- The KWOK perf suite has a new `topologyHeavy` test (config T, below) running in CI with the gate on and off,
  n ≥ 10 interleaved runs per arm (the #2997 lesson: at n=10 detection power for a 2x regression is 64%, so use
  n=20 for release gates).
- Metrics in "Observability" are exported.

Too risky to default on because:
- The new state lives in cluster state and is updated from informer event handlers (a new writer path).
- Correctness depends on exact diffing of pod/node/namespace events.
- Group GC races with Solves that hold snapshots.
- Parity of 6 known quirks has to be proven on real clusters, not only fixtures.

### Beta (gate default on)

All of the following must hold:
1. **Differential soak.** At least 2 weeks with sample rate 0.05-1.0 on at least 3 clusters: config T KWOK, a 1k-node
   real AWS cluster, and one 5k+-node cluster from the data-collection cohort. Required:
   `topology_index_differential_mismatches_total{cause!="race"} == 0`, and `cause="race"` below 0.1% of compared
   groups.
2. **KWOK config T thresholds** (gate on vs gate off, same build, n=20 interleaved, Mann-Whitney p<0.05 for each
   "improve" row):

   | metric (source) | today (integration) | Beta threshold |
   |---|---|---|
   | drift `scheduling.NewTopology` cum share, 120 s CPU profile (`harness/compare.sh`) | 10.45% | **≤ 2.0%** |
   | drift `Topology.countDomains` + `CacheReader.List` under NewTopology (`harness/breakdown.sh`) | 16.5 s / 120 s | **≤ 1.0 s** |
   | consolidation `NewTopology` cum share | 1.86% | ≤ 0.5% |
   | consolidation disruption scheduling sum / n (`karpenter_scheduler_scheduling_duration_seconds{controller="disruption"}`) | 0.46 s | ≤ 0.40 s and not worse than gate-off |
   | drift controller alloc / drifted eval | 332 GB / 332 = 1.0 GB | ≤ 0.8 GB |
   | provisioning P95 heap (`KarpenterP95HeapMB`) | gate-off value | ≤ gate-off + 15% |
   | correctness (`harness/check_topology.py`) | 0 pending, 0 anti-affinity violations, zone-skew "violations" 14-16 | identical classes and counts within run-to-run noise |
3. **Existing perf suite does not regress** (`test/suites/performance/*_test.go`). `KarpenterAvgCPUCores` and
   `KarpenterP95HeapMB` stay within the current inline thresholds for every case. Examples:
   `hostNameSpreadingXL/consolidation` heap < 730 MB and CPU < 1.70 cores (`host_name_spreading_xl_test.go:95-97`),
   `drift/drift` heap < 250 MB and CPU < 1.05 (`drift_performance_test.go:94-96`). The new `topologyHeavy` case gets
   inline thresholds set to the Beta gate-on median + 20%. Providers can override them via
   `KARPENTER_PERF_THRESHOLDS` (`thresholds.go`).
4. **Scale numbers** from the large-cluster run (Validation §5), all at 15k nodes / 150k pods / 500 registered
   groups:
   - index event overhead p99 under 2 µs/event under `Cluster.mu`;
   - snapshot acquire p99 under 1 µs, and publish p99 under 100 µs at batch 16;
   - retained index memory ≤ 300 B/pod (prototype 287 B/pod, L scenario);
   - per-simulation topology cost ≤ 5 ms with 500 groups, where today's is 8.81 s on base and about 0.6-1 s after
     #23.
5. No open bugs labelled `area/topology-index` with severity ≥ important.
6. Fallback rate under 1% of groups in steady state (`topology_index_fallbacks_total` / `topology_index_lookups_total`).

### GA (gate removed, legacy path deleted)

- At least 2 minor releases at Beta with no mismatch-class bug reports. The differential mode is kept through the
  first GA release, then deleted with the legacy path.
- AWS provider and at least one other provider (KWOK core, or Azure if available) have run the perf suite at Beta
  defaults.
- The follow-up semantic fixes (list above) have each been decided. Fixed or explicitly kept, each with its own PR
  and release note. They may land after GA. GA only requires the parity path to be the sole path.
- The `cloudprovider.InstanceType` immutability contract is documented, with a test in the provider conformance kit.

## Observability

All new metrics use `metrics.Namespace` and the `cluster_state` subsystem (`state/metrics.go:28`), except where noted.
They are emitted only when the gate is on. Label cardinality is bounded: `group_type ∈ {spread, affinity,
anti_affinity, inverse_anti_affinity}`, `topology_key` limited to a bounded set (zone, hostname, capacity-type, and
`other`).

| metric | type | labels | meaning / use |
|---|---|---|---|
| `karpenter_cluster_state_topology_index_pods` | gauge | `state={counted,ignored}` | Pod facts held. `ignored` = unscheduled/terminal/terminating, kept for parity quirk 2 |
| `karpenter_cluster_state_topology_index_label_sets` | gauge | | Distinct interned (ns, labels − strip keys). pods/label_sets is the live redundancy ratio |
| `karpenter_cluster_state_topology_index_node_shapes` | gauge | | Distinct node shapes (labels − unique keys + taints) |
| `karpenter_cluster_state_topology_index_memory_bytes` | gauge | `component={facts,labelsets,postings,counts,universe}` | Estimated retained bytes, computed on publish from slice capacities |
| `karpenter_cluster_state_topology_index_registered_groups` | gauge | `group_type, topology_key` | Groups with maintained counts |
| `karpenter_cluster_state_topology_index_group_registrations_total` / `_group_evictions_total` | counter | `group_type` | Churn of the refcount/TTL GC. High registrations with low steady groups means the TTL is too short |
| `karpenter_cluster_state_topology_index_registration_duration_seconds` | histogram | | Cost of the first-use scan |
| `karpenter_cluster_state_topology_index_generation` | gauge | | Current published generation |
| `karpenter_cluster_state_topology_index_snapshot_lag_generations` | histogram | `controller={provisioner,disruption}` | Generations between `index.gen` and the snapshot a Solve used. With flush-on-acquire this is normally 0. Non-zero means the reader didn't need the latest |
| `karpenter_cluster_state_topology_index_snapshot_age_seconds` | histogram | `controller` | Wall time since the snapshot the Solve used was published (backstop for idle readers) |
| `karpenter_cluster_state_topology_index_events_total` | counter | `kind={pod,node,namespace}, result={noop,diff}` | Confirms the 1-minute requeues are no-ops (`noop` ≫ `diff`) |
| `karpenter_cluster_state_topology_index_event_duration_seconds` | histogram | `kind` | Writer cost under `Cluster.mu` |
| `karpenter_scheduler_topology_index_fallbacks_total` (scheduler subsystem, `scheduling/metrics.go:32`) | counter | `reason={unsynced,unsupported_selector,registration_error,stale_snapshot}` | Per-group fallback to legacy `countDomains` |
| `karpenter_scheduler_topology_index_lookups_total` | counter | `result={hit,registered}` | Denominator for the fallback rate |
| `karpenter_scheduler_topology_index_differential_checks_total` / `_differential_mismatches_total` | counter | `group_type, topology_key, cause={counts,presence,owners,inverse_set,race}` | Differential mode. Beta gate = 0 non-race mismatches |

Logs:
- One ERROR per group hash per 10 min on a differential mismatch, with the group identity and the first 10 differing
  domains.
- V(1) on group registration/eviction.

No new events or status conditions. The feature has no user-facing object.

## Validation plan

### 1. Unit differential tests (package `scheduling`, `state`)

| test | reference | inputs | asserts |
|---|---|---|---|
| `TestTopologyIndexDifferential` (state) | naive map recount from the informer objects (today's `countDomains` logic, copied verbatim as `countDomainsReference`) | 300 seeds × 400 steps: pod add/bind/label change/terminate/Succeeded/delete, node add/relabel/taint/delete, namespace relabel, group register/GC, with a held old snapshot | counts, presence, matched set per group are **exactly** equal, both on-demand and maintained, and the held snapshot is unchanged |
| `TestNewTopologyParity` (scheduling) | `NewTopology` gate-off | the existing 443 scheduling specs' fixtures + random workloads from `research/selector-index/gen.go` | `topologyGroups` / `inverseTopologyGroups` equal by hash: domains, emptyDomains, owners, counts |
| `TestOverlayParity` | gate-off Topology with `excludedPods` | random candidate exclusions + Record sequences | per-group `Get` returns equal requirements, with the random start offset seeded identically on both sides |
| parity quirk pins | | one spec per quirk 1-6 | today's behaviour, named `parity: <quirk>` so the follow-up PR flips exactly one |
| `TestDomainUniverseParity` | `buildDomainGroupsReference` (already in #25, 2000 seeds) | extended with the L1 cache key: NodePool generation bump, IT requirement identity change | cached ≡ freshly built |

Mutation checks (as #25 and #26 do): drop the terminating flag, drop a namespace from the postings, skip the −1 for
an excluded pod, skip presence on node delete, skip the GC refcount decrement. Each must fail at least one test.

### 2. Fuzz

- `FuzzSelectorCompile`: random `metav1.LabelSelector` (nil, empty, invalid) plus random pods. Compiled index match
  must equal `labels.Selector.Matches` and the `TopologyListOptions` semantics. Ported from
  `research/selector-index/diff_test.go`, which ran 123k execs with 0 failures.
- `FuzzIndexOps`: an op stream driving `TopoIndex` vs the naive reference, run 10 min in CI nightly.
- `FuzzCOWVector`: random set/publish/hold, checked against a `[]T` reference, under `-race`.

### 3. envtest suites

- Run `./pkg/controllers/provisioning/...`, `./pkg/controllers/disruption/...` and `./pkg/controllers/state/...` with
  `--feature-gates=TopologyIndex=true` as an extra CI matrix leg. The suites set the option through
  `test.Options()`, so add `FeatureGates.TopologyIndex` to the test options defaults.
- Known noise: 6 + 1 DRA "Consumable capacity" failures also fail on base with envtest 1.34.1 (see every PR's
  test.log). They must fail identically in both legs.
- Run the same matrix leg with the differential sample rate = 1.0. It must show 0 mismatches. Fail the leg on any
  `differential_mismatches_total > 0`.
- `-race` on the Topology specs plus a new concurrent spec: provisioner and disruption Solve on one snapshot while
  the writer publishes.

### 4. KWOK: config T + perf suite

- Config T is defined in `prs/harness/patch_gen.py` (10 tainted NodePools, 100 Deployments, 3000 pods; zone spread on
  all, hostname spread on every 5th, anti-affinity on i%10==1). Upstream it as
  `test/suites/performance/topology_heavy_test.go`, using the existing report machinery and `*Threshold("topologyHeavy/<phase>")`.
- Phases: scale-out, drift all, consolidate to 50%. Fix the consolidation phase timeout so the phase completes. Today
  it times out at 60 min on both arms.
- Per arm (gate off/on, same image): n=20 interleaved runs (ABAB) on the same cluster. For each phase, collect the
  120 s CPU and alloc profiles and `results.tsv`, as `harness/compare.sh` does.
- Report profile shares (`harness/breakdown.sh`) and per-call metrics, never whole-phase totals. Node counts vary
  ±60 between runs of the same build (`prs/99-integration/comment.md`).
- Run `harness/check_topology.py` on every run.
- Add a **scale variant**, T-XL: 50 NodePools, 500 Deployments, 15k pods, 1 all-namespace anti-affinity group and
  20% `namespaceSelector` groups. It exercises the hot-namespace and all-namespace paths, and needs tier-4xl.

### 5. Large-cluster data collection (label-set redundancy, node shapes, group shapes)

Purpose: replace the generator-derived assumptions with real data. The assumptions are 20x label-set redundancy
after stripping, 23 node shapes per 1002 nodes, and dense vs sparse postings (`research/SYNTHESIS.md` "Gaps").

**Safety:** only read-only `get`/`list` calls. Use a ReadOnly / view-only kubeconfig context. Treat every target
cluster as production. Run outside peak hours on very large clusters. `--raw` with `limit` paginates, so the API
server serves at most 500 objects per call from the watch cache (`resourceVersion=0`). No secrets, configmaps or pod
specs beyond scheduling fields are retained. `--anonymize` HMACs every label value, namespace and name with a local
salt before anything is written to disk, so the output can be shared.

```bash
# 0. Context check (read-only identity; refuse to run against an admin context by habit)
kubectl config current-context
kubectl auth can-i --list | head -20          # expect get/list/watch only
kubectl auth can-i delete pods -A             # expect "no"

# 1. Collect (memory-bounded, paginated). Writes JSONL under ./topo-dump-<ctx>-<ts>/
python3 -I collect_topology_facts.py --out ./topo-dump --anonymize --salt "$(openssl rand -hex 16)"

# 2. Analyze (offline, no cluster access)
python3 -I analyze_topology_facts.py ./topo-dump > report.md
```

`collect_topology_facts.py` (stdlib only):

```python
#!/usr/bin/env python3
"""Read-only dump of the facts a TopologyIndex would hold. Paginates with `kubectl get --raw` so memory stays bounded."""
import argparse, datetime, hashlib, hmac, json, os, subprocess, urllib.parse

STRIP = {"statefulset.kubernetes.io/pod-name", "apps.kubernetes.io/pod-index", "batch.kubernetes.io/job-completion-index"}

def raw(path):
    out = subprocess.run(["kubectl", "get", "--raw", path], check=True, capture_output=True).stdout
    return json.loads(out)

def paged(base):
    cont = None
    while True:
        q = {"limit": "500"}
        if cont: q["continue"] = cont
        else: q["resourceVersion"] = "0"          # serve first page from watch cache
        page = raw(base + "?" + urllib.parse.urlencode(q))
        yield from page.get("items", [])
        cont = page.get("metadata", {}).get("continue")
        if not cont: return

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True); ap.add_argument("--anonymize", action="store_true"); ap.add_argument("--salt", default="")
    a = ap.parse_args()
    key = a.salt.encode()
    def h(s):  # anonymize values but keep equality and well-known keys readable
        if not a.anonymize or s is None: return s
        return hmac.new(key, s.encode(), hashlib.sha256).hexdigest()[:16]
    def hl(labels): return {k: h(v) for k, v in (labels or {}).items()}
    def hsel(sel):
        if sel is None: return None
        return {"matchLabels": hl(sel.get("matchLabels")),
                "matchExpressions": [{"key": e["key"], "operator": e["operator"], "values": [h(v) for v in e.get("values") or []]}
                                     for e in sel.get("matchExpressions") or []]}
    def terms(ts):
        return [{"topologyKey": t.get("topologyKey"), "labelSelector": hsel(t.get("labelSelector")),
                 "namespaces": [h(n) for n in t.get("namespaces") or []], "namespaceSelector": hsel(t.get("namespaceSelector")),
                 "matchLabelKeys": t.get("matchLabelKeys")} for t in ts or []]
    ctx = subprocess.run(["kubectl", "config", "current-context"], capture_output=True, text=True).stdout.strip()
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in h(ctx))
    d = f"{a.out}-{safe}-{datetime.datetime.now(datetime.timezone.utc):%Y%m%dT%H%M%S}"; os.makedirs(d)
    with open(f"{d}/pods.jsonl", "w") as f:
        for p in paged("/api/v1/pods"):
            md, sp, st = p["metadata"], p.get("spec", {}), p.get("status", {})
            aff = sp.get("affinity") or {}
            pa, paa = aff.get("podAffinity") or {}, aff.get("podAntiAffinity") or {}
            own = (md.get("ownerReferences") or [{}])[0]
            f.write(json.dumps({
                "ns": h(md["namespace"]), "labels": hl(md.get("labels")), "node": h(sp.get("nodeName")),
                "phase": st.get("phase"), "terminating": md.get("deletionTimestamp") is not None,
                "owner": own.get("kind"), "ownerUID": h(own.get("uid")),
                "tsc": [{"topologyKey": c.get("topologyKey"), "maxSkew": c.get("maxSkew"), "minDomains": c.get("minDomains"),
                         "whenUnsatisfiable": c.get("whenUnsatisfiable"), "labelSelector": hsel(c.get("labelSelector")),
                         "matchLabelKeys": c.get("matchLabelKeys"), "nodeAffinityPolicy": c.get("nodeAffinityPolicy"),
                         "nodeTaintsPolicy": c.get("nodeTaintsPolicy")} for c in sp.get("topologySpreadConstraints") or []],
                "affReq": terms(pa.get("requiredDuringSchedulingIgnoredDuringExecution")),
                "antiReq": terms(paa.get("requiredDuringSchedulingIgnoredDuringExecution")),
                "affPref": terms([w.get("podAffinityTerm") for w in pa.get("preferredDuringSchedulingIgnoredDuringExecution") or []]),
                "antiPref": terms([w.get("podAffinityTerm") for w in paa.get("preferredDuringSchedulingIgnoredDuringExecution") or []]),
                "nodeSelectorKeys": sorted((sp.get("nodeSelector") or {}).keys()),
                "hasNodeAffinity": bool(aff.get("nodeAffinity")), "tolerations": len(sp.get("tolerations") or []),
            }) + "\n")
    with open(f"{d}/nodes.jsonl", "w") as f:
        for n in paged("/api/v1/nodes"):
            f.write(json.dumps({"name": h(n["metadata"]["name"]), "labels": hl(n["metadata"].get("labels")),
                                "taints": [{"key": t["key"], "value": h(t.get("value")), "effect": t["effect"]} for t in n.get("spec", {}).get("taints") or []]}) + "\n")
    with open(f"{d}/namespaces.jsonl", "w") as f:
        for n in paged("/api/v1/namespaces"):
            f.write(json.dumps({"name": h(n["metadata"]["name"]), "labels": hl(n["metadata"].get("labels"))}) + "\n")
    for res, path in (("nodepools", "/apis/karpenter.sh/v1/nodepools"),):
        try:
            with open(f"{d}/{res}.jsonl", "w") as f:
                for o in paged(path):
                    t = o["spec"]["template"]["spec"]
                    f.write(json.dumps({"name": h(o["metadata"]["name"]), "generation": o["metadata"].get("generation"),
                                        "requirements": [{"key": r["key"], "operator": r["operator"], "n": len(r.get("values") or [])} for r in t.get("requirements") or []],
                                        "taints": len(t.get("taints") or [])}) + "\n")
        except subprocess.CalledProcessError:
            pass  # not a Karpenter cluster
    print(d)

if __name__ == "__main__":
    main()
```

`analyze_topology_facts.py` (stdlib only). It reports what the RFC's sizing depends on:

```python
#!/usr/bin/env python3
import collections, json, sys
d = sys.argv[1]
STRIP = {"statefulset.kubernetes.io/pod-name", "apps.kubernetes.io/pod-index", "batch.kubernetes.io/job-completion-index"}
UNIQ_NODE = {"kubernetes.io/hostname", "node.kubernetes.io/instance-id", "karpenter.sh/nodeclaim", "eks.amazonaws.com/nodeclaim"}
load = lambda f: [json.loads(l) for l in open(f"{d}/{f}")]
pods, nodes, nss = load("pods.jsonl"), load("nodes.jsonl"), load("namespaces.jsonl")
counted = [p for p in pods if p["node"] and p["phase"] not in ("Succeeded", "Failed") and not p["terminating"]]
ls = lambda p, strip: (p["ns"], tuple(sorted((k, v) for k, v in p["labels"].items() if not (strip and k in STRIP))))
full, stripped = collections.Counter(ls(p, False) for p in counted), collections.Counter(ls(p, True) for p in counted)
keyvals = collections.defaultdict(set)
for p in counted:
    for k, v in p["labels"].items(): keyvals[k].add(v)
sets = len(stripped) or 1
suggest = sorted(k for k, v in keyvals.items() if sets > 1000 and len(v) > sets / 2)   # SuggestStripKeys rule
perns = collections.Counter(p["ns"] for p in counted)
shape = collections.Counter((tuple(sorted((k, v) for k, v in n["labels"].items() if k not in UNIQ_NODE)),
                             tuple(sorted((t["key"], t["value"], t["effect"]) for t in n["taints"]))) for n in nodes)
def sel_kind(s):
    if s is None: return "nil"
    ops = {e["operator"] for e in s["matchExpressions"]}
    if not s["matchLabels"] and not ops: return "empty"
    if ops - {"In"}: return "+".join(sorted(ops - {"In"}))
    return "matchLabels/In"
groups = collections.Counter(); kinds = collections.Counter(); keys = collections.Counter(); nsscope = collections.Counter()
for p in pods:
    for typ, ts in (("spread", p["tsc"]), ("affinity", p["affReq"] + p["affPref"]), ("anti", p["antiReq"] + p["antiPref"])):
        for t in ts:
            g = (typ, p["ns"], json.dumps(t, sort_keys=True)); groups[g] += 1
            kinds[sel_kind(t.get("labelSelector"))] += 1; keys[t["topologyKey"]] += 1
            nsscope["namespaceSelector" if t.get("namespaceSelector") is not None else ("namespaces" if t.get("namespaces") else "own")] += 1
print(f"# Topology facts: {d}\n")
print(f"- pods {len(pods)}, counted {len(counted)}, terminating {sum(p['terminating'] for p in pods)}, namespaces {len(nss)}, nodes {len(nodes)}")
print(f"- label sets: full {len(full)} (ratio {len(counted)/max(1,len(full)):.1f}x), stripped {len(stripped)} (ratio {len(counted)/sets:.1f}x)")
print(f"- (labelset,node) pairs {len({(ls(p, True), p['node']) for p in counted})}")
print(f"- top label keys by cardinality: {sorted(((len(v), k) for k, v in keyvals.items()), reverse=True)[:10]}")
print(f"- SuggestStripKeys candidates: {suggest}")
print(f"- hot namespaces (pods): {perns.most_common(5)}")
print(f"- largest label set (pods): {stripped.most_common(1)[0][1] if stripped else 0}")
print(f"- node shapes (labels-unique+taints): {len(shape)} for {len(nodes)} nodes ({len(nodes)/max(1,len(shape)):.1f}x)")
print(f"- distinct topology terms (≈ groups, before matchLabelKeys/nodeFilter): {len(groups)}; by selector kind {dict(kinds)}")
print(f"- by topologyKey {dict(keys.most_common(8))}; namespace scope {dict(nsscope)}")
print(f"- est. index memory: facts {len(counted)*16/1e6:.1f} MB + labelsets ~{sets*1024/1e6:.1f} MB + postings ~{len(counted)*8*7/1e6:.1f} MB (sorted []uint32, ~7 labels/pod)")
```

The cohort to collect from:
- one EKS 1k-node cluster;
- one 5k+ node cluster (with #3197's reporters, if willing, using `--anonymize`);
- one multi-tenant cluster with more than 200 namespaces;
- the KWOK T and T-XL runs, as a cross-check against `prs/harness/check_topology.py` data.

Decision rules: if the stripped ratio is below 5x on any real cluster, or `SuggestStripKeys` flags a key not in the
default strip list, revisit L2 interning (Open Question 2). If the largest posting is above 10% of counted pods in a
single namespace, add the hand-rolled bitset tier for that posting class.

## Remaining NewScheduler costs (non-topology): which RFC owns them

After all tier PRs, `Provisioner.NewScheduler` is still 37.2% of drift CPU. NewTopology is only 18.7 s of it. The rest,
measured in the same 120 s drift profile (`prs/99-integration/comment.md`), is:

| frame | 120 s drift CPU | code | what it does / why it's expensive | per-simulation input that varies? |
|---|---|---|---|---|
| `lo.FilterMap` in `scheduling.NewScheduler` | **17.7 s** | `scheduling/scheduler.go:183-200` (closure calls `NewNodeClaimTemplate(np)` at :185 and `filterInstanceTypesByRequirements` at :186 → `nodeclaim.go:598`) | The NodeClaimTemplate pre-filter. For each NodePool it filters *all* the NodePool's instance types against the template requirements with an empty pod and an empty-daemon overhead group. `filterInstanceTypesByRequirements` deliberately doesn't short-circuit, for error messages (`nodeclaim.go:635-640`), and calls `fits()` and the offering checks per IT. AWS `InjectOfferings` returns new `InstanceType` structs every `GetInstanceTypes`, so the `sync.Once` allocatable precompute is cold each simulation (`perf5-opt/sim.md` #4: `fits` 10.2 s of 14.1 s, `computeAllocatable` 7.0 s) | No. Depends on NodePool + IT list only |
| `buildDaemonOverheadGroups` | 9.0 s | `scheduler.go:205`, `:1017-1052`; `isDaemonPodCompatible` `:1068-1090` | For each template, for each IT option, filters every DaemonSet pod with `isDaemonPodCompatible`, which rebuilds `NewStrictPodRequirements(pod)` per (IT, pod) inside the relax loop. It then builds `podSetKey` strings and `RequestsForPods` | No. Depends on templates + DaemonSet pods (+ prediction store) |
| `NewReservationManager` | 7.6 s | `scheduler.go:166` → `reservationmanager.go:33-52` | Walks every offering of every IT of every NodePool. `Offering.CapacityType()` → `Requirement.Any()` (`cloudprovider/types.go:596`, `scheduling/requirement.go:271-274`) allocates `values.UnsortedList()` just to read element 0 | Seed: no. `creditReservationCapacity` (`scheduler.go:167-169`) is per simulation but is a cheap credit on top |
| `GetInstanceTypes` per NodePool | 11.5 s | `provisioner.go:313-314` | Provider call per NodePool per simulation. AWS rebuilds offerings (`InjectOfferings`, `cacheKeyFromInstanceType` hashing; see #3204 and aws#7986) | No, within a pass. Offerings availability can change between passes (ICE cache, reservation capacity) |

Together: 45.8 s per 120 s drift window, about 2.5x the remaining NewTopology cost. Also known from NodeRepair, partly
fixed by fork PR #12: existing-node daemon compatibility, `DeepCopyNodes` (`helpers.go:68`) and `GetPendingPods`
deep copies (`helpers.go:83`).

**Recommendation: a sibling RFC, "Per-pass scheduling snapshot", not this one.** Reasons:
- **Different invariants.** The topology index is *event-maintained* state that must be exact at every generation
  (pods move constantly). The four costs above are *pure functions of slowly changing inputs*: NodePool spec
  generation, the IT list identity/offerings version, DaemonSet pod specs, and reservation capacity. They are best
  served by memoization keyed on versions, which is the lesson prior-art draws from the ecache removal, not by an
  index.
- **Different staleness contract.** Offerings and reservation capacity are allowed to change between passes, and
  terminate-first credits depend on them (`scheduler.go:164-169`). A per-pass snapshot, the same lifetime as the
  candidate list (`perf5-opt/sim.md` #5 `provisioning.WithSchedulingSnapshot(ctx)`), or a cross-pass memo keyed on
  (NodePool UID+generation, `GetInstanceTypes` result identity, DaemonSet generation set), is a separate decision
  with separate risks: stale offerings causing launch failures, and ICE.
- **Different reviewers and gate.** It touches the cloudprovider contract (stable IT identity across calls) and
  reservations/terminate-first. This RFC is scheduling-topology only.
- **Shared piece.** Both need the same L1 key: (NodePool UID, Generation, identity of each `it.Requirements`). Define
  that key once, in L1 of this RFC (`DomainUniverse` cache key), and have the sibling RFC's `TemplateSnapshot` reuse
  it. Then a NodePool update invalidates universe, templates, daemon groups and the reservation seed together.
- **Cheap fixes that need no RFC** (standalone PRs, can land now):
  1. Pre-filter `instanceTypes[np.Name]` with `compatible(it, nct.Requirements)` before
     `filterInstanceTypesByRequirements` in the `FilterMap` closure (`scheduler.go:186`). Measured −30/−35% on a
     restricted NodePool. Only the "why nothing matched" error booleans change (`perf5-opt/sim.md` #4).
  2. `Requirement.Any()` In-case: `for v := range r.values { return v }` (`requirement.go:274`), about 1.5 s in D.
  3. Hoist `NewStrictPodRequirements(pod)` out of the per-IT loop in `buildDaemonOverheadGroups`/
     `isDaemonPodCompatible`. Compute it once per DaemonSet pod per template, so the IT check is only
     `it.Requirements.Intersects`. This is the same idea as fork PR #12 for existing nodes.

## Open Questions

1. **L1 cache key robustness.** Is pointer identity of `it.Requirements` safe for all providers, or should the key
   be a content hash computed once per `GetInstanceTypes` result? A hash costs about the old `buildDomainGroups`
   loop once per pass, not per simulation. Differential mode will detect violations, but only on sampled
   simulations.
2. **Interning strip keys.** Is the static list plus `SuggestStripKeys` enough, or does any real cohort cluster show
   an operator-injected per-pod label (1.0x redundancy)? That depends on the data collection.
3. **Differential mode at Beta.** Keep sampling on by default at 0.01 for Beta to catch field mismatches, at roughly
   one legacy `countDomains` per 100 simulations? Or off by default and opt-in?
4. **Group TTL.** 5 or 10 min? It trades registration churn (rollouts change `pod-template-hash`, so matchLabelKeys
   groups re-register on every rollout) against memory. Decide with the `group_registrations_total` vs
   `registered_groups` data from the soak.
5. **Gate name.** `TopologyIndex` vs `ClusterStateTopology`.
6. **Perf CI budget.** n=20 interleaved KWOK runs per arm for release gates is about 2 days of tier-4xl time for
   config T. Run it nightly on mainline instead of per PR, with per-PR n=5 smoke runs?
