# Scheduling topology from cluster state

_Draft RFC. The detailed designs are linked as appendices under `designs/topology-from-cluster-state/`._

**Decisions so far:**
1. Parity first.
2. No new dependencies.
3. Random tie-break offset per Solve.
4. L2 is fed from informer event handlers, not cluster-state reconciles. This removes reconcile lag and requires a test hydration helper.
5. Only L2 is feature-gated (`TopologyIndex`). L3 ships behind a plain option (`--topology-engine`), which is removed after the shadow comparison stays clean.
6. Extensibility: `CountSource` is the only seam, and L2 is a general fact index.

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


## Proposal

Split scheduling topology into three layers. Each layer matches a different rate of change. Behaviour stays exactly the same (parity first), apart from tie-breaks among domains with equal counts. Everything ships behind the `TopologyIndex` feature gate.

| Layer | What it holds | Lifetime | Where |
|---|---|---|---|
| **L1: DomainUniverse** | domains per topology key, built lazily from NodePools × instance types, interned to dense `DomainID`s | per `Topology`; the domain dictionaries are process-wide and append-only | `pkg/scheduling/topology` (new leaf package) + scheduling |
| **L2: TopologyIndex** | facts about pods (namespace, interned label set, node, flags), node slots (domains, taint set, shape), namespaces; base counts per registered group, kept up to date incrementally | process lifetime; published as an immutable snapshot | `pkg/controllers/state/topoindex` |
| **L3: per-Solve overlay** | read-only base counts + local deltas (`Record`, excluded pods as −1), domain presence, owner and selector indexes, per-pod plan | one `Topology` (one Solve); sealed when the Solve ends | `pkg/controllers/provisioning/scheduling` |

```
 informers ──► state.Cluster ──► topoindex.Index ──(atomic snapshot, generation)──┐
                                                                                    ▼
 NodePools × ITs ──► L1 key universe (lazy) ──► NewTopology ──► L3 overlay ◄── CountSource
                                                     │                          (L2 adapter | list fallback)
                                                     └── Solve: Get/Record on base+delta, sealed at end
```

### Contract between L2 and L3 (reconciled)

The two section drafts each defined this interface their own way. We settle on the following:

- **L3 owns the interface** (`CountSource`, defined in L1-L3 §0). The scheduler consumes it, and two implementations satisfy it:
  - `listCountSource`: today's `countDomains` logic refactored. It is permanent. It serves unit tests, the gate-off path, and the differential reference.
  - `indexCountSource`: a thin adapter over `topoindex.Snapshot` (L2 §5.1). The mapping:
    - `Counts(key)` → `snap.Counts(groupKey)` when the group is registered at this generation. Otherwise `snap.CountOnDemand(spec)` plus `ix.Register(spec)`, so a Solve never waits and never mixes generations.
    - `ExcludedContributions` → `snap.PodByUID` + `snap.Contribution`.
    - `InverseAntiAffinityGroups` → `snap.InverseTerms()`, filtered by the excluded owners. Groups whose only owners are excluded are dropped, as today.
    - `Generation` → `snap.Gen()`.
- **Shared types live in a new leaf package, `pkg/scheduling/topology`.** That package holds `CountKey`, `BaseCounts`, `DomainID`/`DomainDict`, `Mask`, and `NodePredicate`. This avoids an import cycle between `state` and `provisioning/scheduling`. `TopologyNodeFilter` implements `NodePredicate` (`Hash`, `Keys`, `ReadsTaints`, `Matches`).
- **Group identity** is `TopologyGroupIdentity`. Its hash is bit-for-bit today's `Hash()` after #24, so groups merge exactly as they do now. `CountKey` is the part of the identity that determines counts: key, namespaces, *list* selector, node filter. Groups that differ only in type, maxSkew, minDomains or seed tolerations share counts in L2.
- **Domain presence (count 0 vs absent) stays in L3.** L3 recomputes it per `Topology` from that Solve's `stateNodes` plus counts after exclusion (L1-L3 §4). L2 only supplies node-shape fast paths.

### L1: DomainUniverse
- After #19 and #25, the universe costs under 1% of `NewTopology` on KWOK. It is built lazily per key from per-NodePool sources.
- **No cache across Solves.** The candidate cache key (NodePool UID+Generation + `it.Requirements` identity) is unreliable:
  - placement-group NodeClasses rebuild `Requirements` on every call;
  - NodeClass status changes don't bump `NodePool.Generation`;
  - the overlay store can drop a NodePool for a single call.
- **Interning:** domains are interned into process-wide, append-only `DomainDict`s per key. Keys with more than 4096 domains switch to sparse string mode. Hostname is always sparse.

### L3: per-Solve overlay (L1-L3 §3–§9)
- **Dense keys** (zone, capacity-type, rack, …): the group copies L2's dense counts into a private `[]int32` (about 38 ns) and applies the deltas. `present`/`nonzero` are bitsets, and the minimum is a masked scan.
- **Hostname groups:** a shared sparse base plus a sparse per-Solve delta. `Register(hostname)` becomes an O(1) append to a log.
- **Groups created mid-Solve** (relaxation, re-`Update` after failure) keep today's behaviour: they don't see earlier `Record`s. Changing that is follow-up F6.
- **Per-pod plan**, built serially before the parallel `CanAdd` rounds: matching groups and the min per owned group. Owner and selector indexes (#23). No `validDomains` allocation on every `Get`.
- **Tie-breaking:** a seeded random start offset per Solve, rotated per group. This matches today's map-order randomness.
- **Lifetime:** `seal()` at the end of Solve releases the snapshot reference. `topologyError` text stays lazy and reads the sealed overlay.

### L2: TopologyIndex (L2 §3–§8)
- **Write path:** hooks in `cluster.go`. `ObserveNode` goes first in `UpdateNode`; `ObservePod` also runs on the NotFound path; every write to `c.bindings` goes through `setBinding`/`deleteBinding`. A new namespace informer is added.
  - Diffing makes the 1-minute requeues no-ops: a resourceVersion fast path, then a field compare. The generation only bumps on real changes.
  - `ix.mu` is nested inside `c.mu`. Readers never take `c.mu`.
- **Data:** a 16 B `PodFact`. Label sets are interned (namespace included) with an adaptive strip of per-pod-unique keys. Postings are sorted `[]uint32` (no new dependencies). Node slots hold **two views**:
  - the informer-cache view, used by forward counting;
  - the cluster-state view, used by inverse anti-affinity.

  Budget: about 45–60 MB at 150k pods / 15k nodes.
- **Read path:** an immutable snapshot behind `atomic.Pointer`, published lazily when a Solve asks. `SnapshotAtLeast(gen)` serves disruption validation. Groups are registered on first use and maintained incrementally after that, with GC by TTL + LRU cap.
- **Sync:** a new `ix.Synced` does a one-time subset check of pods and namespaces. Until it passes, `NewScheduler` uses `listCountSource` (this also covers leader failover).

### Extensibility

The design is meant to host future consumers without another rewrite:
- **`CountSource` is the only seam the scheduler sees.** New sources plug in behind it without touching Solve. Examples: a per-pass snapshot (the sibling RFC for the non-topology `NewScheduler` costs) or a test fake.
- **L2 is a generic fact index, not a topology cache.** Pods keyed by (namespace, interned label set, node, flags) and nodes keyed by shape can answer any selector-by-domain question. Candidates:
  - PDB selector matching (`pkg/utils/pdb/pdb.go:96` matches every PDB selector against each pod);
  - default-constraint Service lookups (`defaultconstraints.go`);
  - capacity-buffer and DRA placement checks.

  These get registered as additional group kinds rather than new indexes.
- **Group kinds are data, not types.** `CountKey` = (topology key, namespaces, selector, node predicate). A new constraint type, for example a future upstream topology-spread variant, only needs a new identity projection plus `Get` logic in L3.
- **The domain dictionaries and node shapes are shared**, so per-pass caches of instance-type and NodePool data can key on the same IDs.
- **Snapshot generations** (`SnapshotAtLeast`) also give disruption a cheap "has anything relevant changed" signal for later work (L2 Q6). That is out of scope for parity.

### Rollout modes (one gate, one debug knob)
- **Gate `TopologyIndex` off (alpha default):** today's code path. L2 is not maintained (open question 8).
- **Gate on:** L3 runs on `indexCountSource`.
- **L3 engine (independent of the gate):** `--topology-engine=overlay|legacy` (L1-L3 PR 8). The overlay goes on by default only after its shadow harness is clean on `listCountSource`.
- **`--topology-index-differential-sample-rate` (hidden):** on sampled simulations, also runs `listCountSource` + the legacy `Topology`, *uses the legacy result*, and counts mismatches by cause (`race` vs `logic`).

### Implementation sequence
L3 lands first, on `listCountSource` only, with no cluster-state changes (L1-L3 §10, PRs 1–8). It goes on by default only after the shadow harness stays clean. L2 then lands behind the gate (L2 PRs), and L1-L3 PR 9 wires it in. Legacy removal (PR 10) comes after one to two releases with clean shadow metrics.

### Implementation sequence (small, mergeable PRs from topo/integration)

Each PR passes the full scheduling and disruption suites. The new path is not on by default until PR 8.

| # | PR | contents | behaviour |
|---|---|---|---|
| 1 | `refactor(scheduling): topology facade + legacy copy + order hook` | facade with `topologyImpl`; verbatim `legacyTopology`; `order` hook (identity in prod); `TopologySeed` option (no-op for legacy) | none |
| 2 | `refactor(scheduling): TopologyGroupIdentity and CountKey` | identity struct and constructor, canonical selector, `CountKey` + `Equal`; test: `identity.hash == legacy Hash()` over fuzz | none |
| 3 | `perf(scheduling): interned domain dictionaries and masks` | `DomainDict`, `Mask` (inline word + rest), `TSet`; `keyUniverse` built from `domainSource`; differential `SeedMask` vs `ForEachDomain` over the #25 generator | none (new code only) |
| 4 | `refactor(scheduling): CountSource with list-based implementation` | `CountSource`, `BaseCounts`, `ExcludedPods`, `listCountSource` (countDomains logic, informer List/Get, memo from #23), inverse via `ForPodsWithAntiAffinity`; `presenceMask`; differential vs legacy `countDomains` maps | none |
| 5 | `feat(scheduling): overlay TopologyGroup and shadow harness` | dense/sparse groups, Record/Register/hostLog, Get/min on masks, rotation; `shadowTopology`; CI job with `KARPENTER_TOPOLOGY_SHADOW=1`; `FuzzTopologyShadow` | none (shadow only) |
| 6 | `perf(scheduling): per-pod topology plan` | `BeginPod`, plan entries, candidate-independent matching, lazy Requirement / no validDomains; shadow stays green | none |
| 7 | `perf(scheduling): seal topology after Solve` | `seal()` and post-seal guards; lazy errors from overlay | none |
| 8 | `perf(scheduling): use overlay topology by default` | option `--topology-engine=overlay\|legacy` (default overlay), KWOK `--topology-shadow-sample`; KWOK config T rerun + profiles | tie-break distribution only (decision 3) |
| 9 | `perf(state): L2 CountSource` (after the L2 PRs) | adapter from `TopologyIndex.Snapshot()`; `NewScheduler` passes it; shadow compares L2-backed vs list-backed sources in CI | freshness only (3.5) |
| 10 | `chore(scheduling): remove legacy topology` | after one or two releases of clean shadow metrics | none |

Follow-up fixes, each its own PR after PR 8 (section 11).

---


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

### 2–5. Fuzz, envtest, KWOK, large-cluster data collection
See [Appendix C](topology-from-cluster-state/rollout-validation-alternatives.md#validation-plan), which has the full plan and the read-only collector/analyzer scripts.

## Open Questions

Merged from the section drafts. Deduplicated, and numbered for review.

1. ~~**What feeds L2?**~~ Decided: informer event handlers. Reconcile lag would show up as false mismatches in shadow mode. Event handlers remove the lag but need a test hydration helper (`ExpectTopologyStateHydrated`). *Recommendation: event handlers.* (L2 §8.2)
2. **Can `Register` scans run outside `ix.mu`?** Running them on a snapshot plus a catch-up log would remove the up-to-6.4 ms writer stall for selectors that span all namespaces. (L2 Q2)
3. **Should domain presence move into L2?** That would mean per-(key, filter) node refcounts. (L2 Q3; L1-L3 §4.1 argues it should stay in L3)
4. **`MaxGroups`/TTL defaults.** Needs real data on distinct groups per cluster; matchLabelKeys with `pod-template-hash` creates one group per ReplicaSet revision. (L2 Q4)
5. **Label-set redundancy and node-shape ratios on real large clusters.** The collector script is in "Validation plan §5". (L2 Q5)
6. **Who resolves namespaceSelector: L3 per Topology, or L2?** (L1-L3 Q3)
7. **Dense/sparse threshold (4096) for custom keys.** (L1-L3 Q2)
8. ~~**Maintain L2 when the gate is off?**~~ Decided: no; the gate is a full kill switch. Doing so lets the gate flip instantly, at the cost of the write path. (L2 Q8)
9. **Default shadow sample rate in perf environments.** (L1-L3 Q4)
10. **Plan invalidation guard and the `seal` vs Validation reuse contract.** (L1-L3 Q5, Q6)
11. **Remaining non-topology `NewScheduler` cost.** `lo.FilterMap` → `filterInstanceTypesByRequirements`, daemon overhead groups, `NewReservationManager`, and `GetInstanceTypes` per NodePool together cost about 2.5× the remaining `NewTopology`. *Recommendation: a sibling RFC, "per-pass scheduling snapshot", plus three small fixes that need no RFC.* (rollout section, "Remaining NewScheduler costs")

## Implementation Notes (in progress)

Implementation is under way as two stacks of PRs on `topo/integration`. L3 is stack #38: #29 → #30 → #31 → #32 → #36. L2 is stack #39: #27 → #28 → #33 → #34 → #35 → #37.

Deviations from the appendices so far:
- **L2-D1, split feeds.** Forward facts (pods, nodes and namespaces as `countDomains` sees them) come from informer event handlers. The inverse anti-affinity view comes from cluster-state writes (`setBinding`/`deleteBinding`, `updatePodAntiAffinities`, `UpdateNode`/`cleanupNode`), because `ForPodsWithAntiAffinity` reads cluster state, not the cache.
- **L2-D2, synchronous first-use registration.** On a miss, the adapter registers the group, scans, publishes and uses that snapshot's counts. Each group pins its own snapshot, so groups in one Solve may come from different generations. That is the same as today's per-group `List`.
- **L2-D3, storage.** Facts are kept in sharded copy-on-write maps keyed by UID or node name, with unsorted swap-remove postings. Group counts are `map[domain]int32`, frozen at publish. L2 has no `DomainDict`; the adapter converts to L3's dictionaries.
- **L2-D4, deferred.** The adaptive strip of per-pod-unique labels and the node-shape memo come after a heap profile.
- **Experiment, not merged.** A branch makes the legacy topology read from the index, for an early gate-on KWOK measurement. It will be dropped once the overlay consumes `CountSource`.

## Appendices
- [A. L1/L3 scheduler-side design](topology-from-cluster-state/L1-L3-scheduler-topology.md)
- [B. L2 cluster-state index design](topology-from-cluster-state/L2-cluster-state-index.md)
- [C. Rollout, validation, alternatives (full), data-collection scripts](topology-from-cluster-state/rollout-validation-alternatives.md)
- D. Research prototypes and benchmarks: summarized in Motivation/Evidence; prototypes are not part of this PR.
