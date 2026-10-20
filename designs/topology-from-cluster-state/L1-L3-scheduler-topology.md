# L1 and L3: scheduler-side topology (DomainUniverse, group identity, per-Solve overlay)

Scope: the rewrite of `pkg/controllers/provisioning/scheduling/{topology,topologygroup,topologydomaingroup,topologynodefilter,topologyindex}.go`
starting from `topo/integration` (core `1d4cec82`, all of #19 to #26 merged). The cluster-state index (L2) is
someone else's section. This section defines the interface L3 consumes from L2 ("Contract with L2"). It also
ships a list-based implementation of that interface, so all of L1/L3 can land and be differential-tested before
L2 exists.

All line numbers refer to branch `topo/integration` (1d4cec82) unless marked otherwise.

**Binding decisions applied here** (from BRIEF.md):
1. **Parity first.** The new path reproduces today's decisions exactly. That includes the NotIn-values-become-domains
   quirk, nil-selector Everything-for-counting vs Nothing-for-selects, inverse anti-affinity counting terminating
   pods, `Matches` dropping compatibilityOptions, groups created mid-Solve not seeing earlier Records, and Update
   errors failing open. Each one is listed as a separate follow-up fix (section 11).
2. No new Go dependencies. Bitsets are hand-rolled.
3. Tie-breaking uses a random start offset per Solve, seeded in tests.
4. Every PR is small and mergeable on its own, and the user reviews and merges them.

---

## 0. Contract with L2

L3 consumes a `CountSource`. L2 implements it with `TopologyIndex.Snapshot()`. L3 also ships
`listCountSource`, which is today's `countDomains` logic (topology.go:421-507) refactored to fill the same
structs. The fallback stays permanently. It is used by unit tests, when the index is disabled, and as the
reference inside the differential harness.

```go
// package scheduling (pkg/controllers/provisioning/scheduling); L2 imports these types, or they move to a
// small leaf package (pkg/scheduling/topology) to avoid an import cycle with pkg/controllers/state.

// CountKey is the projection of a group identity that determines its base counts. Groups that differ only in
// Type, MaxSkew, MinDomains or SeedTolerations share a CountKey, and therefore share counts.
type CountKey struct {
	TopologyKey string
	Namespaces  []string        // sorted, deduped; already resolved (namespaceSelector evaluated by L3, see 0.3)
	// ListSelector is EXACTLY what TopologyListOptions(ns, rawSelector).LabelSelector produces (topology.go:647-667):
	// nil rawSelector -> labels.Everything(); any invalid requirement -> labels.Nothing(). This is NOT the selector
	// used for Selects/Record (that one is LabelSelectorAsSelector, nil -> Nothing). Parity quirk Q2.
	ListSelector labels.Selector
	NodeFilter   *TopologyNodeFilter // nil == matches all; evaluated as Matches(node.Spec.Taints, NewLabelRequirements(node.Labels)) with NO compatibility options (Q5)
	hash         uint64              // = CountKey.Hash(); L2 map key. Equality is (hash, Equal()) to rule out collisions.
}

// DomainRef is a domain of CountKey.TopologyKey. ID is valid for dense keys (DomainDict); Name always.
type DomainRef struct { ID DomainID; Name string }

// BaseCounts is immutable once returned. It may be shared by any number of concurrent Solves.
type BaseCounts struct {
	Key   string
	Dict  *DomainDictView // dense keys: the dictionary prefix these IDs index into; nil for sparse (hostname) keys
	Dense []int32         // by DomainID, len <= Dict.Len(); pods counted per domain
	Nonzero Mask          // Dense[i] > 0
	Sparse map[string]int32 // sparse keys only: non-zero counts by domain name (hostname, incl. node-name fallback)
}

type CountSource interface {
	// Counts returns, for the snapshot this source is pinned to, the per-domain count of pods p with
	//   p.Namespace in key.Namespaces && key.ListSelector.Matches(p.Labels) && !IgnoredForTopology(p)
	//   && node(p.Spec.NodeName) exists in the Node informer view
	//   && key.NodeFilter matches that Node's labels/taints
	//   && domain = node.Labels[key.TopologyKey], or node.Name if TopologyKey == hostname and the label is absent;
	//      pods whose node lacks the key are skipped.
	// Excluded pods are NOT subtracted (L3 does it, see ExcludedContributions). Must be safe for concurrent callers
	// (two schedulers); may register the key for incremental maintenance as a side effect.
	Counts(ctx context.Context, key *CountKey) (*BaseCounts, error)

	// ExcludedContributions calls fn once for every excluded pod that Counts(key) counted, with the domain it was
	// counted in. L3 applies these as -1 deltas.
	ExcludedContributions(key *CountKey, excluded *ExcludedPods, fn func(DomainRef))

	// InverseAntiAffinityGroups yields one entry per distinct inverse identity among bound pods that have
	// REQUIRED pod anti-affinity (cluster.ForPodsWithAntiAffinity semantics, cluster.go:252-268: binding known,
	// node in cluster state with Node != nil; terminating pods INCLUDED (Q3); domain = Node.Labels[key], no
	// hostname fallback, pods whose node lacks the key own the group but contribute no count). Owners that are
	// in `excluded` are already removed, along with their counts. Groups whose only owners are excluded are
	// omitted, because today they never come into existence (topology.go:371-384).
	InverseAntiAffinityGroups(ctx context.Context, excluded *ExcludedPods, fn func(id *TopologyGroupIdentity, counts *BaseCounts)) error

	// Generation identifies the snapshot (used for Validation's SnapshotAtLeast and for test assertions).
	Generation() uint64
}

// ExcludedPods is built once per Topology from the `pods` argument of NewTopology (topology.go:114-116).
type ExcludedPods struct {
	UIDs sets.Set[types.UID]
	// resolved by the source on first use: only excluded pods that are countable in this snapshot
	// (bound, not terminal/terminating, node known). For provisioning most excluded pods are pending and drop
	// out here in O(1) each, so per-group work is O(countable excluded) rather than O(pending pods).
	countable atomic.Pointer[[]PodRef]
}
```

### 0.1 What L2 must reproduce exactly (parity checklist for the L2 designer)

| # | today | location |
|---|---|---|
| a | count = matches of the *list* selector (nil ⇒ all pods in ns) | topology.go:428, 647-667 |
| b | pods skipped when `!IsScheduled \|\| IsTerminal \|\| IsTerminating` | topology.go:467, 685-687 |
| c | node looked up in the informer (`kubeClient.Get`), so a missing node means the pod is skipped. The node does **not** need to be in `stateNodes` (e.g. disruption candidates). | topology.go:474-485, 511-526 |
| d | hostname fallback to `node.Name` | topology.go:491-494 |
| e | node filter is evaluated on the Node object's labels and `Spec.Taints` | topology.go:501 |
| f | inverse groups: required anti-affinity only, terminating pods counted, `Node.Labels[key]` only | topology.go:371-415, cluster.go:252-268, 1006-1015 |
| g | `ListSelector` with `In` of zero values ⇒ Nothing (`labels.NewRequirement` error) | topology.go:660-663 |

### 0.2 What L3 does NOT need from L2
Domain presence, the universe, maxSkew/minDomains, owners, selection for Record. Presence depends on
`stateNodes`, and `stateNodes` differs per simulation (section 4.3). L2 does not need to know about it.

### 0.3 Namespace resolution
`buildNamespaceList` (topology.go:611-632) Lists namespaces through the kube client for every term that has a
`namespaceSelector`. In v1, L3 keeps doing this per Topology and memoizes it per (pod namespace, term
namespaces, selector string) within one Topology. If L2 ever indexes namespace labels, it can add
`Namespaces(sel labels.Selector) []string` to the source. That is an optional extension and not needed for
parity.

---

## 1. L1 DomainUniverse after #19/#25: what's left

### 1.1 State on topo/integration
- #19 dedupes taint groups (topologydomaingroup.go:35-52).
- #25 builds one `domainSource` per NodePool (topology.go:142-163). Domains are computed with
  `ValuesSeq`/`IntersectionValuesSeq`, without `Requirements.Add` (topology.go:169-203). They are built lazily per
  key on first read (topology.go:130-140).
- KWOK drift profile: `buildDomainGroups` went from 30.3 s to 0.9 s (`domainGroup`) per 120 s window
  (prs/01-taint-dedupe/comment.md). The research measured 19–443 µs per key uncached
  (research/universe-requirements §2).

### 1.2 Decision: no cross-Solve cache. The universe is per Topology, lazy per key
**Recommendation: do not cache L1 across Solves.**
- The remaining cost is under 1% of NewTopology.
- The cache key is unreliable because of hidden inputs:
  - **Placement-group NodeClasses** (AWS `offering.go:169-172`): when a PG is resolved, `InjectOfferings`
    builds a *new* `scheduling.Requirements` map for every instance type on every `GetInstanceTypes` call.
    The "it.Requirements identity" key therefore misses on every call. A memo would keep growing until it is
    bounded, and it would pin dead maps.
  - Reusing a pointer address after GC can produce a false hit with a `uintptr` key. A correct key needs a
    strong reference or `weak.Pointer`.
  - **NodeClass changes are not in `NodePool.Generation`.** Subnets change zones, and AMIs change the IT
    cache key (instancetype.go:254-270). Only it.Requirements identity reflects them.
  - **The NodeOverlay store can drop a NodePool for one call.** `ApplyAll` returns `UnevaluatedNodePoolError`
    until the store has evaluated the NodePool (nodeoverlay/store.go:61-66). `NewScheduler` then skips that
    NodePool (provisioner.go:313-319), so its domains vanish from this call's universe. That is parity-relevant:
    a whole-universe cache keyed on "the set of NodePools" must not reuse an entry built before the drop. A
    per-NodePool cache handles the drop correctly. A union cache does not.
- **Sharing across schedulers is not needed.** Provisioner and disruption each build their own universe for
  sub-millisecond cost. This avoids a concurrent shared structure.

**Alternative, if profiles ever show L1 again:** a per-NodePool, per-key memo keyed by
`(np.UID, np.Generation, hash(template labels, taints, requirements), weak.Pointer to each it.Requirements map)`.
Keep it as a requirement-class table (about 20 KB per key) with eviction at 2× the live size. Placement-group
NodePools bypass the memo. This is out of scope for this rewrite.

### 1.3 What L1 becomes: interned output
L1 keeps `domainSource`/`insertDomains` (topology.go:142-203) verbatim, including quirks Q1 (NotIn values become
domains) and "NodePool In values are inserted with zero ITs". Only the *output* changes. Instead of
`TopologyDomainGroup map[string][][]Taint`, each key produces:

```go
type DomainID uint16          // per key; dense keys only

// DomainDict is a process-wide, append-only interner per topology key (all keys except kubernetes.io/hostname).
// Shared by L1, L2 and L3. IDs are never reused or removed. Appends take a mutex; readers use a view.
type DomainDict struct {
	key   string
	mu    sync.Mutex
	ids   map[string]DomainID          // guarded by mu
	names atomic.Pointer[[]string]      // id -> name; republished on append (copy-on-append of the header only)
}
type DomainDictView struct{ names []string } // immutable prefix; Len() = len(names)

type keyUniverse struct {
	dict    *DomainDictView
	present Mask                  // all universe domains for the key
	taintSet []TSet               // by DomainID: interned taint-set IDs (ID 0 = empty set absorbs all, as Insert does)
	taintSets []scheduling.Taints // by taint-set ID
	// seed masks memoized per toleration signature (hashTolerations(tols)); built on first use
	seeds map[uint64]Mask
}
// SeedMask(policy, tolerations) == {d : ForEachDomain(pod{Tolerations: tolerations}, policy) would visit d}.
// For Ignore it is `present`. For Honor it costs one ToleratesPod per taint set, then one AND per domain.
func (u *keyUniverse) SeedMask(policy corev1.NodeInclusionPolicy, tolerations []corev1.Toleration) Mask
```

- **The global dict is the "interning domains to IDs per key" answer.** Dense keys (zone, zone-id,
  capacity-type, custom keys) have tiny, stable cardinality. One process-wide dict means L2's `BaseCounts.Dense`
  indexes the same ID space as L3's masks, with no translation.
- **Unbounded-cardinality guard.** A key becomes *sparse* (hostname-style: string-keyed, no presence bitset)
  when its dict passes 4096 entries. The mode is decided per key per Topology, from the dict length at
  Topology creation. L2 reads the same mode bit. Sparse mode is exact, just slower.
- **Alternative (rejected): per-Topology local dicts with an L2→L3 ID translation per group.** It costs O(#domains)
  per group, which is trivial for zones. It keeps L1 and L2 fully decoupled, but it doubles the bookkeeping, and
  hostname still needs a separate path. If the L2 designer prefers L2-private dicts, this is the fallback, and
  only `BaseCounts` changes.
- **Why the seed mask only depends on identity:** `populate` (topologygroup.go:143-172) uses the creator pod's
  tolerations for Honor groups. Since #22, those tolerations are hashed: spread groups through
  `nodeFilter.Tolerations` (topologynodefilter.go:89), affinity groups through `seedTolerations`
  (topologygroup.go:125-128, 251). Anti-affinity groups seed with Ignore (topologygroup.go:155-156). So
  `SeedMask(identity)` reproduces `ForEachDomain(creatorPod, …)` exactly, because `ToleratesPod` reads only
  `pod.Spec.Tolerations`.
- **Lifetime.** `keyUniverse` is per Topology, built lazily under the same "serial phase only" rule as today
  (topology.go:128-129). The process-wide dict outlives Topologies.

---

## 2. Group identity

```go
// TopologyGroupIdentity is everything that decides which pods share a group and how the group behaves.
// Immutable after construction; shared by L3 groups and (via CountKey) L2.
type TopologyGroupIdentity struct {
	Type        TopologyType
	Key         string
	Namespaces  []string            // sorted
	RawSelector *metav1.LabelSelector // canonical: matchLabelKeys merged as In requirements on a COPY (#21),
	                                   // expressions deduped by hash and sorted, values sorted
	MaxSkew     int32
	MinDomains  *int32
	NodeFilter  TopologyNodeFilter  // zero for (anti-)affinity
	SeedTolerations []corev1.Toleration // pod affinity only (#22)

	// derived once, at construction:
	hash      uint64           // == today's TopologyGroup.Hash() (topologygroup.go:234-254), bit for bit
	selector  labels.Selector  // LabelSelectorAsSelector(RawSelector), Nothing on error/nil (topologygroup.go:144-147)
	countKey  *CountKey        // ListSelector = TopologyListOptions(...).LabelSelector; NodeFilter = &NodeFilter or nil
	seedPolicy corev1.NodeInclusionPolicy // Ignore for anti, Honor for affinity, NodeFilter.TaintPolicy for spread
}
```

Rules:
- **The hash is unchanged.** Identity equality is today's `Hash()`, so groups merge exactly as they do now
  (parity). `newTopologyGroupIdentity` (topologygroup.go:98-139) becomes the constructor and also fills the
  derived fields. Costly derived fields (`selector`, `countKey`) are only built after the dedupe miss, as in #24.
- **Canonicalization changes how expressions are stored, not how they hash.** `hashSelector` already treats
  expressions as a set and values as a set (topologygroup.go:262-272). The canonical form keeps the set
  semantics, so `hash` is unchanged. A test asserts this: `hash(canonical(x)) == hash(x)` over the fuzz corpus.
- **minDomains** is in the identity and the hash (#22). L2 doesn't see it, because it is not in `CountKey`.
- **Node filter hash**: use `TopologyNodeFilter.Hash()` (topologynodefilter.go:73-91). `CountKey.Equal`
  compares `NodeSelectorRequirements()` per term plus the policies and the toleration set.
- **Collision policy:** L3 keeps today's behaviour, where hash equality means the same group. L2 must use
  `(hash, Equal)`, because two L3 identities with different hashes can share a CountKey.

---

## 3. L3 overlay data model

```go
type Topology struct {                          // replaces topology.go:47-78
	src        CountSource                      // pinned snapshot (L2) or listCountSource
	universe   map[string]*keyUniverse          // lazy per key (section 1.3)
	groups, inverse map[uint64]*TopologyGroup   // by identity hash
	index, inverseIndex *topologyGroupIndex     // #26, unchanged (topologyindex.go)
	excluded   *ExcludedPods
	stateNodes []*state.StateNode
	presence   map[presenceKey]*presenceMask    // section 4.3, memo per (key, node-filter hash)
	hostLog    []string                         // Register(hostname) log, section 4.4
	seed       uint64                           // per-Solve tie-break seed (section 6)
	plans      map[types.UID]*podPlan           // section 5; written serially, read in parallel rounds
	version    uint64                           // bumped by Record/Register/Update; plans check it
	sealed     bool
}

type TopologyGroup struct {                     // replaces topologygroup.go:56-76
	id      *TopologyGroupIdentity
	owners  map[types.UID]struct{}
	rot     int                                 // tie-break start ID = mix(seed, id.hash) % dict.Len()
	// exactly one of:
	dense   *denseCounts
	sparse  *sparseCounts
}

type denseCounts struct {
	dict    *DomainDictView   // dict prefix at group creation; extended locally (see "unknown domains")
	counts  []int32           // PRIVATE copy: base.Dense - excluded, then += Records
	present Mask              // registered domains (count 0 vs absent matters)
	nonzero Mask              // counts > 0; emptyDomains == present &^ nonzero (research/bitset-domains finding 1)
	local   map[string]DomainID // domains first seen in this Solve that aren't in dict (Record/Register of unknown values)
}

type sparseCounts struct {            // hostname and over-cardinality keys
	base     *BaseCounts             // shared, read-only (Sparse map)
	delta    map[string]int32        // -excluded and +Record; lazily allocated
	nNonzero int32                   // number of domains with base+delta > 0
	presence *presenceMask           // stateNodes passing filter at creation (shared, see 4.3)
	seed     Mask/[]string           // universe domains for this key (rare for hostname)
	hostFrom int                     // index into Topology.hostLog at creation
}
```

### 3.1 Why dense groups copy instead of using base+delta
For dense keys a full copy is cheaper than a delta layer: 6 zones take 38 ns and 40 B
(research/bitset-domains §5). Reads become a single load, and `nonzero` stays exact. The base
`BaseCounts` is never written. Hostname groups use base + sparse delta, because copying 15k entries costs
255 µs and 1.6 MB per group today.

### 3.2 Group creation (replaces `populate` + `countDomains`, topologygroup.go:143-172, topology.go:421-507)
For a forward group, at the first `Update` that sees its hash (topology.go:238-252):
1. `u := t.universe[key]`, then `seed := u.SeedMask(id.seedPolicy, tolerations)`.
2. `base, err := t.src.Counts(ctx, id.countKey)`. Errors propagate exactly as `countDomains` errors do today.
3. Copy the counts, then apply `src.ExcludedContributions(countKey, t.excluded, fn: counts[d]--)`.
4. `present = seed | t.stateNodePresence(key, nodeFilter) | nonzero(counts)`.
5. Set `rot`, and index the group (#26).

**Presence invariant (the subtle one).** Today a domain is present iff it is in (a) the seed, (b) a `stateNodes`
node passing the filter (topology.go:447-464), or (c) a node holding a counted, non-excluded pod
(topology.go:504, through `Record`). (c) uses the count *after* exclusion. A domain whose only pods are
excluded, on a node that is not in `stateNodes` (a disruption candidate, helpers.go:68-72), is **absent** today.
That changes min-skew and minDomains. So presence is computed from `nonzero(base - excluded)`, never from
`base.Nonzero`.

For inverse groups (topology.go:388-415): identity, seed (Ignore), and counts come from
`InverseAntiAffinityGroups`. `present = seed | nonzero`. There is **no** stateNode presence for inverse groups
today, and the new path keeps it that way. Pending pods that have required anti-affinity still create or own
inverse groups through `Update` (topology.go:225-230), with zero counts, as now.

### 3.3 Operations: exact mapping
Here `pod`, `node` and `nonzero` are masks over present domains. `node` comes from `Requirement.Has` per present
domain, or from iterating `In` values. `pod` comes from the plan.

| today | new |
|---|---|
| `Record(ds…)` topologygroup.go:189-194 | dense: id := lookupOrLocal(d); counts[id]++; set nonzero, present. sparse: delta[d]++, adjust nNonzero |
| `Register(ds…)` :203-210 | dense: lookupOrLocal and set present. hostname: `Topology.Register` appends to `hostLog` once (O(1), instead of O(groups) at topology.go:342-353) |
| `domainMinCount` :350-371 | hostname: 0. Otherwise the min over `present & pod`; `minDomains` uses `popcount(present & pod)` |
| spread :279-348 | hostname fast path unchanged (count = base+delta+self). Otherwise iterate `present & node` from `rot`, keep the domains with `c+self-min <= maxSkew`, and the first strict minimum wins |
| affinity :374-438 | hostname fast path: `nNonzero == 0` replaces `len(domains)==len(emptyDomains)`; `anyCompatiblePodDomain` iterates nonzero sparse entries. Otherwise `pod & nonzero & node`, then bootstrap with two picks from `rot`: first in `present & pod & node`, first in `present & pod` |
| anti :454-489 | hostname: `count(h)==0`. Otherwise `(present &^ nonzero) & pod & node` |
| `Unregister` :212-217 | kept with clear semantics; no callers (research/bitset-domains finding 4) |

`Requirement.Intersection(...).Has(d) ⇔ pod.Has(d) && node.Has(d)` for the affinity bootstrap
(topologygroup.go:421) was shown exact by the solve-hotpath differential (11,973 calls). The new differential
harness checks it again.

**Unknown domains.** A Record of a value that isn't in the dict (for example the value set of an anti-affinity
NodeClaim requirement, or quirk Q1 values) creates a local ID in `denseCounts.local`, in the serial phase only.
Today that domain becomes present (map insert), and the new path does the same. Get calls in the parallel round
never intern. A lookup miss means "absent", which matches today's map miss.

### 3.4 Concurrency
- **Parallel round** (`parallelizeUntil`, scheduler.go:664-701, 703-738, 740-835): only `AddRequirements` runs
  there (existingnode.go:164, nodeclaim.go:218), reading groups, plans, masks, `BaseCounts` and dict views.
  Nothing is written.
- **Serial phase**: `Update`, `Record`, `Register`, `BeginPod`, and group creation. Record and Register run in
  `Add` after `wg.Wait` (existingnode.go:182, nodeclaim.go:272-273). `NewNodeClaim` is built inside the round
  (scheduler.go:774) and never touches topology state. `NewExistingNode` Registers serially in `NewScheduler`
  (existingnode.go:77).
- **Shared across Solves**: `BaseCounts` (immutable) and `DomainDict`, where appends are serialized by a mutex
  and views are immutable prefixes. Two concurrent Solves never share an L3 object.
- The race detector runs the 8-worker round in the harness (section 9).

### 3.5 Groups created mid-Solve (relaxation, failed-pod re-Update)
Today `Update` runs from `trySchedule` (scheduler.go:585) and `Solve` (scheduler.go:521). A group created then
runs `countDomains` against cluster pods only, so it **does not see earlier Records in this Solve** or earlier
`Register(hostname)` calls. Hostname groups get presence only from `stateNodes`.

**Decision: preserve this (parity).** The overlay gets it for free: a new group's counts come from
`base - excluded` and nothing else, and sparse groups set `hostFrom = len(hostLog)` at creation.
- Making mid-Solve groups see prior placements (by replaying a Solve-level placement log) is follow-up F6.
  Arguably it is a correctness fix (a relaxed pod can violate skew against pods placed earlier in the same
  Solve), but it changes decisions.
- **One deliberate non-parity, in freshness only:** today a mid-Solve `countDomains` re-Lists the informer, so it
  can see pods that bound after the Solve started. The overlay reads the snapshot pinned at `NewTopology`. That
  is more consistent: it matches `stateNodes`, which is a snapshot too. It is unobservable in tests with a static
  informer. `listCountSource` also re-Lists, so the difference only exists with L2.

---

## 4. Presence from stateNodes, and why it is recomputed per Topology

### 4.1 Why not from L2
`stateNodes` differs per call:
- disruption removes candidates and keeps only `Active()` nodes (helpers.go:67-72);
- provisioning uses `DeepCopyNodes()` (provisioner.go:396).

Presence (count 0 vs absent) feeds `domainMinCount`'s `numPodSupportedDomains` (topologygroup.go:357-369) and
the candidate set in spread and anti-affinity. A cluster-wide presence from L2 would make a candidate's zone a
0-count domain when today it is absent. That flips minDomains decisions and global-min decisions for exactly the
disruption simulations this work targets.

### 4.2 Two count-0 vs absent effects to keep
1. A domain in `present` with count 0 lowers the global min to 0. An absent one doesn't.
2. Anti-affinity can only choose `present &^ nonzero`. An existing node whose zone is absent from the universe and
   from `stateNodes` presence (impossible for its own zone, unless its filter fails) cannot take the pod.

### 4.3 How (cheap)
`presenceMask` is keyed by `(topologyKey, nodeFilter.Hash())` and computed once per Topology by iterating
`stateNodes`, with the same shared `topologyNodeFilterMemo` (topologynodefilter.go:139-194). Groups sharing a
filter share the mask. Quirk kept: the **filter** is evaluated on `n.Node.Labels`/`n.Node.Spec.Taints`, but the
**domain** comes from `n.Labels()`, which is NodeClaim labels while unregistered (topology.go:438-463,
statenode.go:380-393). In-flight nodes (`Node == nil`) are skipped.
- Dense keys: `Mask` over DomainIDs.
- Hostname: a bitset over `stateNodes` indexes (15k bits = 2 KB), plus the list of hostname strings. It is only
  read by error messages and by the non-fast-path fallback.

Cost: O(|stateNodes| × distinct (key, filter)), not × groups. At 15k nodes and about 10 filters that is
roughly 1–3 ms per Topology, compared with 11.1 ms *per group* before #23.

### 4.4 Hostname presence is exact but lazy
On the hostname fast path presence is never read (research/bitset-domains finding 4; PR #20 shows that every
nodeDomains is `In[1]` or `In[]`). For the fallback path and for `topologyError`, a group's hostname presence is
materialized on demand as `seed ∪ presence(stateNodes) ∪ hostLog[hostFrom:] ∪ nonzero`. This is exact under the
parity rules in 3.5, and costs nothing unless it is read.

---

## 5. Per-pod plan and allocation cuts

```go
type podPlan struct {
	version  uint64                 // Topology.version at build; AddRequirements falls back to inline if stale
	podReqs  scheduling.Requirements // identity check: must be the StrictRequirements passed to AddRequirements
	entries  []planEntry            // owned groups (index.ownedBy) ∪ inverse groups selecting the pod
}
type planEntry struct {
	tg            *TopologyGroup
	podDomains    *scheduling.Requirement // podReqs.Get(key) or Exists (topology.go:287-290)
	podMask       Mask
	selfSelecting bool                    // tg.id.selector + namespaces (topologygroup.go:492-494)
	min           int32                   // domainMinCount(podDomains), candidate-independent
}
```

- **Built in `Scheduler.add`** (scheduler.go:627), serially, before `addToExistingNode`. This uses
  `t.BeginPod(p, s.cachedPodData[p.UID].StrictRequirements)`.
  - It stays valid for all three rounds, because a Record only happens when a round succeeds, and that ends
    `add`.
  - `Update` (relaxation) deletes the plan. The next `add` rebuilds it.
  - `GetTopologyZoneConstraints` (scheduler.go:912) uses `podData.Requirements`, not Strict, so it doesn't use
    the plan.
- **Candidate-independence of `getMatchingTopologies`** (topology.go:636-645): inverse groups have a zero node
  filter, so `tg.Counts(p, taints, reqs)` reduces to `selects(p)` (topologynodefilter.go:104-116 with empty
  policies). The whole matching set therefore moves into the plan. Per candidate, the work left is: compile
  `nodeMask` (O(present) `Has` calls, or iterate `In` values) → masked Get → build one `*Requirement`.
- **Allocation cuts in Get:**
  - drop `validDomains` (topologygroup.go:286), which is built on every call but only read by
    `GetTopologyZoneConstraints`;
  - `Get` returns a `Mask` or a chosen `DomainID`;
  - `AddRequirements` allocates exactly one `scheduling.NewRequirement` per group per successful candidate. It
    must stay fresh, because `Requirements.Add` stores the pointer (requirements.go:134) and callers later call
    `SetMinValues` (nodeclaim.go:229-233);
  - the hostname fast path reads `nodeDomains` once.
  - Expected: 40–110 ns, 0–1 allocs per Get, versus 339–8,967 ns and 3–6 allocs today
    (research/solve-hotpath §5).
- **Missing plan.** Unit tests call `AddRequirements` directly. They compute the plan inline, which is identical
  but slower. In tests, a debug assertion compares the inline plan with the cached plan.

---

## 6. Tie-breaking (binding: random start offset per Solve)

- `Topology.seed` comes from `rand.Uint64()` at `NewTopology`, or from option `TopologySeed(s)` in tests.
- Each group has `rot = splitmix(seed ^ id.hash) % dict.Len()`. A per-group rotation avoids every group preferring
  the same zone in a Solve.
- Scans go over IDs `rot..n-1, 0..rot-1`, and the first element wins (strict `<` for the spread min).
- Where today is random:
  - the spread `minDomain` among equal counts (map order at topologygroup.go:324, and also
    `nodeDomains.Values()` order at :309, which is set order);
  - the affinity bootstrap picks (:422-435).
- Where today is deterministic: everything else.
- **Distribution.** Today is fresh randomness per call. Now one rotation per Solve per group. Within one Solve,
  Records change the counts, so successive ties still rotate across domains. Across Solves the start varies. No
  zone-a bias.

---

## 7. topologyError and overlay lifetime

- **Lazy text.** `topologyError` (topology.go:207-216) keeps `{group, podDomains, nodeDomains}`. `Error()`
  renders counts from the group at call time, as today. The rendering is a `map[string]int32` over present
  domains, truncated to 25 by `pretty.Map`.
  - Today the map reflects the group's *final* state, because errors are stringified after Solve (Results.Record,
    disruption validation). The overlay keeps the same semantics.
  - For hostname groups, the message would list present zero-count hosts from the lazy presence (4.4). Parity of
    message text is not a goal, and the differential compares error-ness only.
- **Lifetime.**
  - `Results.NewNodeClaims` and `ExistingNodes` hold `*Topology` (nodeclaim.go:49, existingnode.go:38).
  - Disruption keeps them in `Command.Results` (disruption/types.go:250) through Validation
    (validation.go:302, which runs a fresh simulation) and execution queueing.
  - After `Solve` no topology method is called on them except `Error()`.
  - **Decision:** `Solve` ends with `t.seal()`. It drops `src` (unpins the snapshot), `plans`, `presence` memos,
    `universe`, and the excluded-pod list. It keeps the groups, which hold only private dense arrays and, for
    sparse groups, a pointer to an immutable `BaseCounts.Sparse`. That costs O(nonzero) and is safe to retain.
  - After seal, any mutating call panics in tests and is a no-op plus error log in production.
- **No refcount handshake with L2.** L2 GC should be TTL-on-last-`Counts()`, not refcounts. A refcount would need
  a release on every path, including `NewScheduler` errors after `NewTopology` (provisioner.go:350-377), and a
  missed release leaks forever. A TTL longer than the longest Solve plus Validation (for example 10 min) is
  leak-proof. If L2 still wants refcounts, `seal()` is the single release point.

---

## 8. Scheduler-facing API (unchanged signatures except two additions)

`NewTopology(...)` keeps its signature (topology.go:87-96). It gains options `WithCountSource(src)` and
`TopologySeed(s)`. The default is `listCountSource`, until L2 lands. Additions:
- `BeginPod(p, strictReqs)`, called from `Scheduler.add`;
- `seal()`, called from `Solve`.

These stay the same: `AddRequirements`, `Record`, `Register`, `Update`, `GetTopologyZoneConstraints`,
`TopologyListOptions`, and `IgnoredForTopology`. The last two are used by test expectations
(expectations.go:938-941).

---

## 9. Differential test harness

**Keep the current implementation as the reference.**
- Move topology.go, topologygroup.go and topologydomaingroup.go verbatim to `topology_legacy.go` as
  `legacyTopology`/`legacyTopologyGroup`. They stay compiled in non-test code, which the KWOK mode below needs.
- The only change to the legacy code is a **domain-order hook**. Each `for d := range t.domains` and
  `range nodeDomains.Values()` that can decide a tie (topologygroup.go:309, 324, 422, 430) iterates through
  `order(key, domains)`.
  - In production, `order` is identity: map order, unchanged behaviour.
  - In the harness, `order` sorts by the new implementation's rotated ID order for that group.
  - With both sides seeded the same, every decision must be **bit-identical**, so comparisons are exact
    equality.
  - This is preferable to "membership in tie set" checks, which need a second implementation of the tie logic.

**Facade.** `Topology` becomes a thin struct over `impl topologyImpl` (an interface with the 7 methods above).
`ExistingNode`/`NodeClaim` keep holding `*Topology`, so their constructors don't change.

**Shadow mode** (`shadowTopology`) forwards every call to `new` and `legacy`:
- **Mutations** (`Update`, `Record`, `Register`) go to both. `Update` errors are compared.
- **`AddRequirements`** runs on both. The harness compares that both error or both succeed, and on success
  compares `NodeSelectorRequirements()` for equality. It is concurrent-safe: both sides are read-only in the
  round, and mismatches go to a mutex-guarded collector.
- **`GetTopologyZoneConstraints`** compares sets and the bool.
- **Group-state check** after `NewTopology` and after each `Update`: for each hash, compare the counts map
  (present domains with counts) and the owners. The reference's `domains` map must equal the new
  `present`+`counts`, except hostname presence, which is compared through the lazy materialization (4.4).

**Where it runs.**
1. **Unit/envtest:** env `KARPENTER_TOPOLOGY_SHADOW=1` makes `NewTopology` return the shadow and fail the test on
   any mismatch. A CI job reruns `./pkg/controllers/provisioning/scheduling/` (about 440 specs),
   `./pkg/controllers/provisioning/`, and `./pkg/controllers/disruption/...` with it set.
2. **Fuzz:** `FuzzTopologyShadow` uses a controller-runtime fake client plus `state.Cluster`, with no envtest. It
   generates:
   - 1–4 NodePools with random In/NotIn/Exists/DNE/Gt/Lt requirements and taints, and fake ITs (reusing the
     #25 generator, topology_internal_test.go:145-226);
   - nodes across 1–8 zones, with some only as in-flight NodeClaims and some missing the hostname label;
   - bound, terminating, terminal and pending pods;
   - TSCs with random maxSkew, minDomains, node policies, matchLabelKeys, and nil/empty/invalid selectors;
   - required and preferred affinity and anti-affinity with namespaces and namespaceSelectors;
   - random excluded sets that include pods on nodes missing from `stateNodes`;
   - `Solve` over a pod list with preferences, so relaxation creates groups mid-Solve.
   It runs with `-race` and 8 workers. Mutation checks: hand-made mutants (present from `base.Nonzero` instead of
   after exclusion, presence from all nodes, mid-Solve replay, inverse without terminating, nil selector Nothing)
   must each be caught within the fuzz budget.
3. **KWOK debug mode:** a hidden flag `--topology-shadow-sample=<fraction>` (default 0) wraps a sample of
   `NewScheduler` calls in the shadow, with the legacy order hook in "observe" mode.
   - Exact compare isn't possible there, because the legacy side is randomized. Instead it records:
     - ties, as a pair "legacy pick ∈ new tie set" (computed by the new side, which can enumerate ties cheaply);
     - all non-tie decisions, exactly.
   - Mismatches go to the metric `karpenter_scheduler_topology_shadow_mismatches_total{kind}` and a V(1) log
     carrying the group identity and both results.
   - Cost: about 2× topology work on sampled calls.
4. **End-to-end:** with the hook aligned, the full `Results` of a legacy-backed and a new-backed scheduler on the
   same inputs must match. The comparison ignores hostname placeholder names, which come from the global
   counter at nodeclaim.go:99. The suite runs this as a smoke test over existing fixtures.

---

## 10. Implementation sequence (small, mergeable PRs from topo/integration)

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

## 11. Parity quirks preserved (each a separate follow-up)

| id | quirk | where |
|---|---|---|
| Q1 | NotIn values become universe domains | topology.go:178-199 (via `ValuesSeq` on complement) |
| Q2 | nil selector: Everything for counting, Nothing for selects/Record | topology.go:647-651 vs topologygroup.go:144-147 |
| Q3 | inverse groups count terminating pods; forward groups don't | cluster.go:1006-1015 vs topology.go:467 |
| Q4 | inverse domain uses Node labels without hostname fallback | topology.go:378, 409 |
| Q5 | `Matches` drops compatibilityOptions for affinity terms | topologynodefilter.go:107 |
| Q6 | groups created mid-Solve don't see prior Records/Registers | topology.go:241-246 |
| Q7 | `Update` errors are logged and ignored (constraints fail open) | scheduler.go:521, 585 |
| Q8 | presence filter on Node labels, domain from `StateNode.Labels()` | topology.go:442, 453-456 |

---

## 12. Open questions
1. **Dict ownership.** Should the process-wide `DomainDict` sit in a leaf package shared with L2 (recommended),
   or should L2 keep private dicts with an L3 translation? Section 1.3 has the alternative.
2. **Sparse-mode threshold.** Per-key sparse mode at 4096 domains: is that right for custom per-rack keys?
3. **Who resolves `namespaceSelector`.** Should resolution stay in L3 per Topology, or should L2 provide it so
   `buildNamespaceList` Lists stop?
4. **KWOK shadow cost.** Is a 1% default sample acceptable in perf environments?
5. **Plan invalidation.** The cached plan keys on the identity of `StrictRequirements`. Any new caller of
   `AddRequirements` with different pod requirements falls back to the inline path. Is a debug assertion enough?
6. **Seal versus Validation.** Should Validation ever reuse the original Topology? Today it re-simulates, so seal
   is safe. If that changes, seal breaks it.
