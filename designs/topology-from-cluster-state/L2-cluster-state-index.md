# L2: Cluster-state TopologyIndex (`pkg/controllers/state`)

Status: design section for the TopologyClusterState RFC. It builds on `topo/integration` (core commit `1d4cec82`,
all 8 open topology PRs merged). Code citations are `file:line` in
branch `topo/integration` (1d4cec82). `cluster.go` is identical to mainline (6eac93ba).

Binding constraints (from BRIEF "Decisions from the user"):
1. **Parity first.** The index must reproduce today's counts exactly, including every quirk. Each quirk is listed in
   §9 as a separate follow-up fix, and none of them is changed here.
2. **No new Go dependencies.** Postings are sorted `[]uint32`. Bitsets are hand-rolled. The COW vector is our own.
3. Tie-breaking (random start offset per Solve) belongs to L3. L2 only promises stable DomainIDs (§4.4).

---

## 1. What L2 replaces

After the 8 PRs, the remaining `NewTopology` cost in the drift KWOK profile is `countDomains`, which does an
informer `List` of pods per group: 14.1 s of 18.7 s in a 120 s window
(`prs/99-integration/comment.md`). Today, every `NewScheduler` (`provisioner.go:350`), and so every disruption
simulation (`disruption/helpers.go:128`) and every validation re-simulation (`disruption/validation.go:302`), does
the following:

| step | code | what L2 provides instead |
|---|---|---|
| list pods per namespace per new group, filtered by `TopologyListOptions` | `topology.go:427-432`, `647-667` | materialized per-group counts in an immutable snapshot (§5) |
| skip `IgnoredForTopology` and excluded pods | `topology.go:466-473`, `685-687` | pod-fact flags (§3.1); excluded pods are subtracted by L3 from per-pod facts (§5.4) |
| `Get` each pod's node from the kube cache, memoized per Topology | `topology.go:474-485`, `511-526` | node slots mirroring the kube cache (§3.2) |
| hostname fallback to node name | `topology.go:491-494` | node slot `hostDomain` (§3.2) |
| nodeFilter per node (memoized per signature by PR 05) | `topology.go:446`, `453`, `501`; `topologynodefilter.go:139-194` | per-shape filter memo held by each registered group (§3.3) |
| domain-presence loop over `stateNodes` | `topology.go:438-464` | **stays per Solve in L3**, using an L2 shape lookup (§7). It depends on the Solve's own `stateNodes`. |
| inverse anti-affinity: iterate `antiAffinityPods` under `c.mu.RLock` per pod, plus a namespace `List` per term | `topology.go:371-415`, `cluster.go:252-268` | incrementally maintained inverse-term counts (§3.5) |
| `buildNamespaceList` → `List` namespaces per term | `topology.go:611-632` | namespace facts in the snapshot (§3.4) |

## 2. Shape of the solution

```
 informer reconcilers ──► Cluster.UpdatePod/DeletePod/UpdateNode/DeleteNode/UpdateNodeClaim/... (hold c.mu.Lock)
                              │  (diff; no-op if nothing topology-relevant changed)
                              ▼
                     topoindex.Index  (writer state; guarded by ix.mu, nested inside c.mu)
                              │  lazy publish on demand (Snapshot()), COW chunks
                              ▼
            atomic.Pointer[Snapshot]  ◄── provisioner Solve / disruption Solves (lock-free, immutable)
```

Code lives in a new package, `pkg/controllers/state/topoindex`. `state.Cluster` owns one `*topoindex.Index` and
exposes it as `Cluster.TopologyIndex()`. The package imports only `k8s.io/*`, `sigs.k8s.io/karpenter/pkg/utils/pod`,
and the stdlib. It must not import `provisioning/scheduling`, because that package already imports `state`. So
anything scheduling-specific, such as the node filter, comes in through small interfaces (§5.1).

## 3. Data model

All IDs are dense `uint32` handed out from LIFO free lists. Snapshots hold *values*, never pointers into mutable
state, so reusing an ID is safe for any snapshot that is already published (`research/incremental-cow`, risks).

### 3.1 Pods

```go
type PodID uint32
type LabelSetID uint32 // interned (namespace, labels minus stripped keys); namespace is part of the set
type NodeID uint32
type ResidualID uint32 // 0 = none; values of stripped keys, one encoded string per pod

// PodFact is the published, pointer-free per-pod record: 16 B.
type PodFact struct {
	Set      LabelSetID
	Node     NodeID     // slot of pod.Spec.NodeName; NoNode if unbound
	Residual ResidualID
	Flags    PodFlags   // see below
}

type PodFlags uint32
const (
	FlagLive          PodFlags = 1 << iota // slot is in use
	FlagScheduled                          // Spec.NodeName != ""
	FlagTerminal                           // Phase Succeeded|Failed          (pod.IsTerminal, utils/pod/scheduling.go:147)
	FlagTerminating                        // DeletionTimestamp != nil        (pod.IsTerminating, :151)
	FlagReqAntiAffinity                    // podutils.HasRequiredPodAntiAffinity (mirrors cluster.go:1011)
	FlagBindingKnown                       // c.bindings has the key          (mirrors cluster.go:257)
)

// writer-private, never published
type podRec struct {
	id          PodID
	uid         types.UID
	rv          string                 // ResourceVersion fast path
	fact        PodFact
	bindingNode string                 // c.bindings value (inverse path uses this, not Spec.NodeName; cluster.go:257)
	invTerms    []InvTermID            // required anti-affinity terms (immutable on a pod)
}
```

**Forward-countable predicate**, exactly `!IgnoredForTopology` (`topology.go:685-687`):
`Scheduled && !Terminal && !Terminating`. **Inverse-countable predicate**, exactly what `ForPodsWithAntiAffinity`
yields (`cluster.go:252-268`): `ReqAntiAffinity && BindingKnown && nodeSlot(bindingNode).InState`. This includes
terminating pods (quirk Q1).

Every pod Karpenter reconciles gets a slot, including pending ones. A pending pod has no node and is not
forward-countable. A slot costs 16 B, and holding it keeps the `Synced` check trivial (§8.1). Terminal pods are kept
until `DeletePod`, because their `FlagReqAntiAffinity` entry mirrors `antiAffinityPods`, which keeps terminal pods
(Q8).

**Label-set interning.** We use the `research/labelset-interning` design: a refcounted table keyed by the canonical
`ns\0k\1v\0...` string with a dense id, and `m[string(buf)]` lookups that don't allocate. A set's
`labels.Set` is immutable once interned and is published as an append-only prefix slice.

**Adaptive strip of per-pod-unique keys.**
- Static strip list: `statefulset.kubernetes.io/pod-name`, `apps.kubernetes.io/pod-index`,
  `batch.kubernetes.io/job-completion-index`.
- `pod-template-hash`, `controller-revision-hash` and `controller-uid` are kept, because they are per-revision.
- Stripped values go into one encoded residual string per pod.
- Every 5 minutes, on the writer side, `SuggestStripKeys` flags any key whose distinct value count is more than half
  the live sets once there are more than 1000 sets. For that key the index then runs `Rebuild` (O(pods), about
  100 ms at 150k, under `ix.mu`, and rare).
- Without this, an operator that injects a per-pod label makes the index about 4x larger than a naive store
  (`labelset-interning` §3).
- Selectors that reference a stripped key are evaluated per pod (shared labels ∪ residual). This path is exact and
  covered by that prototype's differential test.

**Postings: sorted `[]uint32` (decision 2).**
- `setPods[LabelSetID] → sorted []PodID` holds only forward-countable pods, so terminating and pending pods never
  enter it (as in `selector-index`).
- `nodePods[NodeID] → sorted []PodID` holds all bound pods, and is used for node relabel and delete re-deltas.
- `nsSets[NSID] → []LabelSetID` holds the live sets per namespace, used for group registration and for testing new
  sets against groups.

Partitioning by label set keeps postings small: the median set is about 6 pods, and the worst case is a big
DaemonSet with about 15k pods. A sorted insert is a memmove of up to 60 KB, about 2 µs. That only happens for a pod
create in a 15k-pod set, so it is acceptable. Roaring would be about 7 MB smaller (4.6 vs 12.3 MB for the payload in
`selector-index`), but it is a new dependency and is ruled out. The 15k-node bitset variant needs 287 MB and is
rejected (`selector-index` memory table).

**UID lookup** (needed by L3 to subtract excluded pods; §5.4). This is a published, COW-sharded hash:
4096 buckets, each a sorted `[]struct{h uint64; id PodID}` keyed by maphash(UID). A publish clones only the buckets
it touched: about 17 buckets × about 0.5 KB per publish at 10k events/min. A colliding `h` on insert marks the bucket
ambiguous. L3 then treats a lookup in it as "unknown" and falls back to the List path for that Solve, with a counted
metric. The probability is about 1e-9 at 150k pods.

### 3.2 Nodes

The forward path reads nodes **from the kube cache, not from state** (`topology.go:515-525`, raw `node.Labels` and
`node.Spec.Taints`). The inverse path reads the **state** Node object (`cluster.go:261-266`). A slot carries both
views:

```go
type ShapeID uint32 // interned (labels minus per-node-unique keys, taint set)

type NodeSlot struct {   // published, value type
	Name       string
	Present    bool     // UpdateNode seen and DeleteNode not yet seen: mirrors the kube cache (forward path)
	KubeShape  ShapeID  // shape of the kube Node last passed to UpdateNode
	KubeUnique UniqueID // per-node-unique label values (hostname, instance-id, node name, ...), interned per node
	InState    bool     // c.nodes[c.nodeNameToProviderID[Name]] != nil && .Node != nil (inverse path)
	StateShape ShapeID  // shape of that state Node (it can lag KubeShape if newStateFromNode failed)
	StateUnique UniqueID
	RV         string   // kube Node ResourceVersion, used by L3 presence to reuse the shape (§7)
	pods       int32    // pods referencing this slot; the slot is freed when !Present && !InState && pods==0
}
```

- **Shapes** follow `labelset-interning` §1: 23 shapes for 1002 real KWOK nodes, and 478–6.6k for 15k synthetic
  nodes. A shape is `labels minus {hostname, instance-id, any key flagged unique}` plus a `TaintSetID`. Taint sets
  are interned the same way, compared by (key, value, effect), which is what `Taints.Tolerates` reads.
- **A slot is created on first sight, from either side.** A pod with `Spec.NodeName=x` whose node hasn't been
  observed yet gets a placeholder slot with `Present=false`, and its pods don't count. When `UpdateNode(x)` arrives,
  the slot flips to `Present`, and its pods are re-deltaed into every registered group. This reproduces
  `getNode`'s NotFound → skip (`topology.go:483-485`, Q4). It also covers the case where the node shows up later.
- **Per-key domain tables.** `domainOf[key] pvec[DomainID]` by NodeID, built on first registration of a key, at
  O(nodes) with a per-shape lookup. For `kubernetes.io/hostname` the domain is
  `labels[hostname]` if present, else `Name` (`topology.go:491-494`; Q3). This applies to the **forward** view only.
  The inverse view has no fallback (`topology.go:409`). A missing key gives `NoDomain`, and the pod is skipped
  (`topology.go:495-497`).

### 3.3 Node filter memo (per registered forward group)

`TopologyNodeFilter.Matches` depends only on node labels for the filter's keys, plus taints. PR 05 already memoizes
it per signature (`topologynodefilter.go:135-194`). In L2, a group stores `filterByShape []int8` (unknown, pass,
fail), filled at registration and extended lazily by the writer when it creates a new shape. If the filter
references a per-node-unique key, `filterByNode` is used instead. This replaces the O(nodes)
`NewLabelRequirements` per group per Solve that `selector-index` measured at 11.1 ms/group before PR 05.

### 3.4 Namespaces

```go
type NSID uint32
type NamespaceFact struct { Name string; Labels labels.Set /*immutable*/; Present bool }
```

These are fed by a new informer controller (§4.6). `Snapshot.SelectNamespaces(sel labels.Selector) []string`
reproduces `buildNamespaceList` (`topology.go:611-632`): selected names ∪ `term.Namespaces`, or the owner namespace
when both are empty. It reads `NamespaceFact.Labels` from this snapshot instead of a kube `List`.

### 3.5 Inverse anti-affinity terms

Today every `NewTopology` walks every required-anti-affinity pod under the lock, and resolves namespaces with a
`List` per term per pod (`topology.go:371-415`). L2 does it incrementally instead:

```go
type InvTermID uint32
type InvTermSpec struct {        // raw, unresolved identity of one required anti-affinity term
	TopologyKey       string
	OwnerNamespace    string
	Namespaces        []string            // sorted term.Namespaces
	NamespaceSelector string              // canonical; "" = nil
	LabelSelector     *metav1.LabelSelector // kept for L3 to build the TopologyGroup; hashed with hashSelector rules
}
type InvTermState struct {
	Spec   InvTermSpec
	Counts DomainCounts // inverse-countable owners, by state-node domain (no hostname fallback)
	Owners []PodID      // sorted, so L3 can subtract excluded owners
}
```

Within a Solve, L3 resolves each term's namespaces from the snapshot. It builds the `TopologyGroup` identity with
`newTopologyGroupIdentity(TopologyTypePodAntiAffinity, ...)` (`topology.go:399`) and **sums** terms whose `Hash()`
collides. That matches today, where two pods with different raw terms that resolve to the same group share one
group (`topology.go:401-408`). The cost is O(distinct terms), not O(anti-affinity pods).

### 3.6 Registered (forward) count groups

```go
type GroupKey string // canonical: key \0 sorted namespaces \0 listSelector.String() \0 hex(nodeFilter.Hash())
type groupState struct {
	spec      GroupSpec
	sel       labels.Selector   // TopologyListOptions semantics (nil -> Everything, invalid -> Nothing)
	matchSets []LabelSetID      // sorted; sets in spec.Namespaces matched by sel
	refinePods bool             // sel references a stripped key -> evaluate per pod
	filter    filterMemo        // §3.3
	counts    DomainCounts      // COW; see below
	lastUsed  atomic.Int64      // unix nanos; shared between writer and snapshots
}
type DomainCounts struct {
	dense  []int32            // by DomainID, for non-hostname keys (6 zones = 24 B; cloned whole on change)
	sparse *pmap[NodeID,int32] // hostname: non-zero only, keyed by NodeID; L3 maps NodeID -> hostDomain string
}
```

Counts are **only the pod contribution**: domain presence (count 0 vs absent) is computed per Solve (§7), because it
depends on the Solve's `stateNodes`. `maxSkew`, `minDomains`, the topology type and `seedTolerations` don't change
counts. So many `TopologyGroup`s (`topologygroup.go:234-254`) share one L2 count group, and the key is deliberately
coarser.

For hostname, counts are keyed by NodeID. If two slots share a `hostDomain` string (duplicate hostname labels), L3
sums them when reading. That matches today's string-keyed `tg.domains` (`topologygroup.go:189-194`).

**Reverse index for maintenance.** `setGroups[LabelSetID] → []GroupID` for the forward path. When a new label set is
interned, it is tested only against the groups registered for its namespace (`nsGroups[NSID]`), which costs about
0.7 µs (`labelset-interning` §2). A dead set is removed from its groups' `matchSets`.

### 3.7 Memory budget at 150k pods / 15k nodes / 500 namespaces / 500 groups

| component | estimate | source |
|---|---|---|
| `podRec` + key map (writer-private, `map[NamespacedName]*podRec`) | ≈ 100 B/pod → 15 MB | Go map overhead + rv string |
| `PodFact` chunked pvec | 16 B/pod → 2.4 MB (+ spine ≈ 5 KB) | `incremental-cow` b2: 34 B/pod retained incl. spine/slack |
| UID buckets | 12 B/pod → 1.8 MB | §3.1 |
| label sets (7.6k sets × ≈1 KB) + residuals (≈27 B/pod for STS / indexed pods) | 8 + 4 MB | `labelset-interning` §3 |
| `setPods` + `nodePods` sorted postings | 2 × 4 B/pod + headers → ≈ 2.5 MB | |
| nodes: slots 15k × ≈96 B, shapes (500–6.6k × ≈1 KB), unique labels (≈200 B/node), domain tables (4 B × nodes × keys) | 1.4 + 0.5–6.6 + 3 + 0.3 MB | `labelset-interning` node side 7.6–21 MB incl. maps |
| groups: 500 × (matchSets + filter memo + counts; hostname sparse ≈ 16 B × non-zero) | ≈ 3 MB | |
| inverse terms | < 1 MB unless anti-affinity pods carry unique terms | |
| **steady total** | **≈ 45–60 MB** (300–400 B/pod) | |
| churn garbage (10k events/min, lazy publish 5.5/s) | ≈ 55 MB/min alloc, ≈ 3 GC/min | `incremental-cow` soak |

For comparison, the controller-runtime pod cache that Karpenter already keeps holds full pod objects, which is
several KB each and so roughly 0.5–1.5 GB at 150k pods. L2 therefore adds about 5–10% to that.

## 4. Write path

### 4.1 Hooks in `cluster.go`

All hooks run where `c.mu` is already held for writing, and none of them adds an API call.

| hook | where | index call | why |
|---|---|---|---|
| `UpdatePod` | `cluster.go:451-463`: insert just before `return err` (`:462`), after `updatePodAntiAffinities` | `ix.ObservePod(pod, c.bindings[key])` | Must run even when `updateNodeUsageFromPod` returns NotFound (`:959-962`), because the forward path counts pods on nodes that state doesn't track. It reads the binding after it has been updated, for the inverse flags. Terminal pods are observed with `FlagTerminal` (`:456-457`). |
| `DeletePod` | `cluster.go:615-623` | `ix.DeletePod(key)` | mirrors `antiAffinityPods.Delete` (`:619`) and the binding delete |
| bindings | every write to `c.bindings`: `:946` (`populateResourceRequests`), `:968` (`updateNodeUsageFromPod`), `:979` (`updateNodeUsageFromPodCompletion`), `:999` (`cleanupOldBindings`), `:686` (`Reset`) | refactor into `c.setBinding(key, node)` / `c.deleteBinding(key)`, which call `ix.SetBinding(key, node, ok)` | `FlagBindingKnown` and `bindingNode` must mirror `c.bindings` exactly for inverse parity. `populateResourceRequests` binds pods that may not have been observed yet; the writer keeps a binding-only `podRec` until the fact arrives. |
| `UpdateNode` | `cluster.go:416-442`: **first statement after the lock (`:419`)**, before the early returns at `:422-433` and before `:427` mutates `Spec.ProviderID` | `ix.ObserveNode(node)` (`Present`, `KubeShape`, `RV`) | The kube cache has managed nodes with an empty providerID, and uninitialized nodes without an instance-type label. `countDomains` sees them through `Get`. So does an UpdateNode whose `newStateFromNode` fails (`:434-436`). |
| `UpdateNode` (tail) | after `:438-439` | `c.syncTopologyNodeInState(node.Name)` | `InState`/`StateShape` change only when `c.nodes` / `nodeNameToProviderID` change |
| `DeleteNode` | `cluster.go:444-449` | `ix.DeleteNode(name)` (`Present=false`), then `c.syncTopologyNodeInState(name)` | NotFound from the node reconciler (`informer/node.go:57-61`) is the kube-cache delete signal |
| `cleanupNode` | `cluster.go:827-840` | `c.syncTopologyNodeInState(name)` | Also reached from `newStateFromNode` on a providerID change (`:819-821`). The tail of UpdateNode restores `InState`. |
| `UpdateNodeClaim`, `DeleteNodeClaim` / `cleanupNodeClaim` | `cluster.go:383-414`, `:772-791` | `c.syncTopologyNodeInState(n.Node.Name)` for the affected state node when it has a Node | `cleanupNodeClaim` can `delete(c.nodes, id)` (`:774-776`). Mostly a no-op because a Node is kept, but cheap and robust. |
| `populateResourceRequests` | `cluster.go:932-949` | bindings only (row above) | No pod facts are taken from this `List`, because they would be stale relative to the pod reconciler. The pod's own reconcile is the source of facts. |
| `Reset` | `cluster.go:672-694` | `ix.Reset()` | tests; see §8.4 |
| `UpdateNamespace` / `DeleteNamespace` (new) | new methods on `Cluster` | `ix.ObserveNamespace` / `ix.DeleteNamespace` | §4.6 |

`syncTopologyNodeInState(name)` computes the inverse-view state with two map lookups, as
`c.nodes[c.nodeNameToProviderID[name]]` with `Node != nil`, and calls `ix.SetNodeInState(name, node /*or nil*/)`.
Recomputing at the end of each mutating method is less fragile than reasoning about each branch.

### 4.2 Diffing: the one-minute requeues are no-ops

Every pod, node and nodeclaim requeues every minute (`informer/pod.go:41`, `:78`; `node.go:68`; `nodeclaim.go:83`).
Pods also requeue on NotFound (`pod.go:73-75`). At 150k pods that is about 2,500 events/s.

1. **Fast path:** `rec.rv == pod.ResourceVersion` and the binding is unchanged gives an immediate return
   (≈ 104 ns, 0 alloc, `incremental-cow` §3).
2. **Otherwise**, compare the topology-relevant fields: namespace, labels (`maps.Equal` against the interned set plus
   residual), `NodeName`, phase terminality, `DeletionTimestamp != nil`, required-anti-affinity presence, and
   binding. A status-only update costs about 430 ns.
3. **Only a real diff** writes the fact, applies group deltas, sets `dirty`, and bumps `gen`.

Node diffs use the same approach: RV, then shape and unique-label compare, then taint compare (≈ 357 ns).
**Generation bumps only on real diffs.** The cost of requeues is about 0.03% of one core.

### 4.3 Group maintenance per event (writer, under `ix.mu`)

A pod fact change is a Z-set delta `{old: −1, new: +1}` (`incremental-cow` §4):
- forward: for each `g ∈ setGroups[old.Set] ∪ setGroups[new.Set]`, add `−contrib(g, old) + contrib(g, new)`.
  `contrib(g, f)` = the domain of `f.Node` under `g.spec.TopologyKey` if f is forward-countable, the slot is
  `Present`, the filter memo passes, and the domain exists (`topology.go:466-504`).
- inverse: for each term in `rec.invTerms`, apply ±1 at `stateDomain(bindingNode, key)` under the inverse predicate.

A node event (Present, shape or unique labels change) re-deltas `nodePods[node]` for the groups whose key domain or
filter result changed. This is about 0.4 µs per registered filter, plus O(pods on the node).

Measured end to end: +150–350 ns/event on top of the fact write at 500 groups, about 0.1 ms/s at 167 real ev/s.

### 4.4 Domain dictionaries

`DomainDict` per topology key (except hostname) is **append-only and process-wide**, owned by L2 and shared with L1.
L1 interns its universe values through the same dict, so DomainIDs agree across layers and across Solves. It is
published as a prefix slice, so a Solve sees a frozen length. Growth is bounded by distinct label values ever seen,
which is tiny for zone, capacity-type and arch. When dead entries exceed 50% and 4096, the dict is compacted with an
ID remap at publish time. Old snapshots keep their old dict.

### 4.5 Locking

- **Keep the global `c.mu` as the single-writer lock.** All hooks already run under `c.mu.Lock()`, which serializes
  the up to 3000 concurrent pod reconcilers (`pod.go:85`).
- **Add `ix.mu sync.Mutex`, always nested inside `c.mu` (lock order `c.mu → ix.mu`).** It guards the writer state
  against `Snapshot()` callers, which do **not** take `c.mu`. Readers never touch `c.mu`, so a Solve never stalls
  informers.
- Holding `c.mu.RLock()` as the snapshot is rejected. `incremental-cow` §1 shows that a 200 ms hold blocked the
  writer for 191 ms and, because Go's RWMutex prefers writers, blocked the *other* scheduler as well.
- Cost under `c.mu`: about 0.1 µs (no-op) to about 2 µs (real diff + 500 groups) per event, against controller-runtime's
  own Get + DeepCopy per reconcile.
- Worst-case `ix.mu` hold:
  - a `Register` scan of an all-namespace selector, about 6.4 ms, which runs on the reader's flush (§5.2);
  - a strip `Rebuild`, about 100 ms, rare.

  Both can block one writer, which already holds `c.mu`. That is acceptable. A follow-up could run the scan on a
  snapshot without `ix.mu` and then catch up from a delta log.

### 4.6 Namespace informer

- Add `pkg/controllers/state/informer/namespace.go` (`state.namespace`), modeled on `node.go`:
  - on Get, call `cluster.UpdateNamespace(ns)`;
  - on NotFound, call `cluster.DeleteNamespace(name)`;
  - otherwise `RequeueAfter: stateRetryPeriod`.
- Register it in `pkg/controllers/controllers.go:134-139`.
- RBAC: namespaces `get/list/watch` is already granted (`kwok/charts/templates/clusterrole.yaml:39`), and
  `buildNamespaceList` already starts a namespace informer lazily. So there is no new watch cost and no chart change.
- Namespace label changes do not re-register anything. Groups are keyed by *resolved* namespaces, so the next Solve
  resolves to a different key and registers a new group, and the old one ages out through TTL (§5.3).

## 5. Read path and publishing

### 5.1 API consumed by L3

```go
package topoindex

// ---- lifecycle / writer (called only by state.Cluster, under c.mu) ----
func New(clk clock.Clock, opts ...Option) *Index
func (ix *Index) ObservePod(pod *corev1.Pod, bindingNode string, bindingKnown bool)
func (ix *Index) DeletePod(key types.NamespacedName)
func (ix *Index) SetBinding(key types.NamespacedName, node string, known bool)
func (ix *Index) ObserveNode(node *corev1.Node)
func (ix *Index) DeleteNode(name string)
func (ix *Index) SetNodeInState(name string, stateNode *corev1.Node) // nil = not in state
func (ix *Index) ObserveNamespace(ns *corev1.Namespace)
func (ix *Index) DeleteNamespace(name string)
func (ix *Index) Reset()

// ---- readers (lock-free fast path) ----
func (ix *Index) Synced(ctx context.Context, kube client.Reader) bool        // §8.1
func (ix *Index) Generation() uint64
func (ix *Index) Snapshot() *Snapshot                                        // flush-on-demand
func (ix *Index) SnapshotAtLeast(ctx context.Context, gen uint64) (*Snapshot, error)

// Register asks the index to maintain counts for spec from now on. It is asynchronous: the group is
// materialized on the next flush. It is idempotent, and it refreshes lastUsed.
func (ix *Index) Register(spec GroupSpec) GroupKey

// NodePredicate is implemented by L3 over scheduling.TopologyNodeFilter (kept immutable once passed in).
type NodePredicate interface {
	Hash() uint64
	Keys() []string                        // label keys read; per-node eval if any is node-unique
	ReadsTaints() bool
	Matches(labels map[string]string, taints []corev1.Taint) bool
}
type GroupSpec struct {
	TopologyKey string
	Namespaces  []string              // resolved, sorted
	Selector    *metav1.LabelSelector // raw; compiled with TopologyListOptions semantics (Q2, Q5)
	NodeFilter  NodePredicate         // nil = all nodes (affinity / anti-affinity)
}

type Snapshot struct{ /* immutable */ }
func (s *Snapshot) Gen() uint64
// Counts returns the materialized counts, or ok=false if the group isn't registered at this generation.
func (s *Snapshot) Counts(k GroupKey) (DomainCountsView, bool)
// CountOnDemand computes the same counts by scanning this snapshot (5 µs typical, 229 µs hot ns, 6.4 ms all-ns).
func (s *Snapshot) CountOnDemand(spec GroupSpec) DomainCountsView
// PodFact/Contribution let L3 subtract excluded pods (UIDs from NewTopology's pods, topology.go:114-116).
func (s *Snapshot) PodByUID(uid types.UID) (PodID, PodFact, bool)
func (s *Snapshot) Contribution(spec GroupSpec, f PodFact) (domain string, ok bool)
// Inverse anti-affinity terms with owner-domain counts (§3.5).
func (s *Snapshot) InverseTerms() iter.Seq[*InvTermState]
// Namespaces / nodes
func (s *Snapshot) SelectNamespaces(sel labels.Selector) iter.Seq[string]
func (s *Snapshot) NodeShape(name, rv string) (ShapeID, bool) // §7 presence fast path
func (s *Snapshot) FilterByShape(p NodePredicate, id ShapeID) bool

type DomainCountsView interface {
	Get(domain string) int32
	ForEachNonZero(func(domain string, n int32))
	Dense() (dict *DomainDict, counts []int32) // nil for hostname
}
```

**How L3 uses it, per Solve:**
1. `snap := ix.Snapshot()`, taken where `countDomains` reads the cache today: at `NewTopology`
   (`provisioner.go:350`), *after* `GetPendingPods`. This preserves today's ordering (§7).
2. For each new group in `Topology.Update` (`topology.go:241-247`):
   - if `snap.Counts(key)` is ok, use it;
   - else call `CountOnDemand(spec)` against the **same** snapshot and then `ix.Register(spec)`.

   Every count in a Solve therefore comes from one generation, and a Solve never waits on registration. Today each
   group's `List` happens at a different instant, so this is strictly more consistent.
3. Subtract excluded pods through `PodByUID` + `Contribution` into the L3 delta (the −1 Z-set).

### 5.2 Publication

This is the `research/incremental-cow` b2 design:
- The writer state is a chunked COW vector: 256-entry chunks, each tagged with the generation of the publish that
  last froze it. A write clones a chunk only if that chunk is shared with a published snapshot.
- The same applies to `setPods`, `nodePods` and per-group counts. Each is its own small COW unit.
- The spine copy is O(N/256), about 600 pointers at 150k pods.

```go
type Index struct {
	mu      sync.Mutex             // nested inside Cluster.mu for writers
	gen     uint64                 // bumped per real diff (guarded by mu); mirrored in genA for lock-free reads
	genA    atomic.Uint64
	pubGen  uint64
	pub     atomic.Pointer[Snapshot]
	pending []GroupSpec            // Register requests awaiting materialization
	...writer state...
}
func (ix *Index) Snapshot() *Snapshot {
	if s := ix.pub.Load(); s.gen == ix.genA.Load() && len(ix.pendingA()) == 0 { return s } // ≈2 ns
	ix.mu.Lock(); defer ix.mu.Unlock()
	ix.materializePendingLocked() // Register scans
	ix.gcGroupsLocked()           // §5.3
	return ix.publishLocked()     // freeze chunks, spine copy, atomic store; a few µs
}
func (ix *Index) SnapshotAtLeast(ctx context.Context, gen uint64) (*Snapshot, error) {
	// Since Snapshot() flushes synchronously, any gen a caller previously observed is satisfied immediately.
	// The wait loop (condition variable on publish) exists for a future async publisher and for tests.
}
```

- **Lazy publish:** publishing costs nothing when no Solve asks. There is no backstop ticker, because flush on
  demand means a snapshot is never staler than the request. The real staleness bound is reconcile lag (§8.2).
- **Generation:** `Snapshot.Gen()` is logged with each scheduling decision. Disruption records it on the `Command`.
  Validation (`validation.go:297-302`) calls `SnapshotAtLeast(cmd.topologyGen)`, which guarantees that a
  re-simulation never sees an *older* topology than the decision did. This is the Nomad `SnapshotMinIndex` contract.
  Optional follow-up, not part of parity: if `Gen` and the candidate nodes are unchanged, validation could skip
  re-simulating topology.

### 5.3 Group lifetime: TTL + LRU cap, no refcount

- A snapshot's groups are immutable values. A Solve holding an old snapshot keeps its count arrays alive through
  the Go GC, even if the index drops the group. So refcounts are **not needed for safety**.
- A refcount would also need a reliable `Release` on every Solve exit path, including panics and cancelled contexts,
  which is fragile.
- Instead, each `Counts()` / `Register()` stores `now` into the group's atomic `lastUsed`. At each flush:
  - drop groups unused for `TTL` (default 10 min, longer than the 5-minute `ConsolidationState` cycle,
    `cluster.go:655`);
  - enforce `MaxGroups` (default 5000, LRU) to bound memory and per-event maintenance.
- Dropping a group removes it from `setGroups` / `nsGroups` and frees its memo (closes the memo-pruning item in
  `incremental-cow` risks).
- A dropped group that is needed again pays one on-demand scan and one registration.

## 6. Semantics decisions (parity-first; each quirk becomes a follow-up, §9)

| # | case | today | L2 behaviour (parity) | alternative (follow-up) and impact |
|---|---|---|---|---|
| S1 | terminating pods | forward skips them (`topology.go:467`, `:686`). Inverse **counts** them: `antiAffinityPods` is filtered only by binding and state node (`cluster.go:252-268`), and the binding survives until terminal/delete (`:972-986`). | Forward predicate excludes `FlagTerminating`. Inverse predicate ignores it. | Unify on kube-scheduler behaviour (it counts terminating pods for anti-affinity, skips them for spread). Impact: changes inverse blocking during rollouts. |
| S2 | nil selector | Forward seeds counts with **Everything**: `TopologyListOptions` (`topology.go:648-651`). `TopologyGroup.selects` uses `LabelSelectorAsSelector(nil)` = **Nothing** (`topologygroup.go:144-147`, `:492-494`), so newly placed pods are never Recorded. | L2 compiles `GroupSpec.Selector` with the `TopologyListOptions` function. That function moves to a shared helper (`pkg/scheduling/topologyselector.go`) used by both the List fallback and L2. L3 keeps `selects` = Nothing. | Pick Nothing (kube-scheduler semantics). Impact: a TSC without a selector stops counting existing pods. |
| S3 | invalid selector | `TopologyListOptions` → Nothing on any `NewRequirement` error (`:653-656`, `:660-663`, unknown operator via `mapOperator` → `""`, `:669-683`). `populate` → Nothing (`topologygroup.go:144-147`). | Same function, so the same result. | none (consistent today) |
| S4 | pod's node unknown to the kube cache (NotFound) | skip (`topology.go:483-485`) | `Present=false` slot → not counted; counted once `UpdateNode` arrives | none |
| S5 | pod's node in the kube cache but not in state (empty providerID `cluster.go:422-426`, uninitialized without instance type `:431-433`, `newStateFromNode` error `:434-436`, pod `UpdatePod` NotFound requeue `:959-962`) | forward **counts** them (`Get` reads the cache); inverse does **not** (needs a binding and `c.nodes`) | `ObserveNode` runs before the early returns, and `ObservePod` runs regardless of the NotFound error. So forward counts them and inverse doesn't. | none |
| S6 | node in state but already gone from the kube cache | forward skips its pods (Get NotFound); presence still includes it (it is in `stateNodes`) | `Present` flips on `DeleteNode`, i.e. after reconcile lag rather than at cache time (§8.2). Presence is per Solve from `stateNodes` (§7), so it matches exactly. | — |
| S7 | hostname fallback | forward: missing hostname label → node name (`topology.go:491-494`). Inverse: no fallback (`:409`). | `hostDomain` in the forward view only | Use `StateNode.HostName()` semantics everywhere |
| S8 | leaked pods on deleted nodes | skipped (comment `topology.go:478-482`) | = S4 | — |
| S9 | presence label sources | domain from `StateNode.Labels()` (NodeClaim labels while unregistered, `statenode.go:380-396`). Filter uses raw `n.Node.Labels` / `n.Node.Spec.Taints` (`topology.go:453`), not `StateNode.Taints()`. Nodes with `Node == nil` are skipped. | L3 reproduces exactly (§7) | Use one source |
| S10 | presence only from the Solve's `stateNodes` (Active, and in disruption minus candidates: `helpers.go:68-71`; provisioner `nodes.Active()`), while pod counts include pods on MarkedForDeletion and candidate nodes that aren't excluded (DaemonSet pods, PDB-blocked pods) | — | Counts are L2-global. Presence is per Solve. | — |
| S11 | inverse domain source | `c.bindings` node name, state `Node.Labels` (`cluster.go:257-266`) | `bindingNode`, `StateShape` / `StateUnique` | — |
| S12 | excluded pods | skipped by UID in both paths (`topology.go:471`, `:375`) | L3 subtracts `Contribution` of its snapshot fact. A pod not in the index contributes nothing, as today. | — |

## 7. Consistency with `DeepCopyNodes` in the same Solve

Today the ordering is:

1. `DeepCopyNodes` (`provisioner.go:393`, `helpers.go:68`);
2. `GetPendingPods`;
3. `NewTopology`, which `List`s and `Get`s from the kube cache at each group's first use.

Counts and nodes are **not** mutually consistent today. Pods are counted against kube-cache nodes, not against
`stateNodes`.

- **Recommendation:** take `ix.Snapshot()` at `NewTopology` entry. Pod-side counts then reflect the moment the
  topology is built, as today, and all groups agree on one generation.
- **Domain presence** keeps using the Solve's own `stateNodes` (S9, S10), so presence and the nodes the Solve can
  place on agree exactly.
- **Presence fast path.** For each `stateNodes[i]` with `Node != nil`:
  - `domain := stateNodes[i].Labels()[key]`, a map lookup;
  - the filter result is `snap.NodeShape(Node.Name, Node.ResourceVersion)`, then `FilterByShape`;
  - if the deep copy's RV differs from the slot's (the node changed in between), evaluate the predicate directly on
    the copy.

  Cost: about 15k × 20–30 ns ≈ 0.3–0.45 ms per distinct `(key, filterHash)` per Solve, shared across groups through
  an L3 memo. It is skipped entirely for hostname, because presence is never read on the single-host fast path
  (`research/bitset-domains` finding 4).
- **Alternative (rejected for now): `Cluster.DeepCopyNodesAndTopology()`.** Under `c.mu.RLock` it would copy the
  nodes and call `ix.Snapshot()` (safe, because writers are excluded by `c.mu` and readers serialize on `ix.mu`),
  giving an exact nodes/counts cut. It would be stricter than today, but it moves the count instant earlier than
  `GetPendingPods`. A pod that binds in between would then be neither pending nor counted, and that undercount is
  worse than the current behaviour. We can revisit it once the pending-pod list also comes from state.

## 8. Sync, startup, failure modes

### 8.1 Hydration gate

`Cluster.Synced` checks only that NodeClaims and Nodes in the kube cache are a subset of state
(`cluster.go:199-246`), and after the first success it checks only providerIDs (`:185-197`). There is **no pod
check**, and the List path doesn't need one, because it reads the cache directly. The index does need one:

- `ix.Synced(ctx, kube)`:
  - until it latches, `List` pods and namespaces with `UnsafeDisableDeepCopy` (one 150k List, tens of ms);
  - require every pod key and namespace name to be present in the index, with the same rule as for nodes;
  - require `Cluster.Synced` to be true, which gives node slots.
- After the latch, it is a no-op, like `hasSynced`.
- It does **not** gate `Cluster.Synced` and does not delay provisioning. The scheduler falls back instead (§8.3).

### 8.2 Leader election, warm-up, relists, lag

- State reconcilers run only on the leader. Controller warmup (`operator.go:212-217`) pre-starts informers on
  standbys, so after failover the index fills at reconcile speed, and Solves use the List fallback until `Synced`.
- On losing the lease the process exits (`LeaderElectionReleaseOnCancel`, `operator.go:193`), so a stale index is
  never served.
- Relists and watch re-establishment surface as ordinary reconciles. A Get that returns NotFound leads to
  `DeletePod`/`DeleteNode`/`DeleteNamespace`. The one-minute requeue bounds divergence.
- **Reconcile lag** is the one intrinsic difference from the List path. `countDomains` reads the cache at the
  instant of the Solve. The index reflects the last *reconcile*, which can lag under heavy churn (workqueue depth).
  Exposing `karpenter_cluster_state_topology_index_lag_seconds` (cache event time → applied) makes this visible.
  Shadow mode (§10) measures how often the lag changes a count.
- **Alternative:** feed facts from cache event handlers (`mgr.GetCache().GetInformer(...).AddEventHandler`). This
  removes the lag, but unit tests use a direct, non-cached client (`pkg/test/environment.go:163-191`) and drive state
  through `cluster.UpdatePod` and the state controllers. The handler path would also need its own binding and state
  mirror for the inverse view. It is rejected for v1 and listed as an open question.

### 8.3 Scheduler behaviour when not synced or degraded

`NewTopology` takes a source: `ix.Synced() && gate on` uses the index; otherwise the current `countDomains`, which
is kept intact. The fallback also runs for a Solve that hits an ambiguous UID bucket (§3.1).
`karpenter_scheduler_topology_source_total{source="index|list", reason}` counts both.

### 8.4 Tests and `Reset`

- `Reset` (`cluster.go:672-694`) calls `ix.Reset()`: a new empty writer state, a published empty snapshot,
  `gen++`, and synced=false.
- `SetSynced` (`:697-701`) does not mark the index synced. Tests opt in with `cluster.TopologyIndex().SetSyncedForTest(true)`.
- **Hydration gap in tests:** many specs `ExpectApplied` bound pods without reconciling them into state. One example
  is `topology_test.go:416-447`: `ExpectProvisioned` applies the pods and calls `cluster.UpdatePod` only for result
  bindings, *after* `Schedule` (`expectations.go:328-353`). The List path sees those pods; the index wouldn't. So the
  index path requires:
  - a helper `ExpectTopologyStateHydrated(ctx, c, cluster)` that lists pods, nodes and namespaces from the API
    server and calls `UpdateNode` / `UpdatePod` / `UpdateNamespace`;
  - calling that helper from `ExpectProvisionedNoBinding` (`expectations.go:355`) before `provisioner.Schedule`, and
    from the disruption suite equivalents.

  This is the main test-infra cost of the migration.

## 9. Parity quirks to fix later (one follow-up PR each; not changed here)

- Q1: terminating pods are skipped by forward counting but counted by the inverse path (S1).
- Q2: a nil selector counts Everything when seeding and Nothing when recording (S2).
- Q3: the hostname fallback is forward-only (S7).
- Q4: forward counting reads kube-cache nodes, while inverse counting reads state nodes and bindings (S5, S11).
- Q5: presence mixes `StateNode.Labels()` with raw Node labels and taints (S9).
- Q6: presence excludes MarkedForDeletion and candidate nodes, while their non-excluded pods still count (S10).
- Q7: `TopologyNodeFilter.Hash` includes tolerations even under `TaintPolicy=Ignore`. This over-splits L2 groups,
  which is harmless for parity, but it is free to fix by normalizing.
- Q8: terminal pods remain in `antiAffinityPods` until delete (`cluster.go:456-461`, `1006-1016`), hidden only by
  the missing binding.

Not owned by L2 (cross-reference): L1's "NodePool `NotIn` value becomes a domain", and L3's `Update` errors failing
open (SYNTHESIS bugs list).

## 10. Migration: feature gate, shadow mode, differential reference

- **Gate:** `TopologyClusterState` (Alpha, default false) in `options.go:61-73`, `:163-174`. It adds an option
  `--topology-index-mode=list|shadow|index` (default `list`). The gate on implies `index` unless overridden.
  - `list`: the index is still maintained, which costs the write path only, so flipping the gate needs no warm-up.
    That can be turned off with `mode=off` to remove all cost.
  - `shadow`: build counts both ways in each Solve, **use the List result**, and compare per group (counts plus the
    union of presence). Mismatches increment `karpenter_cluster_state_topology_index_mismatch_total{kind}` and are
    logged once per group hash with `gen`, plus the pod and node RVs involved. Because of reconcile lag (§8.2), a
    mismatch is re-checked once after `SnapshotAtLeast(gen)` with the pods' current RVs before being counted.
  - `index`: use the index, and fall back to List when unsynced (§8.3).
- **The `countDomains` List path stays** as the fallback and as the reference implementation until the quirks in
  §9 are fixed. Each quirk fix lands in *both* paths and flips one parity row.
- **Unit-level differential:**
  - run the full scheduling + disruption suites with `mode=shadow-strict`, where a mismatch fails the test, and
    with `index`;
  - add a randomized differential test in `topoindex` against a naive map reference and against an emulated
    `countDomains` on a `cache.Indexer`. Reuse `research/selector-index/diff_test.go` and
    `research/incremental-cow` `TestTopoIndexDifferential`.
  - The differential covers pod churn, binding flips, terminating/terminal transitions, nodes appearing after their
    pods, node relabel, delete and Present-but-not-InState nodes, namespace relabel, strip rebuild, group TTL GC,
    and old snapshots held across publishes. It also runs under `-race` with two concurrent readers.
- **Rollout:**
  1. land the write path + `shadow`;
  2. run a KWOK soak (config T from `prs/99-integration`) with zero mismatches, apart from lag-attributed ones;
  3. switch to `index` in the KWOK perf suite;
  4. Beta.

## 11. Cost summary (expected; from research prototypes, 150k pods / 15k nodes)

| | today (post-PRs) | with L2 |
|---|---|---|
| per Solve, 500 groups, counts | ~118 ms–8.8 s depending on PR 05 memo hit rate (`selector-index` table) + List | warm: 500 × ~5 µs on-demand only for unregistered; registered = O(1) pointer read |
| per Solve presence | O(nodes) per group (memoized per signature by PR 05) | O(nodes) per distinct (key, filter), hostname skipped: ≈0.3–0.45 ms each |
| inverse anti-affinity | O(anti-affinity pods) + a namespace List per term per pod | O(distinct terms) |
| write path | — | 0.1 µs no-op requeue, ≈0.5–2 µs real event, +150–350 ns per event at 500 groups |
| memory | — | ≈45–60 MB + ≈55 MB/min garbage |

## 12. Open questions

1. Should facts be fed from cache event handlers to remove reconcile lag (§8.2)? That needs a test strategy for the
   non-cached test client.
2. Can `Register` scans run outside `ix.mu`, on a snapshot plus a catch-up delta log, to remove the 6.4 ms
   all-namespace writer stall?
3. Should presence also move into L2, as per-(key, filter) node refcounts plus L3 subtraction of non-Active and
   candidate nodes, to get O(excluded nodes) per Solve? This needs nodes and snapshot cut together (the §7
   alternative).
4. `MaxGroups` and TTL defaults: we need real data on the distinct groups per cluster (matchLabelKeys with
   `pod-template-hash` creates one group per ReplicaSet revision).
5. Label-set redundancy (assumed 10–50x) and node-shape ratios need a real large-cluster dump
   (SYNTHESIS "Gaps").
6. Should `Snapshot.Gen` let disruption validation skip re-simulation when nothing topology-relevant changed? This
   goes beyond parity, so it would be a separate RFC item.
7. Hostname counts are keyed by NodeID, and duplicate hostname labels are summed by L3. Is a published
   `hostDomain → []NodeID` index needed, or is an L3-side map per Solve enough? Only groups with non-zero entries
   need it.
8. Should the index be maintained in `list` mode by default, at the write-path cost, so the gate flip is instant? Or
   should it be off entirely until the gate is on?
