# Partitionable Devices — Implementation Notes

Running notes capturing scoping decisions, observations, and follow-ups for the Karpenter partitionable devices implementation.

---

## Relationship to Consumable Capacity

SharedCounters (KEP-4815) and Consumable Capacity (KEP-5075) operate at different abstraction levels but share infrastructure in the allocator:

| Aspect | SharedCounters | Consumable Capacity |
|--------|----------------|---------------------|
| Tracking granularity | Per-pool (counter sets are pool-level) | Per-device (capacity is device-level) |
| What varies per allocation | Nothing — fixed per device definition | Consumed amount varies per request |
| Controller needs | Only "which devices are allocated" | "Which devices" + "how much each consumed" |
| Backtracking cost | Deduct/restore fixed amounts | Deduct/restore computed amounts |
| Cross-NodeClaim sharing | Yes (in-cluster pool devices) | Yes (in-cluster multi-alloc devices) |

**Key implication:** Counter state reconstruction is cheaper than capacity state reconstruction because it uses static device definitions. No per-allocation metadata storage is needed — just the allocated device set.

---

## SharedCounters in Separate ResourceSlice — Implications for Karpenter

The API requires `SharedCounters` and `Devices` to be in **separate** ResourceSlices within the same pool. This affects Karpenter's pool gathering:

1. **ResourceSlice with only SharedCounters:** Has no devices, no node affinity. Contributes counter set definitions to the pool.
2. **ResourceSlice with Devices:** Has devices with `ConsumesCounters` referencing counter sets from (1).

Karpenter's pool assembly must handle slices that contribute counter sets but no devices. Today, a slice without devices is effectively ignored. The pool must now track counter-set-only slices as contributing to `pool.resourceSliceCount`.

**Template pool analogy:** Template pools also need counter sets. The cloud provider's `ResourceSliceTemplate` must carry both the counter set definitions and the device templates. Since templates are in-memory (not API objects), the "separate slice" constraint doesn't apply — counter sets and devices can coexist in the template structure.

---

## PerDeviceNodeSelection — Scope for Karpenter

Multi-host devices are the primary use case for `PerDeviceNodeSelection`. In Karpenter's model:

- **In-cluster multi-host devices:** These exist on already-running nodes. Karpenter would encounter them as in-cluster ResourceSlices with `PerDeviceNodeSelection: true`. The device's `NodeSelector` restricts which nodes can use it. For Karpenter's scheduling, this means the device's topology requirements must be intersected with the NodeClaim's candidate instance types.

- **Template multi-host devices:** Unlikely in practice. Template devices represent what a *single* instance will provide. Multi-host devices span multiple instances and are managed by cluster-level controllers, not pre-provisioned by the cloud provider.

**Decision:** Implement `PerDeviceNodeSelection` for in-cluster devices. Template support is deferred (no known use case).

---

## Counter Consumption vs. Counter Definition Values

An important asymmetry: devices declare counter VALUES they consume, while counter sets declare the TOTAL available. The consumed value can be zero for a counter:

```yaml
consumesCounters:
- counterSet: gpu-0-counters
  counters:
    encoders: { value: "0" }   # This MIG profile has 0 encoders
    decoders: { value: "1" }
```

Zero-valued counter consumption is valid and must NOT be treated as "doesn't consume this counter." It explicitly declares consumption of 0 from that counter — which always passes the budget check but documents that the device participates in the counter set's accounting.

**Decision:** Include zero-valued counters in tracking. Don't skip them or treat them as absent.

---

## Upstream Bug Relevance (PR #139040)

The `checkAvailableCounters` bug (missing multi-allocatable devices from counter deduction) is relevant when a device:
1. Has `ConsumesCounters` (participates in counter sets)
2. Has `AllowMultipleAllocations: true` (tracked via capacity, not binary set)

This combination means: "A device that is a PARTITION of a larger device AND can serve multiple consumers." Example: a MIG profile that also offers bandwidth sharing.

In Karpenter's design, we avoid this bug because counter deduction iterates both `ExclusiveDevices` AND `keys(ConsumedCapacity)` — covering all allocated devices regardless of their allocation mode.

---

## Pool Ordering — First Fit Implications

The upstream scheduler picks devices in ResourceSlice order (first fit, not best fit). For partitionable devices, this means the order devices appear in the ResourceSlice affects allocation quality:

- If the full GPU is listed first, it's always tried before partitions → full GPU allocated even when a partition would suffice
- Drivers are advised to list smallest-to-largest

**Impact on Karpenter:** Our DFS also iterates devices in slice order. We inherit the same first-fit behavior. The cloud provider (for template devices) should declare partitions smallest-to-largest. For in-cluster devices, we respect whatever order the driver published.

**Future:** Upstream KEP-4970 tracks scoring/ordering improvements. When that lands, we'll need to adapt our device iteration to match.

---

## Template Counter Sets — Open Design Question

How does the cloud provider declare counter sets for template devices?

**Option A: Inline on ResourceSliceTemplate**
```go
type ResourceSliceTemplate struct {
    Devices     []Device
    CounterSets []CounterSet  // NEW
}
```
Simple, matches the per-instance-type granularity. Each instance type template carries its own counter budget.

**Option B: Separate template structure**
Counter sets as a separate template field, referenced by pool name within the template. More flexible but complex.

**Recommendation:** Option A. Counter sets are inherently per-physical-device (per-GPU). Since template devices represent what one instance type provides, counter sets are naturally per-instance-type. The separation required by the API (counter sets in different ResourceSlice) is an API constraint, not a logical one — templates don't have that constraint.

---

## Validation Timing

Pool validation catches driver bugs (invalid counter references). Important timing considerations:

1. **At pool assembly:** Catch errors early, mark pool invalid before any allocation attempt. This is the right time.
2. **Not at admission:** Counter set references cross ResourceSlice boundaries — the API server validates one object at a time and cannot verify cross-object references.
3. **Not at tryDevice time:** Too late, too expensive to repeat per device.

**Error surfacing:** When a pool fails validation, Karpenter should log a warning with the pool name, driver, and specific validation failure. No retry — the error persists until the driver publishes corrected ResourceSlices.

---

## Multi-Host Devices and Karpenter's Provisioning Model

Multi-host devices fundamentally differ from Karpenter's "provision one node for a pod" model:

1. The device spans N nodes → pod needs access from one specific node
2. The ResourceClaim is shared across pods → multiple pods schedule with the same claim
3. Node selection is per-device, not per-slice → different devices route to different nodes

**Karpenter's role:** For an unscheduled pod referencing a shared claim for a multi-host device:
- If the claim is already allocated → use the device's `NodeSelector` as a topology constraint
- If the claim is pending → allocate a suitable device, derive topology from it

The "allocate" case is interesting: Karpenter might allocate a multi-host device and then provision one of the N nodes it spans. The other N-1 nodes must already exist or be provisioned by other mechanisms.

**Decision:** Support the "claim already allocated" case (topology extraction from per-device node selector). The "allocate multi-host device" case is complex and deferred until we have concrete demand.

---

## Design Decision: Off-Node Counter Deduction Strategy

### Problem

SharedCounters are pool-level — allocating a device on node-A depletes counters that affect allocatability on node-B (same pool). When computing available counters, the allocator must account for ALL allocated devices in the pool, including those on nodes that don't match the current NodeClaim's requirements.

Karpenter's `Pool.Devices` currently only contains devices from **matching** slices. If an off-node device is allocated and consuming counters, the allocator can't look up its `ConsumesCounters` to deduct from the budget.

Upstream (concrete-node scheduler) solves this by storing `DeviceSlicesNotTargetingNode` on the pool — keeping off-node device definitions available for counter deduction at allocation time.

Karpenter must choose where this resolution happens.

### Option 1: Pool carries non-matching device definitions

Pool gathering stores off-node devices on the pool (not as allocation candidates, but as definitions for counter lookup). The allocator iterates all pool devices, checks which are allocated, and deducts their `ConsumesCounters`.

**Pros:**
- Mirrors upstream 1:1 — easy to cross-reference and validate correctness
- Pool is the single source of truth for ALL device definitions in the pool
- Allocator is self-contained: pool + allocated device IDs = everything it needs
- Controller stays simple — just reports device IDs, doesn't interpret `ConsumesCounters` semantics
- If device definitions change (ResourceSlice update), pool rebuild picks it up automatically

**Cons:**
- Breaks the current clean model: "pool = devices you can allocate from"
- Memory: Pool grows with all devices in the pool regardless of allocatability. For a 100-node × 8-GPU pool = 800 device definitions (~500B–2KB each) carried per pool view, multiplied by N NodeClaims evaluated in a scheduling loop.
- CPU in scheduling hot path: O(all devices in pool) per pool to find allocated ones and deduct counters (set lookup per device). Upstream caches this per allocator instance, but Karpenter evaluates many pods × many ITs per loop.
- `FilterPools()` must preserve non-matching devices across narrowing operations
- `pool.go` grows more complex — non-matching devices need a separate field and different handling during filtering

### Option 2: Controller pre-computes counter deductions

The controller resolves `ConsumesCounters` for each allocated device and passes aggregate counter consumption per pool to the allocator.

```go
// PreallocatedCounterConsumption: poolID → counterSetName → counterName → total consumed
PreallocatedCounterConsumption map[PoolID]map[string]map[string]resource.Quantity
```

**Pros:**
- Pool stays lean — only allocatable devices, clean separation preserved
- Memory: A few hundred bytes per pool (pre-summed quantities), shared across all NodeClaims. No duplication.
- CPU in scheduling path: O(1) per counter check — `remaining = total - preallocated - inflight - allocating`. No device iteration or set lookups during allocation.
- Architecturally consistent with CC: `PreallocatedConsumedCapacity` is pre-computed by the controller (variable per-allocation, MUST be pre-computed). `PreallocatedCounterConsumption` (fixed per-device, CAN be pre-computed) follows the same data flow.
- Controller work is O(allocated devices in pool), triggered by claim status changes (infrequent relative to scheduling decisions)

**Cons:**
- Controller must understand `ConsumesCounters` semantics (it currently interprets claim allocation results, not device definitions)
- Different from upstream — harder to validate by cross-referencing upstream code
- Controller must join claim status (allocated device IDs) with ResourceSlice data (device definitions) to resolve `ConsumesCounters`
- If a device's `ConsumesCounters` changes (ResourceSlice update mid-lifecycle), controller must recompute
- Creates semantic asymmetry: CC pre-computation is necessary (consumed amounts are variable), counter pre-computation is a performance choice (amounts are fixed per device definition)
- Less transparent for debugging: "why is this counter consumed?" requires tracing through controller logic rather than looking at pool + allocated set directly

### Analysis: Why they differ from upstream

Upstream can afford Option 1 because it schedules **one pod on one concrete node** at a time — the O(devices) iteration runs once. Karpenter evaluates **N pods × M instance types** in a single scheduling loop, multiplying the cost.

The key asymmetry with CC that makes Option 2 viable: counter consumption is **deterministic from the device definition**. Knowing "device X is allocated" + reading `device.ConsumesCounters` = full picture. No per-allocation metadata needed. The controller doesn't add information — it pre-computes what the allocator could derive, but moves that work off the hot path.

### Decision: Option 1 (match upstream)

The primary use case (MIG, SR-IOV) is node-local — all devices sharing a counter set are on the same node. The cross-node case (multi-host TPU) is rare and already scoped as limited support. This means the performance concerns with Option 1 are largely theoretical:
- In a node-local pool, there are few or no "non-matching" devices (they're all on the same node the slice targets)
- The O(devices) iteration is bounded by devices per physical GPU (~10-20 MIG partitions), not devices per cluster
- Multi-host pools (where non-matching devices actually exist) are uncommon and small relative to the GPU MIG case

Given this, matching upstream is the right call:
- 1:1 correspondence makes correctness validation trivial
- Pool is self-contained — allocator doesn't depend on controller pre-computation
- Controller stays simple — no `ConsumesCounters` interpretation
- When multi-host support expands, the mechanism already works correctly
- Avoids premature optimization for a hot path that isn't actually hot in the common case

---

## Follow-Up Work

1. **Scoring/ordering for counter-efficient allocation** — minimize counter waste when multiple partitions could satisfy a request (blocked on upstream KEP-4970)
2. **Counter-aware consolidation** — when consolidating, prefer releasing devices that free counters for larger partitions
3. **Multi-host device allocation** — provisioning decisions that account for multi-node device topologies
4. **Mixins (KEP-5234)** — compact device definitions via shared attribute/capacity templates
