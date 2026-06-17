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

## Follow-Up Work

1. **Scoring/ordering for counter-efficient allocation** — minimize counter waste when multiple partitions could satisfy a request (blocked on upstream KEP-4970)
2. **Counter-aware consolidation** — when consolidating, prefer releasing devices that free counters for larger partitions
3. **Multi-host device allocation** — provisioning decisions that account for multi-node device topologies
4. **Mixins (KEP-5234)** — compact device definitions via shared attribute/capacity templates
