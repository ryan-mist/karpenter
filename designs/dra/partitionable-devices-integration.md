# Partitionable Devices Integration

## Table of Contents

- [Overview](#overview)
- [Scope](#scope)
- [Device Model Changes](#device-model-changes)
  - [cloudprovider.Device Extension](#cloudproviderdevice-extension)
  - [API Server Slice Conversion](#api-server-slice-conversion)
  - [Template Device Support](#template-device-support)
- [Pool Management Changes](#pool-management-changes)
  - [Counter Set Gathering](#counter-set-gathering)
  - [Pool Completeness](#pool-completeness)
  - [Pool Validation](#pool-validation)
- [Counter State Tracking](#counter-state-tracking)
  - [Available Counter Computation](#available-counter-computation)
  - [Preallocated Counter State](#preallocated-counter-state)
  - [Inflight Counter State](#inflight-counter-state)
  - [Per-IT Counter State (DFS-local)](#per-it-counter-state-dfs-local)
  - [Backtracking](#backtracking)
- [Allocator Integration](#allocator-integration)
  - [Placement in tryDevice](#placement-in-trydevice)
  - [Counter Verification Logic](#counter-verification-logic)
  - [Interaction with Consumable Capacity](#interaction-with-consumable-capacity)
  - [Commit Protocol Extension](#commit-protocol-extension)
- [PerDeviceNodeSelection](#perdevicenodeselection)
  - [Topology Requirement Extraction](#topology-requirement-extraction)
  - [NodeClaim Compatibility](#nodeclaim-compatibility)
- [Controller Changes](#controller-changes)
  - [Counter Consumption Tracking](#counter-consumption-tracking)
  - [Public API Change](#public-api-change)
- [Key Design Decisions](#key-design-decisions)
- [Implementation Sequencing](#implementation-sequencing)

---

## Overview

### Problem Statement

Karpenter's DRA allocator currently treats devices as independently allocatable — allocating one device has no side effects on other devices in the same pool. KEP-4815 (Partitionable Devices) introduces **SharedCounters** that create dependencies between devices: allocating one device depletes shared counters, potentially making other devices in the pool unallocatable.

Integrating partitionable devices into Karpenter's allocator presents challenges unique to our scheduling model:

1. **Counter sets span multiple ResourceSlices.** SharedCounters are declared in separate ResourceSlices from devices. Pool gathering must be extended to collect counter set definitions and resolve cross-slice references before the allocator can evaluate device eligibility.

2. **Instance type superposition and template devices.** Template devices from cloud providers may declare `ConsumesCounters`. Each instance type provides different template devices with potentially different counter consumption patterns. Counter budgets must be evaluated independently per instance type — IT-A's template allocations should not affect IT-B's counter availability.

3. **Cross-NodeClaim counter contention.** In-cluster devices with SharedCounters are shared across NodeClaims. When pod-A's allocation depletes counters for a pool device, pod-B (on a different NodeClaim) must see the reduced counter budget. This mirrors how `InflightConsumedCapacity` works for consumable capacity — but tracking counter quantities rather than capacity dimensions.

4. **PerDeviceNodeSelection breaks the "one slice, one node" assumption.** Karpenter currently assumes all devices in a ResourceSlice share the same node affinity. Multi-host devices have per-device node selectors, requiring the topology extraction logic to resolve affinity per-device rather than per-slice.

### Key References

| Reference | Path |
|-----------|------|
| Upstream KEP semantics | `designs/dra/partitionable-devices.md` |
| Core allocator design | `designs/dra/scheduling.md` |
| Consumable capacity integration | `designs/dra/consumable-capacity-integration.md` |
| Upstream implementation | `k8s.io/dynamic-resource-allocation@v0.35.0/structured/internal/experimental/` |
| Device model | `pkg/cloudprovider/dynamicresources.go` |
| Allocator DFS | `pkg/scheduling/dynamicresources/allocator.go` |
| Pool management | `pkg/scheduling/dynamicresources/types.go` |
| Device allocation controller | `pkg/controllers/dynamicresources/deviceallocation/controller.go` |

---

## Scope

### In Scope

- `SharedCounters` field parsing from ResourceSlices
- `ConsumesCounters` field on devices (in-cluster and template)
- Counter budget computation (total available - already consumed)
- Counter eligibility check during `tryDevice`
- Transient counter tracking during DFS with backtracking
- Cross-pod counter tracking (across `Allocate()` calls)
- Pool validation for counter set references
- `PerDeviceNodeSelection` support for topology extraction
- Controller tracking of counter consumption from allocated claims

### Deferred

- **Multi-host gang scheduling:** DRA restricts pods to valid nodes but doesn't guarantee all pods schedule. Gang scheduling is a separate concern.
- **Counter-based scoring/ordering:** The scheduler picks first-fit, not best-fit. Scoring is tracked in upstream KEP-4970.
- **Mixins (KEP-5234):** Compact device definition via shared attribute/capacity templates. Orthogonal to counter mechanics.
- **Cross-pool counter sets:** Only references within the same pool are supported.

---

## Device Model Changes

### cloudprovider.Device Extension

The `cloudprovider.Device` struct is extended with counter consumption declarations using the upstream Kubernetes API types directly:

```go
type Device struct {
    Name                     unique.Handle[string]
    Attributes               map[resourcev1.QualifiedName]resourcev1.DeviceAttribute
    Capacity                 map[resourcev1.QualifiedName]resourcev1.DeviceCapacity
    AllowMultipleAllocations bool
    ConsumesCounters         []resourcev1.DeviceCounterConsumption  // NEW
}
```

For devices with no counter consumption (majority today), `ConsumesCounters` is nil — no behavioral change.

### API Server Slice Conversion

The `apiServerSlice` conversion (in `types.go`) stores the upstream types directly — no unwrapping into intermediate types:

1. **`SharedCounters`**: stored as `[]resourcev1.CounterSet` on the slice (via `SharedCounters()` accessor)
2. **`ConsumesCounters`**: stored as `[]resourcev1.DeviceCounterConsumption` on each device

No conversion loops are needed — the API types are used as-is throughout the pool layer.

### Template Device Support

Cloud providers can declare `ConsumesCounters` on template devices. This enables Karpenter to simulate counter-constrained GPU scheduling before nodes exist.

**Template counter sets** are provided alongside template devices in `ResourceSliceTemplate`:

```go
type ResourceSliceTemplate struct {
    // ... existing fields ...
    SharedCounters []resourcev1.CounterSet  // NEW — counter sets for template pool
}
```

Template counter sets are instance-type-specific — different GPU models have different counter budgets. The allocator treats template counter sets identically to in-cluster counter sets during per-IT evaluation.

---

## Pool Management Changes

### Pool Struct Extension

The `Pool` struct (`pool.go`) is extended with counter sets and non-targeting devices:

```go
type Pool struct {
    Key PoolKey
    Slices []ResourceSlice
    Devices []DeviceWithID
    Incomplete bool
    Invalid bool

    // CounterSets holds the counter set definitions for this pool.
    // counterSetName → counterName → Counter (with .Value as resource.Quantity).
    CounterSets map[string]map[string]resourcev1.Counter

    // NonTargetingDevices holds devices from slices that don't match the current
    // NodeClaim's requirements but have ConsumesCounters. These are NOT allocation
    // candidates — they exist solely so the allocator can deduct their counter
    // consumption when they are allocated (on other nodes).
    // This mirrors upstream's DeviceSlicesNotTargetingNode.
    NonTargetingDevices []DeviceWithID
}
```

### Counter Set Gathering

During pool gathering:
1. Iterate all ResourceSlices in the pool
2. For slices with `SharedCounters` (no `Devices`): merge into `Pool.CounterSets`
3. For matching slices with `Devices`: parse devices as before (now including `ConsumesCounters`) → `Pool.Devices`
4. For non-matching slices with `Devices`: retain only devices that have `ConsumesCounters` → `Pool.NonTargetingDevices`
5. Validate: counter set names unique within pool

### sliceMatchesRequirements Change

Counter-set slices have no node affinity (no `NodeSelector`, `NodeName`, or `AllNodes`). Rather than special-casing them in `GatherPools`, extend `sliceMatchesRequirements` to return `true` for them — they are unconditionally relevant to the pool:

```go
func sliceMatchesRequirements(s ResourceSlice, requirements scheduling.Requirements) bool {
    if s.Potential() {
        panic("potential slices must not be passed to pool gathering or filtering")
    }
    if s.AllNodes() {
        return true
    }
    if s.SharedCounters() != nil {
        return true
    }
    if ns := s.NodeSelector(); ns != nil {
        return nodeSelectorsMatch(ns, requirements)
    }
    return false
}
```

This keeps `GatherPools` unchanged — counter-set slices flow through `addSlice(s, true)` naturally. A device slice with no affinity fields still returns `false` (preserving detection of buggy drivers), because it won't have `SharedCounters`.

### ResourceSlice Interface Addition

Add `SharedCounters()` to the `ResourceSlice` interface:

```go
type ResourceSlice interface {
    // ... existing methods ...
    SharedCounters() []resourcev1.CounterSet
}
```

Implementations:
- `apiServerSlice`: returns `slice.Spec.SharedCounters` directly (no conversion needed)
- `templateSlice`: returns `template.SharedCounters`

### Pool Gathering Changes (pool.go)

`GatherPools` is unchanged — `sliceMatchesRequirements` handles counter-set slices.

The `build()` function branches on `SharedCounters()` when processing entries. Counter-set slices are accumulated separately and validated via helper functions after the main loop:

```go
func (b *poolBuilder) build(key PoolKey) *Pool {
    pool := &Pool{Key: key}

    if int64(len(b.entries)) != b.resourceSliceCount {
        pool.Incomplete = true
    }

    var counterSetSlices []ResourceSlice
    var nonTargetingDeviceSlices []ResourceSlice
    seen := sets.New[unique.Handle[string]]()
    for _, e := range b.entries {
        if e.slice.SharedCounters() != nil {
            counterSetSlices = append(counterSetSlices, e.slice)
            continue
        }
        if e.matched {
            // Matching device slice — devices are allocation candidates
            pool.Slices = append(pool.Slices, e.slice)
            topoReqs := sliceTopologyRequirements(e.slice)
            for _, d := range e.slice.Devices() {
                pool.Invalid = pool.Invalid || seen.Has(d.Name)
                seen.Insert(d.Name)
                pool.Devices = append(pool.Devices, newDeviceWithID(key, d, topoReqs))
            }
        } else {
            nonTargetingDeviceSlices = append(nonTargetingDeviceSlices, e.slice)
            // Non-matching device slice — retain devices with ConsumesCounters
            // for counter deduction (not allocation candidates).
            // No duplicate name detection — matches upstream behavior where only
            // targeting devices are validated for name uniqueness.
            for _, d := range e.slice.Devices() {
                if len(d.ConsumesCounters) > 0 {
                    pool.NonTargetingDevices = append(pool.NonTargetingDevices, newDeviceWithID(key, d, nil))
                }
            }
        }
    }

    counterSets, valid := getAndValidateCounterSets(counterSetSlices)
    pool.CounterSets = counterSets
    pool.Invalid = pool.Invalid || !valid
    pool.Invalid = pool.Invalid || !validateDeviceCounterConsumption(counterSets, pool.Slices)
    pool.Invalid = pool.Invalid || !validateDeviceCounterConsumption(counterSets, nonTargetingDeviceSlices)

    if len(pool.Slices) == 0 && len(pool.NonTargetingDevices) == 0 {
        return nil
    }
    return pool
}
```

### FilterPools Changes (pool.go)

`filterPool()` must preserve `NonTargetingDevices` across narrowing. When requirements tighten:
- Previously-matching devices that no longer match move to `NonTargetingDevices` (if they have `ConsumesCounters`)
- Existing `NonTargetingDevices` are always preserved (they never become candidates)

```go
func filterPool(pool *Pool, requirements scheduling.Requirements) *Pool {
    p := &Pool{
        Key:                 pool.Key,
        Incomplete:          pool.Incomplete,
        Invalid:             pool.Invalid,
        CounterSets:         pool.CounterSets,
        NonTargetingDevices: pool.NonTargetingDevices, // always preserved
    }
    for _, s := range pool.Slices {
        if sliceMatchesRequirements(s, requirements) {
            // Still matches — remains an allocation candidate
            p.Slices = append(p.Slices, s)
            topoReqs := sliceTopologyRequirements(s)
            for _, d := range s.Devices() {
                p.Devices = append(p.Devices, DeviceWithID{...})
            }
        } else {
            // No longer matches — demote counter-consuming devices
            for _, d := range s.Devices() {
                if len(d.ConsumesCounters) > 0 {
                    p.NonTargetingDevices = append(p.NonTargetingDevices, DeviceWithID{...})
                }
            }
        }
    }
    if len(p.Slices) == 0 && len(p.NonTargetingDevices) == 0 {
        return nil
    }
    return p
}
```

### Pool Completeness

Counter sets may reside in separate ResourceSlices from devices. The pool is only complete when `len(observedSlices) == pool.resourceSliceCount`. An incomplete pool is unusable because:

- Missing counter-set slices → under-estimating total counters → incorrect availability
- Missing device slices → under-estimating consumption → over-allocation

This matches existing completeness logic (pools must be fully observed before use). No behavioral change — just reinforcing that the check matters more now. Counter-set slices pass through `sliceMatchesRequirements` (which returns `true` for them) and are counted toward completeness like any other slice.

### Pool Validation

Validation is inlined in `build()` (not a separate method). It covers BOTH targeting and non-targeting device slices — a non-targeting device with an invalid counter reference means the pool state is corrupt:

```go
func getAndValidateCounterSets(slices []ResourceSlice) (map[string]map[string]resourcev1.Counter, bool) {
    counterSets := make(map[string]map[string]resourcev1.Counter)
    valid := true
    for _, slice := range slices {
        for _, counterSet := range slice.SharedCounters() {
            if _, found := counterSets[counterSet.Name]; found {
                valid = false  // duplicate counter set name
            }
            counterSets[counterSet.Name] = counterSet.Counters
        }
    }
    return counterSets, valid
}

func validateDeviceCounterConsumption(counterSets map[string]map[string]resourcev1.Counter, slices []ResourceSlice) bool {
    for _, slice := range slices {
        for _, device := range slice.Devices() {
            for _, consumption := range device.ConsumesCounters {
                counterSet, found := counterSets[consumption.CounterSet]
                if !found {
                    return false  // references unknown counter set
                }
                for counterName := range consumption.Counters {
                    if _, found := counterSet[counterName]; !found {
                        return false  // references unknown counter within set
                    }
                }
            }
        }
    }
    return true
}
```

If validation fails, the pool is marked `Invalid = true` and no devices from it are allocatable. This matches upstream behavior.

---

## Counter State Tracking

### Available Counter Computation

At the start of allocation, compute remaining counter budgets per pool. The allocator extracts `resource.Quantity` values from `Pool.CounterSets` (which stores `resourcev1.Counter`) for arithmetic, then iterates ALL devices in the pool (both targeting and non-targeting), checks whether each is allocated, and deducts its counters. This matches upstream's `checkAvailableCounters` pattern:

```go
type AvailableCounters struct {
    // counters[counterSetName][counterName] = remaining quantity
    counters map[string]map[string]resource.Quantity
}

func computeAvailableCounters(pool *Pool, allocatedDevices sets.Set[DeviceID]) *AvailableCounters {
    // Start with full counter set budgets
    available := clone(pool.CounterSets)
    
    // Deduct counters consumed by already-allocated devices.
    // Iterate BOTH targeting and non-targeting devices — an off-node device
    // that is allocated still depletes shared counters for this pool.
    for _, device := range pool.Devices {
        if !allocatedDevices.Has(device.ID) {
            continue
        }
        deductCounters(available, device.Device)
    }
    for _, device := range pool.NonTargetingDevices {
        if !allocatedDevices.Has(device.ID) {
            continue
        }
        deductCounters(available, device.Device)
    }
    return &AvailableCounters{counters: available}
}

func deductCounters(available map[string]map[string]resource.Quantity, device cloudprovider.Device) {
    for _, consumption := range device.ConsumesCounters {
        for counterName, amount := range consumption.Counters {
            remaining := available[consumption.CounterSet][counterName]
            remaining.Sub(amount)
            available[consumption.CounterSet][counterName] = remaining
        }
    }
}
```

This computation is cached per pool per allocator instance — it runs once when the pool is first accessed during allocation, not on every `tryDevice` call.

### Preallocated Counter State

The controller provides the allocator with the set of allocated devices per pool (from ResourceClaim status). For counter tracking, we need to know which devices are allocated so their counter consumption can be deducted from the pool's counter budgets.

**Key insight:** Counter consumption is deterministic from the device definition — unlike consumable capacity (where the consumed amount depends on the request), SharedCounters are fixed per-device. We only need to know WHICH devices are allocated, not HOW MUCH they consumed. The device's `ConsumesCounters` declaration (on the pool) tells us the rest.

The existing `PreallocatedDevices` set (exclusive devices) and `PreallocatedConsumedCapacity` map (multi-allocatable devices) together provide the allocated device set. The allocator uses this to identify allocated devices when iterating the pool's `Devices` and `NonTargetingDevices` during `computeAvailableCounters`.

This is why the pool carries `NonTargetingDevices` — the allocator needs the device definitions to look up `ConsumesCounters` for off-node allocated devices. Without them on the pool, the allocator couldn't resolve what counters an off-node device consumes.

### Inflight Counter State

As pods are scheduled within a scheduling loop, counter deductions from earlier `Allocate()` calls must be visible to later calls:

```go
type AllocationTracker struct {
    // ... existing fields ...
    
    // InflightCounterConsumption tracks counter deductions from devices allocated
    // by earlier pods in this scheduling loop. Map: poolID → counterSetName → counterName → consumed.
    InflightCounterConsumption map[PoolID]map[string]map[string]resource.Quantity
}
```

On `Commit()`: the child allocator's counter deductions are merged into `InflightCounterConsumption`.

On `ReleaseInstanceTypes()`: if a committed allocation's instance type is pruned, its counter contribution must be subtracted from `InflightCounterConsumption`.

### Per-IT Counter State (DFS-local)

Within a single DFS tree (one pod, one instance type), counter deductions from tentative allocations are tracked locally on the child allocator:

```go
type allocator struct {
    // ... existing fields ...
    
    // allocatingCounters tracks counter deductions for devices tentatively allocated
    // in the current DFS. Reset per-IT via restoreState().
    allocatingCounters map[string]map[string]resource.Quantity  // counterSet → counter → amount
}
```

This is reset in `restoreState()` between IT attempts, matching how `allocatedDevices` and `allocatingCapacity` are reset.

### Backtracking

On successful tentative allocation of a device with `ConsumesCounters`:

```go
for _, consumption := range device.ConsumesCounters {
    for counterName, amount := range consumption.Counters {
        a.allocatingCounters[consumption.CounterSet][counterName].Add(amount)
    }
}
```

On backtrack:

```go
for _, consumption := range device.ConsumesCounters {
    for counterName, amount := range consumption.Counters {
        a.allocatingCounters[consumption.CounterSet][counterName].Sub(amount)
    }
}
```

---

## Allocator Integration

### Placement in tryDevice

The counter check inserts into `tryDevice` alongside existing checks:

```
Current flow (with consumable capacity):
  1.  IsAllocated check
  1b. Capacity verification (multi-alloc)
  2.  Selector match (CEL)
  3.  Constraints
  4.  Topology compatibility
  5.  Record + recurse + backtrack

New flow:
  1.  IsAllocated check
  1b. Capacity verification (multi-alloc)
  1c. Counter verification             → NEW
  2.  Selector match (CEL)
  3.  Constraints
  4.  Topology compatibility
  5.  Record + recurse + backtrack
  5b. On record: deduct counters       → NEW
  5c. On backtrack: restore counters   → NEW
```

Counter verification runs for ALL devices with `ConsumesCounters` (regardless of whether they are multi-allocatable). A device without `ConsumesCounters` skips the check entirely.

### Counter Verification Logic

```go
func (a *allocator) checkCounters(device cloudprovider.Device, poolID PoolID) bool {
    if len(device.ConsumesCounters) == 0 {
        return true  // no counter constraints
    }
    for _, consumption := range device.ConsumesCounters {
        for counterName, needed := range consumption.Counters {
            total := a.pool.CounterSets[consumption.CounterSet][counterName]
            preallocated := a.preallocatedCounters[consumption.CounterSet][counterName]
            inflight := a.inflightCounters[consumption.CounterSet][counterName]
            allocating := a.allocatingCounters[consumption.CounterSet][counterName]
            
            remaining := total - preallocated - inflight - allocating
            if needed.Cmp(remaining) > 0 {
                return false  // insufficient counters
            }
        }
        return true
    }
}
```

Three layers of counter consumption (matching the capacity verification pattern):
- `preallocated` — from cluster state (existing allocations)
- `inflight` — from earlier pods in this scheduling loop
- `allocating` — from earlier allocations in this DFS tree

### Interaction with Consumable Capacity

Counter verification and capacity verification are **independent checks that both must pass**:

```go
// In tryDevice:
if device.AllowMultipleAllocations {
    if !a.checkCapacity(device, request) {
        continue  // insufficient capacity
    }
}
if len(device.ConsumesCounters) > 0 {
    if !a.checkCounters(device, poolID) {
        continue  // insufficient counters
    }
}
```

A device that passes capacity but fails counters is rejected. A device that passes counters but fails capacity is rejected. Both must pass.

### Commit Protocol Extension

On `Commit()`, the child allocator reports counter deductions to the top-level tracker:

```go
func (a *Allocator) Commit(child *allocation) {
    // ... existing commit logic (devices, capacity) ...
    
    // Merge counter deductions into inflight state
    for counterSet, counters := range child.allocatingCounters {
        for counterName, amount := range counters {
            a.tracker.InflightCounterConsumption[poolID][counterSet][counterName].Add(amount)
        }
    }
}
```

---

## PerDeviceNodeSelection

### Topology Requirement Extraction

Currently, topology requirements are extracted from the ResourceSlice's `NodeName`/`NodeSelector`/`AllNodes` field. With `PerDeviceNodeSelection`, each device carries its own node affinity.

**In-cluster devices with PerDeviceNodeSelection:**

```go
func topologyForDevice(slice ResourceSlice, device Device) scheduling.Requirements {
    if slice.PerDeviceNodeSelection {
        // Use device-level node affinity
        switch {
        case device.NodeName != nil:
            return requirementsFromNodeName(*device.NodeName)
        case device.NodeSelector != nil:
            return requirementsFromNodeSelector(*device.NodeSelector)
        case device.AllNodes != nil && *device.AllNodes:
            return nil  // no topology constraint
        default:
            return nil  // no constraint specified
        }
    }
    // Fall back to slice-level (existing behavior)
    return topologyFromSlice(slice)
}
```

### NodeClaim Compatibility

When evaluating whether a device is compatible with a NodeClaim's candidate instance types, per-device topology must be intersected with the NodeClaim's requirements:

- `device.NodeName = "node-X"` → only compatible if NodeClaim can schedule on node-X
- `device.NodeSelector` → only compatible if there exists an instance type that satisfies the selector
- `device.AllNodes = true` → always compatible (same as slice-level `AllNodes`)

For **template devices**, per-device node selection is less relevant — template devices exist only on the instance type they belong to. However, if a cloud provider uses `PerDeviceNodeSelection` on template slices (unusual), the same logic applies.

---

## Controller Changes

### Counter Consumption Tracking

**No new controller state or API changes are needed.**

The device allocation controller already tracks which devices are allocated per claim. The existing `AllocatedDeviceState` struct (from consumable capacity integration) provides the allocated device sets:

```go
type AllocatedDeviceState struct {
    ExclusiveDevices sets.Set[cloudprovider.DeviceID]
    ConsumedCapacity map[cloudprovider.DeviceID]map[resourcev1.QualifiedName]resource.Quantity
}
```

The allocator uses these sets to identify which devices are allocated when computing available counters. The counter deduction logic lives in the allocator (via `computeAvailableCounters`), which looks up each allocated device's `ConsumesCounters` from the pool's device definitions (`Devices` + `NonTargetingDevices`).

This is the fundamental difference from consumable capacity: counter consumption is a fixed property of the device definition (declared on the ResourceSlice), not a variable property of each allocation. The controller doesn't need to interpret `ConsumesCounters` semantics — it just provides the "which devices are allocated" signal. The pool provides the "what do they consume" definitions. The allocator joins the two.

This matches upstream, where `checkAvailableCounters` iterates pool device slices and checks `allocatedState.AllocatedDevices.Has(deviceID)` — the pool carries the definitions, the allocated state carries the IDs.

---

## Key Design Decisions

### 1. Counter state is computed from device definitions, not stored separately

**Decision:** The allocator computes counter consumption by looking up each allocated device's `ConsumesCounters` declaration. No separate "consumed counters" field is stored on allocations or tracked by the controller.

**Rationale:** Unlike consumable capacity (where consumed amounts vary per-allocation due to rounding/requests), counter consumption is fixed per-device. `ConsumesCounters` on the device definition IS the source of truth. Storing it redundantly would create drift risk.

**Alternative considered:** Tracking counter consumption as a field on allocation results (like `ConsumedCapacity`). Rejected because it's redundant with the device definition and adds unnecessary API surface.

### 2. Pool carries non-targeting devices for counter deduction (matching upstream)

**Decision:** The pool stores off-node devices with `ConsumesCounters` in a separate `NonTargetingDevices` field. The allocator iterates these alongside targeting devices to compute available counters. No controller pre-computation of counter consumption.

**Rationale:** This matches upstream's `DeviceSlicesNotTargetingNode` approach. The performance cost of iterating non-targeting devices is negligible in practice because the primary use case (MIG) is node-local — all MIG partitions sharing a counter set are on the same node, so `NonTargetingDevices` is typically empty. The cross-node case (multi-host TPU) is rare and has small device counts per pool. Matching upstream provides 1:1 correctness validation, keeps the controller simple (no `ConsumesCounters` interpretation), and avoids premature optimization. See `partitionable-devices-notes.md` for the full analysis of both options.

**Alternative considered:** Controller pre-computes aggregate counter consumption per pool (`PreallocatedCounterConsumption`), eliminating the need for non-targeting devices on the pool. Rejected because the performance benefit is marginal for real workloads, it diverges from upstream, and it adds semantic responsibility to the controller that doesn't belong there.

### 3. Pool validation is fail-fast for the entire pool

**Decision:** If any device in a pool references a non-existent counter set or counter, the entire pool is marked invalid.

**Rationale:** Partial validation is unsound — a device referencing a missing counter set might be the one preventing other devices from being allocated (via shared counter depletion). Silently ignoring the invalid device could allow over-allocation. The upstream allocator uses the same strategy.

### 4. Counter sets are gathered at pool assembly time, not lazily

**Decision:** Counter sets are resolved when pools are gathered (during scheduler initialization / pool refresh). Invalid references are caught early.

**Rationale:** Lazy resolution during `tryDevice` would require error handling in the hot path and would repeat the resolution for every candidate device. Early resolution amortizes the cost and surfaces errors before allocation begins.

### 5. Template pools can have counter sets

**Decision:** Cloud providers can declare counter sets on template pools (via `ResourceSliceTemplate.CounterSets`). Template devices can declare `ConsumesCounters`.

**Rationale:** This enables GPU cloud providers to express MIG-style partitioning on template devices. Without it, Karpenter cannot simulate MIG allocation for instance types not yet provisioned — a critical gap for the primary KEP-4815 use case (dynamic GPU partitioning).

### 6. Counter check placement: after IsAllocated, before CEL

**Decision:** Counter verification runs after the binary `IsAllocated` check and capacity check, but before CEL selector evaluation.

**Rationale:** Counter checks are cheaper than CEL compilation/evaluation (simple quantity comparison vs. expression evaluation). Running them first prunes ineligible devices before the expensive selector step. The IsAllocated check is the cheapest filter and stays first.

### 7. PerDeviceNodeSelection resolves per-device, not per-slice

**Decision:** When `PerDeviceNodeSelection: true`, topology requirements are resolved per-device during `tryDevice`, not pre-computed per-slice during pool gathering.

**Rationale:** Pre-computation would require storing per-device topology on the pool structure, complicating pool assembly. Since topology is only evaluated for devices that pass all other checks (IsAllocated, capacity, counters, CEL), the per-device resolution runs infrequently. The marginal cost is negligible.

### 8. Three-layer counter tracking mirrors capacity tracking

**Decision:** Counter state uses the same three-layer pattern as consumable capacity: preallocated (cluster state) + inflight (earlier pods) + allocating (current DFS).

**Rationale:** The problems are structurally identical — shared resources consumed across time horizons (existing allocations → same scheduling loop → same DFS tree). Reusing the pattern reduces cognitive overhead and enables a consistent backtracking/commit protocol.

---

## Implementation Sequencing

### Commit 1: Device Model & Pool Changes ✓

Foundation layer. Extends `cloudprovider.Device` with `ConsumesCounters`, adds `SharedCounters()` to `ResourceSlice` interface, updates `sliceMatchesRequirements` + pool gathering/filtering to handle counter-set slices and non-targeting devices, and adds pool validation.

- [cloudprovider.Device Extension](#cloudproviderdevice-extension)
- [API Server Slice Conversion](#api-server-slice-conversion)
- [Pool Struct Extension](#pool-struct-extension)
- [ResourceSlice Interface Addition](#resourceslice-interface-addition)
- [sliceMatchesRequirements Change](#slicematchesrequirements-change)
- [Pool Gathering Changes (pool.go)](#pool-gathering-changes-poolgo)
- [FilterPools Changes (pool.go)](#filterpools-changes-poolgo)
- [Pool Completeness](#pool-completeness)
- [Pool Validation](#pool-validation)

### Commit 2: Counter State & Verification Logic ✓

Core computation layer and DFS integration — counter tracking types, verification function, and wiring into the allocation path. Commits 2 and 3 from the original plan were merged into a single commit since the state and integration are tightly coupled.

- [Available Counter Computation](#available-counter-computation)
- [Preallocated Counter State](#preallocated-counter-state)
- [Inflight Counter State](#inflight-counter-state)
- [Per-IT Counter State (DFS-local)](#per-it-counter-state-dfs-local)
- [Counter Verification Logic](#counter-verification-logic)
- [Placement in tryDevice](#placement-in-trydevice)
- [Backtracking](#backtracking)
- [Interaction with Consumable Capacity](#interaction-with-consumable-capacity)
- [Commit Protocol Extension](#commit-protocol-extension)

### Commit 3: Template Counter Verification

Extends counter verification to template devices. Template counter budgets are per-IT (not shared across NodeClaims) and initialized from `ResourceSliceTemplate.SharedCounters`.

- Template counter budget initialization via `buildTemplateCounters`
- `checkCounters` branching on `deviceID.Template` to use `templateRemainingCounters`

### Commit 4: PerDeviceNodeSelection

Topology handling for multi-host devices.

- [Topology Requirement Extraction](#topology-requirement-extraction)
- [NodeClaim Compatibility](#nodeclaim-compatibility)

### Dependency Graph

```
Commit 1: Device Model & Pool Changes ✓
  │
  ├──→ Commit 2: Counter State & Verification Logic ✓ (merged original commits 2+3)
  │       │
  │       ▼
  ├──→ Commit 3: Template Counter Verification (depends on 2)
  │
  └──→ Commit 4: PerDeviceNodeSelection (independent of 2, 3)
```

Commit 3 is scoped to template devices only. Commit 4 is independent and can be developed in parallel.
