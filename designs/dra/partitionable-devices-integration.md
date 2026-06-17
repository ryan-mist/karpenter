# Partitionable Devices Integration

## Table of Contents

- [Overview](#overview)
- [Scope](#scope)
- [Device Model Changes](#device-model-changes)
  - [cloudprovider.Device Extension](#cloudproviderdevice-extension)
  - [cloudprovider.CounterSet Type](#cloudprovidercounterset-type)
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

The `cloudprovider.Device` struct is extended with counter consumption declarations:

```go
type Device struct {
    Name                     unique.Handle[string]
    Attributes               map[resourcev1.QualifiedName]resourcev1.DeviceAttribute
    Capacity                 map[resourcev1.QualifiedName]resourcev1.DeviceCapacity
    AllowMultipleAllocations bool
    ConsumesCounters         []DeviceCounterConsumption  // NEW
}

type DeviceCounterConsumption struct {
    CounterSet string                          // references CounterSet.Name in same pool
    Counters   map[string]resource.Quantity    // counter values consumed on allocation
}
```

For devices with no counter consumption (majority today), `ConsumesCounters` is nil — no behavioral change.

### cloudprovider.CounterSet Type

Counter sets are a pool-level concept, not per-device:

```go
type CounterSet struct {
    Name     string
    Counters map[string]resource.Quantity  // counterName → total available
}
```

### API Server Slice Conversion

The `apiServerSlice` conversion (in `types.go`) is extended to:

1. **Parse `SharedCounters`** from slices that declare them (slices with `SharedCounters` and no `Devices`)
2. **Parse `ConsumesCounters`** on each device

```go
// For slices with SharedCounters (no Devices):
for _, cs := range slice.Spec.SharedCounters {
    counters := make(map[string]resource.Quantity, len(cs.Counters))
    for name, counter := range cs.Counters {
        counters[name] = counter.Value
    }
    s.counterSets = append(s.counterSets, cloudprovider.CounterSet{
        Name:     cs.Name,
        Counters: counters,
    })
}

// For devices with ConsumesCounters:
for _, consumption := range d.ConsumesCounters {
    counters := make(map[string]resource.Quantity, len(consumption.Counters))
    for name, counter := range consumption.Counters {
        counters[name] = counter.Value
    }
    device.ConsumesCounters = append(device.ConsumesCounters, cloudprovider.DeviceCounterConsumption{
        CounterSet: consumption.CounterSet,
        Counters:   counters,
    })
}
```

### Template Device Support

Cloud providers can declare `ConsumesCounters` on template devices. This enables Karpenter to simulate counter-constrained GPU scheduling before nodes exist.

**Template counter sets** are provided alongside template devices in `ResourceSliceTemplate`:

```go
type ResourceSliceTemplate struct {
    // ... existing fields ...
    CounterSets []cloudprovider.CounterSet  // NEW — counter sets for template pool
}
```

Template counter sets are instance-type-specific — different GPU models have different counter budgets. The allocator treats template counter sets identically to in-cluster counter sets during per-IT evaluation.

---

## Pool Management Changes

### Counter Set Gathering

Pool assembly (`Pool` struct in `types.go`) is extended to aggregate counter sets:

```go
type Pool struct {
    // ... existing fields (devices, slices) ...
    CounterSets map[string]map[string]resource.Quantity  // counterSetName → counterName → total
}
```

During pool gathering:
1. Iterate all ResourceSlices in the pool
2. For slices with `SharedCounters`: merge into `Pool.CounterSets`
3. For slices with `Devices`: parse devices as before (now including `ConsumesCounters`)
4. Validate: counter set names unique within pool

### Pool Completeness

Counter sets may reside in separate ResourceSlices from devices. The pool is only complete when `len(observedSlices) == pool.resourceSliceCount`. An incomplete pool is unusable because:

- Missing counter-set slices → under-estimating total counters → incorrect availability
- Missing device slices → under-estimating consumption → over-allocation

This matches existing completeness logic (pools must be fully observed before use). No behavioral change — just reinforcing that the check matters more now.

### Pool Validation

After gathering, validate the pool before any allocation attempt:

```go
func (p *Pool) Validate() error {
    // 1. Counter set names unique (already checked during gathering)
    // 2. For each device with ConsumesCounters:
    for _, device := range p.Devices {
        for _, consumption := range device.ConsumesCounters {
            // Counter set must exist in pool
            cs, ok := p.CounterSets[consumption.CounterSet]
            if !ok {
                return fmt.Errorf("device %q references unknown counter set %q", ...)
            }
            // Each referenced counter must exist in the counter set
            for counterName := range consumption.Counters {
                if _, ok := cs[counterName]; !ok {
                    return fmt.Errorf("device %q references unknown counter %q in set %q", ...)
                }
            }
        }
    }
    return nil
}
```

If validation fails, the pool is marked invalid and no devices from it are allocatable. This matches upstream behavior.

---

## Counter State Tracking

### Available Counter Computation

At the start of allocation, compute remaining counter budgets per pool:

```go
type AvailableCounters struct {
    // counters[counterSetName][counterName] = remaining quantity
    counters map[string]map[string]resource.Quantity
}

func computeAvailableCounters(pool *Pool, allocatedDevices []DeviceID) *AvailableCounters {
    // Start with full counter set budgets
    available := clone(pool.CounterSets)
    
    // Deduct counters consumed by already-allocated devices
    for _, deviceID := range allocatedDevices {
        device := pool.LookupDevice(deviceID)
        for _, consumption := range device.ConsumesCounters {
            for counterName, amount := range consumption.Counters {
                available[consumption.CounterSet][counterName].Sub(amount)
            }
        }
    }
    return &AvailableCounters{counters: available}
}
```

### Preallocated Counter State

The controller provides the allocator with the set of allocated devices per pool (from ResourceClaim status). For counter tracking, we need to know which devices are allocated so their counter consumption can be deducted.

**Key insight:** Counter consumption is deterministic from the device definition — unlike consumable capacity (where the consumed amount depends on the request), SharedCounters are fixed per-device. We only need to know WHICH devices are allocated, not HOW MUCH they consumed. The device's `ConsumesCounters` declaration tells us the rest.

This means the existing `PreallocatedDevices` set (exclusive devices) and `PreallocatedConsumedCapacity` map (multi-allocatable devices) together provide enough information. If a device appears in either → its counters are deducted.

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

The device allocation controller already tracks which devices are allocated per claim. For counter state, the allocator needs to know which devices are allocated in each pool so it can compute counter deductions.

**No new controller state is needed.** The existing `allocatedDevices` map (from the controller) provides the set of allocated device IDs. The allocator combines this with pool device definitions (which contain `ConsumesCounters`) to compute counter state at allocation time.

This is the fundamental difference from consumable capacity: counter consumption is a fixed property of the device definition (it's declared on the ResourceSlice), not a variable property of each allocation (like consumed capacity amounts). The allocator only needs to know "is this device allocated?" — not "how much did it consume?" — because the answer to "how much" is always the device's `ConsumesCounters` declaration.

### Public API Change

The `AllocatedDeviceState` struct (from consumable capacity integration) already carries the device sets needed:

```go
type AllocatedDeviceState struct {
    ExclusiveDevices sets.Set[cloudprovider.DeviceID]
    ConsumedCapacity map[cloudprovider.DeviceID]map[resourcev1.QualifiedName]resource.Quantity
}
```

Both `ExclusiveDevices` and `keys(ConsumedCapacity)` represent allocated devices. The allocator iterates the union to compute counter deductions:

```go
for deviceID := range allocatedState.ExclusiveDevices {
    deductCounters(deviceID, pool)
}
for deviceID := range allocatedState.ConsumedCapacity {
    deductCounters(deviceID, pool)
}
```

No change to the controller's public API is required. The information already flows through.

---

## Key Design Decisions

### 1. Counter state is computed from device definitions, not stored separately

**Decision:** The allocator computes counter consumption by looking up each allocated device's `ConsumesCounters` declaration. No separate "consumed counters" field is stored on allocations or tracked by the controller.

**Rationale:** Unlike consumable capacity (where consumed amounts vary per-allocation due to rounding/requests), counter consumption is fixed per-device. `ConsumesCounters` on the device definition IS the source of truth. Storing it redundantly would create drift risk.

**Alternative considered:** Tracking counter consumption as a field on allocation results (like `ConsumedCapacity`). Rejected because it's redundant with the device definition and adds unnecessary API surface.

### 2. Pool validation is fail-fast for the entire pool

**Decision:** If any device in a pool references a non-existent counter set or counter, the entire pool is marked invalid.

**Rationale:** Partial validation is unsound — a device referencing a missing counter set might be the one preventing other devices from being allocated (via shared counter depletion). Silently ignoring the invalid device could allow over-allocation. The upstream allocator uses the same strategy.

### 3. Counter sets are gathered at pool assembly time, not lazily

**Decision:** Counter sets are resolved when pools are gathered (during scheduler initialization / pool refresh). Invalid references are caught early.

**Rationale:** Lazy resolution during `tryDevice` would require error handling in the hot path and would repeat the resolution for every candidate device. Early resolution amortizes the cost and surfaces errors before allocation begins.

### 4. Template pools can have counter sets

**Decision:** Cloud providers can declare counter sets on template pools (via `ResourceSliceTemplate.CounterSets`). Template devices can declare `ConsumesCounters`.

**Rationale:** This enables GPU cloud providers to express MIG-style partitioning on template devices. Without it, Karpenter cannot simulate MIG allocation for instance types not yet provisioned — a critical gap for the primary KEP-4815 use case (dynamic GPU partitioning).

### 5. Counter check placement: after IsAllocated, before CEL

**Decision:** Counter verification runs after the binary `IsAllocated` check and capacity check, but before CEL selector evaluation.

**Rationale:** Counter checks are cheaper than CEL compilation/evaluation (simple quantity comparison vs. expression evaluation). Running them first prunes ineligible devices before the expensive selector step. The IsAllocated check is the cheapest filter and stays first.

### 6. PerDeviceNodeSelection resolves per-device, not per-slice

**Decision:** When `PerDeviceNodeSelection: true`, topology requirements are resolved per-device during `tryDevice`, not pre-computed per-slice during pool gathering.

**Rationale:** Pre-computation would require storing per-device topology on the pool structure, complicating pool assembly. Since topology is only evaluated for devices that pass all other checks (IsAllocated, capacity, counters, CEL), the per-device resolution runs infrequently. The marginal cost is negligible.

### 7. Three-layer counter tracking mirrors capacity tracking

**Decision:** Counter state uses the same three-layer pattern as consumable capacity: preallocated (cluster state) + inflight (earlier pods) + allocating (current DFS).

**Rationale:** The problems are structurally identical — shared resources consumed across time horizons (existing allocations → same scheduling loop → same DFS tree). Reusing the pattern reduces cognitive overhead and enables a consistent backtracking/commit protocol.

---

## Implementation Sequencing

### Commit 1: Device Model & Pool Changes

Foundation layer. Extends `cloudprovider.Device` with `ConsumesCounters`, adds `CounterSet` type, updates pool gathering and validation.

- [cloudprovider.Device Extension](#cloudproviderdevice-extension)
- [cloudprovider.CounterSet Type](#cloudprovidercounterset-type)
- [API Server Slice Conversion](#api-server-slice-conversion)
- [Counter Set Gathering](#counter-set-gathering)
- [Pool Validation](#pool-validation)

### Commit 2: Counter State & Verification Logic

Core computation layer — counter tracking types and the verification function.

- [Available Counter Computation](#available-counter-computation)
- [Preallocated Counter State](#preallocated-counter-state)
- [Inflight Counter State](#inflight-counter-state)
- [Per-IT Counter State (DFS-local)](#per-it-counter-state-dfs-local)
- [Counter Verification Logic](#counter-verification-logic)

### Commit 3: Allocator Integration

DFS integration — connects counter verification into the allocation decision path.

- [Placement in tryDevice](#placement-in-trydevice)
- [Backtracking](#backtracking)
- [Interaction with Consumable Capacity](#interaction-with-consumable-capacity)
- [Commit Protocol Extension](#commit-protocol-extension)

### Commit 4: PerDeviceNodeSelection

Topology handling for multi-host devices.

- [Topology Requirement Extraction](#topology-requirement-extraction)
- [NodeClaim Compatibility](#nodeclaim-compatibility)

### Dependency Graph

```
Commit 1: Device Model & Pool Changes
  │
  ├──→ Commit 2: Counter State & Verification Logic
  │       │
  │       ▼
  ├──→ Commit 3: Allocator Integration (depends on 1, 2)
  │
  └──→ Commit 4: PerDeviceNodeSelection (independent of 2, 3)
```

Commits 2-3 are sequential (counter logic → DFS integration). Commit 4 is independent and can be developed in parallel.
