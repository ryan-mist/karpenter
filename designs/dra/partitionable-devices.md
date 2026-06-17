# Partitionable Devices (KEP-4815)

## Table of Contents

- [Overview](#overview)
- [API Surface](#api-surface)
  - [SharedCounters (on ResourceSliceSpec)](#sharedcounters-on-resourceslicespec)
  - [ConsumesCounters (on Device)](#consumescounters-on-device)
  - [PerDeviceNodeSelection](#perdevicenodeselection)
- [Allocation Algorithm](#allocation-algorithm)
  - [Counter State Initialization](#counter-state-initialization)
  - [Device Eligibility Check](#device-eligibility-check)
  - [Transactional Counter Tracking](#transactional-counter-tracking)
  - [Pool Validation](#pool-validation)
- [Use Cases](#use-cases)
  - [GPU Partitioning (NVIDIA MIG)](#gpu-partitioning-nvidia-mig)
  - [Multi-Host TPU Slices](#multi-host-tpu-slices)
- [Interaction with Other Features](#interaction-with-other-features)
- [Upstream Implementation References](#upstream-implementation-references)

---

## Overview

### Problem Statement

In baseline DRA structured parameters, each device listed in a ResourceSlice is independently allocatable — allocating one device has no effect on the availability of other devices. This model cannot express **overlapping partitions**: multiple device entries that compete for the same underlying physical resources.

Real-world scenarios requiring overlapping partitions:

- **NVIDIA MIG (Multi-Instance GPU):** A single GPU can be partitioned into various profiles (1g.5gb, 2g.10gb, 3g.20gb, etc.). These partitions consume memory slices and compute resources from the same physical device. Allocating a 2g.10gb partition depletes the resources needed for certain 1g.5gb partitions, making them unallocatable.
- **TPU topology slices:** A pool of interconnected TPUs across multiple nodes can be allocated as various slice sizes (2x4, 4x4, 4x8). Allocating a 4x4 slice consumes the TPUs that would be needed for constituent 2x4 slices.
- **SR-IOV:** Virtual functions share bandwidth/queue resources from the same physical function.

### Solution

Partitionable devices introduces **SharedCounters** — named sets of numeric counters that represent the shared resources of a physical device. Each device declares the counters it will consume via `ConsumesCounters`. The scheduler tracks counter availability: when a device is allocated, its declared counters are deducted from the counter set, potentially making other devices unallocatable.

This is fundamentally **mutual exclusion through resource depletion** — overlapping partitions share counters, and allocation of one depletes what's available for others.

### Feature Gate

`DRAPartitionableDevices` — controls all new fields on kube-apiserver and kube-scheduler.

- Alpha: Kubernetes 1.33 (disabled by default)
- Beta: Kubernetes 1.36 (enabled by default)

### Key Distinction from Consumable Capacity

| Aspect | SharedCounters (KEP-4815) | Consumable Capacity (KEP-5075) |
|--------|--------------------------|-------------------------------|
| What it tracks | Structural resource budget across device variants | Runtime capacity across multiple allocations |
| When checked | Once at allocation time | On every allocation |
| Per what | Per device (binary: allocated or not) | Per allocation share (quantity-based) |
| Key question | "Can this device be allocated given what else is already allocated?" | "Is there enough remaining capacity for another share?" |
| Counter source | Explicit `ConsumesCounters` declaration | Implicit from request's `Capacity.Requests` |

A device can be BOTH partitionable (SharedCounters) AND multi-allocatable (consumable capacity). The checks are independent — both must pass for allocation to succeed.

---

## API Surface

### SharedCounters (on ResourceSliceSpec)

```
ResourceSliceSpec:
  SharedCounters: []CounterSet                    # NEW — counter sets for this slice
    CounterSet:
      Name: string                                # DNS label, unique within pool
      Counters: map[string]Counter                # max 32 per set
        Counter:
          Value: resource.Quantity                 # available amount
```

**Key semantics:**
- `SharedCounters` must be in a **separate ResourceSlice** from devices (mutually exclusive with `Devices` field)
- Counter sets are scoped to the resource pool (identified by `driver + pool.name`)
- Multiple ResourceSlices in the same pool can contribute counter sets
- Counter set names must be unique within the pool
- Maximum 8 counter sets per ResourceSlice

### ConsumesCounters (on Device)

```
Device:
  Name: string
  Attributes: map[QualifiedName]DeviceAttribute
  Capacity: map[QualifiedName]DeviceCapacity
  ConsumesCounters: []DeviceCounterConsumption    # NEW
    DeviceCounterConsumption:
      CounterSet: string                          # references CounterSet.Name in same pool
      Counters: map[string]Counter                # counters consumed when allocated
```

**Key semantics:**
- Each device declares the counters it will consume from referenced counter sets
- At most 2 counter set references per device
- Maximum 32 counters per `DeviceCounterConsumption`
- Total consumed counters across all devices in a ResourceSlice must not exceed 2048
- The referenced `CounterSet` must exist in the same resource pool (resolved at allocation time, not admission)
- When the device is allocated, its declared counters are subtracted from available counters
- When the device is deallocated (claim released), counters are restored

### PerDeviceNodeSelection

```
ResourceSliceSpec:
  PerDeviceNodeSelection: bool                    # NEW — node selection moved to devices

Device:
  NodeName: *string                               # which node has this device
  NodeSelector: *core.NodeSelector                # nodes where device is available
  AllNodes: *bool                                 # device available on all nodes
```

**Key semantics:**
- `PerDeviceNodeSelection: true` is mutually exclusive with slice-level `NodeName`/`NodeSelector`/`AllNodes`
- When set, each device independently declares its node affinity
- Enables a single ResourceSlice to contain devices spanning multiple nodes (multi-host devices)
- The `AllocationResult.NodeSelector` is derived from the allocated device's node selector, not the ResourceSlice's
- Must be published by a control-plane component (not a per-node DRA driver) since no single node "owns" the slice

---

## Allocation Algorithm

### Counter State Initialization

At the start of a scheduling cycle, the scheduler computes available counters for each counter set by:

1. **Load counter set definitions:** Sum all `SharedCounters` declarations across ResourceSlices in the pool → `totalCounters[counterSetName][counterName] = value`
2. **Deduct allocated devices:** For each allocated device (from ResourceClaim status), subtract its `ConsumesCounters` from the corresponding counter sets → `availableCounters = totalCounters - consumedByAllocated`

```
availableCounters[pool][counterSet][counter] =
    counterSetDefinition[counter].value
    - Σ device.consumesCounters[counterSet][counter]
        for all device in allocatedDevicesInPool
```

### Device Eligibility Check

When the scheduler considers allocating a device, it verifies counter availability:

```
For each entry in device.consumesCounters:
  counterSetName = entry.counterSet
  For each (counterName, amount) in entry.counters:
    if amount > availableCounters[pool][counterSetName][counterName]:
      REJECT device (insufficient counters)
```

If the counter check (plus all other checks: selectors, constraints, capacity) passes, the device can be allocated.

### Transactional Counter Tracking

Within a scheduling cycle, counter state must reflect in-progress allocations:

1. **On tentative allocation:** Deduct the device's consumed counters from `availableCounters`
2. **On backtrack:** Restore the device's consumed counters to `availableCounters`
3. **On finalization:** Counter deductions become permanent for the duration of the scheduling cycle

This mirrors the DFS backtracking pattern used for exclusive device allocation and consumable capacity tracking.

### Pool Validation

The allocator performs runtime validation before using a pool (since cross-ResourceSlice references cannot be validated at admission):

1. Device names are unique within the pool
2. Counter set names are unique within the pool
3. Referenced counter sets in `ConsumesCounters` exist in the pool
4. Referenced counter names within counter sets exist in the counter set definition

If any validation fails, the **entire pool** is marked invalid — no devices from that pool can be allocated. This prevents partial/inconsistent allocation from buggy drivers.

---

## Use Cases

### GPU Partitioning (NVIDIA MIG)

A single A100 GPU is represented as a counter set containing all its physical resources. Multiple device entries represent the possible MIG profiles:

**Counter set (separate ResourceSlice):**
```yaml
sharedCounters:
- name: gpu-0-counters
  counters:
    memory-slices: { value: "8" }
    multiprocessors: { value: "98" }
    copy-engines: { value: "7" }
    decoders: { value: "5" }
    jpeg-engines: { value: "1" }
    ofa-engines: { value: "1" }
```

**Devices (separate ResourceSlice, same pool):**
```yaml
devices:
- name: gpu-0-full          # Full GPU
  consumesCounters:
  - counterSet: gpu-0-counters
    counters:
      memory-slices: { value: "8" }
      multiprocessors: { value: "98" }
      copy-engines: { value: "7" }
      decoders: { value: "5" }
      jpeg-engines: { value: "1" }
      ofa-engines: { value: "1" }

- name: gpu-0-mig-3g.20gb-0   # Partition: slots 0-3
  consumesCounters:
  - counterSet: gpu-0-counters
    counters:
      memory-slices: { value: "4" }
      multiprocessors: { value: "42" }
      copy-engines: { value: "3" }
      decoders: { value: "2" }

- name: gpu-0-mig-1g.5gb-0    # Partition: slot 0
  consumesCounters:
  - counterSet: gpu-0-counters
    counters:
      memory-slices: { value: "1" }
      multiprocessors: { value: "14" }
      copy-engines: { value: "1" }
```

Allocating `gpu-0-mig-3g.20gb-0` deducts 4 memory-slices, making `gpu-0-full` (needs 8) unallocatable. But `gpu-0-mig-1g.5gb-0` (needs 1 memory-slice from remaining 4) remains allocatable.

### Multi-Host TPU Slices

A 4x4 TPU topology (16 TPUs across 4 nodes) with valid slice sizes:

**Counter set:**
```yaml
sharedCounters:
- name: tpu-counters
  counters:
    tpus-node-1: { value: "4" }
    tpus-node-2: { value: "4" }
    tpus-node-5: { value: "4" }
    tpus-node-6: { value: "4" }
```

**Devices (with per-device node selection):**
```yaml
perDeviceNodeSelection: true
devices:
- name: tpu-4x4              # Full 16 TPUs across 4 nodes
  nodeSelector: { nodes: [1,2,5,6] }
  consumesCounters:
  - counterSet: tpu-counters
    counters:
      tpus-node-1: { value: "4" }
      tpus-node-2: { value: "4" }
      tpus-node-5: { value: "4" }
      tpus-node-6: { value: "4" }

- name: tpu-2x4-top          # 8 TPUs on nodes 1,2
  nodeSelector: { nodes: [1,2] }
  consumesCounters:
  - counterSet: tpu-counters
    counters:
      tpus-node-1: { value: "4" }
      tpus-node-2: { value: "4" }

- name: tpu-2x2-node-1       # 4 TPUs on node 1
  nodeName: node-1
  consumesCounters:
  - counterSet: tpu-counters
    counters:
      tpus-node-1: { value: "4" }
```

Allocating `tpu-2x4-top` depletes counters for nodes 1 and 2, making `tpu-4x4` and `tpu-2x2-node-1` unallocatable — but leaving `tpu-2x4-bottom` (nodes 5,6) available.

---

## Interaction with Other Features

### With Consumable Capacity (KEP-5075)

SharedCounters and Capacity serve different purposes and are checked independently:

- **SharedCounters:** "Can this device be allocated at all, given existing device allocations in the pool?" — binary, per-device
- **Capacity:** "Is there enough remaining capacity on this multi-allocatable device for another share?" — quantitative, per-allocation

A device can declare both `ConsumesCounters` (it shares resources with other device variants) AND `AllowMultipleAllocations: true` with `Capacity` (it can serve multiple claims). Both checks must pass independently.

**Example:** A MIG profile device (partitioned from the GPU via counters) could also be multi-allocatable (serving multiple pods with capacity-tracked bandwidth shares). Allocation requires: (1) sufficient GPU counters for the MIG profile, AND (2) sufficient remaining capacity on the MIG device for the new share.

### With Exclusive Devices (Baseline)

Exclusive devices with no `ConsumesCounters` are unaffected — the counter check is skipped. SharedCounters only apply to devices that explicitly declare counter consumption.

### With DistinctAttribute Constraint

DistinctAttribute and SharedCounters are orthogonal. A set of partition devices might all share a counter set but have distinct attribute values. The DistinctAttribute constraint prevents allocating two devices with the same attribute value; SharedCounters prevents allocating devices whose combined counter consumption exceeds the budget. Both can apply simultaneously.

### With PerDeviceNodeSelection and Multi-Host Scheduling

Multi-host devices introduce a scheduling challenge: a single device spans multiple nodes. The `AllocationResult.NodeSelector` is set from the allocated device's node selector (not the ResourceSlice's), which constrains all pods sharing the ResourceClaim to run only on nodes within the device's scope.

DRA does NOT provide gang scheduling guarantees — it only restricts scheduling to valid nodes. Multiple pods sharing a multi-host ResourceClaim may fail to schedule if individual nodes lack other resources (CPU, memory). Higher-level frameworks must handle this.

### With Pool Completeness

Counter sets span multiple ResourceSlices within a pool. The scheduler must observe ALL ResourceSlices in a pool (matching `pool.resourceSliceCount`) before evaluating counter availability. An incomplete pool (missing slices) cannot be safely used because:
- Counter set definitions might be missing (underestimating total)
- Device entries might be missing (underestimating consumption)

---

## Upstream Implementation References

All paths relative to `k8s.io/dynamic-resource-allocation@v0.35.0`:

| File | Contents |
|------|----------|
| `structured/internal/experimental/allocator_experimental.go` | Counter state init (`checkAvailableCounters`), counter deduction on allocate, counter restore on backtrack |
| `structured/internal/experimental/pool.go` | Pool-level counter set storage, cross-ResourceSlice counter set gathering |
| `api/types.go` | `CounterSet`, `DeviceCounterConsumption`, `Counter`, `PerDeviceNodeSelection` type definitions |
| `resourceslice/tracker/tracker.go` | `EnablePartitionableDevices` feature flag integration |

### Upstream Bug Fix: PR #139040 (May 2026)

`checkAvailableCounters()` rebuilds counter consumption by iterating allocated devices. It originally only checked `AllocatedDevices.Has(deviceID)`, missing multi-allocatable devices tracked in `AggregatedCapacity`/`AllocatedSharedDeviceIDs`. The fix uses `IsDeviceAllocated()` which checks all three allocation sources.

**Relevance:** When implementing SharedCounters alongside consumable capacity, counter rebuilding must query both exclusive and shared allocation state to correctly compute remaining counter budgets.
