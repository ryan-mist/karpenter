# DRA Consumable Capacity (KEP-5075)

## Table of Contents

- [Overview](#overview)
- [API Surface](#api-surface)
  - [Device Fields](#device-fields)
  - [ResourceClaim Request Fields](#resourceclaim-request-fields)
  - [Allocation Result Fields](#allocation-result-fields)
  - [Constraint Fields](#constraint-fields)
  - [CEL Extensions](#cel-extensions)
- [Allocation Algorithm](#allocation-algorithm)
  - [Capacity Aggregation](#capacity-aggregation)
  - [Capacity Verification](#capacity-verification)
  - [Transactional Tracking](#transactional-tracking)
  - [ShareID Generation](#shareid-generation)
  - [Default Consumption Logic](#default-consumption-logic)
- [RequestPolicy and Rounding](#requestpolicy-and-rounding)
  - [Default Policy](#default-policy)
  - [ValidValues Policy](#validvalues-policy)
  - [ValidRange Policy](#validrange-policy)
  - [Rounding Summary](#rounding-summary)
- [DistinctAttribute Constraint](#distinctattribute-constraint)
- [Device Lifecycle Transitions](#device-lifecycle-transitions)
- [Interaction with Other Features](#interaction-with-other-features)
- [Upstream Implementation References](#upstream-implementation-references)

---

## Overview

### Problem Statement

In the baseline DRA structured model, each device can only be allocated to a single ResourceClaim at a time. If multiple pods need access to the same physical device, they must reference the same ResourceClaim — which requires coordination, limits namespace isolation, and prevents independent lifecycle management.

This is insufficient for several real-world scenarios:

- **Network devices (CNI DRA drivers):** A NIC has finite bandwidth. Multiple pods should be able to independently request bandwidth shares without coordinating on a shared ResourceClaim.
- **Shared accelerators:** A GPU or FPGA with partitionable compute/memory should allow independent claims to consume portions of its capacity.
- **Storage bandwidth:** Storage controllers with limited IOPS should allow concurrent consumers.

### Solution

Consumable capacity introduces **multi-allocatable devices** — devices that can be allocated to multiple independent ResourceClaims simultaneously, with the scheduler tracking consumed capacity to ensure total usage stays within device limits.

Each allocation on a shared device:
1. Consumes a specific amount of one or more named capacities
2. Receives a unique `ShareID` identifying that allocation instance
3. Is independently tracked and releasable

### Feature Gate

`DRAConsumableCapacity` — controls all new fields across kube-scheduler, kubelet, and kube-apiserver.

- Alpha: Kubernetes 1.34 (disabled by default)
- Beta: Kubernetes 1.36 (enabled by default, PR #136611)
- Continues beta in Kubernetes 1.37 (KEP PR #6102)
- GA: planned (referenced in 1.37 KEP PR)

---

## API Surface

### Device Fields

On `resourcev1.Device` (within a ResourceSlice):

```
Device:
  Name: string
  AllowMultipleAllocations: *bool        # NEW — enables multi-allocation
  Capacity: map[QualifiedName]DeviceCapacity
    DeviceCapacity:
      Value: resource.Quantity
      RequestPolicy: *CapacityRequestPolicy  # NEW — consumption constraints
        CapacityRequestPolicy:
          Default: *resource.Quantity         # consumed when no request specified
          # Exactly one of ValidValues or ValidRange:
          ValidValues: []resource.Quantity    # up to 10 entries, sorted ascending
          ValidRange: *CapacityRequestPolicyRange
            Min: resource.Quantity            # required
            Max: *resource.Quantity           # optional
            Step: *resource.Quantity          # optional
```

**Key semantics:**
- `AllowMultipleAllocations: true` — device can serve multiple independent claims
- `AllowMultipleAllocations: false` or nil — device is exclusive (one allocation at a time, same as today)
- `Capacity` defines the device's total available resources (e.g., `bandwidth: 100Gbps`, `memory: 80Gi`)
- `RequestPolicy` constrains how consumers request from each capacity dimension

### ResourceClaim Request Fields

On `ExactDeviceRequest` (and `DeviceSubRequest` for FirstAvailable):

```
ExactDeviceRequest:
  DeviceClassName: string
  Selectors: [...]
  Count: int64
  Capacity: *CapacityRequirements          # NEW
    CapacityRequirements:
      Requests: map[QualifiedName]resource.Quantity  # required amounts per dimension
```

**Key semantics:**
- `Capacity.Requests` specifies how much of each capacity dimension the allocation needs
- If omitted, default consumption logic applies (see [Default Consumption Logic](#default-consumption-logic))
- Requested amounts are subject to rounding per the device's `RequestPolicy`

### Allocation Result Fields

On `DeviceRequestAllocationResult`:

```
DeviceRequestAllocationResult:
  Driver: string
  Pool: string
  Device: string
  ShareID: *types.UID                      # NEW — unique per allocation share
  ConsumedCapacity: map[QualifiedName]resource.Quantity  # NEW — actual consumed
```

On `AllocatedDeviceStatus` (kubelet/driver status correlation):

```
AllocatedDeviceStatus:
  Driver: string
  Pool: string
  Device: string
  ShareID: *string                         # NEW — correlates with allocation result
```

**Key semantics:**
- `ShareID` is a UUID generated for each allocation on a multi-allocatable device. It uniquely identifies this share among all allocations on the same device.
- The combination of Driver, Pool, Device, and ShareID must match between `DeviceRequestAllocationResult` and `AllocatedDeviceStatus`
- `ConsumedCapacity` records the actual consumed amounts (may differ from requested due to rounding)
- For exclusive (non-multi-allocatable) devices, `ShareID` is nil and `ConsumedCapacity` is empty

### Constraint Fields

On `DeviceConstraint`:

```
DeviceConstraint:
  Requests: []string                       # scope (existing)
  MatchAttribute: *FullyQualifiedName      # existing
  DistinctAttribute: *FullyQualifiedName   # NEW
```

**Key semantics:**
- `DistinctAttribute` requires all devices allocated for the constrained requests to have **unique** values for the named attribute
- Primary use case: prevent the same multi-allocatable device from being allocated multiple times within one claim (use the device name or a unique identifier attribute)

### CEL Extensions

New property available in device selector CEL expressions:

```
device.allowMultipleAllocations    // bool
```

Enables DeviceClass selectors to filter for only multi-allocatable devices:
```yaml
selectors:
  - cel:
      expression: "device.allowMultipleAllocations == true"
```

---

## Allocation Algorithm

### Capacity Aggregation

Before evaluating whether a device can satisfy a new allocation, the scheduler computes the device's **currently consumed capacity** by summing `ConsumedCapacity` from all existing allocation results that reference that device:

```
currentConsumed[device][capacityName] = Σ claim.status.allocation.devices.results[*].consumedCapacity[capacityName]
                                         where result.device == device
```

This aggregation happens once at the start of a scheduling cycle and produces a `ConsumedCapacityCollection` (a map from DeviceID to ConsumedCapacity).

### Capacity Verification

For each candidate device during allocation, the scheduler performs the **CmpRequestOverCapacity** check:

```
For each capacity dimension requested:
  consumed = calculateConsumedCapacity(request, device.capacity[name].requestPolicy)
  total_used = currentConsumed[device][name] + allocatingCapacity[device][name] + consumed
  if total_used > device.capacity[name].value:
    REJECT device (insufficient capacity)
```

Where:
- `currentConsumed` — capacity already committed by existing allocations in the cluster
- `allocatingCapacity` — capacity tentatively reserved by earlier allocations in the same scheduling cycle (not yet committed)
- `consumed` — the new allocation's contribution (after rounding per RequestPolicy)

All three must fit within the device's declared capacity.

### Transactional Tracking

Within a single scheduling cycle (one `Allocate()` call that may service multiple claims), the scheduler tracks in-flight allocations via `allocatingCapacity`:

1. **On tentative allocation:** Insert the calculated `consumedCapacity` for the device into `allocatingCapacity`
2. **On backtrack (DFS failure):** Remove the entry from `allocatingCapacity`
3. **On finalization:** `allocatingCapacity` entries become part of the allocation result's `ConsumedCapacity` fields

This ensures that multiple allocations targeting the same device within one scheduling cycle correctly account for each other without double-spending capacity.

### ShareID Generation

When a device with `AllowMultipleAllocations: true` is allocated, a new UUID is generated as the `ShareID`. This ID:
- Uniquely identifies this allocation share among all shares on the device
- Appears in `DeviceRequestAllocationResult.ShareID`
- Is used to correlate `AllocatedDeviceStatus` entries with allocation results
- Enables kubelet and DRA drivers to track per-share network data and state

For exclusive devices (`AllowMultipleAllocations: false`), no ShareID is generated.

### Default Consumption Logic

When a request does not specify `Capacity.Requests` for a given dimension:

| Scenario | Consumed Amount |
|----------|----------------|
| Device has no capacity defined for the dimension | 0 (not applicable) |
| Device has capacity but no RequestPolicy | Full device capacity (exclusive consumption) |
| Device has capacity with `RequestPolicy.Default` set | The default value |
| Device is multi-allocatable with capacity but no policy or default | Full device capacity |

The key principle: **if you don't ask for a specific amount, you get everything** (unless the driver explicitly sets a default). This preserves backwards compatibility — existing exclusive devices continue to consume their full capacity.

---

## RequestPolicy and Rounding

Drivers declare consumption constraints via `RequestPolicy` on each capacity dimension. This determines how requested amounts are adjusted before being recorded as consumed.

### Default Policy

```yaml
capacity:
  bandwidth:
    value: "100Gi"
    requestPolicy:
      default: "10Gi"
```

- If request specifies `bandwidth: 25Gi` → consumed = 25Gi (used as-is)
- If request omits bandwidth → consumed = 10Gi (policy default)
- If no RequestPolicy at all and request omits → consumed = 100Gi (full capacity)

### ValidValues Policy

```yaml
capacity:
  bandwidth:
    value: "100Gi"
    requestPolicy:
      validValues: ["10Gi", "25Gi", "50Gi", "100Gi"]
```

Rounding rule: **smallest valid value ≥ requested amount**

| Requested | Consumed | Reason |
|-----------|----------|--------|
| 8Gi | 10Gi | Rounds up to first valid value ≥ 8Gi |
| 10Gi | 10Gi | Exact match |
| 30Gi | 50Gi | Rounds up past 25Gi to 50Gi |
| 150Gi | **FAIL** | Exceeds all valid values |

If the request exceeds all ValidValues, allocation fails for that device.

### ValidRange Policy

```yaml
capacity:
  bandwidth:
    value: "100Gi"
    requestPolicy:
      validRange:
        min: "10Gi"
        max: "100Gi"    # optional
        step: "5Gi"     # optional
```

Rounding rule: **round up to Min + ⌈(request - Min) / Step⌉ × Step**

| Requested | With Step=5Gi, Min=10Gi | Reason |
|-----------|-------------------------|--------|
| 8Gi | 10Gi | Below Min, use Min |
| 10Gi | 10Gi | Equals Min |
| 12Gi | 15Gi | ⌈(12-10)/5⌉ × 5 + 10 = 15 |
| 23Gi | 25Gi | ⌈(23-10)/5⌉ × 5 + 10 = 25 |
| 105Gi | **FAIL** | Exceeds Max |

Without Step: request is used as-is if within [Min, Max]. Below Min → use Min. Above Max → fail.

Without Max: no upper bound from the range itself (device capacity is still the hard limit).

### Rounding Summary

```
calculateConsumedCapacity(request, policy):
  if request is nil:
    return policy.default (or full capacity if no default)
  if policy has validValues:
    return smallest value in validValues where value >= request
    (fail if none exists)
  if policy has validRange:
    if request < min: return min
    if max exists and request > max: FAIL
    if step exists: return min + ceil((request - min) / step) * step
    return request
  return request  # no policy constraints
```

---

## DistinctAttribute Constraint

`DistinctAttribute` is a new constraint type that enforces uniqueness of an attribute value across all devices allocated for the constrained requests.

### Semantics

```yaml
constraints:
  - requests: ["bandwidth-request"]
    distinctAttribute: "resource.k8s.io/device-name"
```

This ensures that within the scope of "bandwidth-request", every allocated device has a **different** value for the named attribute. If two candidate devices share the same attribute value, only one can be selected.

### List-Type Attributes (KEP-5491, post-v0.35.0)

KEP-5491 extends `DeviceAttribute` with list fields (`IntValues`, `BoolValues`, `StringValues`, `VersionValues`). This changes DistinctAttribute semantics by attribute type:

- **Scalar attributes:** "distinct" means values are not equal (unchanged)
- **List-type attributes:** "distinct" means pairwise disjoint sets (non-empty intersection → constraint violation)

This is not present in the vendored v0.35.0 code. When Karpenter upgrades the DRA dependency to a version containing KEP-5491, DistinctAttribute evaluation must be updated to handle list-type comparison.

### Primary Use Case

Preventing the same multi-allocatable device from being allocated multiple times to fill multiple slots in a single claim. Without this constraint, if a claim requests `count: 2` on a multi-allocatable device pool with one device, the allocator could try to fill both slots with the same device.

With `distinctAttribute` on a unique device identifier (like the device name), each slot must use a distinct device.

### Evaluation During DFS

The constraint is stateful (like MatchAttribute):
- **Add:** Record the attribute value. If it matches any previously recorded value → reject.
- **Remove (backtrack):** Remove the recorded value.

Unlike MatchAttribute (which requires all values to be **equal**), DistinctAttribute requires all values to be **different**.

---

## Device Lifecycle Transitions

### Dedicated → Multi-Allocatable

When a device's `AllowMultipleAllocations` changes from false/nil to true:
- Existing exclusive allocations remain valid
- The device accepts new shared allocations once existing claims are released
- While an exclusive allocation exists, no new allocations are possible (the exclusive allocation consumes all capacity)

### Multi-Allocatable → Dedicated

When `AllowMultipleAllocations` changes from true to false/nil:
- Existing shared allocations remain valid
- No new allocations are accepted until all existing shares are released
- Once empty, the device returns to exclusive behavior

### RequestPolicy Changes

- Apply only to future allocations
- Existing allocations retain their recorded `ConsumedCapacity`
- No rollback or adjustment of in-flight shares

---

## Interaction with Other Features

### With MatchAttribute Constraint

MatchAttribute and consumable capacity are orthogonal. A multi-allocatable device can participate in MatchAttribute constraints — the attribute value comparison works identically regardless of whether the device is shared.

### With Partitionable Devices (SharedCounters, KEP-4815)

SharedCounters and Capacity serve different purposes:
- **SharedCounters** are checked once at initial allocation time (structural constraint)
- **Capacity** is checked on every allocation (runtime consumption tracking)

A device can be both partitionable (SharedCounters) and multi-allocatable (Capacity). The checks are independent — both must pass.

### With All Mode

In All mode (`allocationMode: All`), all matching devices are allocated. For multi-allocatable devices in All mode:
- Each device is allocated once per request (All mode doesn't allocate the same device multiple times in one request)
- The capacity check still applies — the request must fit within remaining capacity
- Combined with DistinctAttribute, prevents double-allocation

### With AdminAccess

Devices with `adminAccess: true` on the claim bypass capacity tracking entirely — admin access implies exclusive control regardless of multi-allocatable status.

### With List-Type Attributes (KEP-5491)

List-type attributes change the semantics of `MatchAttribute` (scalar = equal, list = non-empty intersection) and `DistinctAttribute` (scalar = not-equal, list = pairwise disjoint). These changes apply uniformly to both exclusive and multi-allocatable devices. Not present in vendored v0.35.0 — forward-looking.

### With Node Allocatable Resource Mappings (KEP-5517)

`ResourceSlice.Spec.Devices.NodeAllocatableResourceMappings` maps device capacity dimensions to node-level resources (cpu, memory). This is architecturally adjacent to consumable capacity (both reason about device capacity dimensions) but operates at a different layer: node resource accounting vs. device-level scheduling. No direct interaction with the allocation algorithm — the mapping is consumed by kubelet for node status reporting.

### With Sharing Affinity (KEP-5981)

`SharingAffinity` acts as a structural gatekeeper before capacity subtraction — it constrains which existing shares a new allocation may coexist with on a multi-allocatable device. Still in development (PR #139507). The capacity verification step runs after sharing affinity passes.

---

## Upstream Implementation References

All paths relative to `k8s.io/dynamic-resource-allocation@v0.35.0`:

| File | Contents |
|------|----------|
| `structured/internal/experimental/consumable_capacity.go` | `CmpRequestOverCapacity()` function, `calculateConsumedCapacity()`, `roundUpRange()`, `roundUpValidValues()` |
| `structured/internal/experimental/allocator_experimental.go` | Main allocator loop, `allocatingCapacity` insert/remove, ShareID generation, device selection with capacity check |
| `structured/internal/experimental/constraint.go` | `distinctAttributeConstraint` type, Add/Remove logic |
| `structured/schedulerapi/types.go` | `ConsumedCapacity`, `ConsumedCapacityCollection`, `SharedDeviceID`, `AllocatedState` types |
| `structured/internal/allocatedstate.go` | `GenerateShareID()` utility, wrapper types |
| `api/types.go` | `Device.AllowMultipleAllocations`, `DeviceCapacity.RequestPolicy`, `CapacityRequestPolicy` |
| `resourceslice/tracker/tracker.go` | `EnableConsumableCapacity` feature flag integration |
