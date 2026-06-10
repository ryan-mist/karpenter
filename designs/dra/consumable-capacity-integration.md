# Consumable Capacity Integration

## Table of Contents

- [Overview](#overview)
- [Scope](#scope)
- [Device Model Changes](#device-model-changes)
  - [cloudprovider.Device Extension](#cloudproviderdevice-extension)
  - [API Server Slice Conversion](#api-server-slice-conversion)
  - [CEL Environment](#cel-environment)
- [Capacity Verification](#capacity-verification)
  - [Placement in tryDevice](#placement-in-trydevice)
  - [Rounding Logic](#rounding-logic)
  - [Default Consumption](#default-consumption)
  - [Backtracking](#backtracking)
- [Allocation Tracking](#allocation-tracking)
  - [IsAllocated Semantic Change](#isallocated-semantic-change)
  - [Preallocated Consumed Capacity](#preallocated-consumed-capacity)
  - [Inflight Consumed Capacity](#inflight-consumed-capacity)
  - [Cross-NodeClaim Sharing](#cross-nodeclaim-sharing)
  - [Commit Protocol Extension](#commit-protocol-extension)
- [DistinctAttribute Constraint](#distinctattribute-constraint)
  - [Interface Implementation](#interface-implementation)
  - [Scoping and Evaluation](#scoping-and-evaluation)
  - [Interaction with Existing Constraints](#interaction-with-existing-constraints)
- [Request Validation](#request-validation)
  - [Parsing CapacityRequests](#parsing-capacityrequests)
  - [Per-Device Validation](#per-device-validation)
- [Commit Protocol and Results](#commit-protocol-and-results)
  - [ShareID Generation](#shareid-generation)
  - [ConsumedCapacity Recording](#consumedcapacity-recording)
  - [Metadata Extensions](#metadata-extensions)
- [Controller Changes](#controller-changes)
  - [Consumed Capacity Aggregation](#consumed-capacity-aggregation)
  - [Shared vs Exclusive Discrimination](#shared-vs-exclusive-discrimination)
  - [Public API Change](#public-api-change)
- [Key Design Decisions](#key-design-decisions)
- [Implementation Sequencing](#implementation-sequencing)

---

## Overview

### Problem Statement

Karpenter's DRA allocator treats every device as exclusively allocated — a device is either fully consumed by one ResourceClaim or entirely free. KEP-5075 (Consumable Capacity) introduces **multi-allocatable devices** that can serve multiple independent ResourceClaims, each consuming a portion of the device's named capacity dimensions.

Integrating consumable capacity into Karpenter's allocator presents three challenges unique to our scheduling model:

1. **Instance type superposition.** A NodeClaim is compatible with multiple instance types, each potentially providing multi-allocatable template devices with different capacity values. The allocator must verify capacity independently per instance type and prune those that cannot satisfy the request — matching how exclusive devices are handled today, but with the added dimension of capacity accounting.

2. **Transient capacity tracking during DFS.** The allocator uses a backtracking DFS to explore device assignments. When a multi-allocatable device is tentatively allocated at one DFS node, subsequent nodes in the same subtree must see the reduced capacity. On backtrack, the capacity must be restored. This is analogous to how `allocatedDevices` tracks binary allocation during the DFS, but requires quantity-based accounting rather than set membership.

3. **Cross-NodeClaim capacity contention.** Multiple pods (potentially bound to different NodeClaims) may consume capacity on the same in-cluster multi-allocatable device. The allocator must track cumulative consumed capacity across scheduling loop iterations (across `Allocate()` calls) so that subsequent pods correctly see reduced capacity.

### Key References

| Reference | Path |
|-----------|------|
| Upstream KEP semantics | `designs/dra/consumable-capacity.md` |
| Core allocator design | `designs/dra/scheduling.md` |
| Upstream implementation | `k8s.io/dynamic-resource-allocation@v0.35.0/structured/internal/experimental/` |
| Device model | `pkg/cloudprovider/dynamicresources.go` |
| Allocator DFS | `pkg/scheduling/dynamicresources/allocator.go` |
| Allocation tracker | `pkg/scheduling/dynamicresources/allocationtracker.go` |
| Constraint system | `pkg/scheduling/dynamicresources/constraint.go` |
| Device allocation controller | `pkg/controllers/dynamicresources/deviceallocation/controller.go` |

---

## Scope

### In Scope

- Capacity verification for `ExactDeviceRequest` (both ExactCount and All modes)
- `AllowMultipleAllocations` device field recognition
- `RequestPolicy` rounding (Default, ValidValues, ValidRange)
- Transient capacity tracking during DFS with backtracking
- Cross-pod consumed capacity tracking (across `Allocate()` calls)
- `DistinctAttribute` constraint type
- `ShareID` generation and `ConsumedCapacity` recording in allocation results
- Controller aggregation of consumed capacity from existing cluster allocations
- CEL expression support for `device.allowMultipleAllocations`

### Deferred

- **FirstAvailable / DeviceSubRequest:** Karpenter only supports `ExactDeviceRequest` today. Capacity on `DeviceSubRequest` is a follow-up tied to `FirstAvailable` support.
- **AdminAccess interaction:** Admin access bypasses capacity tracking entirely (out of scope in baseline allocator).
- **Partitionable devices (SharedCounters):** Orthogonal feature, not gated by `DRAConsumableCapacity`.
- **Device taints:** Separate feature, no interaction with capacity semantics.

---

## Device Model Changes

### cloudprovider.Device Extension

The `cloudprovider.Device` struct currently carries only `Name` and `Attributes`:

```go
// Current (pkg/cloudprovider/dynamicresources.go:58-64)
type Device struct {
    Name       unique.Handle[string]
    Attributes map[resourcev1.QualifiedName]resourcev1.DeviceAttribute
}
```

We extend it with capacity metadata:

```go
type Device struct {
    Name                    unique.Handle[string]
    Attributes              map[resourcev1.QualifiedName]resourcev1.DeviceAttribute
    Capacity                map[resourcev1.QualifiedName]resourcev1.DeviceCapacity
    AllowMultipleAllocations bool
}
```

Both fields are populated from `resourcev1.Device` during slice conversion. For exclusive devices, `AllowMultipleAllocations` is `false` and `Capacity` may be nil or empty — neither changes existing behavior.

Template devices (from cloud provider `ResourceSliceTemplate`) use the same struct and may also declare capacity for multi-allocatable template devices.

### API Server Slice Conversion

The `apiServerSlice.Devices()` method (`types.go:127-142`) currently discards capacity. It is extended to populate the new fields:

```go
s.devices[i] = cloudprovider.Device{
    Name:                     unique.Make(d.Name),
    Attributes:               attrs,
    Capacity:                 d.Capacity,                          // NEW
    AllowMultipleAllocations: d.AllowMultipleAllocations != nil &&
                              *d.AllowMultipleAllocations,         // NEW
}
```

### CEL Environment

The CEL evaluation environment for device selectors must expose the new `device.allowMultipleAllocations` boolean property. This enables DeviceClass selectors to filter for only multi-allocatable devices:

```yaml
selectors:
  - cel:
      expression: "device.allowMultipleAllocations == true"
```

The `DeviceMatchesSelectors` helper in `request.go` must pass the new fields to the `dracel.Device{}` struct used for CEL evaluation.

---

## Capacity Verification

### Placement in tryDevice

The capacity verification check inserts into `tryDevice` (`allocator.go:648-739`) between the existing `IsAllocated` check and the CEL selector match:

```
Current flow:
  1. IsAllocated check         → skip if already consumed
  2. Selector match (CEL)      → skip if device doesn't match
  3. Constraints               → skip if constraint violated
  4. Topology compatibility    → skip if requirements incompatible
  5. Record + recurse          → backtrack on failure

New flow:
  1.  IsAllocated check        → for multi-alloc devices, returns false (see §Allocation Tracking)
  1b. Capacity verification    → NEW: reject if insufficient remaining capacity
  2.  Selector match (CEL)     → unchanged
  3.  Constraints              → unchanged (+ DistinctAttribute)
  4.  Topology compatibility   → unchanged
  5.  Record + recurse
  5b. On record: track consumed capacity  → NEW
  5c. On backtrack: restore consumed capacity → NEW
```

The capacity check runs only for devices where `AllowMultipleAllocations == true`. Exclusive devices are handled entirely by the binary `IsAllocated` check (unchanged behavior).

### Verification Logic

For each capacity dimension in the request:

```
consumed = calculateConsumedCapacity(request[dim], device.capacity[dim].requestPolicy)
totalUsed = preallocatedConsumed[device][dim] + allocatingCapacity[device][dim] + consumed
if totalUsed > device.capacity[dim].value:
    REJECT
```

Where:
- `preallocatedConsumed` — capacity already committed by existing allocations in the cluster (from controller) plus capacity committed by earlier pods in this scheduling loop (from top-level tracker)
- `allocatingCapacity` — capacity tentatively reserved by earlier allocations in the *same DFS tree* (current pod, current IT)
- `consumed` — the new allocation's contribution after rounding

### Rounding Logic

The `calculateConsumedCapacity` function applies the device's `RequestPolicy` to determine actual consumption:

1. **No request specified:** Use `RequestPolicy.Default` if set, otherwise full device capacity
2. **ValidValues:** Smallest valid value ≥ requested amount; fail if none exists
3. **ValidRange:** Round up to `Min + ⌈(request - Min) / Step⌉ × Step`; fail if exceeds Max
4. **No policy:** Use requested amount as-is

This logic mirrors upstream's `calculateConsumedCapacity` in `consumable_capacity.go` and can be ported directly.

### Default Consumption

When a request does not specify `Capacity.Requests` for a given dimension:

| Scenario | Consumed Amount |
|----------|----------------|
| Device has no capacity for the dimension | 0 (dimension not applicable) |
| Device has capacity, no RequestPolicy | Full device capacity (exclusive consumption of that dimension) |
| Device has capacity with `RequestPolicy.Default` | The default value |

### Backtracking

Capacity consumed during the DFS must be tracked and restored on backtrack, mirroring how `allocatedDevices` (a set) is modified:

**On successful allocation of a multi-allocatable device:**
```go
a.allocatingCapacity[deviceID] = addCapacity(a.allocatingCapacity[deviceID], consumed)
```

**On backtrack:**
```go
a.allocatingCapacity[deviceID] = subCapacity(a.allocatingCapacity[deviceID], consumed)
```

The consumed amount for the current allocation must be stored locally in the `tryDevice` stack frame (or on `deviceAllocationMetadata`) to enable correct restoration.

---

## Allocation Tracking

### IsAllocated Semantic Change

The current `IsAllocated()` method (`allocationtracker.go:142-179`) returns a binary answer: the device is either consumed or free. For multi-allocatable devices, this binary model doesn't apply — a device can be partially consumed and still available.

**Design decision:** `IsAllocated()` returns `false` for multi-allocatable devices that still have remaining capacity. The capacity verification step in `tryDevice` gates admission instead.

Specifically:
- **Exclusive devices** (unchanged): `IsAllocated()` returns `true` if the device appears in `PreallocatedDevices` or `InflightClusterAllocations`
- **Multi-allocatable devices**: `IsAllocated()` returns `false` (they are never "fully allocated" from a binary standpoint). The capacity check in `tryDevice` determines whether the device can accept the new allocation.

To implement this, `IsAllocated()` must be able to distinguish multi-allocatable from exclusive devices. This requires that the preallocated state (from the controller) carries the `AllowMultipleAllocations` flag, or that multi-allocatable devices are stored in a separate collection.

### Preallocated Consumed Capacity

The controller provides the allocator with the consumed capacity state from existing cluster allocations. This replaces (for multi-allocatable devices) the binary `PreallocatedDevices` set:

```go
type AllocationTracker struct {
    // Existing — exclusive devices from API server
    PreallocatedDevices sets.Set[DeviceID]

    // NEW — consumed capacity for multi-allocatable devices from API server
    PreallocatedConsumedCapacity map[DeviceID]map[resourcev1.QualifiedName]resource.Quantity

    // ... existing inflight fields ...

    // NEW — consumed capacity committed by earlier pods in this scheduling loop
    InflightConsumedCapacity map[DeviceID]map[resourcev1.QualifiedName]resource.Quantity
}
```

Multi-allocatable devices appear in `PreallocatedConsumedCapacity` rather than `PreallocatedDevices`. A device with zero consumed capacity (all claims released) does not appear in either map.

### Inflight Consumed Capacity

As pods are scheduled within a single scheduling loop, capacity committed by earlier `Allocate()` calls must be visible to later calls. This is tracked on `InflightConsumedCapacity` at the top-level `AllocationTracker`:

- **On Commit():** The child allocator's per-IT consumed capacity is merged into `InflightConsumedCapacity`
- **On ReleaseInstanceTypes():** If a committed allocation's instance type is pruned, its consumed capacity contribution must be subtracted from `InflightConsumedCapacity`

### Cross-NodeClaim Sharing

Multi-allocatable in-cluster devices can be shared across NodeClaims — this is a fundamental difference from exclusive devices. For exclusive devices, `IsAllocated` returns `true` when a device is allocated for a *different* NodeClaim (pessimistic assumption). For multi-allocatable devices, cross-NodeClaim sharing is permitted as long as capacity remains — the capacity check handles correctness.

The `InflightConsumedCapacity` map is global (not per-NodeClaim) because capacity consumed by any NodeClaim reduces the total available to all others. This matches the physical reality: if NC-A consumes 40/100 bandwidth on a device, NC-B sees only 60 remaining regardless of which instance type either collapses to.

### Commit Protocol Extension

The existing `allocation.Commit()` (`allocator.go:181-201`) calls `allocationTracker.Commit(a)` and registers per-IT device allocations. For consumable capacity:

1. Multi-allocatable device IDs are recorded in `InflightConsumedCapacity` rather than `InflightClusterAllocations`
2. Per-device consumed capacity (computed during the DFS) is summed into the tracker's map
3. The `InflightClusterAllocationsByNodeClaim` inverse index is extended to also track multi-allocatable devices (needed for `ReleaseInstanceTypes`)

---

## DistinctAttribute Constraint

### Interface Implementation

`DistinctAttribute` is a new constraint type implementing the existing `Constraint` interface (`constraint.go:31-37`). It enforces that all devices allocated for the constrained requests have **unique** values for the named attribute — the inverse of `MatchAttributeConstraint` which requires values to be **equal**.

```go
type DistinctAttributeConstraint struct {
    RequestNames  sets.Set[string]
    AttributeName resourcev1.FullyQualifiedName
    // allocatedValues tracks attribute values in insertion order.
    // Used for duplicate detection on Add() and LIFO removal on Remove().
    allocatedValues []resourcev1.DeviceAttribute
}
```

**Add() logic:**
1. If constraint doesn't apply to this request name → return `true` (no-op)
2. Look up the named attribute on the device via `LookupAttribute`
3. If attribute absent → return `false` (device cannot participate in uniqueness check)
4. Iterate `allocatedValues` — if any entry matches the new value → return `false` (duplicate)
5. Append value to `allocatedValues` → return `true`

**Remove() logic:**
1. Pop last entry from `allocatedValues` (LIFO, matching DFS backtracking order)

### Upstream Bug: Map-Keyed State

The upstream implementation (`k8s.io/dynamic-resource-allocation@v0.35.0/structured/internal/experimental/constraint.go:44-84`) keys its constraint state by `requestName` in a `map[string]DeviceAttribute`. This causes a bug for requests with `count > 1`:

**Example:** A pod requests 2Gi bandwidth on 3 NIC shares with `distinctAttribute` on device-name to ensure distinct physical NICs:

```yaml
requests:
- name: nics
  exactly:
    deviceClassName: multi-homed-nic
    count: 3
    capacity:
      requests:
        networking.example.com/bandwidth: "2Gi"
constraints:
- requests: ["nics"]
  distinctAttribute: "networking.example.com/device-name"
```

With 3 NICs (nic-0: "nic-0", nic-1: "nic-1", nic-2: "nic-0"):

```
Slot 0: add("nics", nic-0) → map["nics"] = "nic-0"
Slot 1: add("nics", nic-1) → "nic-1" ≠ "nic-0" → accept
         map["nics"] = "nic-1"  ← OVERWRITES "nic-0"
Slot 2: add("nics", nic-2) → "nic-0" ≠ "nic-1" → accept ← BUG
         (map forgot slot 0 already used "nic-0")
```

All upstream tests avoid this by using separate requests each with `count: 1` (giving unique map keys). The bug is latent but real for `count > 1`.

**Our fix:** Use a slice (`allocatedValues []DeviceAttribute`) instead of a map. Each Add appends, each Remove pops. All prior values remain visible for duplicate checking regardless of whether slots share a request name.

### Template Device Support

DistinctAttribute works with template devices — there is no in-cluster limitation. The mechanism depends on the attribute being checked:

**For device-name (primary use case):** `LookupAttribute` is extended to synthesize a `resource.k8s.io/device-name` attribute from `device.Name` when that well-known key is requested. Device names are structurally unique within a pool (pool validation rejects duplicates), so this is always sound and requires no cloud provider changes.

```go
// In LookupAttribute, synthesize device-name:
if attributeName == "resource.k8s.io/device-name" {
    v := deviceID.Device.Value()
    return &resourcev1.DeviceAttribute{StringValue: &v}
}
```

**For topology attributes (physical-port, NUMA, etc.):** Cloud providers publish concrete attribute values on template devices. If a provider knows "eni-0 is on port-A and eni-1 is on port-B," it knows the values and should publish them as attributes.

**Why not inverse-of-bindings:** We explored using the inverse of `AttributeBindings` (which declare "these devices share a value") to infer distinctness. This is rejected because:
- Distinctness is **not transitive**: A≠B and B≠C does NOT imply A≠C (unlike sameness: A=B, B=C → A=C)
- Cannot build transitive closure like AttributeBindings does — would need O(N²) pairwise storage
- In practice, if the cloud provider knows devices are distinct, it knows their values

### Scoping and Evaluation

Like `MatchAttributeConstraint`, `DistinctAttribute` supports request scoping via the `Requests` field on `DeviceConstraint`. When `Requests` is empty, the constraint applies to all requests in the claim.

The constraint is evaluated in the existing constraint loop in `tryDevice` (`allocator.go:679-689`) alongside `MatchAttributeConstraint`. No changes to the constraint evaluation mechanism are needed.

### Interaction with Existing Constraints

`DistinctAttribute` and `MatchAttribute` can coexist on the same claim targeting different attributes. For example:
- `MatchAttribute: gpu.example.com/numa` — all devices must share NUMA node
- `DistinctAttribute: resource.k8s.io/device-name` — all devices must be distinct

There is no interaction between the two constraint types beyond both being evaluated in the same loop.

**No binding fallback needed.** Unlike `MatchAttribute` (which supports runtime-only attributes via `AttributeBindingFallback`), `DistinctAttribute` requires the attribute to be present on the device (either published explicitly or synthesized for `resource.k8s.io/device-name`). If a device lacks the attribute, it is simply rejected.

---

## Request Validation

### Parsing CapacityRequests

The `RequestData` struct (`request.go:40-61`) is extended with a capacity field:

```go
type RequestData struct {
    // ... existing fields ...

    // CapacityRequests contains the per-dimension capacity requirements from
    // ExactDeviceRequest.Capacity.Requests. nil when no capacity is requested.
    CapacityRequests map[resourcev1.QualifiedName]resource.Quantity
}
```

In `validateExactRequest()` (`request.go:169+`), the field is populated from the claim spec:

```go
if req.Capacity != nil {
    rd.CapacityRequests = req.Capacity.Requests
}
```

### Per-Device Validation

Capacity dimension names are **not** pre-validated during request validation. A request may reference capacity dimensions that don't exist on all candidate devices — only on some. This is valid: a device that doesn't have a requested capacity dimension simply doesn't support the request and is skipped during `tryDevice`.

This matches the upstream pattern where `requestsContainNonExistCapacity` is checked per-device during allocation, not at claim validation time.

### Construction in ValidateClaimRequest

The constraint construction in `ValidateClaimRequest` (`request.go:87-99`) is extended to handle `DistinctAttribute`:

```go
for _, c := range claim.Spec.Devices.Constraints {
    switch {
    case c.MatchAttribute != nil:
        // ... existing code ...
    case c.DistinctAttribute != nil:
        dac := &DistinctAttributeConstraint{
            RequestNames:  sets.New(c.Requests...),
            AttributeName: resourcev1.QualifiedName(*c.DistinctAttribute),
        }
        data.Constraints = append(data.Constraints, dac)
    default:
        return nil, fmt.Errorf("claim %q: unsupported constraint type", claim.Name)
    }
}
```

---

## Commit Protocol and Results

### ShareID Generation

When a multi-allocatable device is allocated (capacity check passes), a UUID is generated as the `ShareID`. This uniquely identifies this allocation share among all shares on the device.

ShareID is generated in `tryDevice` at the point where the allocation is recorded (after all checks pass, before recursion). It is stored on `deviceAllocationMetadata` for later inclusion in the allocation result.

For exclusive devices, no ShareID is generated (nil).

### ConsumedCapacity Recording

The actual consumed capacity (after rounding) for each dimension is recorded alongside the device allocation. This value is computed during the capacity verification step and stored on the metadata for inclusion in the final allocation result.

### Metadata Extensions

The `deviceAllocationMetadata` struct (`allocator.go:452-456`) is extended:

```go
type deviceAllocationMetadata struct {
    claimIndex       int
    deviceWithID     DeviceWithID
    shareID          *types.UID                                       // NEW
    consumedCapacity map[resourcev1.QualifiedName]resource.Quantity   // NEW
}
```

The `ResourceClaimAllocationMetadata.Devices` field (currently `map[InstanceTypeID][]DeviceID`) is enriched to carry per-device metadata:

```go
type DeviceAllocationResult struct {
    DeviceID         DeviceID
    ShareID          *types.UID
    ConsumedCapacity map[resourcev1.QualifiedName]resource.Quantity
}

// Devices now carries richer allocation metadata per instance type.
Devices map[InstanceTypeID][]DeviceAllocationResult
```

This enables downstream consumers (integration tests, finalization logic) to construct proper `DeviceRequestAllocationResult` objects with ShareID and ConsumedCapacity.

---

## Controller Changes

### Consumed Capacity Aggregation

The device allocation controller (`pkg/controllers/dynamicresources/deviceallocation/controller.go`) currently tracks which devices are allocated from `ResourceClaim.Status.Allocation.Devices.Results`. For consumable capacity, it must also aggregate per-device consumed capacity across all claims.

In `reconcileClaim` (`controller.go:120-174`), for each result:
- If `result.ShareID` is non-nil → shared allocation: accumulate `result.ConsumedCapacity` into a per-device capacity map
- If `result.ShareID` is nil → exclusive allocation: add to `allocatedDevices` set (unchanged)

New controller state:

```go
type Controller struct {
    // ... existing fields ...

    // consumedCapacity aggregates consumed capacity per device across all shared allocations.
    consumedCapacity map[cloudprovider.DeviceID]map[resourcev1.QualifiedName]resource.Quantity

    // consumedCapacityPerClaim tracks each claim's contribution for accurate subtraction on deallocation.
    consumedCapacityPerClaim map[types.NamespacedName]map[cloudprovider.DeviceID]map[resourcev1.QualifiedName]resource.Quantity
}
```

### Shared vs Exclusive Discrimination

The discriminant is `DeviceRequestAllocationResult.ShareID`:
- `ShareID != nil` → shared allocation on a multi-allocatable device → tracked in `consumedCapacity`
- `ShareID == nil` → exclusive allocation → tracked in `allocatedDevices` (existing behavior)

This is stable: once the upstream scheduler marks an allocation as shared (by generating a ShareID), that status is permanent for the lifetime of the claim.

### Public API Change

The `NewAllocator` constructor (`allocator.go:128-145`) currently accepts:

```go
func NewAllocator(
    inClusterSlices []ResourceSlice,
    allocatedDevices sets.Set[cloudprovider.DeviceID],
    attributeBindings AttributeBindings,
    kubeClient client.Client,
) *Allocator
```

The `allocatedDevices` parameter is extended to also carry consumed capacity:

```go
type AllocatedDeviceState struct {
    // ExclusiveDevices contains devices that are exclusively allocated (one claim owns them).
    ExclusiveDevices sets.Set[cloudprovider.DeviceID]
    // ConsumedCapacity maps multi-allocatable devices to their aggregated consumed capacity.
    ConsumedCapacity map[cloudprovider.DeviceID]map[resourcev1.QualifiedName]resource.Quantity
}

func NewAllocator(
    inClusterSlices []ResourceSlice,
    allocatedState AllocatedDeviceState,
    attributeBindings AttributeBindings,
    kubeClient client.Client,
) *Allocator
```

The controller exposes this via a new method that returns both pieces (or via the existing `AllocatedDevices()` extended to return the new struct).

---

## Key Design Decisions

### 1. IsAllocated returns false for multi-allocatable devices

**Decision:** Multi-allocatable devices bypass the binary `IsAllocated` check entirely. Capacity verification in `tryDevice` gates admission.

**Rationale:** The binary model cannot express "partially consumed." Rather than adding a third state to `IsAllocated`, we cleanly separate the two models: exclusive devices use the fast binary check, multi-allocatable devices use the capacity check. This avoids complicating the hot path for the common (exclusive) case.

**Alternative considered:** Extend `IsAllocated` to return a tri-state (allocated, available, check-capacity). Rejected because it conflates two orthogonal concerns and requires all callers to handle the new state.

### 2. allocatingCapacity lives on the child allocator, reset per-IT

**Decision:** The `allocatingCapacity` map lives on the child `allocator` struct alongside `allocatedDevices` and is reset in `restoreState()` between IT attempts.

**Rationale:** Each instance type represents an alternative universe — the NodeClaim will collapse to exactly one IT. Capacity consumed during IT-A's DFS doesn't coexist with IT-B's. This matches how `allocatedDevices` works today (reset per-IT). Only capacity from a *previous* pod's committed allocation persists across IT evaluations (via the top-level tracker).

### 3. Cross-NodeClaim capacity sharing is implicit

**Decision:** Multi-allocatable devices can be shared across NodeClaims without any explicit cross-NodeClaim coordination. The `InflightConsumedCapacity` map on the top-level tracker is global (not per-NodeClaim).

**Rationale:** Unlike exclusive devices (where allocation for NC-A means NC-B can't use it), multi-allocatable devices allow concurrent use as long as capacity permits. The capacity check naturally handles this — if NC-A commits 40/100, NC-B sees 60 remaining. No special "is this device owned by another NodeClaim?" logic is needed.

### 4. Controller uses ShareID as the discriminant

**Decision:** `result.ShareID != nil` → shared allocation tracked in consumed capacity; `result.ShareID == nil` → exclusive tracked in allocated devices set.

**Rationale:** ShareID is the canonical upstream signal that an allocation is shared. It's set by the scheduler at allocation time and immutable for the claim's lifetime. Using it avoids needing to cross-reference the device's `AllowMultipleAllocations` flag (which could change after allocation).

### 5. Per-device capacity validation (no pre-validation of dimension names)

**Decision:** Don't pre-validate that `Capacity.Requests` dimension names exist on any candidate device during `ValidateClaimRequest`. Validate per-device during `tryDevice`.

**Rationale:** A request may target a capacity dimension that exists on some devices but not others. This is valid — devices without the dimension are simply ineligible. Pre-validation would require iterating all devices at validation time, which is expensive and doesn't match the upstream pattern.

### 6. No binding fallback for DistinctAttribute

**Decision:** If a device lacks the attribute referenced by a `DistinctAttribute` constraint, it is rejected. No `AttributeBindingFallback` path.

**Rationale:** The primary use case for `DistinctAttribute` is preventing the same multi-allocatable device from being selected multiple times (`distinctAttribute: resource.k8s.io/device-name`). Device name is always known. Runtime-only attributes that need binding fallback don't have a meaningful "distinct" use case — you can't verify uniqueness for values you don't know.

### 7. Consumed capacity stored per allocation, not per device on metadata

**Decision:** `deviceAllocationMetadata` carries the consumed capacity for *this specific allocation*, not a reference to the device's total consumed state.

**Rationale:** Each allocation consumes a specific amount determined by the request and rounding. This amount must be recorded for backtracking (restore exactly what was consumed) and for the final result (populate `ConsumedCapacity` on the allocation result). Storing it per-allocation-entry is the natural fit.

### 8. Constraint.Reset() for IT transitions

**Decision:** Add a `Reset()` method to the `Constraint` interface. Call it in `restoreState()` before each IT attempt.

**Rationale:** When `dfs()` returns `true` (IT succeeds), `tryDevice` returns without running the backtrack block — constraints are left in their "fully allocated" state (pinned values, allocated device IDs). Since `restoreState()` does NOT reset constraints, the next IT inherits stale state. For `MatchAttributeConstraint`, this means the next IT is forced to match the previous IT's pinned attribute value even though it's a completely independent evaluation.

This is a **pre-existing bug** that affects `MatchAttributeConstraint` today — it is not specific to consumable capacity or DistinctAttribute. We fix it as part of this work because DistinctAttribute would be affected by the same issue.

**Alternative considered:** Clone `ClaimData` per-IT attempt instead of resetting. Rejected due to allocation overhead (constraints are rebuilt from scratch each time rather than cheaply cleared).

### 9. Synthesize device-name for DistinctAttribute

**Decision:** Extend `LookupAttribute` to synthesize a `resource.k8s.io/device-name` attribute from `device.Name` when that well-known key is requested. This enables DistinctAttribute to work with template devices without requiring cloud providers to redundantly publish device name as an attribute.

**Rationale:** Device names are structurally unique within a pool (pool validation rejects duplicates). The primary use case for DistinctAttribute is preventing same-device multi-allocation, which requires a device-identity attribute. Since names are always known (even on template devices), synthesizing the attribute is sound and requires zero cloud provider changes.

**Alternative considered:** Inverse-of-bindings (cloud provider declares "these devices are guaranteed different"). Rejected because distinctness is not transitive (A≠B, B≠C does NOT imply A≠C), so no closure is possible. In practice, if a provider knows devices are distinct, it knows their values and should publish them as attributes.

### 10. Slice-based state tracking for DistinctAttribute

**Decision:** Use an append/pop slice (`allocatedValues []DeviceAttribute`) for DistinctAttribute constraint state, not a `map[requestName]value`.

**Rationale:** The upstream implementation keys by `requestName`, which breaks for `count > 1` — all slots share the same key, causing map overwrites that destroy the history of previously seen values. Our slice approach preserves all values for duplicate checking and pops correctly during LIFO backtracking.

---

## Implementation Sequencing

The work items have dependencies that constrain ordering:

```
1. Device Model Changes
   └── No dependencies. Foundation for everything else.

2. Capacity Types & Verification Logic (new capacity.go)
   └── Depends on: (1) Device model for DeviceCapacity type

3. Controller Changes
   └── Depends on: (1) Device model, (2) capacity types

4. Allocation Tracker Changes
   └── Depends on: (2) capacity types

5. Request Validation (CapacityRequests parsing)
   └── Depends on: (1) Device model

6. DFS / tryDevice Integration
   └── Depends on: (2) verification logic, (4) tracker, (5) request parsing

7. DistinctAttribute Constraint
   └── Independent — can parallel with (2)-(6)

8. Commit Protocol & Result Extensions
   └── Depends on: (6) DFS integration
```

Suggested PR sequence:
1. Device model + capacity types + verification logic
2. Controller consumed capacity aggregation
3. Allocation tracker extensions + DFS integration
4. DistinctAttribute constraint (can parallel with 2-3)
5. Commit protocol + result metadata extensions
