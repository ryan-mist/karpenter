# DRA Prioritized Alternatives (KEP-4816)

## Table of Contents

- [Overview](#overview)
- [API Surface](#api-surface)
  - [DeviceRequest (v1beta2)](#devicerequest-v1beta2)
  - [DeviceSubRequest](#devicesubrequest)
  - [Result Format](#result-format)
  - [Constraint and Config Referencing](#constraint-and-config-referencing)
  - [Validation Rules and Limits](#validation-rules-and-limits)
- [Allocation Algorithm](#allocation-algorithm)
  - [Sub-Request Iteration](#sub-request-iteration)
  - [State Management (Backtracking)](#state-management-backtracking)
  - [Scoring](#scoring)
- [Interaction with Other Features](#interaction-with-other-features)
- [Upstream Implementation References](#upstream-implementation-references)

---

## Overview

### Problem Statement

In baseline DRA, a `DeviceRequest` specifies exactly one device configuration: a single DeviceClass, selector set, and count. If a workload can use multiple types of devices (e.g., prefer an A100 GPU but can fall back to an H100, or prefer 4 GPUs but can work with 2), the user must create separate ResourceClaims or separate pods — preventing the scheduler from automatically finding the best available option.

Real-world scenarios requiring prioritized alternatives:

- **GPU preference with fallback:** A training workload prefers A100 80GB but can run (slower) on H100 or even 2× smaller GPUs.
- **Graceful degradation:** An inference workload prefers a single large accelerator but can shard across multiple smaller ones.
- **Cross-cluster portability:** A claim that works across clusters with different hardware by listing alternatives in preference order.
- **Cost optimization:** Prefer cheaper/more-available devices, fall back to expensive ones only when necessary.

### Solution

Prioritized alternatives introduces **FirstAvailable** — an ordered list of sub-requests within a single `DeviceRequest`. The scheduler tries each sub-request in priority order (first = highest priority), using the first one that can be satisfied. This enables automatic fallback without user intervention or multiple claims.

Each entry in the `FirstAvailable` list is a complete device request specification (class, selectors, count). Exactly one sub-request is selected per allocation — they do not combine.

### Feature Gate

`DRAPrioritizedList` — controls `FirstAvailable` fields on kube-apiserver, kube-scheduler, and kube-controller-manager. Depends on `DynamicResourceAllocation` being enabled.

- Alpha: Kubernetes 1.33
- Beta: Kubernetes 1.34
- GA: Kubernetes 1.36

---

## API Surface

### DeviceRequest (v1beta2)

```
DeviceRequest:
  Name: string                              # DNS label, used for constraint/config references
  # Exactly one of:
  Exactly: *SpecificDeviceRequest           # single device request (existing behavior)
  FirstAvailable: []DeviceSubRequest        # NEW — ordered alternatives, tried in priority order

SpecificDeviceRequest:
  DeviceClassName: string                   # required
  Selectors: []DeviceSelector              # max 32
  AllocationMode: DeviceAllocationMode     # ExactCount (default) or All
  Count: int64                             # default 1, used with ExactCount
  AdminAccess: *bool                       # featureGate=DRAAdminAccess
  # (consumable capacity fields, tolerations, etc.)
```

`Exactly` and `FirstAvailable` are mutually exclusive (`+oneOf=deviceRequestType`). One must be set.

### DeviceSubRequest

```
DeviceSubRequest:
  Name: string                              # DNS label, unique within parent's FirstAvailable list
  DeviceClassName: string                   # required
  Selectors: []DeviceSelector              # max 32
  AllocationMode: DeviceAllocationMode     # ExactCount (default) or All
  Count: int64                             # default 1, used with ExactCount
  # (consumable capacity fields, tolerations, etc.)
```

**Key differences from `SpecificDeviceRequest`:**
- Has its own `Name` field (used in constraint/config references)
- No `AdminAccess` field (admin access is not supported via prioritized alternatives)
- Cannot be nested (no recursive `FirstAvailable`)

### Result Format

When a sub-request is selected, the allocation result encodes which one via a qualified name:

```
DeviceRequestAllocationResult:
  Request: string    # format: "<parent-request-name>/<sub-request-name>"
```

For requests using `Exactly` (no alternatives), `Request` is just the request name (e.g., `"gpu"`).  
For requests using `FirstAvailable`, `Request` is `"<request>/<subrequest>"` (e.g., `"gpu/a100-preferred"`).

The `IsSubRequestRef()` helper identifies qualified references. DRA drivers must understand this format to correctly make devices available to containers.

### Constraint and Config Referencing

Constraints and config entries can reference requests at two granularities:

| Reference format | Meaning |
|-----------------|---------|
| `"gpu"` (parent name) | Applies regardless of which sub-request is chosen |
| `"gpu/a100-preferred"` (qualified) | Applies ONLY if that specific sub-request is chosen |

In the constraint matching logic, a constraint referencing the parent name matches ALL sub-requests of that parent:

```
constraint.matches("gpu", "a100-preferred") → true  (parent name matches)
constraint.matches("gpu/a100-preferred", "") → true  (exact sub-request match)
constraint.matches("gpu/h100-fallback", "") → false  (different sub-request)
```

In `PodSpec.containers[].resources.claims`, only the parent request name may be used — sub-request names are not valid there.

### Validation Rules and Limits

| Rule | Value |
|------|-------|
| Max entries in `FirstAvailable` | 8 (`FirstAvailableDeviceRequestMaxSize`) |
| Max selectors per sub-request | 32 (`DeviceSelectorsMaxSize`) |
| Sub-request `Name` | DNS label, unique within parent's list |
| `DeviceClassName` on sub-request | Required |
| `AdminAccess` on sub-request | Not supported (field absent) |
| Nesting | Not allowed (sub-requests cannot have their own `FirstAvailable`) |
| `Exactly` vs `FirstAvailable` | Mutually exclusive, one must be set |

**v1beta1 compatibility:** When `FirstAvailable` is set, `DeviceClassName` must NOT be set on the parent request. This allows older schedulers to detect claims they cannot handle (missing `DeviceClassName` → error rather than silent misallocation).

**ResourceQuota:** Quota is enforced for ALL sub-requests under every `FirstAvailable` list. The user must have quota for every possible alternative, not just the one eventually selected.

---

## Allocation Algorithm

### Sub-Request Iteration

The allocator processes `FirstAvailable` as an additional iteration level in the DFS. For a request with sub-requests:

```
allocateOne(requestIndices, allocateSubRequest=false):
  if request has sub-requests AND NOT allocateSubRequest:
    for subRequestIndex = 0, 1, 2, ...:
      set r.subRequestIndex = subRequestIndex
      success = allocateOne(r, allocateSubRequest=true)
      if success:
        record selectedSubRequestIndex = subRequestIndex
        return true
      // else: state already rolled back by DFS backtracking, try next
    return false  (all sub-requests exhausted)
```

Key properties:
1. **Strict priority order:** Index 0 tried first, always. No reordering or optimization.
2. **First success wins:** Once a sub-request allocates successfully, no further alternatives are tried.
3. **Per-sub-request data:** Each sub-request has its own `requestData` entry (class, selectors, count) keyed by `{claimIndex, requestIndex, subRequestIndex}`.
4. **No partial success:** If a sub-request can fill some device slots but not all, it fails entirely — backtracking rolls back all tentative allocations within that attempt.

### State Management (Backtracking)

There is **no explicit state checkpoint/restore** at the sub-request iteration level. Instead, the mechanism relies on the existing DFS backtracking:

1. Each `allocateDevice()` call returns a `deallocate` closure that reverses all state mutations
2. When device allocation fails during DFS for sub-request N, all tentative allocations within that attempt are rolled back via their deallocate closures
3. When control returns to the sub-request loop, the state is already clean — the DFS naturally unwinds

The `deallocate` closure reverses:
- Constraint state (MatchAttribute pins, DistinctAttribute seen values)
- `allocatingDevices` set membership
- `allocatingCapacity` for multi-allocatable devices (consumable capacity)
- Counter budget deductions (partitionable devices)
- Result slice entries (truncated back to pre-attempt length)

This design means no explicit snapshot/restore is needed — the existing backtracking mechanism is sufficient because each sub-request attempt is a complete DFS invocation that either fully succeeds (and the loop exits) or fully fails (and DFS has already unwound all state).

### Scoring

The kube-scheduler's dynamicresources plugin implements scoring to prefer nodes where higher-priority sub-requests were selected:

**Score computation per node:**

```
For each claim allocated on this node:
  For each request with FirstAvailable:
    For sub-request at index i (if it was the selected one):
      score += FirstAvailableDeviceRequestMaxSize - i
```

- Index 0 (highest priority): score contribution = 8
- Index 1: score contribution = 7
- ...
- Index 7 (lowest priority): score contribution = 1

Scores from multiple claims/requests on the same node are summed.

**Normalization:** `DefaultNormalizeScore` linearly maps scores so the best node gets 100, worst gets 0.

**Plugin weight:** 2 (reflects user preference importance).

**Karpenter relevance:** Karpenter doesn't do multi-node scoring (it's not choosing between existing nodes). However, the priority ordering matters for which instance types are preferred — a NodeClaim compatible with higher-priority sub-requests should be preferred over one that only satisfies lower-priority alternatives.

---

## Interaction with Other Features

### Constraints (MatchAttribute / DistinctAttribute)

Each sub-request defines its own constraints independently. Cross-request constraints resolve against the **selected** sub-request's allocated devices:

- Constraint referencing parent name `"gpu"` → applies to whichever sub-request is selected
- Constraint referencing `"gpu/a100"` → applies only if that specific sub-request is chosen

During DFS for a sub-request, constraint state is built up as devices are allocated. If the sub-request fails and backtracking occurs, constraint state is fully restored (pins removed, distinct values popped).

### Consumable Capacity (KEP-5075)

Each sub-request can independently specify capacity requirements. If sub-request 0 requests 40Gi of GPU memory and sub-request 1 requests 20Gi:

- Capacity check uses the **selected** sub-request's requirements
- On backtrack from sub-request 0, any consumed capacity tracking is rolled back
- Sub-request 1's capacity check starts from the same baseline

### Partitionable Devices (KEP-4815)

Sub-requests can target different device classes with different counter profiles:

- Sub-request 0: targets `gpu-mig-3g.20gb` (consumes 3 memory-slices, 42 multiprocessors)
- Sub-request 1: targets `gpu-mig-1g.5gb` (consumes 1 memory-slice, 14 multiprocessors)

Counter budget is checked per the selected sub-request's device `ConsumesCounters` declarations. On backtrack, counter deductions from failed sub-request attempts are fully restored.

### AdminAccess

**Not available** on `DeviceSubRequest`. AdminAccess is only supported through `SpecificDeviceRequest` (i.e., `Exactly`). This is a deliberate exclusion to limit the blast radius of prioritized alternatives.

### AllDevices Mode (All)

A sub-request can use `AllocationMode: All`. If the all-mode sub-request cannot be satisfied (not enough matching devices exist in the pool), it fails → backtrack → try next sub-request. Common pattern: "allocate all A100s on this node, or fall back to requesting exactly 2 of any GPU."

### Count Variation

Different sub-requests can request different counts. This enables graceful degradation:

```yaml
firstAvailable:
- request: {deviceClassName: gpu-a100, count: 4}   # prefer 4 GPUs
- request: {deviceClassName: gpu-a100, count: 2}   # fall back to 2
- request: {deviceClassName: gpu-any, count: 1}    # last resort: 1 of anything
```

### Cross-Request Constraints

When a constraint spans multiple requests (e.g., `requests: ["gpu", "nic"]` with `matchAttribute: dra.k8s.io/pcieRoot`), and "gpu" uses `FirstAvailable`:

- The constraint resolves against whichever sub-request of "gpu" was selected
- If "gpu/a100" is selected, the PCIe root constraint checks the A100 device's PCIe root against the NIC device's PCIe root
- Unselected sub-requests have NO effect on cross-request constraints

---

## Upstream Implementation References

### Allocator Code

`k8s.io/dynamic-resource-allocation/structured/internal/incubating/`

- `allocator_incubating.go` — `allocateOne()` sub-request iteration loop, `requestData.selectedSubRequestIndex`, `requestData.parentRequest` detection
- Key data structures:
  - `requestIndices{claimIndex, requestIndex, subRequestIndex}` — indexes into `requestData` map
  - `deviceIndices{claimIndex, requestIndex, subRequestIndex, deviceIndex}` — full DFS position
  - `requestData.selectedSubRequestIndex` — which sub-request was chosen

### Scheduler Plugin

`pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go`

- `Score()` / `computeScore()` — priority-based scoring using `FirstAvailableDeviceRequestMaxSize - i`
- `NormalizeScore()` — linear normalization via `helper.DefaultNormalizeScore`
- `resourceclaim.IsSubRequestRef(request)` — detects `"parent/sub"` format
- `resourceclaim.CreateSubRequestRef(reqName, subReqName)` — creates qualified reference

### API Types

`k8s.io/api/resource/v1beta2/types.go`

- `DeviceRequest{Exactly, FirstAvailable}`
- `DeviceSubRequest{Name, DeviceClassName, Selectors, AllocationMode, Count}`
- `SpecificDeviceRequest` (replaces inline fields on DeviceRequest)
- `FirstAvailableDeviceRequestMaxSize = 8`

### Constraint Matching

`structured/internal/incubating/allocator_incubating.go`

- `matchAttributeConstraint.matches(requestName, subRequestName)` — checks parent name OR qualified name
- Pattern: parent-name reference → applies to all sub-requests; qualified reference → applies to specific sub-request only
