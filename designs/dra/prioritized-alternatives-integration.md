# Prioritized Alternatives Integration

## Table of Contents

- [Overview](#overview)
- [Scope](#scope)
- [Request Validation Changes](#request-validation-changes)
  - [ClaimData and RequestData Extension](#claimdata-and-requestdata-extension)
  - [Parsing FirstAvailable](#parsing-firstavailable)
  - [Constraint Referencing](#constraint-referencing)
  - [Device Limit Validation](#device-limit-validation)
- [DFS Extension](#dfs-extension)
  - [New Iteration Level](#new-iteration-level)
  - [State Management](#state-management)
  - [Sub-Request Backtracking](#sub-request-backtracking)
  - [All Mode Interaction](#all-mode-interaction)
- [Constraint System Changes](#constraint-system-changes)
  - [Constraint Interface Change](#constraint-interface-change)
  - [Request Name Matching](#request-name-matching)
  - [Cross-Request Constraints](#cross-request-constraints)
- [Result Tracking](#result-tracking)
  - [Selected Sub-Request Recording](#selected-sub-request-recording)
  - [DeviceAllocationResult Extension](#deviceallocationresult-extension)
  - [Metadata Propagation](#metadata-propagation)
- [Instance Type Scoring Implications](#instance-type-scoring-implications)
- [Key Design Decisions](#key-design-decisions)
- [Implementation Sequencing](#implementation-sequencing)

---

## Overview

### Problem Statement

Karpenter's DRA allocator currently supports only `Exactly` requests — a single `ExactDeviceRequest` per entry in `claim.Spec.Devices.Requests`. KEP-4816 (Prioritized Alternatives) introduces `FirstAvailable` — an ordered list of sub-requests tried in priority order, where the first satisfiable sub-request wins.

Integrating prioritized alternatives into Karpenter's allocator presents challenges specific to our model:

1. **DFS depth extension.** The current DFS traverses `claims → requests → slots → devices`. FirstAvailable adds a level between requests and slots: `claims → requests → sub-requests → slots → devices`. When a sub-request fails, all state from that attempt must be cleanly rolled back before trying the next alternative.

2. **Instance type superposition and sub-request selection.** Different instance types may satisfy different sub-requests for the same request (IT-A can fill sub-request 0, IT-B can only fill sub-request 1). The allocator must allow this — the selected sub-request is per-IT, not global. However, constraint and requirement accumulation across ITs must account for the fact that different sub-requests may contribute different topology requirements.

3. **Constraint scoping.** Constraints can reference either the parent request name (applies regardless of which sub-request is chosen) or a qualified `"parent/sub"` name (applies only if that specific sub-request is selected). The existing `appliesTo(requestName)` pattern must be extended to handle this two-level naming.

4. **Cross-sub-request state isolation.** Unlike the existing DFS where backtracking is device-by-device, sub-request transitions require a complete slate wipe of all devices allocated during that sub-request's attempt. The existing backtracking mechanism (where each `tryDevice` reverses its own allocation on failure) is sufficient — when a sub-request's DFS fails, all its tentative allocations have already been individually rolled back.

### Key References

| Reference | Path |
|-----------|------|
| Upstream KEP semantics | `designs/dra/prioritized-alternatives.md` |
| Core allocator design | `designs/dra/scheduling.md` |
| Consumable capacity integration | `designs/dra/consumable-capacity-integration.md` |
| Partitionable devices integration | `designs/dra/partitionable-devices-integration.md` |
| Upstream implementation | `k8s.io/dynamic-resource-allocation/structured/internal/incubating/allocator_incubating.go` |
| Request validation | `pkg/scheduling/dynamicresources/request.go` |
| Allocator DFS | `pkg/scheduling/dynamicresources/allocator.go` |
| Constraint system | `pkg/scheduling/dynamicresources/constraint.go` |

---

## Scope

### In Scope

- `FirstAvailable` field parsing on `DeviceRequest`
- Sub-request iteration in priority order within the DFS
- State isolation between sub-request attempts (no leakage)
- Constraint name matching for parent names and qualified `"parent/sub"` names
- Per-IT sub-request selection (different ITs may satisfy different sub-requests)
- Selected sub-request recording in allocation metadata
- Device limit validation across sub-requests (worst-case sizing)
- Interaction with consumable capacity and partitionable devices during sub-request backtracking

### Deferred

- **Scoring / preference optimization:** Karpenter doesn't choose between existing nodes (it creates them). Instance type priority is handled by the existing IT ordering. No scoring plugin needed.
- **AdminAccess on sub-requests:** Not supported in upstream API (`DeviceSubRequest` has no `AdminAccess` field). Not an issue.
- **Cross-claim sub-request consistency:** Upstream explicitly excludes guaranteeing all claims in a Deployment choose the same alternative. Karpenter follows suit.
- **Config per-sub-request:** Config entries using `"parent/sub"` format are a kube-scheduler concern (device configuration at actual allocation time). Karpenter simulates feasibility, not config application.

---

## Request Validation Changes

### ClaimData and RequestData Extension

The `RequestData` struct is extended to model the parent/sub-request relationship:

```go
type RequestData struct {
    // Name is the request name from the claim spec (parent request name).
    Name string
    // SubRequests holds the ordered alternatives for a FirstAvailable request.
    // nil when the request uses Exactly (single request, no alternatives).
    // When non-nil, the fields below (Class, Selectors, NumDevices, etc.) are unused —
    // each SubRequestData carries its own copy of these fields.
    SubRequests []SubRequestData

    // Fields below are used for Exactly requests (SubRequests == nil):
    Class          *resourcev1.DeviceClass
    NumDevices     int
    AllocationMode resourcev1.DeviceAllocationMode
    AllDevices          []DeviceWithID
    AllTemplateDevicesByIT map[InstanceTypeID][]DeviceWithID
    Selectors      []resourcev1.DeviceSelector
    CapacityRequests map[resourcev1.QualifiedName]resource.Quantity
}

// SubRequestData holds the parsed metadata for a single alternative within a FirstAvailable request.
type SubRequestData struct {
    // Name is the sub-request's own name (unique within the parent's FirstAvailable list).
    Name string
    // QualifiedName is "parentName/subName" — used for constraint matching and result recording.
    QualifiedName string

    Class          *resourcev1.DeviceClass
    NumDevices     int
    AllocationMode resourcev1.DeviceAllocationMode
    AllDevices          []DeviceWithID
    AllTemplateDevicesByIT map[InstanceTypeID][]DeviceWithID
    Selectors      []resourcev1.DeviceSelector
    CapacityRequests map[resourcev1.QualifiedName]resource.Quantity
}
```

A request with `SubRequests != nil` is a FirstAvailable request. The DFS detects this and iterates sub-requests. A request with `SubRequests == nil` is an Exactly request — unchanged behavior.

### Parsing FirstAvailable

In `ValidateClaimRequest`, the existing check:

```go
if req.Exactly == nil {
    return nil, fmt.Errorf("claim %q request %q: only Exactly requests are supported", claim.Name, req.Name)
}
```

Becomes:

```go
switch {
case req.Exactly != nil:
    rd, err := validateExactRequest(ctx, kubeClient, claim.Name, req.Name, req.Exactly, pools, templateDevicesByIT, celCache)
    if err != nil {
        return nil, err
    }
    data.Requests = append(data.Requests, *rd)

case len(req.FirstAvailable) > 0:
    rd, err := validateFirstAvailableRequest(ctx, kubeClient, claim.Name, req.Name, req.FirstAvailable, pools, templateDevicesByIT, celCache)
    if err != nil {
        return nil, err
    }
    data.Requests = append(data.Requests, *rd)

default:
    return nil, fmt.Errorf("claim %q request %q: neither Exactly nor FirstAvailable set", claim.Name, req.Name)
}
```

The new `validateFirstAvailableRequest` iterates the sub-request list, calling `validateExactRequest` for each (since `DeviceSubRequest` fields match `ExactDeviceRequest`):

```go
func validateFirstAvailableRequest(
    ctx context.Context,
    kubeClient client.Client,
    claimName string,
    parentName string,
    subRequests []resourcev1.DeviceSubRequest,
    pools []*Pool,
    templateDevicesByIT map[InstanceTypeID][]DeviceWithID,
    celCache *dracel.Cache,
) (*RequestData, error) {
    rd := &RequestData{
        Name:        parentName,
        SubRequests: make([]SubRequestData, 0, len(subRequests)),
    }
    for i := range subRequests {
        sub := &subRequests[i]
        exactReq := subRequestToExact(sub)
        subRD, err := validateExactRequest(ctx, kubeClient, claimName, sub.Name, exactReq, pools, templateDevicesByIT, celCache)
        if err != nil {
            return nil, fmt.Errorf("claim %q request %q sub-request %q: %w", claimName, parentName, sub.Name, err)
        }
        rd.SubRequests = append(rd.SubRequests, SubRequestData{
            Name:                   sub.Name,
            QualifiedName:          parentName + "/" + sub.Name,
            Class:                  subRD.Class,
            NumDevices:             subRD.NumDevices,
            AllocationMode:         subRD.AllocationMode,
            AllDevices:             subRD.AllDevices,
            AllTemplateDevicesByIT: subRD.AllTemplateDevicesByIT,
            Selectors:             subRD.Selectors,
            CapacityRequests:       subRD.CapacityRequests,
        })
    }
    return rd, nil
}
```

`subRequestToExact` converts a `DeviceSubRequest` into an `ExactDeviceRequest` for reuse of the existing validation logic:

```go
func subRequestToExact(sub *resourcev1.DeviceSubRequest) *resourcev1.ExactDeviceRequest {
    return &resourcev1.ExactDeviceRequest{
        DeviceClassName: sub.DeviceClassName,
        Selectors:       sub.Selectors,
        AllocationMode:  sub.AllocationMode,
        Count:           sub.Count,
        // Capacity, Tolerations, etc. — pass through if present on DeviceSubRequest
    }
}
```

### Constraint Referencing

Constraints use `RequestNames sets.Set[string]` to determine which requests they apply to. With FirstAvailable, a constraint can reference:

- `"gpu"` — the parent name, applies to ALL sub-requests of "gpu"
- `"gpu/a100"` — a qualified name, applies ONLY to that specific sub-request

The `appliesTo` logic is updated:

```go
// appliesTo checks whether a constraint applies to a given request.
// parentName is the top-level request name (e.g., "gpu").
// subName is the sub-request name (e.g., "a100"), or "" for Exactly requests.
func (m *MatchAttributeConstraint) appliesTo(parentName, subName string) bool {
    if m.RequestNames.Len() == 0 {
        return true
    }
    // Match on parent name (applies to all sub-requests of this parent)
    if m.RequestNames.Has(parentName) {
        return true
    }
    // Match on qualified name (applies only to specific sub-request)
    if subName != "" && m.RequestNames.Has(parentName+"/"+subName) {
        return true
    }
    return false
}
```

The `Constraint` interface signature changes to pass both names:

```go
type Constraint interface {
    Add(parentName, subName string, device cloudprovider.Device, deviceID DeviceID) bool
    Remove(parentName, subName string, device cloudprovider.Device, deviceID DeviceID)
    Reset()
}
```

For Exactly requests, `subName` is always `""`. For FirstAvailable, `subName` is the sub-request's own name (e.g., `"a100"`).

### Device Limit Validation

The device limit check in `ValidateClaimRequest` must account for sub-requests. Since we don't know which sub-request will be selected, we use **worst-case** (maximum device count across all sub-requests):

```go
for _, req := range data.Requests {
    if req.SubRequests != nil {
        // Use the maximum NumDevices across all sub-requests.
        maxDevices := 0
        for _, sub := range req.SubRequests {
            numDevices := sub.NumDevices + len(sub.AllDevices)
            if numDevices > maxDevices {
                maxDevices = numDevices
            }
        }
        baseTotalDevices += maxDevices
    } else {
        baseTotalDevices += req.NumDevices
        baseTotalDevices += len(req.AllDevices)
    }
}
```

This is conservative — it prevents accepting claims that MIGHT exceed the limit depending on which alternative is chosen. The upstream scheduler handles this more precisely (trying sub-requests and checking against `AllocationResultsMaxSize` per-attempt), but for Karpenter's feasibility simulation, worst-case is correct.

---

## DFS Extension

### New Iteration Level

The current DFS signature is:

```go
func (a *allocator) dfs(claimIdx, reqIdx, slotIdx int) bool
```

With FirstAvailable, when `reqIdx` points to a request with `SubRequests != nil`, we iterate sub-requests before proceeding to slots. The signature remains unchanged — the sub-request iteration is handled inline:

```go
func (a *allocator) dfs(claimIdx, reqIdx, slotIdx int) bool {
    // ... existing base cases (claimIdx >= len, reqIdx >= len) ...

    cd := a.claimData[claimIdx]
    rd := &cd.Requests[reqIdx]

    // FirstAvailable: iterate sub-requests in priority order.
    if rd.SubRequests != nil && slotIdx == 0 {
        return a.dfsFirstAvailable(claimIdx, reqIdx, cd, rd)
    }

    // Exactly: existing behavior (or continuing within a sub-request after dfsFirstAvailable
    // has set up the active sub-request).
    // ...
}
```

The new `dfsFirstAvailable` function:

```go
func (a *allocator) dfsFirstAvailable(claimIdx, reqIdx int, cd *ClaimData, rd *RequestData) bool {
    for subIdx := range rd.SubRequests {
        a.activeSubRequest = &rd.SubRequests[subIdx]
        a.activeSubRequestIdx = subIdx

        if a.dfsSubRequest(claimIdx, reqIdx, cd, a.activeSubRequest) {
            return true
        }
        // State is already clean — the DFS fully unwinds on failure.
        // No explicit restore needed here.
    }
    a.activeSubRequest = nil
    a.activeSubRequestIdx = -1
    return false
}
```

`dfsSubRequest` drives the slot iteration for the active sub-request, using the sub-request's own fields:

```go
func (a *allocator) dfsSubRequest(claimIdx, reqIdx int, cd *ClaimData, sub *SubRequestData) bool {
    numSlots := a.numSlotsForSub(sub)
    if sub.AllocationMode == resourcev1.DeviceAllocationModeAll {
        return a.dfsAllModeSub(claimIdx, reqIdx, 0, cd, sub)
    }
    return a.dfsExactCountSub(claimIdx, reqIdx, 0, cd, sub)
}
```

These are thin wrappers that use the sub-request's `Selectors`, `AllDevices`, `AllTemplateDevicesByIT`, etc. instead of `rd`'s fields.

After a sub-request's DFS succeeds (the recursion into `dfs(claimIdx, reqIdx+1, 0)` at the end of slots returns true), the sub-request is "locked in" for this IT and we proceed to the next request.

### State Management

**No explicit checkpoint/restore is needed.** The critical insight (matching upstream) is that when a sub-request's DFS fails, all its tentative device allocations have already been individually rolled back by `tryDevice`'s backtracking. The sub-request loop sees a clean slate for the next attempt.

State that is tracked per-DFS-attempt and naturally unwound:
- `allocatedDevices` set — each device inserted by `tryDevice` is removed on backtrack
- `allocatedDevicesMetadata` — appended and truncated in lock-step
- `allocatingCounters` / `templateAllocatingCounters` — deducted and restored per device
- `allocatingCapacity` / `templateAllocatingCapacity` — deducted and restored per device
- Constraints — `Add()` matched by `Remove()` in `tryDevice` backtrack
- Requirement snapshots and pool filtering — pushed/popped in `tryDevice`

Because `tryDevice` always restores state on failure (returns false), and the sub-request's slot-filling loop only returns true if ALL slots succeed, a failed sub-request leaves no residual state.

### Sub-Request Backtracking

The sub-request level adds another layer to the existing backtracking scheme:

```
Per instance type:
  restoreState() — clears everything for fresh IT attempt
  For each claim:
    For each request:
      If FirstAvailable:
        For each sub-request (priority order):      ← NEW LEVEL
          Fill all slots via dfsExactCountSub/dfsAllModeSub
            tryDevice (existing) — auto-backtracks on failure
          If all slots filled:
            Recurse to next request → if true, done
            Else: backtrack all slots (DFS unwinds)  ← EXISTING
          If failed: state is already clean, try next sub-request
      Else (Exactly):
        Fill all slots (existing dfsExactCount/dfsAllMode)
```

The only new responsibility is tracking WHICH sub-request succeeded for result recording (see [Result Tracking](#result-tracking)).

### All Mode Interaction

A sub-request can use `AllocationMode: All`. If the all-mode sub-request fails (e.g., not enough matching devices in the pool for this IT), the DFS for that sub-request fails, backtracking unwinds all tentative allocations, and the next sub-request is tried.

`dfsAllModeSub` uses `sub.AllDevices` and `sub.AllTemplateDevicesByIT[a.itID]` to determine the slot count — each sub-request has independently computed eligible device sets. A sub-request requesting "all A100s" may have 0 eligible devices for an IT that only has H100s, causing immediate failure and fallthrough to the next alternative.

---

## Constraint System Changes

### Constraint Interface Change

The `Constraint` interface gains the `subName` parameter:

```go
type Constraint interface {
    Add(parentName, subName string, device cloudprovider.Device, deviceID DeviceID) bool
    Remove(parentName, subName string, device cloudprovider.Device, deviceID DeviceID)
    Reset()
}
```

Callers in `tryDevice`:

```go
// Before (current):
con.Add(rd.Name, dw.Device, deviceID)
con.Remove(rd.Name, dw.Device, deviceID)

// After:
parentName, subName := a.constraintNames(rd)
con.Add(parentName, subName, dw.Device, deviceID)
con.Remove(parentName, subName, dw.Device, deviceID)
```

Where `constraintNames` resolves based on whether we're inside a sub-request:

```go
func (a *allocator) constraintNames(rd *RequestData) (string, string) {
    if a.activeSubRequest != nil {
        return rd.Name, a.activeSubRequest.Name
    }
    return rd.Name, ""
}
```

### Request Name Matching

The `appliesTo` method on all constraint types is updated:

```go
func (m *MatchAttributeConstraint) appliesTo(parentName, subName string) bool {
    if m.RequestNames.Len() == 0 {
        return true
    }
    if m.RequestNames.Has(parentName) {
        return true
    }
    if subName != "" && m.RequestNames.Has(parentName+"/"+subName) {
        return true
    }
    return false
}
```

This means:
- A constraint listing `requests: ["gpu"]` applies to all sub-requests under "gpu"
- A constraint listing `requests: ["gpu/a100"]` applies only when the "a100" sub-request is active
- A constraint listing `requests: ["gpu", "nic"]` applies to any sub-request under "gpu" AND any device under "nic"

### Cross-Request Constraints

When a constraint spans multiple requests and one uses FirstAvailable, the constraint only evaluates against the **active** sub-request's devices. If request "gpu" has sub-requests and the DFS is currently trying sub-request "gpu/a100", the constraint sees devices from the a100 attempt. If that attempt fails and we try "gpu/h100", the constraint state has been fully reset (via `Remove` calls during backtracking) — it starts fresh for the h100 attempt.

No special handling is needed — the existing `Add`/`Remove` protocol on `tryDevice` ensures correctness automatically.

---

## Result Tracking

### Selected Sub-Request Recording

The child `allocator` struct gains a field tracking the active sub-request:

```go
type allocator struct {
    // ... existing fields ...

    // activeSubRequest points to the sub-request currently being filled during DFS.
    // nil when processing an Exactly request.
    activeSubRequest *SubRequestData
    // activeSubRequestIdx is the index of the active sub-request within rd.SubRequests.
    // -1 when not in a FirstAvailable request.
    activeSubRequestIdx int

    // selectedSubRequests records which sub-request was selected for each FirstAvailable request
    // during a successful DFS for this IT. Keyed by (claimIdx, reqIdx).
    selectedSubRequests map[requestKey]int
}

type requestKey struct {
    claimIdx int
    reqIdx   int
}
```

When `dfsFirstAvailable` finds a successful sub-request:

```go
if a.dfsSubRequest(claimIdx, reqIdx, cd, a.activeSubRequest) {
    a.selectedSubRequests[requestKey{claimIdx, reqIdx}] = subIdx
    return true
}
```

### DeviceAllocationResult Extension

The `DeviceAllocationResult` struct in `ResourceClaimAllocationMetadata.Devices` is extended to track the request name (including qualified sub-request name if applicable):

```go
type DeviceAllocationResult struct {
    DeviceID         DeviceID
    ConsumedCapacity map[resourcev1.QualifiedName]resource.Quantity
    // RequestName is the request name for this allocation result.
    // For Exactly requests: the parent request name (e.g., "gpu").
    // For FirstAvailable: qualified "parent/sub" format (e.g., "gpu/a100").
    RequestName string
}
```

When recording results in the IT success path (after `dfs` returns true), the metadata builder uses:

```go
for di, da := range a.allocatedDevicesMetadata {
    meta.Devices[itID] = append(meta.Devices[itID], DeviceAllocationResult{
        DeviceID:         da.deviceWithID.ID,
        ConsumedCapacity: da.consumedCapacity,
        RequestName:      da.requestName,
    })
}
```

Where `da.requestName` is populated during `tryDevice` recording:

```go
a.allocatedDevicesMetadata = append(a.allocatedDevicesMetadata, deviceAllocationMetadata{
    claimIndex:       claimIdx,
    deviceWithID:     dw,
    consumedCapacity: consumed,
    requestName:      a.currentRequestName(rd),
})
```

```go
func (a *allocator) currentRequestName(rd *RequestData) string {
    if a.activeSubRequest != nil {
        return a.activeSubRequest.QualifiedName
    }
    return rd.Name
}
```

### Metadata Propagation

The selected sub-request index per-IT is stored in metadata for potential use by:
- Integration tests (verifying the correct alternative was chosen)
- Future optimization (preferring ITs where higher-priority sub-requests succeeded)

This is informational — no correctness logic depends on it.

---

## Instance Type Scoring Implications

Karpenter doesn't implement kube-scheduler's multi-node scoring (it creates new nodes rather than choosing between existing ones). However, the priority order of sub-requests has implications for instance type preference:

**Observation:** If IT-A satisfies sub-request 0 (highest priority) and IT-B only satisfies sub-request 2 (lower priority), IT-A is "better" from the user's perspective.

**Design decision:** We do NOT add scoring logic. Rationale:

1. The existing Karpenter IT ordering (from the cloud provider) already encodes preference based on cost/availability.
2. Adding DRA-specific IT scoring would require a mechanism to weigh DRA preference against cost/capacity preference — this is complex and not yet required.
3. The DFS naturally prefers higher-priority sub-requests by trying them first. ITs where the high-priority sub-request fails are still valid (they satisfy a lower-priority alternative) — they just represent graceful degradation.

**Future consideration:** If a scoring mechanism is needed, it could use the same `FirstAvailableDeviceRequestMaxSize - subIdx` formula upstream uses, weighted against the existing IT preference score. This is deferred.

---

## Key Design Decisions

### 1. Sub-request iteration inline in the DFS, not a separate recursive level

**Decision:** `dfsFirstAvailable` is called directly from `dfs()` when `slotIdx == 0` and the request has sub-requests. It loops over sub-requests, calling sub-type-specific DFS functions that mirror `dfsExactCount`/`dfsAllMode`.

**Rationale:** Adding a formal `subReqIdx` parameter to `dfs()` would require changing the signature everywhere and complicating the base cases. Instead, the sub-request iteration is encapsulated — `dfs()` only needs to detect FirstAvailable and delegate. Mirrors upstream's approach of handling sub-requests as a loop within `allocateOne`.

### 2. No explicit state checkpoint at the sub-request level

**Decision:** Rely entirely on the existing DFS backtracking (device-by-device `tryDevice` unwind) to clean up after a failed sub-request attempt.

**Rationale:** The upstream allocator uses the same approach. When a sub-request's DFS fails, it means all recursive slot-filling attempts returned false, which means all `tryDevice` calls that allocated devices have already backtracked (removed from `allocatedDevices`, restored counters/capacity, removed constraints, popped requirement snapshots). No residual state remains. This is simpler and more correct than maintaining an explicit checkpoint/restore — there's no risk of forgetting to restore a field.

**Verification:** After `dfsSubRequest` returns false, assert in debug builds:
- `len(a.allocatedDevicesMetadata)` equals what it was before the call
- `a.allocatedDevices.Len()` equals what it was before the call

### 3. Different ITs may select different sub-requests

**Decision:** The selected sub-request is per-IT. IT-A might satisfy sub-request 0 while IT-B only satisfies sub-request 1. Both are valid surviving ITs.

**Rationale:** Each IT gets an independent DFS (existing design — `restoreState()` between ITs). The sub-request iteration within each IT's DFS independently determines which alternative is feasible. This is correct because:
- The NodeClaim will collapse to exactly one IT
- When it collapses to IT-A, kube-scheduler will allocate sub-request 0 (matching what we simulated)
- When it collapses to IT-B, kube-scheduler will allocate sub-request 1

The `selectedSubRequests` map is per-DFS (reset via `restoreState` between ITs) and recorded per-IT in the results.

### 4. Constraint interface gains subName parameter (not a qualifiedName single arg)

**Decision:** Pass `parentName` and `subName` as separate arguments rather than a single combined `qualifiedName`.

**Rationale:** The matching logic needs BOTH pieces independently:
- Checking `m.RequestNames.Has(parentName)` — does the constraint apply to all sub-requests?
- Checking `m.RequestNames.Has(parentName + "/" + subName)` — does it apply to this specific sub-request?

Passing them separately avoids needing to split a combined string inside every constraint, and makes the empty-subName case (Exactly requests) natural — callers pass `""` for subName, and the qualified-name check simply doesn't fire.

### 5. Device limit uses worst-case across sub-requests

**Decision:** For the `AllocationResultsMaxSize` validation, use the maximum `numDevices` across all sub-requests in a FirstAvailable request.

**Rationale:** We don't know at validation time which sub-request will be selected — it may vary by IT. Using worst-case is conservative (may reject claims that would actually fit) but safe (never accepts claims that would exceed limits). The alternative (per-attempt checking during DFS) is possible but adds complexity; upstream handles this via `errAllocationResultMaxSizeExceeded` during DFS, which we can adopt as a follow-up if the conservative approach causes real-world rejections.

### 6. matchKey for CEL cache includes sub-request identity

**Decision:** The `matchKey` struct (used to cache CEL selector evaluations) must distinguish between sub-requests, since each sub-request has different selectors:

```go
type matchKey struct {
    DeviceID       DeviceID
    ClaimIndex     int
    RequestIndex   int
    SubRequestIndex int  // -1 for Exactly requests
}
```

**Rationale:** Different sub-requests have different selectors — a device matching sub-request 0's selectors may not match sub-request 1's. Without sub-request discrimination in the cache key, a cached `true` from sub-request 0 would incorrectly apply to sub-request 1.

### 7. selectedSubRequests reset in restoreState

**Decision:** `selectedSubRequests` is reset (cleared) in `restoreState()` along with other per-IT state.

**Rationale:** Each IT independently determines which sub-requests it selects. The map is only read when recording results for a successful IT, and each IT fills it fresh during its DFS.

---

## Implementation Sequencing

### Commit 1: Request Validation (ClaimData/RequestData extension + parsing)

Foundation layer. Extends the request model to parse `FirstAvailable` into `SubRequestData`.

- [ClaimData and RequestData Extension](#claimdata-and-requestdata-extension)
- [Parsing FirstAvailable](#parsing-firstavailable)
- [Device Limit Validation](#device-limit-validation)

No behavioral change — requests with `FirstAvailable` are parsed but the DFS still rejects them until commit 2.

### Commit 2: Constraint Interface Change

Updates the `Constraint` interface to pass `parentName` + `subName`, and updates `appliesTo` on all constraint types.

- [Constraint Interface Change](#constraint-interface-change)
- [Request Name Matching](#request-name-matching)

For Exactly requests, callers pass `subName=""` — no behavior change. This unblocks commit 3.

### Commit 3: DFS Extension (sub-request iteration)

The core feature. Adds `dfsFirstAvailable`, `dfsSubRequest`, `dfsExactCountSub`, `dfsAllModeSub`.

- [New Iteration Level](#new-iteration-level)
- [State Management](#state-management)
- [Sub-Request Backtracking](#sub-request-backtracking)
- [All Mode Interaction](#all-mode-interaction)
- [matchKey extension](#6-matchkey-for-cel-cache-includes-sub-request-identity)

After this commit, FirstAvailable requests are fully functional.

### Commit 4: Result Tracking

Records which sub-request was selected per-IT in the allocation metadata.

- [Selected Sub-Request Recording](#selected-sub-request-recording)
- [DeviceAllocationResult Extension](#deviceallocationresult-extension)
- [Metadata Propagation](#metadata-propagation)

### Dependency Graph

```
Commit 1: Request Validation
  │
  ├──→ Commit 2: Constraint Interface
  │       │
  │       ▼
  ├──→ Commit 3: DFS Extension (depends on 1 + 2)
  │       │
  │       ▼
  └──→ Commit 4: Result Tracking (depends on 3)
```

All commits are incremental — each compiles and passes tests independently. Commit 3 is the bulk of the work.
