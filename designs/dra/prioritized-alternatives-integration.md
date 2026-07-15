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

The `RequestData` struct is extended with a unified design — the same struct represents both top-level requests and sub-requests. This mirrors upstream's single `requestData` type and eliminates duplicate fields:

```go
type RequestData struct {
    // Name is the request's own name.
    // For top-level: the DeviceRequest.Name (e.g., "gpu").
    // For sub-requests: the DeviceSubRequest.Name (e.g., "a100").
    Name string
    // ParentName is the parent request's name. Empty for top-level (Exactly) requests.
    // Non-empty for sub-requests within a FirstAvailable list.
    ParentName string

    // SubRequests holds the ordered alternatives for a FirstAvailable request.
    // nil for Exactly requests and for sub-request entries themselves (no nesting).
    SubRequests []RequestData

    // Shared fields — used identically by both top-level and sub-request entries.
    // For FirstAvailable parents (SubRequests != nil), these fields are unused on the
    // parent entry itself — each sub-request carries its own values.
    Class          *resourcev1.DeviceClass
    NumDevices     int
    AllocationMode resourcev1.DeviceAllocationMode
    AllDevices          []DeviceWithID
    AllTemplateDevicesByIT map[InstanceTypeID][]DeviceWithID
    Selectors      []resourcev1.DeviceSelector
    CapacityRequests map[resourcev1.QualifiedName]resource.Quantity
}
```

**Discriminators:**
- `len(rd.SubRequests) > 0` → FirstAvailable parent (iterate sub-requests)
- `rd.ParentName != ""` → sub-request entry (has a parent)
- Both empty/nil → plain Exactly request (existing behavior)

**Why unified:** The DFS, `tryDevice`, `dfsExactCount`, `dfsAllMode`, constraint calls — all consume `*RequestData` uniformly. No separate `Sub`-variant functions needed. When the DFS operates on a sub-request, it reads `rd.Selectors`, `rd.NumDevices`, `rd.AllocationMode`, etc. from the sub-request's `RequestData` entry directly — the same code path as an Exactly request.

**`QualifiedName()` helper** (not a stored field — computed):

```go
func (rd *RequestData) QualifiedName() string {
    if rd.ParentName != "" {
        return rd.ParentName + "/" + rd.Name
    }
    return rd.Name
}
```

**`IsFirstAvailable()` helper:**

```go
func (rd *RequestData) IsFirstAvailable() bool {
    return len(rd.SubRequests) > 0
}
```

### Parsing FirstAvailable

#### Interface-Based Shared Validation

Following upstream's `requestAccessor` pattern, we define an interface over the common fields shared by `ExactDeviceRequest` and `DeviceSubRequest`. This eliminates duplication between the two entry points entirely — one `buildRequestData` function handles both:

```go
// deviceRequestAccessor abstracts the common fields between ExactDeviceRequest and DeviceSubRequest.
type deviceRequestAccessor interface {
    GetDeviceClassName() string
    GetSelectors() []resourcev1.DeviceSelector
    GetAllocationMode() resourcev1.DeviceAllocationMode
    GetCount() int64
    GetCapacity() *resourcev1.CapacityRequirements
}
```

Two thin wrapper types implement it:

```go
type exactRequestAccessor struct{ req *resourcev1.ExactDeviceRequest }
func (a exactRequestAccessor) GetDeviceClassName() string                        { return a.req.DeviceClassName }
func (a exactRequestAccessor) GetSelectors() []resourcev1.DeviceSelector         { return a.req.Selectors }
func (a exactRequestAccessor) GetAllocationMode() resourcev1.DeviceAllocationMode { return a.req.AllocationMode }
func (a exactRequestAccessor) GetCount() int64                                   { return a.req.Count }
func (a exactRequestAccessor) GetCapacity() *resourcev1.CapacityRequirements     { return a.req.Capacity }

type subRequestAccessor struct{ sub *resourcev1.DeviceSubRequest }
func (a subRequestAccessor) GetDeviceClassName() string                        { return a.sub.DeviceClassName }
func (a subRequestAccessor) GetSelectors() []resourcev1.DeviceSelector         { return a.sub.Selectors }
func (a subRequestAccessor) GetAllocationMode() resourcev1.DeviceAllocationMode { return a.sub.AllocationMode }
func (a subRequestAccessor) GetCount() int64                                   { return a.sub.Count }
func (a subRequestAccessor) GetCapacity() *resourcev1.CapacityRequirements     { return a.sub.Capacity }
```

#### Core Validation Function

All validation logic lives in one place:

```go
func buildRequestData(
    ctx context.Context,
    kubeClient client.Client,
    claimName string,
    requestName string,
    req deviceRequestAccessor,
    pools []*Pool,
    templateDevicesByIT map[InstanceTypeID][]DeviceWithID,
    celCache *dracel.Cache,
) (*RequestData, error) {
    // 1. Resolve DeviceClass
    class := &resourcev1.DeviceClass{}
    if err := kubeClient.Get(ctx, types.NamespacedName{Name: req.GetDeviceClassName()}, class); err != nil {
        return nil, fmt.Errorf("claim %q request %q: DeviceClass %q not found: %w",
            claimName, requestName, req.GetDeviceClassName(), err)
    }

    // 2. Combine and validate selectors
    selectors, err := combineSelectors(class, req.GetSelectors())
    if err != nil {
        return nil, fmt.Errorf("claim %q request %q: %w", claimName, requestName, err)
    }
    if err := compileCEL(selectors, celCache); err != nil {
        return nil, fmt.Errorf("claim %q request %q: %w", claimName, requestName, err)
    }

    // 3. Build RequestData
    rd := &RequestData{
        Name:           requestName,
        Class:          class,
        Selectors:      selectors,
        NumDevices:     int(req.GetCount()),
        AllocationMode: resourcev1.DeviceAllocationModeExactCount,
    }
    if req.GetCapacity() != nil {
        rd.CapacityRequests = req.GetCapacity().Requests
    }

    // 4. Handle All mode
    if req.GetAllocationMode() == resourcev1.DeviceAllocationModeAll {
        rd.AllocationMode = resourcev1.DeviceAllocationModeAll
        rd.NumDevices = 0
        rd.AllDevices, err = collectAllModePoolDevices(ctx, selectors, pools, celCache)
        if err != nil {
            return nil, fmt.Errorf("claim %q request %q: %w", claimName, requestName, err)
        }
        rd.AllTemplateDevicesByIT = collectAllModeTemplateDevices(ctx, selectors, rd.AllDevices, templateDevicesByIT, celCache)
    }

    return rd, nil
}
```

#### Entry Points (now trivial)

```go
func validateExactRequest(ctx, kubeClient, claimName, requestName string,
    req *resourcev1.ExactDeviceRequest, pools, templateDevicesByIT, celCache,
) (*RequestData, error) {
    return buildRequestData(ctx, kubeClient, claimName, requestName,
        exactRequestAccessor{req}, pools, templateDevicesByIT, celCache)
}

func validateFirstAvailableRequest(ctx, kubeClient, claimName, parentName string,
    subRequests []resourcev1.DeviceSubRequest, pools, templateDevicesByIT, celCache,
) (*RequestData, error) {
    parent := &RequestData{Name: parentName, SubRequests: make([]RequestData, 0, len(subRequests))}
    for i := range subRequests {
        rd, err := buildRequestData(ctx, kubeClient, claimName, subRequests[i].Name,
            subRequestAccessor{&subRequests[i]}, pools, templateDevicesByIT, celCache)
        if err != nil {
            return nil, err
        }
        rd.ParentName = parentName
        parent.SubRequests = append(parent.SubRequests, *rd)
    }
    return parent, nil
}
```

#### Top-Level Dispatch (unchanged)

```go
switch {
case req.Exactly != nil:
    rd, err := validateExactRequest(ctx, kubeClient, claim.Name, req.Name, req.Exactly, pools, templateDevicesByIT, celCache)
case len(req.FirstAvailable) > 0:
    rd, err := validateFirstAvailableRequest(ctx, kubeClient, claim.Name, req.Name, req.FirstAvailable, pools, templateDevicesByIT, celCache)
}
```

**Key advantages:**
- **Zero duplication** — all validation logic (class resolution, selector merging, CEL, allocation mode, capacity) lives in `buildRequestData` exactly once
- **Trivial entry points** — `validateExactRequest` is 1 line; `validateFirstAvailableRequest` is a loop of 1 line + set ParentName
- **No type conversion** — no `subRequestToExact`; each accessor reads fields from its own API type directly
- **Upstream pattern** — mirrors `requestAccessor` in `k8s.io/dynamic-resource-allocation/structured/internal/incubating`
- **Easy to extend** — new fields (tolerations, etc.) are added to the interface + both accessors; `buildRequestData` handles them once

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

> **Implemented (`a413c929`).** The final implementation diverged from the stateful
> `activeSubRequest` design originally sketched here. Instead of tracking the active
> sub-request in allocator fields with "re-entry routing," the DFS threads the sub-request
> index as an explicit parameter (`subReqIdx`). This is simpler — no field to save/restore,
> no re-entry guards — and is documented below as-built.

The DFS signature gains a `subReqIdx` parameter (`-1` for Exactly requests and for the
FirstAvailable parent before a sub-request is chosen):

```go
// Before:
func (a *allocator) dfs(claimIdx, reqIdx, slotIdx int) bool
// After:
func (a *allocator) dfs(claimIdx, reqIdx, subReqIdx, slotIdx int) bool
```

The effective request is resolved inline from `(reqIdx, subReqIdx)`. When the parent is a
FirstAvailable request (`subReqIdx < 0 && len(rd.SubRequests) > 0`), the DFS delegates to
`dfsFirstAvailable`, which iterates sub-requests by re-entering `dfs` with a concrete
`subReqIdx`:

```go
func (a *allocator) dfs(claimIdx, reqIdx, subReqIdx, slotIdx int) bool {
    // ... ctx cancellation + base case (claimIdx >= len) ...

    cd := a.claimData[claimIdx]

    // Advance past completed requests/claims.
    if reqIdx >= len(cd.Requests) {
        return a.dfs(claimIdx+1, 0, -1, 0)
    }

    // Resolve the effective request: top-level for Exactly/parent, sub-request for FirstAvailable.
    var rd *RequestData
    if subReqIdx < 0 {
        rd = &cd.Requests[reqIdx]
    } else {
        rd = &cd.Requests[reqIdx].SubRequests[subReqIdx]
    }

    // Parent-level FirstAvailable: begin sub-request iteration.
    if subReqIdx < 0 && len(rd.SubRequests) > 0 {
        return a.dfsFirstAvailable(claimIdx, reqIdx, rd)
    }

    numSlots := a.numSlots(rd)
    // All mode requires at least one device (see "Zero-device All mode requires an explicit
    // guard" below). Without this, a zero-slot All-mode (sub-)request would fall into the
    // slotIdx >= numSlots branch and "succeed" by allocating nothing.
    if rd.AllocationMode == resourcev1.DeviceAllocationModeAll && numSlots == 0 {
        return false
    }
    if slotIdx >= numSlots {
        return a.dfs(claimIdx, reqIdx+1, -1, 0)
    }
    if rd.AllocationMode == resourcev1.DeviceAllocationModeAll {
        return a.dfsAllMode(claimIdx, reqIdx, subReqIdx, slotIdx, cd, rd)
    }
    return a.dfsExactCount(claimIdx, reqIdx, subReqIdx, slotIdx, cd, rd)
}
```

**Why no re-entry guard is needed:** because `subReqIdx` is a parameter (not allocator
state), every re-entry into `dfs` — whether from `tryDevice`'s `slotIdx+1` recursion or from
advancing to `reqIdx+1` — carries the correct sub-request context explicitly. Advancing to the
next request resets `subReqIdx` to `-1` (`a.dfs(claimIdx, reqIdx+1, -1, 0)`), so the next
request re-enters `dfsFirstAvailable` if it too is a FirstAvailable parent. There is nothing to
save or restore.

**Cross-claim backtracking works naturally:** When the final slot's `tryDevice` recurses and
`dfs` advances to subsequent claims, those claims run their full DFS. If a subsequent claim
fails (e.g., insufficient capacity because this sub-request consumed too much), the entire
recursive chain unwinds — through the subsequent claims, back through this sub-request's
slots — and `dfsFirstAvailable`'s loop tries the next sub-request with a clean slate.

Example: Claim 1 sub-request 0 (80Gi GPU) fills its slot, recurses to claim 2 (20Gi) which
fails (capacity exhausted). Backtracking unwinds claim 1's allocation, `dfsFirstAvailable`
tries sub-request 1 (20Gi), which succeeds and leaves room for claim 2. (Covered by the
"should backtrack across claims when sub-request consumes too much capacity" test.)

The `dfsFirstAvailable` function is a plain priority-order loop — the slot-count / All-mode
branching happens on re-entry into `dfs`, so it needs neither `cd` nor a manual dispatch to
`dfsExactCount`/`dfsAllMode`:

```go
// dfsFirstAvailable iterates sub-requests in priority order for a FirstAvailable request.
func (a *allocator) dfsFirstAvailable(claimIdx, reqIdx int, rd *RequestData) bool {
    for subIdx := range rd.SubRequests {
        if a.dfs(claimIdx, reqIdx, subIdx, 0) {
            a.selectedSubRequests[RequestKey{ClaimIndex: claimIdx, RequestIndex: reqIdx}] = subIdx
            return true
        }
    }
    return false
}
```

**Key benefit of the unified model:** No `dfsExactCountSub` or `dfsAllModeSub` functions
needed. `dfsExactCount`, `dfsAllMode`, and `tryDevice` each gained the `subReqIdx` parameter
(threaded through purely so `matchKey` can distinguish sub-requests — see design decision #6)
and otherwise read the sub-request's `Selectors`, `AllDevices`, `AllTemplateDevicesByIT`,
`NumDevices`, etc. identically to an Exactly request, since `rd` was resolved to the
sub-request entry in `dfs`.

If the entire remaining DFS tree succeeds (subsequent claims satisfied), the sub-request is
"locked in" for this IT and recorded in `selectedSubRequests`. If it fails, backtracking
unwinds all the way and `dfsFirstAvailable` tries the next sub-request.

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
        For each sub-request (priority order):      ← NEW LEVEL (re-enter dfs with subReqIdx)
          If All-mode with 0 eligible devices → fail this sub-request  ← min-1 guard
          Fill all slots via existing dfsExactCount/dfsAllMode (unified RequestData)
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

Each sub-request `RequestData` has independently computed `AllDevices` and `AllTemplateDevicesByIT` — the existing `dfsAllMode` uses these directly. A sub-request requesting "all A100s" may have 0 eligible devices for an IT that only has H100s.

#### Zero-device All mode requires an explicit guard

Upstream enforces a hard rule: **All mode requires at least one device.** An `All` request (or sub-request) that matches zero eligible devices is *unsatisfiable*, not a trivial success. See `allocator_incubating.go` (`allocateOne`):

```go
// At least one device is required for 'All' allocation mode.
if doAllDevices && len(requestData.allDevices) == 0 {
    return false, nil
}
```

Karpenter's DFS must special-case this because `numSlots` for an All-mode request is `len(AllDevices) + len(AllTemplateDevicesByIT[itID])`. When that is `0`, the natural `slotIdx >= numSlots` check would treat the request as *already fully satisfied* (all zero slots filled) and advance to the next request — succeeding with an empty allocation. For a FirstAvailable All-mode sub-request that is actively wrong: the empty sub-request would "lock in" and the fallback would never be tried; for a standalone All-mode request it would report an allocation the real kube-scheduler rejects.

The guard sits in `dfs()` immediately after computing `numSlots`, mirroring upstream:

```go
numSlots := a.numSlots(rd)
// All mode requires at least one device (matches upstream): a zero-device All-mode
// (sub-)request is unsatisfiable, not a trivial success.
if rd.AllocationMode == resourcev1.DeviceAllocationModeAll && numSlots == 0 {
    return false
}
if slotIdx >= numSlots {
    return a.dfs(claimIdx, reqIdx+1, -1, 0)
}
```

Because the guard lives in the shared `dfs()` path, it fires uniformly for both standalone All-mode requests and FirstAvailable All-mode sub-requests. A sub-request requesting "all A100s" with 0 eligible devices for an IT that only has H100s now fails immediately and falls through to the next alternative — the intended behavior.

> **Note — this fixed a latent base-allocator bug.** Before this guard, a standalone
> (non-FirstAvailable) All-mode request matching zero devices also succeeded with an empty
> allocation. That path predates prioritized alternatives; the guard corrects both cases at
> once since they share `dfs()`. Covered by the "should fail when an All-mode request matches
> zero devices" (standalone) and "should fall back from All-mode to ExactCount when All-mode
> matches zero devices" (FirstAvailable) tests.

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

Where `constraintNames` resolves based on the active request's unified fields:

```go
func (a *allocator) constraintNames(rd *RequestData) (string, string) {
    if a.activeSubRequest != nil {
        // rd.Name is the parent name; activeSubRequest.Name is the sub-request name.
        return rd.Name, a.activeSubRequest.Name
    }
    return rd.Name, ""
}
```

Alternatively, using the unified `ParentName` field directly from the request being processed:

```go
func (rd *RequestData) constraintNames() (string, string) {
    if rd.ParentName != "" {
        return rd.ParentName, rd.Name
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

> **Implemented (commit 4).** The design diverged from the commit-3 sketch below in two ways, both
> validated against upstream: the per-device request name is stored as the `RequestName{Parent, Sub}`
> **struct** (not a pre-joined string), and the commit-3 `selectedSubRequests` map is **removed**
> rather than surfaced — the per-device name subsumes it. Documented as-built below.

### Design: record the selected name per-device, not the index

Upstream never persists the selected sub-request *index*. The only thing in the API result that
encodes the choice is the per-device `DeviceRequestAllocationResult.Request` string:
`internalDeviceResult.requestName()` returns `parentRequest + "/" + request` for a selected
sub-request and just `request` for a plain request (`allocator_incubating.go`). Downstream tooling
(e.g. kube-scheduler's `computeScore`) *reconstructs* the index by matching that string back against
`req.FirstAvailable[i].Name`. Two consequences for Karpenter:

1. **The name must round-trip.** We record `parent/sub` where `sub` is exactly the sub-request's
   `Name`, so `resourceclaim.IsSubRequestRef` / `BaseRequestRef` parse it correctly.
2. **The commit-3 `selectedSubRequests` map is redundant.** Once every allocated device carries its
   qualified request name, the selection is fully recoverable from the per-device metadata — the same
   way upstream recovers it. The map (and its commit-1 `RequestKey` helper type) were **deleted** in
   commit 4; nothing read them.

Upstream stores the two names *separately* on `internalDeviceResult` (`parentRequest` + `request`)
and joins them once at result-emit time. We mirror this by storing the `RequestName{Parent, Sub}`
struct from commit 2 and calling `.String()` only at the API-emit / binding boundary — avoiding
early joining and any double-join risk.

### DeviceAllocationResult Extension

`DeviceAllocationResult` (in `ResourceClaimAllocationMetadata.Devices`) gains the owning request name:

```go
type DeviceAllocationResult struct {
    DeviceID         DeviceID
    ConsumedCapacity map[resourcev1.QualifiedName]resource.Quantity
    // RequestName is the claim request that owns this device. For Exactly requests, only Parent is
    // set (e.g. "gpu"); for a selected FirstAvailable sub-request, both Parent and Sub are set so
    // RequestName.String() yields the qualified "parent/sub" form (e.g. "gpu/a100") that the API
    // server requires in DeviceRequestAllocationResult.Request.
    RequestName RequestName
}
```

`deviceAllocationMetadata` gains a matching `requestName RequestName` field, populated in `tryDevice`
directly off the effective request `rd` — which `dfs` has already resolved to the concrete
(sub-)request entry, so no `activeSubRequest` lookup or index bookkeeping is needed:

```go
a.allocatedDevicesMetadata = append(a.allocatedDevicesMetadata, deviceAllocationMetadata{
    claimIndex:       claimIdx,
    deviceWithID:     dw,
    consumedCapacity: consumed,
    requestName:      rd.Name,  // {Parent, Sub} for a selected sub-request; {Parent} for Exactly
})
```

The IT success path copies it onto the result verbatim:

```go
meta.Devices[itID] = append(meta.Devices[itID], DeviceAllocationResult{
    DeviceID:         da.deviceWithID.ID,
    ConsumedCapacity: da.consumedCapacity,
    RequestName:      da.requestName,
})
```

This is the natural payoff of threading `subReqIdx` in commit 3: the request being processed is
always the correct one, so the qualified name falls out of `rd.Name` with no extra state. Since the
name is recorded per allocated device, a Count=2 sub-request produces two device results that both
carry the same qualified name — matching upstream's per-device denormalization.

### `dfsFirstAvailable` simplification

With the `selectedSubRequests` write removed, `dfsFirstAvailable` is a plain priority-order loop that
returns on the first sub-request whose subtree completes:

```go
func (a *allocator) dfsFirstAvailable(claimIdx, reqIdx int, rd *RequestData) bool {
    for subIdx := range rd.SubRequests {
        if a.dfs(claimIdx, reqIdx, subIdx, 0) {
            return true
        }
    }
    return false
}
```

### Integration payoff: real bindings for FirstAvailable

The consumer this unblocks is `ExpectDRAResourceClaimsBound` (test framework). Before commit 4 it
reverse-engineered each device's request name via `requestNamesForDevices`, walking `claim.Spec` in
order and consuming each request's `Count`. That hack **cannot** produce a valid binding for a
FirstAvailable request: the parent request has no `Count`, and the API server requires the qualified
`parent/sub` name, not the bare parent name. Commit 4 replaces the reconstruction with a direct read:

```go
Request: device.RequestName.String(),
```

`requestNamesForDevices` was deleted. This is what makes FirstAvailable claims actually bindable in
integration tests — previously there was zero FirstAvailable coverage under `test/suites/dra/`
because there was no correct way to form the binding.

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

### 1. Unified RequestData struct (no separate SubRequestData type)

**Decision:** A single `RequestData` struct represents both top-level requests and sub-requests. The discriminator is `ParentName != ""` (sub-request) vs `len(SubRequests) > 0` (FirstAvailable parent) vs both empty/nil (Exactly request).

**Rationale:** Upstream uses a single `requestData` struct with a `requestAccessor` interface for polymorphism. We achieve the same result more simply — since `RequestData` already has all the fields a sub-request needs (`Class`, `Selectors`, `NumDevices`, etc.), making `SubRequests []RequestData` self-referential eliminates field duplication, removes the need for separate `dfsExactCountSub`/`dfsAllModeSub` functions, and lets `validateExactRequest` return a `*RequestData` that's directly usable as a sub-request entry (just set `ParentName`).

### 2. Sub-request iteration inline in the DFS, not a separate recursive level

**Decision:** `dfsFirstAvailable` is called directly from `dfs()` when `slotIdx == 0` and the request has sub-requests. It loops over sub-requests, calling the existing `dfsExactCount`/`dfsAllMode` with the active sub-request's `*RequestData`.

**Rationale:** Adding a formal `subReqIdx` parameter to `dfs()` would require changing the signature everywhere and complicating the base cases. Instead, the sub-request iteration is encapsulated — `dfs()` only needs to detect FirstAvailable and delegate. The unified struct means the existing DFS functions work unmodified with sub-request entries. Mirrors upstream's approach of handling sub-requests as a loop within `allocateOne`.

### 2. No explicit state checkpoint at the sub-request level

**Decision:** Rely entirely on the existing DFS backtracking (device-by-device `tryDevice` unwind) to clean up after a failed sub-request attempt.

**Rationale:** The upstream allocator uses the same approach. When a sub-request's DFS fails, it means all recursive slot-filling attempts returned false, which means all `tryDevice` calls that allocated devices have already backtracked (removed from `allocatedDevices`, restored counters/capacity, removed constraints, popped requirement snapshots). No residual state remains. This is simpler and more correct than maintaining an explicit checkpoint/restore — there's no risk of forgetting to restore a field.

**Verification:** After a sub-request's DFS returns false, assert in debug builds:
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

### Commit 1: Request Validation (unified RequestData + parsing) ✓

Foundation layer. Adds `ParentName`, `SubRequests []RequestData`, `QualifiedName()`, `IsFirstAvailable()` to `RequestData`. Implements `validateFirstAvailableRequest`.

- [ClaimData and RequestData Extension](#claimdata-and-requestdata-extension)
- [Parsing FirstAvailable](#parsing-firstavailable)
- [Device Limit Validation](#device-limit-validation)

No behavioral change — requests with `FirstAvailable` are parsed but the DFS still rejects them until commit 2. Implemented in commits `ac9948f8` + `96471594`.

### Commit 2: Constraint Interface Change ✓

Updates the `Constraint` interface to pass `parentName` + `subName`, and updates `appliesTo` on all constraint types.

- [Constraint Interface Change](#constraint-interface-change)
- [Request Name Matching](#request-name-matching)

For Exactly requests, callers pass `subName=""` — no behavior change. This unblocks commit 3. Implemented using a `RequestName{Parent, Sub}` struct parameter (semantically equivalent to separate string args). Implemented in commit `0e2751e1`.

### Commit 3: DFS Extension (sub-request iteration) ✓

The core feature. Adds `dfsFirstAvailable` and threads a `subReqIdx` parameter through
`dfs`/`dfsExactCount`/`dfsAllMode`/`tryDevice`. Reuses the existing slot-filling functions via
the unified `*RequestData` (no separate `Sub` variants needed). Implemented in commit
`a413c929`.

- [New Iteration Level](#new-iteration-level)
- [State Management](#state-management)
- [Sub-Request Backtracking](#sub-request-backtracking)
- [All Mode Interaction](#all-mode-interaction)
- [matchKey extension](#6-matchkey-for-cel-cache-includes-sub-request-identity)

After this commit, FirstAvailable requests are fully functional.

**Divergences from the original sketch (all documented in-place above):**
- Used a threaded `subReqIdx` parameter instead of stateful `activeSubRequest` fields + re-entry
  routing — simpler, nothing to save/restore.
- Added a **min-1 guard for zero-device All mode** in `dfs()` — required for correctness, also
  fixes the pre-existing standalone-All-mode case. See [All Mode Interaction](#all-mode-interaction).
- Extended `countersFeasible` (partitionable devices) to handle FirstAvailable: an IT is pruned
  only if *every* sub-request is All-mode and all fail their counter budget; any ExactCount
  sub-request short-circuits to "assume feasible." Conservative — never prunes a viable IT.

**Test coverage note:** the core allocator behaviors (fallback ordering, per-IT selection,
cross-claim backtracking, constraints scoped to parent vs. `parent/sub`, template devices, All
mode + zero-device fallthrough, IT pruning) are covered. Some edge cases were intentionally left
for later (noted here rather than silently skipped): consumable-capacity interaction with
FirstAvailable sub-request backtracking, and DistinctAttribute (still unsupported at parse time).

### Commit 4: Result Tracking ✓

Records the selected (sub-)request name on each allocated device and surfaces it through the
allocation metadata, replacing the integration-test reverse-engineering hack.

- [DeviceAllocationResult Extension](#deviceallocationresult-extension)
- [Integration payoff: real bindings for FirstAvailable](#integration-payoff-real-bindings-for-firstavailable)

Adds `RequestName` to `DeviceAllocationResult` and `requestName` to `deviceAllocationMetadata`
(read off `rd.Name` in `tryDevice`). Rewires `ExpectDRAResourceClaimsBound` to read
`device.RequestName.String()` instead of `requestNamesForDevices` (deleted).

**Divergences from the commit-3 sketch (all documented in-place above):**
- Stored the `RequestName{Parent, Sub}` **struct** per device rather than a pre-joined string —
  matches upstream's store-separately-join-once (`internalDeviceResult`).
- **Removed** the commit-3 `selectedSubRequests` map and the commit-1 `RequestKey` type — the
  per-device name subsumes the selected index (upstream never persists the index either).

### Dependency Graph

```
Commit 1: Request Validation ✓
  │
  ├──→ Commit 2: Constraint Interface ✓
  │       │
  │       ▼
  ├──→ Commit 3: DFS Extension ✓ (depends on 1 + 2)
  │       │
  │       ▼
  └──→ Commit 4: Result Tracking ✓ (depends on 3)
```

All commits are incremental — each compiles and passes tests independently. Commit 3 is the bulk of the work.
