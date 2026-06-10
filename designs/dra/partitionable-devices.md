# Partitionable Devices (KEP-4815)

## Status: Out of Scope (future work)

Partitionable devices (SharedCounters) are explicitly deferred from the current DRA allocator implementation. This doc captures upstream context for when we implement support.

---

## Overview

SharedCounters allow a device to declare shared numeric constraints that are consumed at allocation time. Unlike consumable capacity (which tracks runtime usage across multiple allocations), SharedCounters are checked once at initial allocation as a structural constraint.

A device can be both partitionable (SharedCounters) AND multi-allocatable (consumable capacity). The checks are independent — both must pass for allocation to succeed.

---

## Upstream Bug: checkAvailableCounters (PR #139040, May 2026)

### The Bug

`checkAvailableCounters()` in the upstream allocator rebuilds counter consumption state by iterating allocated devices. When determining which devices have consumed counters, it only checked:

```go
alloc.allocatedState.AllocatedDevices.Has(deviceID)
```

Multi-allocatable devices with shared allocations do NOT appear in `AllocatedDevices` — they are tracked in `AggregatedCapacity` and `AllocatedSharedDeviceIDs` instead. This caused counter consumption from shared allocations to be invisible, leading to potential over-allocation of SharedCounter budgets.

### The Fix

The fix uses `IsDeviceAllocated()` which checks all three sources:
- `AllocatedDevices` (exclusive allocations)
- `AllocatedSharedDeviceIDs` (shared allocation identifiers)
- `AggregatedCapacity` (consumed capacity tracking)

### Relevance to Karpenter

When we implement SharedCounters, we need to ensure counter state rebuilding accounts for multi-allocatable devices. Our consumable capacity design routes multi-allocatable devices away from binary allocation sets entirely (they never enter `PreallocatedDevices`), so the counter rebuilding logic must query the consumed capacity tracker to discover which multi-allocatable devices have active allocations.

---

## Key References

| Reference | Path |
|-----------|------|
| Upstream SharedCounters | `k8s.io/dynamic-resource-allocation@v0.35.0/structured/internal/experimental/allocator_experimental.go` |
| Bug fix PR | kubernetes/kubernetes#139040 |
| Consumable capacity interaction | `designs/dra/consumable-capacity.md` § Interaction with Other Features |
