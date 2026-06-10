# DRA Context Loader

Load full DRA (Dynamic Resource Allocation) architecture context for working on DRA extensions.

## Instructions

Read the following files to build context, then summarize what you've loaded:

1. `designs/dra/scheduling.md` - Full allocator design
2. `designs/dra/cloudprovider.md` - Cloud provider interface extensions  
3. `designs/dra/nodeclaim-lifecycle.md` - NodeClaim lifecycle with DRA
4. `pkg/scheduling/dynamicresources/allocator.go` - Core allocator implementation
5. `pkg/scheduling/dynamicresources/types.go` - Core types and interfaces
6. `pkg/scheduling/dynamicresources/allocationtracker.go` - Device allocation state machine
7. `pkg/scheduling/dynamicresources/pool.go` - Pool gathering and filtering
8. `pkg/scheduling/dynamicresources/request.go` - Request validation
9. `pkg/scheduling/dynamicresources/constraint.go` - MatchAttribute constraint
10. `pkg/scheduling/dynamicresources/attributebindings.go` - Attribute binding graph
11. `pkg/cloudprovider/dynamicresources.go` - Cloud provider DRA types
12. `pkg/controllers/dynamicresources/deviceallocation/controller.go` - Device tracking controller
13. `designs/dra/consumable-capacity.md` - Upstream KEP-5075 semantics
14. `designs/dra/consumable-capacity-integration.md` - Integration design for Karpenter
15. `designs/dra/consumable-capacity-notes.md` - Implementation notes and scoping decisions

After reading, confirm context is loaded and summarize the architecture in 3-5 sentences. Then ask what DRA work the user wants to do.

## Architecture Quick Reference

### Data Flow
```
DeviceAllocation Controller (watches ResourceClaims)
    → allocatedDevices seed set
    → Allocator construction (per scheduling loop)
        → Per-pod Allocate() [read-only, parallelizable]
            → ClassifyClaims (in-cluster/in-memory/unallocated)
            → GatherPools / FilterPools (from cache or fresh)
            → ValidateClaimRequest (CEL, classes, selectors)
            → DFS per instance type (in-cluster devices first, then templates)
                → tryDevice: IsAllocated → SelectorMatch → Constraints → Topology
                → Backtracking on failure
        → Commit() [sequential, mutates shared state]
            → AllocationTracker.Commit
            → poolCache update
            → claimAllocationMetadata update
        → ReleaseInstanceType() [when ITs pruned]
```

### Key Types
- `Allocator` - Top-level, shared across pods in a scheduling loop
- `allocator` (lowercase) - Per-Allocate() child with mutable DFS state
- `AllocationTracker` - PreallocatedDevices + InflightCluster/Template allocations
- `Pool` - Group of ResourceSlices sharing (driver, poolName)
- `ClaimData` - Validated claim with Requests + Constraints
- `MatchAttributeConstraint` - Stateful constraint with pin/backtrack
- `AttributeBindings` - Transitive binding graph for runtime-only attributes
- `NodeClaim` interface - Abstracts existing/pre-initialized/in-flight nodes
- `ResourceSlice` interface - Abstracts API server slices vs cloud provider templates

### Instance Type Superposition
A NodeClaim may be compatible with many instance types. The allocator:
1. Runs DFS independently per IT
2. Accumulates requirements across ITs (prevents disjoint topology)
3. Returns surviving ITs (those whose DFS succeeded)
4. Scheduler intersects with NodeClaim's current IT set

### Thread Safety
- `Allocate()` calls are read-only on shared state (can run in parallel)
- `Commit()` and `ReleaseInstanceType()` are sequential (mutate shared state)
- CEL cache is per-child to avoid write contention
