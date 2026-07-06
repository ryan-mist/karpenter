# Karpenter

Kubernetes Node Autoprovisioner. Automatically provisions new nodes in response to unschedulable pods.

## Build & Test

```bash
# Build (from package root, output is verbose - redirect to file)
go build ./...

# Run tests
go test ./... -count=1

# Run specific package tests
go test ./pkg/scheduling/dynamicresources/... -count=1

# Run tests requiring DRAConsumableCapacity (needs 1.36+ envtest binary)
KUBEBUILDER_ASSETS=$(setup-envtest use 1.36.0 -p path) go test ./pkg/controllers/dynamicresources/deviceallocation/... -count=1

# Run integration tests (requires cluster)
go test ./test/suites/dra/... -count=1
```

## Project Structure

- `pkg/controllers/provisioning/scheduling/` - Core scheduler (evaluates pods against NodeClaims)
- `pkg/scheduling/` - Scheduling primitives (requirements, topology, etc.)
- `pkg/scheduling/dynamicresources/` - DRA device allocator
- `pkg/controllers/dynamicresources/deviceallocation/` - Device allocation tracking controller
- `pkg/cloudprovider/` - Cloud provider interface (InstanceType, DynamicResources, etc.)
- `pkg/controllers/provisioning/` - Provisioning loop (creates NodeClaims)
- `pkg/controllers/nodeclaim/` - NodeClaim lifecycle controllers
- `pkg/controllers/node/` - Node lifecycle controllers
- `designs/` - Design documents
- `dra-kwok-driver/` - KWOK-based DRA driver for integration testing
- `test/suites/` - Integration test suites

## Key Patterns

- Uses controller-runtime (sigs.k8s.io/controller-runtime)
- Tests use Ginkgo/Gomega
- `unique.Handle[string]` used extensively for interned string comparison (DRA IDs)
- `scheduling.Requirements` is the central type for node constraint tracking
- NodeClaims represent a superposition of candidate instance types until one is selected

## DRA (Dynamic Resource Allocation)

The DRA allocator simulates Kubernetes DRA scheduling for Karpenter's model where NodeClaims represent multiple candidate instance types.

**Implementation code:** `/Users/ryanmist/Desktop/karp/karpenter` (main branch)
**Planning/design docs:** `/Users/ryanmist/Desktop/karpenter-plan` (this repo)

### Merged Features

- **Base allocator** (DFS + commit protocol) — merged as `feat/dra-allocator`
- **Consumable Capacity (KEP-5075)** + **Partitionable Devices (KEP-4815)** — merged in commit `61dff3a2` ("feat: dra consumable capacity + partitionable devices support (#3110)")

### Active Development: Prioritized Alternatives (KEP-4816)

FirstAvailable sub-request lists with ordered fallback semantics.

- Upstream KEP: https://github.com/kubernetes/enhancements/blob/master/keps/sig-scheduling/4816-dra-prioritized-list/README.md
- Feature gate: `DRAPrioritizedList` (alpha K8s 1.34)
- Design docs (when created): `designs/dra/prioritized-alternatives.md`, `designs/dra/prioritized-alternatives-integration.md`

### Key Entry Points

- `pkg/scheduling/dynamicresources/allocator.go` - Core allocator (DFS + commit protocol)
- `pkg/scheduling/dynamicresources/types.go` - NodeClaim/ResourceSlice interfaces
- `pkg/controllers/dynamicresources/deviceallocation/controller.go` - Device tracking
- `designs/dra/scheduling.md` - Authoritative allocator design doc

### Completed Design Docs

- `designs/dra/consumable-capacity.md` — KEP-5075 upstream semantics
- `designs/dra/consumable-capacity-integration.md` — Karpenter integration (implemented)
- `designs/dra/partitionable-devices.md` — KEP-4815 upstream semantics
- `designs/dra/partitionable-devices-integration.md` — Karpenter integration (implemented)

### Remaining Out of Scope

adminAccess, device taints, consolidation, non-node-local in-flight devices, multi-solution optimization.
