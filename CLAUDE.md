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

Active development branch: `feat/dra-allocator`. The DRA allocator simulates Kubernetes DRA scheduling for Karpenter's model where NodeClaims represent multiple candidate instance types.

Key entry points:
- `pkg/scheduling/dynamicresources/allocator.go` - Core allocator (DFS + commit protocol)
- `pkg/scheduling/dynamicresources/types.go` - NodeClaim/ResourceSlice interfaces
- `pkg/controllers/dynamicresources/deviceallocation/controller.go` - Device tracking
- `designs/dra/scheduling.md` - Authoritative design doc

Currently out of scope: adminAccess, partitionable devices, consumable capacity, device taints, consolidation, FirstAvailable requests.
