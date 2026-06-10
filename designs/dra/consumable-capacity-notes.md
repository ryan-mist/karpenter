# Consumable Capacity — Implementation Notes

Running notes capturing scoping decisions, observations, and follow-ups for the Karpenter consumable capacity implementation.

---

## Request Type Support

Consumable capacity adds a `Capacity *CapacityRequirements` field to each device request type:

- `ExactDeviceRequest.Capacity` — used with `Exactly` dispatch
- `DeviceSubRequest.Capacity` — used within `FirstAvailable` prioritized lists

The capacity accounting logic is identical regardless of which struct it comes from — once a concrete request is resolved, the same verification check runs.

**Today Karpenter only supports `ExactDeviceRequest`** (with both `ExactCount` and `All` allocation modes). `FirstAvailable` / `DeviceSubRequest` is explicitly out of scope.

**Decision:** We'll implement consumable capacity for `ExactDeviceRequest` only. Support for `Capacity` on `DeviceSubRequest` is a follow-up tied to `FirstAvailable` support.

---

## Device.Capacity — Existing Gap in CEL Evaluation

`Device.Capacity` has been in the upstream API since v1alpha3 (k8s 1.31). Before KEP-5075, it served a single purpose: **CEL selector expressions**. The upstream allocator passes capacity to the CEL environment so selectors like `device.capacity["memory"].compareTo(quantity("8Gi")) >= 0` work.

Karpenter's `cloudprovider.Device` currently only carries `Name` and `Attributes` — not `Capacity`. This means capacity-based CEL selectors would fail in Karpenter today. This is a gap in the baseline allocator, not something introduced by consumable capacity.

For exclusive-only allocation, `Device.Capacity` has no role in the *allocation decision* (binary: free/taken). But it should have been exposed for CEL *selection*. Adding `Capacity` to `cloudprovider.Device` for the consumable capacity work fixes this gap naturally.

**Decision:** The `cloudprovider.Device` extension to add `Capacity` is both a consumable-capacity prerequisite AND a fix for a pre-existing CEL evaluation gap. We should treat it as a baseline fix rather than purely a consumable-capacity addition.

---

## Upstream DistinctAttribute Bug — count > 1

The upstream `distinctAttributeConstraint` (`k8s.io/dynamic-resource-allocation@v0.35.0/structured/internal/experimental/constraint.go:44-84`) keys state by `requestName` in a `map[string]DeviceAttribute`. For a single request with `count > 1`, all slots share the same map key — each insertion overwrites the previous slot's value, destroying the constraint's memory of what was already allocated.

### Scenario: Multi-Homed Pod with Bandwidth Shares

A traffic-routing pod needs 3 independent network paths for redundancy. It requests 2Gi bandwidth on each, constrained to distinct NICs so a single NIC failure doesn't take out multiple paths.

**ResourceSlice** — CNI driver publishes 3 multi-allocatable NICs:

```yaml
apiVersion: resource.k8s.io/v1
kind: ResourceSlice
metadata:
  name: node-worker-01-nics
spec:
  driver: networking.example.com
  pool:
    name: node-worker-01-nics
    generation: 1
    resourceSliceCount: 1
  nodeName: node-worker-01
  devices:
  - name: nic-0
    basic:
      attributes:
        networking.example.com/device-name:
          string: "nic-0"
        networking.example.com/speed:
          string: "25Gbps"
      capacity:
        networking.example.com/bandwidth:
          value: "25Gi"
      allowMultipleAllocations: true
  - name: nic-1
    basic:
      attributes:
        networking.example.com/device-name:
          string: "nic-1"
        networking.example.com/speed:
          string: "25Gbps"
      capacity:
        networking.example.com/bandwidth:
          value: "25Gi"
      allowMultipleAllocations: true
  - name: nic-2
    basic:
      attributes:
        networking.example.com/device-name:
          string: "nic-2"
        networking.example.com/speed:
          string: "25Gbps"
      capacity:
        networking.example.com/bandwidth:
          value: "25Gi"
      allowMultipleAllocations: true
```

**DeviceClass:**

```yaml
apiVersion: resource.k8s.io/v1
kind: DeviceClass
metadata:
  name: multi-alloc-nic
spec:
  selectors:
  - cel:
      expression: >-
        device.driver == "networking.example.com" &&
        device.allowMultipleAllocations == true
```

**ResourceClaim** — valid claim that triggers the upstream allocator bug:

```yaml
apiVersion: resource.k8s.io/v1
kind: ResourceClaim
metadata:
  name: multi-homed-paths
  namespace: prod
spec:
  devices:
    requests:
    - name: net-paths
      exactly:
        deviceClassName: multi-alloc-nic
        count: 3
        capacity:
          requests:
            networking.example.com/bandwidth: "2Gi"
    constraints:
    - requests: ["net-paths"]
      distinctAttribute: "networking.example.com/device-name"
```

Intent: "Give me 3 bandwidth shares on 3 DIFFERENT NICs." This claim is perfectly valid — the bug is in the upstream allocator's constraint tracking.

**Pod:**

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: traffic-router
  namespace: prod
spec:
  containers:
  - name: router
    image: registry.example.com/traffic-router:v2
    resources:
      claims:
      - name: network
        request: net-paths
  resourceClaims:
  - name: network
    resourceClaimName: multi-homed-paths
```

### Bug Trace

The upstream allocator iterates devices for request "net-paths" with count:3. Since devices are multi-allocatable, `deviceInUse` doesn't block re-visits. The DistinctAttribute constraint is the only guard against same-device reuse.

```
Slot 0: try nic-0
  constraint.add("net-paths", "", nic-0):
    attribute = lookupAttribute(nic-0, "networking.example.com/device-name") → "nic-0"
    numDevices == 0 → first device, always accept
    m.attributes["net-paths"] = "nic-0"
    numDevices = 1
  Capacity check: 2Gi <= 25Gi ✓
  → ALLOCATE nic-0 for slot 0

Slot 1: try nic-0 again (multi-allocatable, not blocked by deviceInUse)
  constraint.add("net-paths", "", nic-0):
    attribute = "nic-0"
    numDevices == 1 → matchesAttribute("nic-0")
    iterates map: "net-paths" → "nic-0", compare "nic-0" == "nic-0" → DUPLICATE
  → REJECT ✓ (correct — adjacent duplicate caught)

Slot 1: try nic-1
  constraint.add("net-paths", "", nic-1):
    attribute = "nic-1"
    matchesAttribute("nic-1"): "nic-0" ≠ "nic-1" → distinct
    m.attributes["net-paths"] = "nic-1"  ← OVERWRITES "nic-0"!
    numDevices = 2
  Capacity check: 2Gi <= 25Gi ✓
  → ALLOCATE nic-1 for slot 1

  Map state: {"net-paths": "nic-1"}, numDevices: 2
  (map has 1 entry despite tracking 2 devices — "nic-0" is gone)

Slot 2: try nic-0 again
  constraint.add("net-paths", "", nic-0):
    attribute = "nic-0"
    matchesAttribute("nic-0"): iterate map → only entry is "net-paths":"nic-1"
    "nic-0" ≠ "nic-1" → looks distinct!
  → ACCEPT ← BUG! nic-0 was already allocated in slot 0!

  Capacity check: 2Gi + 2Gi = 4Gi <= 25Gi ✓ (second share on nic-0)
  → ALLOCATE nic-0 for slot 2
```

### Incorrect Result

```yaml
status:
  allocation:
    devices:
      results:
      - request: net-paths
        driver: networking.example.com
        pool: node-worker-01-nics
        device: nic-0
        shareID: "a1b2c3d4-..."
        consumedCapacity:
          networking.example.com/bandwidth: "2Gi"
      - request: net-paths
        driver: networking.example.com
        pool: node-worker-01-nics
        device: nic-1
        shareID: "e5f6a7b8-..."
        consumedCapacity:
          networking.example.com/bandwidth: "2Gi"
      - request: net-paths
        driver: networking.example.com
        pool: node-worker-01-nics
        device: nic-0                    # ← DUPLICATE of slot 0
        shareID: "c9d0e1f2-..."
        consumedCapacity:
          networking.example.com/bandwidth: "2Gi"
```

nic-0 appears twice — single NIC failure takes out 2/3 network paths. The DistinctAttribute constraint was supposed to prevent this.

### Root Cause

`m.attributes` is `map[string]DeviceAttribute` keyed by `requestName`. All 3 slots share key `"net-paths"`. Each `add()` call overwrites the previous entry. By slot 2, only slot 1's value ("nic-1") remains in the map — slot 0's value ("nic-0") was destroyed by the overwrite at slot 1.

### Our Fix (slice-based)

```go
type DistinctAttributeConstraint struct {
    RequestNames    sets.Set[string]
    AttributeName   resourcev1.QualifiedName
    allocatedValues []resourcev1.DeviceAttribute  // append on Add, pop on Remove
}

func (d *DistinctAttributeConstraint) Add(...) bool {
    // ... lookup attribute ...
    for _, v := range d.allocatedValues {
        if AttributeValuesEqual(&v, attribute) {
            return false  // checks ALL prior values, not just last
        }
    }
    d.allocatedValues = append(d.allocatedValues, *attribute)
    return true
}

func (d *DistinctAttributeConstraint) Remove(...) {
    d.allocatedValues = d.allocatedValues[:len(d.allocatedValues)-1]  // LIFO pop
}
```

With this fix, slot 2 checks "nic-0" against ALL prior values `["nic-0", "nic-1"]` → finds duplicate at index 0 → correctly REJECTS. The allocator would then try nic-2, which has a distinct value, and succeed with `[nic-0, nic-1, nic-2]`.

**Decision:** Our implementation uses the slice approach. All upstream tests avoid the bug by using separate requests (count:1 each, giving unique map keys). The bug is latent but real for `count > 1`.

---

## DistinctAttribute with Template Devices

DistinctAttribute is NOT limited to in-cluster devices. Two mechanisms:

1. **Device-name (primary use case):** Synthesize `resource.k8s.io/device-name` in `LookupAttribute` from `device.Name`. Device names are structurally unique within a pool. Works for both template and in-cluster devices with zero cloud provider burden.

2. **Topology attributes (physical-port, NUMA):** Cloud provider publishes concrete values on template devices. If it knows the topology, it knows the values.

**Inverse-of-bindings explored and rejected:** Distinctness is not transitive (A≠B, B≠C ⊬ A≠C), so no closure is possible. In practice, knowing devices are distinct implies knowing their distinguishing values. Revisit if a concrete use case surfaces where the provider knows distinctness but not values.

---

## Constraint State Reset — Pre-Existing Bug

When `dfs()` returns `true` (IT succeeds), constraints are left in "fully allocated" state. `restoreState()` between IT attempts does NOT reset them. The next IT inherits stale constraint state (pinned values, allocated device IDs).

This affects `MatchAttributeConstraint` **today** — not just DistinctAttribute. Example: IT-A pins NUMA to "node-0". IT-B starts with that pin still active and is forced to match "node-0" even though its devices might only have "node-1".

**Decision:** Add `Reset()` to the `Constraint` interface. Call it in `restoreState()`. This is a prerequisite fix for correct consumable capacity support but also fixes a pre-existing baseline bug.
