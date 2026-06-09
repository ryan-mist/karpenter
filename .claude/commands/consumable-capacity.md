# Consumable Capacity Context Loader

Load full DRA Consumable Capacity (KEP-5075) context for working on this feature.

## Instructions

Read the following file to build context:

1. `designs/dra/consumable-capacity.md` - Complete upstream feature reference (API, algorithm, rounding, constraints)

After reading, confirm context is loaded and provide a 3-sentence summary. Then ask what consumable capacity work the user wants to do.

## Quick Reference

### The Invariant
```
For each capacity dimension on a multi-allocatable device:
  committed + in_flight + new_request ≤ device.capacity[dimension].value
```

### API Fields (New)
- `Device.AllowMultipleAllocations *bool` — enables multi-allocation
- `DeviceCapacity.RequestPolicy` — Default / ValidValues / ValidRange consumption constraints
- `ExactDeviceRequest.Capacity.Requests` — per-dimension capacity requirements
- `DeviceRequestAllocationResult.ShareID` — UUID per allocation share
- `DeviceRequestAllocationResult.ConsumedCapacity` — actual consumed amounts
- `DeviceConstraint.DistinctAttribute` — uniqueness constraint for shared devices
- CEL: `device.allowMultipleAllocations` — boolean property for selectors

### Rounding Rules
- **ValidValues**: smallest value ≥ request (FAIL if exceeds all)
- **ValidRange+Step**: Min + ⌈(request - Min)/Step⌉ × Step (FAIL if > Max)
- **ValidRange no Step**: as-is within [Min, Max]
- **No policy, no request**: full device capacity consumed

### Feature Gate
`DRAConsumableCapacity` — alpha in K8s 1.34, beta target 1.36

### Upstream Code
`k8s.io/dynamic-resource-allocation@v0.35.0/structured/internal/experimental/`
