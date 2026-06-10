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
