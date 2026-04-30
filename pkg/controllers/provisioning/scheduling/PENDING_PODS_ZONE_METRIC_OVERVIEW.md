# Pending Pods by Effective Zone Metric

## Overview

The `karpenter_scheduler_pending_pods_by_effective_zone` metric tracks pending pods dimensioned by their **effective zone constraint**. This metric helps operators understand how zone constraints impact pod scheduling and identify workloads affected by zonal outages.

## Metric Definition

```
karpenter_scheduler_pending_pods_by_effective_zone{controller="provisioner", zone="<zone>"}
```

**Labels:**
- `controller`: The controller name (typically "provisioner")
- `zone`: The effective zone constraint (see values below)

## Zone Label Values

The `zone` label can have the following values:

1. **Specific zone name** (e.g., `"us-west-2a"`, `"us-east-1b"`)
   - Pod is constrained to exactly one zone
   
2. **`"flexible"`**
   - Pod can schedule to multiple zones (2 or more)
   - OR pod has no specific zone requirements
   
3. **`"none"`**
   - No intersection between pod's required zones and zones where offerings exist
   - Indicates a misconfiguration (pod can never schedule)

## How Effective Zone is Computed

The effective zone is calculated by **intersecting** two sets of zone constraints:

### 1. Zone Requirements (from pod + NodePool + volume + topology)
Combined requirements from:
- Pod's node selectors (`spec.nodeSelector`)
- Pod's node affinity (`spec.affinity.nodeAffinity`)
- NodePool requirements (`spec.template.spec.requirements`)
- Volume topology (PVCs bound to specific zones)
- Topology spread constraints

### 2. Offering Zones (from instance types)
Zones where instance type offerings exist, based on:
- NodeClass subnet configurations
- Instance type availability in each zone
- **Note:** Considers ALL offerings, regardless of whether they're currently available

### Intersection Logic

```
effectiveZones = requirementZones ∩ offeringZones

if len(effectiveZones) == 0:
    zone = "none"
elif len(effectiveZones) == 1:
    zone = effectiveZones[0]  // Actual zone name
else:
    zone = "flexible"
```

## Example Scenarios

### Scenario 1: Pod Constrained by PVC
```yaml
# Pod has no zone requirement
# PVC is in us-west-2a
# NodeClass has subnets in us-west-2a, us-west-2b, us-west-2c
```
**Result:** `zone="us-west-2a"` (volume constrains to single zone)

### Scenario 2: Pod Flexible, Offerings Constrained
```yaml
# Pod has no zone requirement  
# NodeClass only has subnets in us-west-2a
```
**Result:** `zone="us-west-2a"` (offerings constrain to single zone)

### Scenario 3: Pod and Offerings Both Flexible
```yaml
# Pod allows us-west-2a OR us-west-2b
# NodeClass has subnets in us-west-2a, us-west-2b, us-west-2c
```
**Result:** `zone="flexible"` (intersection = 2 zones)

### Scenario 4: No Intersection (Misconfiguration)
```yaml
# Pod requires us-west-2a
# NodeClass only has subnets in us-west-2b
```
**Result:** `zone="none"` (pod can never schedule - configuration error)

### Scenario 5: Zone Outage Impact
```yaml
# Pod requires us-west-2a OR us-west-2b
# NodeClass has offerings in us-west-2a only (us-west-2b marked unavailable via zonal shift)
```
**Result:** `zone="us-west-2a"` (intersection = 1 zone due to outage)

## Use Cases

### Identifying Zonal Outage Impact
```promql
# Pods constrained to us-west-2a (affected if this zone has outage)
karpenter_scheduler_pending_pods_by_effective_zone{zone="us-west-2a"}

# All zone-constrained pods (sum across all specific zones)
sum(karpenter_scheduler_pending_pods_by_effective_zone{zone!="flexible",zone!="none"})
```

### Identifying Misconfigured Workloads
```promql
# Pods that can never schedule (zone mismatch)
karpenter_scheduler_pending_pods_by_effective_zone{zone="none"}
```

### Understanding Zone Flexibility
```promql
# Flexible pods (low risk from zonal outages)
karpenter_scheduler_pending_pods_by_effective_zone{zone="flexible"}

# Constrained pods (high risk from zonal outages)
sum(karpenter_scheduler_pending_pods_by_effective_zone{zone!="flexible",zone!="none"})
```

### Capacity Planning
```promql
# Distribution of pending pods across zones
topk(5, karpenter_scheduler_pending_pods_by_effective_zone)
```

## Implementation Details

### Efficient Computation
The effective zone is computed **once** during instance type filtering in `filterInstanceTypesByRequirements()`:
- Happens when all requirements have been combined (pod + NodePool + volume + topology)
- Stored in `InstanceTypeFilterError.effectiveZone`
- No redundant computation when tracking the metric

### Code Locations
- **Metric definition**: `pkg/controllers/provisioning/scheduling/metrics.go`
- **Zone computation**: `pkg/controllers/provisioning/scheduling/nodeclaim.go` → `computeEffectiveZone()`
- **Metric tracking**: `pkg/controllers/provisioning/scheduling/scheduler.go` → `trackPendingPodsByEffectiveZone()`

## Cardinality

The metric creates one series per unique zone value per controller. In a typical AWS region with 3-4 availability zones:
- ~3-4 series for specific zones
- 1 series for "flexible"
- 1 series for "none" (if misconfigurations exist)

**Total**: ~5-6 series per controller (low cardinality)
