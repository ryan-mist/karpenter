# Prioritized Alternatives Context Loader

Load full DRA Prioritized Alternatives (KEP-4816) context for working on this feature.

## Instructions

Read the following files to build context:

1. `designs/dra/prioritized-alternatives.md` - Upstream KEP-4816 semantics
2. `designs/dra/prioritized-alternatives-integration.md` - Karpenter integration design
3. `designs/dra/scheduling.md` - Base allocator design (for understanding DFS/backtracking context)


After reading, confirm context is loaded and provide a 3-sentence summary. Then ask what prioritized alternatives work the user wants to do.

## Quick Reference

### The Algorithm

```
For a DeviceRequest with firstAvailable:
  For each sub-request in priority order (index 0 = highest):
    Save full allocator state
    Attempt allocation as normal ExactDeviceRequest
    If success → record selected index, done
    If failure → restore state completely, try next
  If all fail → DeviceRequest unsatisfiable
```

### API Structure

```yaml
requests:
- name: gpu
  firstAvailable:
  - request:          # sub-request 0 (highest priority)
      deviceClassName: gpu-a100
      selectors: [...]
      count: 1
  - request:          # sub-request 1 (fallback)
      deviceClassName: gpu-any
      count: 2
```

### Key Invariants

- Exactly ONE sub-request selected per DeviceRequest
- State FULLY restored between sub-request attempts (constraints, capacity, counters)
- First satisfiable sub-request wins (no optimization)
- Cross-request constraints resolve against the SELECTED sub-request's devices only

### Backtracking Depth

```
instance types → claims → requests → sub-requests (NEW) → device slots → candidates
```

### Feature Gate

`DRAPrioritizedList` — alpha in K8s 1.34

### Upstream KEP

https://github.com/kubernetes/enhancements/blob/master/keps/sig-scheduling/4816-dra-prioritized-list/README.md
