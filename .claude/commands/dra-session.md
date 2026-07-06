# DRA Expert Session

Activate DRA expert delegation mode for this session.

## Instructions

For the rest of this session, when the user asks DRA-related questions or requests DRA work:

1. **Route to the appropriate expert agent** rather than answering directly:
   - **Karpenter DRA allocator** questions (DFS, AllocationTracker, pools, constraints, commit protocol, instance type superposition, NodeClaim interface) → spawn `dra-expert` agent
   - **Consumable capacity / KEP-5075** questions (multi-allocatable devices, capacity accounting, RequestPolicy, rounding, DistinctAttribute, ShareID) → spawn `consumable-capacity-expert` agent
   - **Partitionable devices / KEP-4815** questions (SharedCounters, counter budgets, overlapping partitions, PerDeviceNodeSelection, multi-host devices, MIG partitioning) → spawn `partitionable-devices-expert` agent
   - **Prioritized alternatives / KEP-4816** questions (FirstAvailable, ordered sub-requests, fallback behavior, sub-request state restoration, preference scoring) → spawn `prioritized-alternatives-expert` agent
   - **Both CC + PD** (how consumable capacity and partitionable devices interact, devices with both ConsumesCounters and AllowMultipleAllocations) → spawn both `consumable-capacity-expert` and `partitionable-devices-expert`
   - **PA interactions** (how FirstAvailable interacts with capacity, counters, or constraints) → spawn `prioritized-alternatives-expert` + the relevant feature expert
   - **Integration design** (how any feature maps into Karpenter's allocator) → spawn the relevant expert + `dra-expert`, then synthesize

2. **Before delegating**, briefly state which agent(s) you're consulting and why (one sentence).

3. **After receiving agent results**, synthesize the answer concisely for the user. Don't just paste the raw agent output — interpret it in context.

4. **For implementation tasks** (writing code, not just questions), read the relevant design docs yourself first then use agents for review/validation of your approach:
   - `designs/dra/scheduling.md` — Allocator design (DFS, pools, constraints, commit protocol)
   - `designs/dra/consumable-capacity.md` — Upstream KEP-5075 semantics
   - `designs/dra/consumable-capacity-integration.md` — Karpenter integration design (how CC maps into the allocator)
   - `designs/dra/consumable-capacity-notes.md` — Scoping decisions, known upstream bugs, follow-ups
   - `designs/dra/partitionable-devices.md` — Upstream KEP-4815 semantics
   - `designs/dra/partitionable-devices-integration.md` — Karpenter integration design (SharedCounters, PerDeviceNodeSelection)
   - `designs/dra/partitionable-devices-notes.md` — Scoping decisions, open questions, follow-ups
   - `designs/dra/prioritized-alternatives.md` — Upstream KEP-4816 semantics (when created)
   - `designs/dra/prioritized-alternatives-integration.md` — Karpenter integration design (when created)

5. **For non-DRA tasks**, proceed normally without delegation.

## Available Agents

| Agent | Domain | Use When |
|-------|--------|----------|
| `dra-expert` | Karpenter DRA allocator internals | Questions about the existing allocator DFS, constraint system, pool management, commit protocol, thread safety |
| `consumable-capacity-expert` | Upstream KEP-5075 semantics | Questions about multi-allocatable devices, capacity verification, rounding rules, DistinctAttribute, ShareID |
| `partitionable-devices-expert` | Upstream KEP-4815 semantics | Questions about SharedCounters, counter budgets, overlapping partitions, PerDeviceNodeSelection, MIG, multi-host TPU |
| `prioritized-alternatives-expert` | Upstream KEP-4816 semantics | Questions about FirstAvailable, ordered sub-request fallback, state restoration between attempts, preference scoring |

## Confirm

After reading this, respond with: "DRA session active. I'll delegate to expert agents for DRA questions — `dra-expert` for Karpenter allocator internals, `consumable-capacity-expert` for KEP-5075 semantics, `partitionable-devices-expert` for KEP-4815 semantics, `prioritized-alternatives-expert` for KEP-4816 semantics."
