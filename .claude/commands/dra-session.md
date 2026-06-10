# DRA Expert Session

Activate DRA expert delegation mode for this session.

## Instructions

For the rest of this session, when the user asks DRA-related questions or requests DRA work:

1. **Route to the appropriate expert agent** rather than answering directly:
   - **Karpenter DRA allocator** questions (DFS, AllocationTracker, pools, constraints, commit protocol, instance type superposition, NodeClaim interface) → spawn `dra-expert` agent
   - **Consumable capacity / KEP-5075** questions (multi-allocatable devices, capacity accounting, RequestPolicy, rounding, DistinctAttribute, ShareID) → spawn `consumable-capacity-expert` agent
   - **Both** (integration design, how consumable capacity fits into Karpenter's allocator) → spawn both agents with the relevant sub-question, then synthesize their answers

2. **Before delegating**, briefly state which agent(s) you're consulting and why (one sentence).

3. **After receiving agent results**, synthesize the answer concisely for the user. Don't just paste the raw agent output — interpret it in context.

4. **For implementation tasks** (writing code, not just questions), read the relevant design docs yourself first then use agents for review/validation of your approach:
   - `designs/dra/scheduling.md` — Allocator design (DFS, pools, constraints, commit protocol)
   - `designs/dra/consumable-capacity.md` — Upstream KEP-5075 semantics
   - `designs/dra/consumable-capacity-integration.md` — Karpenter integration design (how CC maps into the allocator)
   - `designs/dra/consumable-capacity-notes.md` — Scoping decisions, known upstream bugs, follow-ups

5. **For non-DRA tasks**, proceed normally without delegation.

## Available Agents

| Agent | Domain | Use When |
|-------|--------|----------|
| `dra-expert` | Karpenter DRA allocator internals | Questions about the existing allocator DFS, constraint system, pool management, commit protocol, thread safety |
| `consumable-capacity-expert` | Upstream KEP-5075 semantics | Questions about multi-allocatable devices, capacity verification, rounding rules, DistinctAttribute, ShareID |

## Confirm

After reading this, respond with: "DRA session active. I'll delegate to expert agents for DRA questions — `dra-expert` for Karpenter allocator internals, `consumable-capacity-expert` for upstream KEP-5075 semantics."
