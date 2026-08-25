# Consolidation Prefilter: A Sound Cheap Gate Before SimulateScheduling

<!-- Reject provably-doomed candidate node-removal sets before paying for a full
     scheduling simulation, at zero correctness risk. -->

## Motivation

Karpenter's multi-node consolidation works by proposing a set of nodes to remove and asking the scheduler whether the displaced pods can be re-homed more cheaply. The question is answered by `SimulateScheduling` (a full scheduler `Solve`), which is the **expensive** step: it does topology tracking, instance-type filtering, and deep copies, and it grows **super-linearly with cluster size** ([kubernetes-sigs/karpenter#2972](https://github.com/kubernetes-sigs/karpenter/issues/2972)). Today's binary search over a sorted candidate list already pays one `Solve` per evaluated set; any richer candidate generator pays far more.

Every one of those `Solve` calls on an obviously-doomed set is wasted work. A removal set that strips more capacity than the remaining nodes can hold, or whose only feasible replacement costs more than what it removes, or that would violate a topology-spread constraint, cannot possibly consolidate — but Karpenter still runs the full simulation to find that out.

This RFC proposes a **prefilter**: a sound, cheap gate that runs *before* `SimulateScheduling` and rejects sets it can *prove* are doomed, so the scheduler is only invoked on sets that might actually consolidate. The prefilter is **orthogonal to the candidate generator** — it makes no assumption about how sets are proposed — so it can ship on its own, independent of the consolidation-algorithm rework tracked separately (see [`09-repacking-pivot.md`](09-repacking-pivot.md)).

### Use Cases

1. **Over-aggressive removal sets today.** Binary search evaluates prefixes that remove too many nodes at once; each infeasible prefix costs a full `Solve`. On the reproduced [#2434](https://github.com/kubernetes-sigs/karpenter/issues/2434) shape, 25% of enumerated sets are provably capacity-doomed.
2. **Large / tight clusters (#2972).** As clusters grow, `SimulateScheduling` cost rises super-linearly while the prefilter cost stays near-linear. On a tight 40-node population, 57% of sampled sets are capacity-doomed and can be discarded in microseconds each.
3. **Many-candidate generators (the enabler).** The repacking formulation ([`09`](09-repacking-pivot.md)) proposes many any-k target shapes (matching maintainer direction in [#3141](https://github.com/kubernetes-sigs/karpenter/issues/3141) scored-candidate-list and [#2814](https://github.com/kubernetes-sigs/karpenter/issues/2814) grouped generation). Such generators are only affordable at scale if most proposals can be rejected cheaply. The prefilter is what unblocks them.
4. **Topology-spread-heavy workloads.** Removing nodes across multiple zones can strand a spread-constrained deployment in a way a single replacement node cannot fix. A cheap topology check rejects these without a `Solve`.

### Non-Goals

- **Computing the plan.** The prefilter never decides *what* to consolidate or *how much* it saves. `SimulateScheduling` remains the sole authority on feasibility and cost.
- **Being complete.** The prefilter is not required to reject every doomed set — only to never reject a *good* one. Missed rejections cost a wasted `Solve`, not correctness.
- **Replacing the generator.** It filters proposals; it does not produce them.
- **Modeling every constraint.** Anti-affinity reachability/cardinality and non-homogeneous or multi-TSC topology groups are explicitly left to fall through to the scheduler (see [Out of Scope](#out-of-scope-fall-through-to-the-scheduler)).

## Proposal

### The Contract

```
prefilter(S) -> REJECT | PASS
```

- `REJECT` ⟹ **provably** no worthwhile consolidation exists for removing set `S` → skip the scheduler call.
- `PASS` ⟹ maybe worthwhile → fall through to `SimulateScheduling`, which decides.

The prefilter sits between the candidate generator and the `Solve`:

```
generator → [ prefilter(S) ] → REJECT ↦ discard (no Solve)
                             → PASS   ↦ SimulateScheduling(S) → decide
```

**Hard contract — soundness / fail-closed.** The prefilter must **never `REJECT` a set the scheduler could consolidate** (zero false negatives). Every check is constructed as a **strict relaxation** of the real re-homing problem, so `reject ⇒ truly doomed`. Anything uncertain, unmodeled, or ambiguous resolves to `PASS`. A `PASS` is merely *necessary*, never *sufficient* — correctness is preserved by deferring to the exact scheduler.

This asymmetry is the whole design: the prefilter is allowed to be blind (under-prune) but never wrong (false-reject). It follows that the prefilter can be enabled unconditionally without changing *which* consolidations Karpenter performs — only how quickly it decides not to attempt the doomed ones.

### The Three Checks

Notation, per candidate set `S`: `P` = displaced pods, `T` = remaining (target) nodes, `d` ∈ {cpu, mem, …} = resource dimensions. `demand_d` = Σ requests of displaced pods; `headroom_d` = Σ `Available()` over remaining nodes; `overflow_d = max(0, demand_d − headroom_d)`; `price(S)` = Σ prices of the removed nodes.

#### 1. Capacity — DELETE feasibility · O(P+T)

`REJECT` if `demand_d > headroom_d` for any `d`: the displaced pods provably cannot fit the remaining nodes with **no new node**, so a DELETE is infeasible. This is a plain per-dimension sum comparison — no graph.

**Soundness.** Aggregate demand exceeding aggregate remaining capacity in any single dimension is a necessary condition for infeasibility (a splittable, per-dimension relaxation of the real integral multi-dimensional packing). If the relaxed problem has no room, the real one certainly doesn't.

**Validated.** 0 false negatives across 2,745 sets, and **provably equivalent to a max-flow** for pure capacity (see [Validation](#validation) and [Alternative: Max-Flow Prefilter](#alternative-max-flow-prefilter)).

#### 2. Cost — REPLACE worthwhileness · O(#instance types)

A REPLACE re-homes the displaced pods onto the remaining nodes **plus one** new node (Karpenter allows at most one replacement, the *m→1* rule). Only the `overflow` must go to that new node. `REJECT the replace` if **no instance type with a permitted offering cheaper than `price(S)` has allocatable ≥ `overflow_d` for every `d`** — i.e. no cheaper node can even hold the leftover, so no replace can both fit and save money. This mirrors Karpenter's own `RemoveInstanceTypeOptionsByPriceAndMinValues`.

**Why this is a separate check — the replacement sinkhole.** The capacity check with a *generous* replacement is a sinkhole on the replace path: a big new node absorbs almost any small replace, so a feasibility check prunes nothing there. Only *cost* distinguishes a worthwhile replace from a no-op.

**Soundness.** By conservation, in any replace the remaining nodes hold ≤ `headroom_d`, so the single new node must hold ≥ `demand_d − headroom_d = overflow_d` in every dimension — a viable replacement's capacity ≥ `overflow_d` is *necessary*. If no permitted type both meets that and is cheaper than `price(S)`, the oracle can only no-op. The check is generous in every uncertain direction (largest capacity for "does it fit", cheapest permitted offering for "is it cheaper", ignores the pods' own scheduling constraints) so it only ever errs toward `PASS`.

**Validated.** 0 false negatives across 1,764 sets; **+23–61% marginal prune over the capacity check** in the cost-dominated regime the capacity sinkhole is blind to (`sims/price-prune-results.md`).

#### 3. Skew — TSC feasibility · O(Z·m)

For a cleanly-identified **homogeneous** group `D` (identical request `r`) governed by a single zonal `DoNotSchedule` TopologySpread constraint (`maxSkew=k`) over domains (zones) `z`:

```
existing_z = D-pods that STAY in zone z (on nodes not in S)          # fixed
cap_z      = additional D-pods zone z's remaining nodes can hold      # Σ floor(avail/r)
final_z    = existing_z + placed_z,  0 ≤ placed_z ≤ cap_z,  Σ placed_z = P_D
feasible ⇔ ∃ assignment with max_z final_z − min_z final_z ≤ k
```

`maxSkew` is encoded by **enumerating the target minimum level `m`**: feasible-for-`m` ⇔ every zone's `placed_z` fits `[L_z(m), U_z(m)]` and `ΣL_z(m) ≤ P_D ≤ ΣU_z(m)`, where `L_z(m) = max(0, m − existing_z)` and `U_z(m) = min(cap_z, m+k − existing_z)`. With no zone-pinning this is a pure O(Z·m) count check (a max-flow is needed only when volume topology pins pods to zones). The **m→1 replacement** adds capacity in one zone: enumerate `z*` (plus the no-replacement/delete case) and boost `cap_{z*}`. `REJECT S` iff **no `(z*, m)` is feasible**. A skew-doomed group dooms the whole set.

**Domains are the LIVE zones only** — a zone with no remaining node is not a spread domain and must not be modeled as a 0-count domain; the replacement's zone is added as a new domain when it lands in an empty zone. (Getting this wrong is unsound — see [Correctness Lessons](#correctness-lessons).)

**Eligibility is fail-closed.** A group qualifies only if it is a single zonal `DoNotSchedule` TSC with exact request homogeneity and no other coupling. Any ambiguity — multiple TSCs, TSC + (anti)affinity, `ScheduleAnyway`, non-zone `topologyKey`, heterogeneous requests, mixed owners, volume pinning — excludes the group, which falls through to the scheduler. Groups are keyed by **owner-reference UID / workload label**, *not* by reusing Karpenter's `TopologyGroup`s (see [Alternative: TopologyGroup Reuse](#alternative-topologygroup-reuse)).

**Soundness.** `cap_z` is exact-or-over (identical items ⇒ Σ per-node floors is exact; ignoring within-zone anti-affinity only over-counts); the `m`-equivalence for `max−min ≤ k` is exact; the replacement is modeled as the largest instance in whichever single zone helps most (most permissive); unmodeled constraints (`minDomains`, competition for the same room, soft terms) only make reality stricter or the model more permissive. So a set infeasible for all `(z*, m)` is truly infeasible.

**Validated.** 0 false negatives across 590 sets; **77% prune where the capacity check gets 0%** — the aggregate check is *blind to skew*, so this prune is entirely marginal (`sims/skew-count-results.md`).

### Combine Rule

```
REJECT S  iff  ( capacity-infeasible(S)  AND  cost-unworthwhile(S) )
               OR  ( any eligible skew group in S is skew-infeasible )
```

A DELETE and a REPLACE are the only two ways `S` can consolidate on capacity/cost grounds, so `S` is capacity/cost-doomed only if *both* fail. Topology is orthogonal: a skew violation dooms `S` regardless of capacity or cost. Checks 1 and 2 compose into a single pass over the overflow (check 2 subsumes check 1's reject: if the overflow exceeds even the largest node, no replacement fits, which is exactly check 1). All three are strict relaxations, so their disjunction is still sound.

### How It Works

The prefilter runs entirely on in-memory cluster state and instance-type metadata; it makes no API calls and launches no simulation.

1. **Gather** (O(P+T)): from `StateNode`s, collect the displaced pods on `S`, the remaining nodes with their `Available()` headroom per dimension and their zone, and `price(S)` from the candidates.
2. **Capacity + cost**: compute `overflow_d`. If `overflow == 0`, a DELETE is capacity-feasible → do not reject on capacity/cost (PASS to checks below). If `overflow > 0`, scan instance types for a permitted offering cheaper than `price(S)` that covers the overflow; if none, the capacity/cost branch rejects.
3. **Skew**: identify eligible groups among the displaced pods (fail-closed), and for each with displaced members, run the O(Z·m) count check over live zones + replacement zone. Any infeasible group rejects.
4. Return `REJECT` if the combine rule fires; else `PASS`.

Because a `PASS` is only necessary, the prefilter never needs to be *right*, only *conservative* — which is what makes it safe to run on every proposed set.

### Interaction with Existing Features

- **Consolidation decision**: unchanged. The prefilter only removes `Solve` calls that would have returned no-op; it cannot change which commands Karpenter issues.
- **Disruption budgets, PDBs, `do-not-disrupt`, `consolidateAfter`**: unaffected. These govern candidacy and execution; the prefilter governs which candidate *sets* reach the simulation.
- **Balanced consolidation** ([`balanced-consolidation.md`](balanced-consolidation.md)): complementary and independent. Scoring decides whether a *feasible* move is worth executing; the prefilter decides whether a set is worth *simulating*. They operate at different stages.
- **Candidate generator**: fully decoupled. Binary search or a future repack heuristic both benefit without changes.

### Observability

- Counter `karpenter_consolidation_prefilter_total{decision}` — `reject` vs `pass`, to track prune rate.
- Counter `karpenter_consolidation_prefilter_reject_reason{reason}` — `capacity_cost` vs `skew`, to attribute prune.
- A **false-negative guard** is a correctness invariant, not a metric: in test/validation the harness asserts every rejected set is a scheduler no-op. In production, an optional debug mode can re-simulate a sample of rejected sets and alert if any would have consolidated (should be impossible by construction).

### Edge Cases

- **Underutilized cluster.** Every removal set is capacity-feasible (`overflow = 0`) and re-homeable within skew → the prefilter rejects nothing (0% prune, 0 FN). Correct: there is nothing doomed to prune; defer to the scheduler.
- **Integrality gap.** Aggregate capacity fits but the pods don't pack integrally (e.g. two 7-CPU pods, one 8-CPU node). The prefilter `PASS`es (aggregate relaxation over-accepts); the scheduler catches it. Sound, just less prune.
- **Empty replacement zone.** Removing all nodes in a zone makes that zone stop being a spread domain; the skew check drops it, and the replacement may reintroduce it. Modeling it as a stuck 0-count domain would be unsound.
- **ODCR / near-zero-cost pools.** The cost check compares against the candidate's own `price(S)`; the same divisor structure that keeps ODCRs ordered below spot cancels, so the check behaves sensibly.

## Alternatives Considered

### Alternative: Max-Flow Prefilter

Model re-homing as a per-dimension transportation max-flow (displaced pods → legal targets + one generous replacement → sink) and reject when flow cannot saturate demand. This is sound and was fully prototyped. **Rejected as the primary check because it is redundant with the O(P+T) aggregate sum for pure capacity**: with no reachability restriction, max-flow-feasible ⟺ `Σdemand ≤ Σcapacity` exactly. Across 2,745 sets the flow's marginal prune over the sum check was **0**, at higher cost. Flow retains one genuine but *unmeasured-in-prevalence* niche — **resource-reachability Hall violations** (capacity stranded behind anti-affine peers), where it caught 61% more doomed sets than the sum with 0 FN — but this is deferred until production data shows such cases are common (`sims/flow-prefilter-results.md`). Flow is blind to *cardinality* Hall violations entirely (the single generous replacement absorbs peer-blocked pods).

### Alternative: TopologyGroup Reuse

Reuse Karpenter's existing `TopologyGroup` structures to identify spread groups for check 3. **Rejected**: those are transient, unexported, per-`Solve` artifacts not reachable at disruption time, and their `owners` are *pod* UIDs, not workload UIDs. Rebuilding them via `NewTopology` is not cheap (per-namespace pod lists per TSC config + O(nodes) lookups + a cluster-wide anti-affinity scan). The design instead groups by **owner-reference UID / workload label** plus exact request-signature homogeneity — cheap, stable, and directly on every pod.

### Alternative: Anti-Affinity Cardinality Check On Spec

Add a count check for mutually-anti-affine pods needing more distinct hosts than exist. Cheap and sound, but **deferred, not built on spec**: Kubernetes' own guidance discourages pod anti-affinity above ~several hundred nodes (the scale this work targets) and positions TSC as the sanctioned spread mechanism; required anti-affinity is a minority default. Expected value is low, so it is gated behind a production measurement of wasted `Solve`s on anti-affinity-doomed sets.

## Validation

All three checks were validated in a research harness (`pkg/controllers/disruption/experiment/`, test files only — **not the shipping implementation**) that drives the **real** `SimulateScheduling` as a ground-truth oracle on an in-memory fake client. For each scenario the harness enumerates removal sets, runs both the cheap check and the real oracle, and asserts the hard invariant `false negatives == 0` (the test fails if any check rejects a set the oracle consolidates).

**Capacity (aggregate check) vs oracle** — 0 FN / 2,745 sets, marginal-over-flow = 0 everywhere:

| scenario | sets | agg prune% | FN |
|---|---|---|---|
| mix-* (underutilized) | 238 ea | 0.0 | 0 |
| tight-homogeneous | 238 | 11.8 | 0 |
| issue-2434 | 837 | 25.1 | 0 |
| scale-40-tight | 280 | 57.1 | 0 |

**Cost / price prune vs oracle** — 0 FN / 1,764 sets:

| scenario | sets | price prune% | capacity-only% | marginal | FN |
|---|---|---|---|---|---|
| medium-full-6 | 57 | 73.7 | 12.3 | +61.4% | 0 |
| medium-full-8 / tight | 238 | 35.3 | 11.8 | +23.5% | 0 |
| large-underutil / half | 57 ea | 0.0 | 0.0 | 0 | 0 |
| issue-2434 | 837 | 26.9 | 25.1 | +1.8% | 0 |
| scale-40-full | 280 | 57.1 | 57.1 | 0 | 0 |

**Skew / TSC count check vs oracle** — 0 FN / 590 sets, and the capacity check is 0% everywhere (skew-blind):

| scenario | sets | skew prune% | capacity% | marginal | FN |
|---|---|---|---|---|---|
| zonal-skew-tight | 57 | 77.2 | 0.0 | +77.2% | 0 |
| zonal-skew-loose | 57 | 0.0 | 0.0 | 0 | 0 |
| mix-zonalTSC / mix-mixed | 238 ea | 0.0 | 0.0 | 0 | 0 |

The three checks are **complementary**: capacity catches gross over-removal, price catches cost no-ops (where the capacity check is a sinkhole), skew catches topology doom (where the capacity check is blind). In the tight regime, the ~23% of skew-check misses are exactly the cost no-ops the price prune covers.

**Combined `prefilter(S)` (end-to-end) vs oracle** — the three checks wired behind one gate and run over an over-generating proposer (all-subsets enumeration = worst-case stress, not yet the repack generator) across an 8-scenario sweep — **0 FN / 2,002 sets**:

| metric | value |
|---|---|
| oracle calls **saved** (sweep) | **29.8%** (up to 77% in doomed regimes) |
| recall on oracle no-ops | 49.5% |
| per-check attribution | capacity 426 · +cost 127 · +skew 44 |
| over-rejection on benign clusters | 0% |
| false negatives | 0 / 2,002 |

Each check owns a distinct regime and none dominates; the gate never eliminates a re-homeable set. The prefilter is orthogonal to the generator, so this stands on its own — the true generator-coupled win (prefilter × a specific proposal stream) awaits the repack heuristic (`sims/combined-prefilter-results.md`).

### Correctness Lessons

1. **Empty zones are not domains (14 FN before the fix).** The first skew implementation modeled all original zones as domains, including zones whose nodes were fully removed — treating an empty zone as a hard 0-count domain inflates skew and rejected sets the oracle consolidated by piling pods into the one live zone. Fix: **domains = zones with a remaining node, plus the replacement's zone**. This matches Kubernetes' skew-over-eligible-domains semantics and is strictly more permissive (sound).
2. **Price basis must come from the real nodepool.** The cost check compares replacement offerings against `price(S)` using the *cheapest permitted* offering. The validation harness pool is on-demand only, so it filters to on-demand; a production implementation that hard-coded on-demand where a nodepool also permits spot could wrongly reject a spot-cheaper replace — an unsound false negative. The offering set **must** be derived from the candidate's nodepool.
3. **Anti-affinity enforcement in simulation.** Populating the cluster's anti-affinity index requires `cluster.UpdatePod` per pod; reconciling node/nodeclaim informers alone does not. Any implementation or test that reasons about anti-affinity must ensure the index is populated, or it silently runs with anti-affinity disabled.

## Backward Compatibility

The prefilter is an internal optimization with no user-facing API surface (no CRD, annotation, or flag change). By the soundness contract it cannot alter which consolidations Karpenter performs — only avoid simulating doomed sets — so existing NodePool specs and behavior are unchanged. It can be introduced behind an internal feature gate and enabled by default once validated, with no migration for users.

## Graduation Criteria

- **Alpha (gated, off by default).** Capacity + cost checks wired in front of the existing generator's `Solve`. The false-negative guard (re-simulate a sample of rejected sets) runs in this phase to confirm zero false negatives on real clusters.
- **Beta (on by default).** Skew check enabled. Prune-rate and reject-reason metrics validated across representative workloads; guard shows zero false negatives.
- **GA (gate removed).** Sustained zero-false-negative evidence in production; prefilter is unconditional. The anti-affinity cardinality check and the max-flow reachability refinement are considered only if metrics show meaningful wasted `Solve`s on those doom types.

## Open Questions

1. **Reschedule-neighborhood bounding.** To keep the per-set gather at O(P+T) independent of total cluster size, targets should be bounded to a reschedule neighborhood rather than every node. What is the right neighborhood definition, and does bounding it ever risk unsoundness (under-counting headroom)?
2. **Prevalence of the deferred doom types.** How common are resource-reachability Hall violations and anti-affinity cardinality doom in real clusters? Production metrics on wasted `Solve`s attributable to them decide whether the deferred checks are ever worth building.
3. **End-to-end payoff with a real generator.** The three checks are already wired behind one `prefilter(S)` and measured over an over-generating enumeration proposer (0 FN / 2,002 sets, 29.8% of `Solve`s saved — see Validation). The remaining step is to couple the gate to a real many-candidate generator (the greedy repack from [`09`](09-repacking-pivot.md)) and measure total `Solve` calls saved per consolidation cycle at scale on its actual proposal stream.
4. **How much homogeneity holds in practice?** The skew check requires exact request-signature homogeneity. VPA, in-place vertical scaling, and injected sidecars break it. What fraction of TSC-governed workloads are cleanly eligible, and therefore how much skew prune is realizable?

## Out of Scope (fall through to the scheduler)

- The full plan cost and exact integral packing — the scheduler computes these.
- Anti-affinity reachability / cardinality — minor at scale, deferred behind production measurement; **do not** build a max-flow or cardinality check on spec.
- Non-homogeneous or multi-TSC topology groups — check 3 fails closed and falls through.
