# Consolidation Prefilter: A Sound Cheap Gate Before SimulateScheduling

<!-- Reject provably-doomed candidate node sets before paying for a full
     scheduling simulation, at zero correctness risk. -->

## Motivation

Karpenter's multi-node consolidation proposes a set of nodes to remove and asks the scheduler whether the displaced pods can be re-homed more cheaply. That question is answered by `SimulateScheduling` (a full scheduler `Solve`) — the **expensive** step: it does topology tracking, instance-type filtering, and deep copies, and it grows **super-linearly with cluster size** ([kubernetes-sigs/karpenter#2972](https://github.com/kubernetes-sigs/karpenter/issues/2972)). Today's binary search already pays one `Solve` per evaluated set; a richer candidate generator pays far more.

Many of those `Solve`s are spent on candidate sets that **cannot possibly consolidate** — Karpenter runs the full simulation only to discover it. Even in simple cases:

- removing several near-full nodes whose pods plainly don't fit the few that remain (capacity),
- a replacement whose only feasible instance type costs more than the nodes removed (cost),
- a removal that would violate a topology-spread constraint (skew).

This RFC proposes a **prefilter**: a sound, cheap gate that runs *before* `SimulateScheduling` and rejects candidate sets it can *prove* are doomed, so the scheduler is invoked only on sets that might actually consolidate. The prefilter is **orthogonal to the candidate generator** — it makes no assumption about how sets are proposed — so optimizing the generator later is independent work.

### Use Cases

1. **Infeasible candidate sets today.** Binary search evaluates prefixes that remove too many nodes at once, and every infeasible one costs a full `Solve` — whether it fails on capacity, cost, or topology spread.
   - *For example*, on the reproduced [#2434](https://github.com/kubernetes-sigs/karpenter/issues/2434) shape, the capacity check alone proves ~25% of enumerated sets doomed (measured against the real scheduler — see [Preliminary Simulations](#preliminary-simulations)).
2. **Large / tight clusters ([#2972](https://github.com/kubernetes-sigs/karpenter/issues/2972)).** As clusters grow, `SimulateScheduling` cost rises super-linearly while the prefilter stays near-linear, so the savings grow with scale. On a tight 40-node population, **57%** of sampled sets are doomed and discardable in microseconds each.

### Non-Goals

- **Computing the plan.** The prefilter never decides *what* to consolidate or *how much* it saves. `SimulateScheduling` remains the sole authority on feasibility and cost.
- **Being complete.** The prefilter need not reject every doomed set — only never a *good* one. A missed rejection costs a wasted `Solve`, not correctness. So doom types it does not model (anti-affinity reachability/cardinality, non-homogeneous or multi-TSC topology groups) simply fall through to the scheduler (see [Out of Scope](#out-of-scope-fall-through-to-the-scheduler)).
- **Replacing the generator.** It filters proposals; it does not produce them.

## Proposal

### The Contract

```
prefilter(S) -> REJECT | PASS
```

- `REJECT` ⟹ **provably** no worthwhile consolidation exists for removing `S` → skip the `Solve`.
- `PASS` ⟹ maybe worthwhile → fall through to `SimulateScheduling`, which decides.

```mermaid
flowchart LR
  G[candidate generator] --> P{prefilter S}
  P -->|REJECT| D[discard, no Solve]
  P -->|PASS| S[SimulateScheduling S] --> R[decision]
```

**Soundness — fail-closed.** The prefilter must **never `REJECT` a set the scheduler could consolidate** (zero false negatives). Each check is a strict relaxation of the real re-homing problem, so a reject is provably doomed; anything uncertain, unmodeled, or ambiguous resolves to `PASS`. It follows that the prefilter can be enabled without changing *which* consolidations Karpenter performs — only how quickly it skips the doomed ones.

### The Three Checks

For a candidate set $S$, let the *displaced* pods be those on the removed nodes and the *remaining* nodes be the survivors. Per resource dimension $d\in\{\text{cpu},\text{mem},\dots\}$, the **overflow** is the part of displaced demand no surviving node can absorb:

$$o_d(S) \;=\; \max\!\Big(0,\; \sum_{p\,\in\,\text{displaced}} \text{req}_d(p) \;-\; \sum_{n\,\in\,\text{remaining}} \text{avail}_d(n)\Big)$$

and $\text{price}(S)$ is the summed price of the removed nodes. The checks cost $O(P{+}T)$ / $O(\#\text{instance types})$ / $O(Z{\cdot}m)$ respectively, where $P,T$ are displaced-pod and remaining-node counts, $Z$ the number of zones, $m$ the enumerated skew level.

#### 1. Capacity — DELETE feasibility · O(P+T)

A DELETE adds no new node, so every displaced pod must fit onto a survivor. If in any dimension the displaced demand exceeds the surviving headroom — equivalently $o_d(S) > 0$ — the pods cannot all fit and the DELETE is infeasible → `REJECT`. One sum per dimension; no packing, no graph.

**Soundness.** $\sum_p \text{req}_d(p) > \sum_n \text{avail}_d(n)$ is a necessary condition for infeasibility — a splittable, per-dimension relaxation of the real integral, multi-dimensional packing. If even the relaxed problem has no room, the real one certainly does not.

**Validated.** 0 false negatives across 2,745 sets, and **provably equivalent to a max-flow** for pure capacity (see [Alternatives Considered](#alternative-max-flow-prefilter)).

#### 2. Cost — REPLACE worthwhileness · O(#instance types)

A REPLACE re-homes the displaced pods onto the remaining nodes **plus one** new node (Karpenter allows at most one replacement — the *m→1* rule). Only the overflow $o_d(S)$ must go to that new node. `REJECT the replace` if **no permitted instance type $I$ satisfies both $\text{price}(I) < \text{price}(S)$ and $\text{alloc}_d(I) \ge o_d(S)\ \forall d$** — no cheaper node can even hold the leftover, so no replace can both fit and save money. This mirrors Karpenter's own `RemoveInstanceTypeOptionsByPriceAndMinValues`.

**Why this is a separate check — the replacement sinkhole.** A capacity check with a *generous* replacement is a sinkhole on the replace path: a big new node absorbs almost any small replace, so a feasibility check prunes nothing there. Only *cost* distinguishes a worthwhile replace from a no-op.

**Soundness.** By conservation, in any replace the remaining nodes hold at most their headroom, so the single new node must hold $\ge o_d(S)$ in every dimension — a viable replacement's allocatable $\ge o_d(S)$ is *necessary*. If no permitted type both meets that and is cheaper than $\text{price}(S)$, the oracle can only no-op. The check is generous in every uncertain direction (largest capacity for "does it fit", cheapest permitted offering for "is it cheaper", ignoring the pods' own scheduling constraints), so it only ever errs toward `PASS`. The price basis **must** come from the candidate's real nodepool (e.g. spot, if the pool allows it) — pricing against a narrower set could wrongly reject a cheaper replace.

**Validated.** 0 false negatives across 1,764 sets; **+23–61% marginal prune over the capacity check** in the cost-dominated regime the capacity sinkhole is blind to.

#### 3. Skew — TSC feasibility · O(Z·m)

For a cleanly-identified **homogeneous** group $D$ (identical request $r$) governed by a single zonal `DoNotSchedule` TopologySpread constraint ($\text{maxSkew}=k$) over zones $z$: let $e_z$ be the $D$-pods that *stay* in zone $z$ (fixed), $c_z = \sum_{n\,\in\,z} \lfloor \text{avail}(n)/r \rfloor$ the extra $D$-pods each surviving zone can hold, and place $p_z$ of the $P_D$ displaced pods per zone. Feasible iff

$$\exists\ \text{assignment with}\quad 0 \le p_z \le c_z,\quad \textstyle\sum_z p_z = P_D,\quad \max_z (e_z{+}p_z) - \min_z (e_z{+}p_z) \le k.$$

$\text{maxSkew}$ is encoded by **enumerating the minimum level $m$**: feasible-for-$m$ ⇔ $\sum_z L_z(m) \le P_D \le \sum_z U_z(m)$ with $L_z(m)=\max(0,\,m-e_z)$ and $U_z(m)=\min(c_z,\,m{+}k-e_z)$. With no zone-pinning this is a pure $O(Z{\cdot}m)$ count check (a max-flow is needed only when volume topology pins pods to zones). The **m→1 replacement** adds one node in some zone $z^\*$: enumerate $z^\*$ (plus the no-replacement/delete case) and boost $c_{z^\*}$. `REJECT S` iff **no $(z^\*, m)$ is feasible**. A skew-doomed group dooms the whole set.

**Domains are the LIVE zones only** (zones with a surviving node), plus $z^\*$ when the replacement lands in an empty one. This is sound because **dropping a domain is monotonically more permissive** (fewer bands to satisfy) — *not* because it matches Kubernetes semantics: real Karpenter counts empty pool-permitted zones as 0-count domains and can provision into them, so our domain set is a strict subset of reality's, which can only ease feasibility. (Modeling an empty zone as a stuck 0-count domain inflates skew and false-rejects — it caused 14 FN before this fix.)

**Eligibility is fail-closed — and the exclusions are load-bearing.** An adversarial audit against the real scheduler produced concrete false negatives when the gate was too loose, so a group qualifies **only** as a single zonal `DoNotSchedule` TSC with exact request homogeneity, grouped by **(namespace, workload/`app` label)**, that additionally has: **no `matchLabelKeys`** (it spreads each revision independently — app-level grouping would conflate revisions into one stricter band: *8 demonstrated FN*); **no `nodeSelector` or required `nodeAffinity`** (pinning shrinks the pod's reachable domain set under the default `nodeAffinityPolicy=Honor`: *2 demonstrated FN*); **no `minDomains`** (safe to ignore since reality is stricter, but excluded conservatively); and no other coupling (multiple TSCs, (anti)affinity, `ScheduleAnyway`, non-zone `topologyKey`, heterogeneous requests, volume pinning). Additionally the replacement zone $z^\*$ must range over the **full pool zone universe** (including zones with no current node — the scheduler can launch there to rescue a single-zone shortage), **terminal/terminating** pods are skipped when counting $e_z$, and demand must be sized by the scheduler's `Ceiling`. Groups are keyed by workload label, *not* by reusing Karpenter's `TopologyGroup`s (see [Alternatives Considered](#alternative-topologygroup-reuse)).

**Soundness.** $c_z$ is exact-or-over (identical items ⇒ $\sum$ per-node floors is exact; ignoring within-zone anti-affinity only over-counts); the $m$-equivalence for $\max-\min \le k$ is exact; the replacement is modeled as the largest instance in whichever single zone helps most (most permissive); unmodeled constraints only make reality stricter or the model more permissive. So a group infeasible for all $(z^\*, m)$ is truly infeasible — *given* the eligibility gate above, whose omissions are the only false-negative source and are all closed.

**Validated.** 0 false negatives across 590 sets plus targeted adversarial probes; the check earns real prune only in the **tight, low-`maxSkew`, multi-zone-shortage** regime a single replacement can't rescue (up to **77–97%**, where the capacity check gets **0%** — the aggregate check is blind to skew) and adds **+42% marginal even beyond the combined capacity+cost gate**; the core count runs in **5–42 ns**. See [Preliminary Simulations](#preliminary-simulations).

### Combine Rule

```
REJECT S  iff  ( capacity-infeasible(S)  AND  cost-unworthwhile(S) )
               OR  ( any eligible skew group in S is skew-infeasible )
```

A DELETE and a REPLACE are the only two ways `S` can consolidate on capacity/cost grounds, so `S` is capacity/cost-doomed only if *both* fail. Topology is orthogonal: a skew violation dooms `S` regardless. Checks 1 and 2 compose into a single pass over the overflow (check 2 subsumes check 1: if the overflow exceeds even the largest node, no replacement fits — which is exactly check 1). All three are strict relaxations, so their disjunction is still sound.

### How It Works

The prefilter runs entirely on in-memory cluster state and instance-type metadata; it makes no API calls and launches no simulation.

1. **Gather** (O(P+T)): from `StateNode`s, collect the displaced pods on `S`, the remaining nodes with their `Available()` headroom per dimension and their zone, and `price(S)`.
2. **Capacity + cost**: compute `overflow_d`. If `overflow == 0`, a DELETE is capacity-feasible → do not reject on capacity/cost. If `overflow > 0`, scan instance types for a permitted offering cheaper than `price(S)` that covers the overflow; if none, this branch rejects.
3. **Skew**: identify eligible groups among the displaced pods (fail-closed) and run the O(Z·m) count check for each. Any infeasible group rejects.
4. Return `REJECT` if the combine rule fires; else `PASS`.

### Observability

- `karpenter_consolidation_prefilter_total{decision}` — `reject` vs `pass`; the prune rate.
- `karpenter_consolidation_prefilter_reject_reason{reason}` — `capacity_cost` vs `skew`; attributes the prune.
- `karpenter_consolidation_prefilter_missed_total` — **missed-prune signal**: `PASS`ed sets that `SimulateScheduling` then returns as **no-op**. This is trivially available (we already run the scheduler on every `PASS`), and it bounds the prune left on the table: a high count means either doom types the prefilter does not yet model (candidates for a new check) or plain cost no-ops. It pairs with the dry-run mode below to verify the hard invariant — a rejected set that the scheduler *would* have consolidated is a false negative and must never occur.

## Preliminary Simulations

**Methodology.** All results below come from a research harness that drives the **real** `SimulateScheduling` as a ground-truth oracle on an in-memory fake client — it is *not* the shipping implementation, but it exercises the exact scheduler the prefilter gates. For each scenario the harness enumerates candidate removal sets, runs both the cheap check and the real oracle, and asserts the hard invariant **`false negatives == 0`** (the test fails if any check rejects a set the oracle consolidates). Scenarios span underutilized, capacity-tight, cost-losing, and topology-skewed clusters, plus adversarial probes purpose-built to try to break soundness. The harness (test-only Go) lives on the fork branch [`experiment/consolidation-prefilter-sims`](https://github.com/ryan-mist/karpenter/tree/experiment/consolidation-prefilter-sims) so these tables are reproducible.

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

**Skew / TSC count check vs oracle** — 0 FN / 590 sets, capacity check 0% everywhere (skew-blind):

| scenario | sets | skew prune% | capacity% | marginal | FN |
|---|---|---|---|---|---|
| zonal-skew-tight | 57 | 77.2 | 0.0 | +77.2% | 0 |
| zonal-skew-loose | 57 | 0.0 | 0.0 | 0 | 0 |
| mix-zonalTSC / mix-mixed | 238 ea | 0.0 | 0.0 | 0 | 0 |

Prune concentrates in the tight, low-`maxSkew`, multi-zone-shortage regime (k1 tight reaches 77–97%; it collapses as `maxSkew` widens and vanishes on roomy clusters) and stays flat in cost (5–42 ns core, $O(Z{\cdot}m)$). An **adversarial + semantics audit** then hardened the eligibility gate: it produced concrete false negatives from `matchLabelKeys` (8) and `nodeAffinity`/`nodeSelector` pinning (2) when those exclusions were missing; with the gate above, those probes drop to 0 rejects / 0 FN while genuine skew doom is still caught. This is why the eligibility conditions in check 3 are stated as *hard requirements*, not nice-to-haves.

**Combined `prefilter(S)` (end-to-end) vs oracle** — the three checks behind one gate, run over an over-generating proposer (all-subsets enumeration = worst-case stress, not yet the real generator) across an 8-scenario sweep — **0 FN / 2,002 sets**:

| metric | value |
|---|---|
| oracle calls **saved** (sweep) | **29.8%** (up to 77% in doomed regimes) |
| recall on oracle no-ops | 49.5% |
| per-check attribution | capacity 426 · +cost 127 · +skew 44 |
| over-rejection on benign clusters | 0% |
| false negatives | 0 / 2,002 |

The three checks are **complementary**: capacity catches gross over-removal, price catches cost no-ops (where the capacity check is a sinkhole), skew catches topology doom (where the capacity check is blind). None dominates, and the gate never eliminates a re-homeable set.

## Alternatives Considered

**Max-flow prefilter.** Model re-homing as a per-dimension transportation max-flow and reject when flow cannot saturate demand. Sound and fully prototyped, but **redundant with the O(P+T) aggregate sum for pure capacity**: with no reachability restriction, max-flow-feasible ⟺ `Σdemand ≤ Σcapacity` exactly, and its marginal prune over the sum was **0** across 2,745 sets — at higher cost. It retains one genuine but unmeasured-in-prevalence niche (resource-reachability Hall violations, capacity stranded behind anti-affine peers), deferred until production data shows it is common. It is blind to *cardinality* Hall violations entirely.

**TopologyGroup reuse** for check 3. Rejected: Karpenter's `TopologyGroup`s are transient, unexported, per-`Solve` artifacts keyed on *pod* UIDs (not workload UIDs) and not cheap to rebuild at disruption time. The design instead groups by **owner-reference UID / workload label** plus exact request-signature homogeneity — cheap, stable, and on every pod.

**Anti-affinity cardinality check.** Cheap and sound, but **deferred, not built on spec**: Kubernetes discourages pod anti-affinity above ~several hundred nodes (the scale this targets) and positions TSC as the sanctioned spread mechanism, so expected value is low. Gate it behind a production measurement of wasted `Solve`s on anti-affinity-doomed sets.

## Backward Compatibility

The prefilter is an internal optimization with no user-facing API surface (no CRD, annotation, or flag). By the soundness contract it cannot alter which consolidations Karpenter performs — only avoid simulating doomed sets — so existing NodePool specs and behavior are unchanged, with no migration.

## Graduation Criteria

- **Alpha — gated, off by default, with a dry-run option.** In dry-run the prefilter computes its verdict but does **not** skip the `Solve`; it records what it *would* have rejected and whether the scheduler agreed (must be a no-op). This confirms zero false negatives on real clusters and measures prune before any behavior changes.
- **Beta — gated, on by default (dry-run still available).** The prefilter skips doomed `Solve`s; prune-rate, reject-reason, and missed-prune metrics validate the win and continued zero false negatives across representative workloads.
- **GA — gate removed.** Sustained zero-false-negative evidence in production; the prefilter is unconditional. The deferred checks (anti-affinity cardinality, max-flow reachability) are revisited only if metrics show meaningful wasted `Solve`s on those doom types.

## Open Questions

1. **Reschedule-neighborhood bounding.** To keep the per-set gather at O(P+T) independent of total cluster size, targets should be bounded to a reschedule neighborhood rather than every node. What is the right neighborhood definition, and does bounding it ever risk unsoundness (under-counting headroom)?
2. **Prevalence of the deferred doom types.** How common are resource-reachability Hall violations and anti-affinity cardinality doom in real clusters? The missed-prune metric decides whether the deferred checks are ever worth building.
3. **End-to-end payoff with a real generator.** The three checks are already wired behind one `prefilter(S)` and measured over an over-generating enumeration proposer (0 FN / 2,002 sets, 29.8% of `Solve`s saved). The remaining step is to couple the gate to a real many-candidate generator and measure total `Solve` calls saved per consolidation cycle at scale.
4. **How much homogeneity holds in practice?** The skew check requires exact request-signature homogeneity, which VPA, in-place vertical scaling, and injected sidecars break. What fraction of TSC-governed workloads are cleanly eligible, and therefore how much skew prune is realizable?

## Out of Scope (fall through to the scheduler)

- The full plan cost and exact integral packing — the scheduler computes these.
- Anti-affinity reachability / cardinality — minor at scale, deferred behind production measurement; **do not** build a max-flow or cardinality check on spec.
- Non-homogeneous or multi-TSC topology groups — check 3 fails closed and falls through.
