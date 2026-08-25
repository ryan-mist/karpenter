/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package experiment

// Cost/price-prune experiment — the load-bearing prefilter check (prefilter-spec.md
// check 2). The capacity (delete) check `aggSolve` is a SINKHOLE on the replace path:
// a generous single new node absorbs almost any small replace, so it prunes nothing
// there — only COST distinguishes a worthwhile replace from a no-op. This file builds
// the price prune and validates it against the real oracle on replace-heavy scenarios:
// measure prune rate, its MARGINAL prune over the capacity check, and (the hard
// contract) FALSE NEGATIVES == 0.
//
// The gate (combined capacity+cost, per removal set S):
//   overflow_d = max(0, Σ demand_d − Σ headroom_d)   // remaining targets only, NO new node
//   REJECT S iff:
//     (a) overflow_d > 0 for some d          — DELETE is capacity-infeasible, AND
//     (b) no instance type with cheapest pool-permitted offering price < price(S)
//         has Capacity_d ≥ overflow_d for every d   — no cheaper single node can hold
//                                                      the leftover ⇒ no worthwhile REPLACE.
//
// Why SOUND (reject ⇒ truly no worthwhile consolidation ⇒ zero false negatives):
//   - (a) overflow_d>0 ⇒ aggregate displaced demand exceeds aggregate remaining
//     headroom in dim d ⇒ the pods provably cannot fit the remaining nodes with no
//     new node ⇒ DELETE is infeasible (a necessary condition, so sound to assert
//     "not deletable").
//   - Conservation: in ANY replace the remaining nodes hold ≤ headroom_d, so the one
//     new node must hold ≥ demand_d − headroom_d = overflow_d in every dimension.
//     Hence a viable replacement's Capacity_d ≥ overflow_d is NECESSARY. If no
//     pool-permitted instance type meets that AND is cheaper than price(S), no replace
//     can both fit and save money ⇒ oracle returns no-op.
//   - Generous in every uncertain direction (so we only ever PASS, never wrongly
//     reject): use each type's Capacity (largest, ≥ real allocatable) for "does it
//     fit", and the CHEAPEST pool-permitted offering for "is it cheaper than S".
//     Ignoring the displaced pods' own scheduling constraints only ADDS candidate
//     replacements ⇒ more PASS ⇒ sound.
//
// A PASS is merely necessary, not sufficient (integrality, TSC, affinity, per-pod
// legality are not modeled here) ⇒ fall through to the exact oracle, which stays the
// sole authority.

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	kscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// priceOverflow returns overflow_d = max(0, Σdemand_d − Σheadroom_d) over the
// REMAINING target nodes only (no replacement). Entries exist only for dims with a
// strictly positive overflow; a missing dim reads as 0. overflow_d>0 in any dim ⇒
// DELETE is capacity-infeasible; overflow is the aggregate leftover any single
// replacement node must absorb.
func priceOverflow(in flowInputs) map[string]int64 {
	of := map[string]int64{}
	for _, d := range dims {
		var demand, headroom int64
		for _, p := range in.displaced {
			demand += d.get(resources.RequestsForPods(p))
		}
		for _, tn := range in.targets {
			headroom += d.get(tn.Available())
		}
		if o := demand - headroom; o > 0 {
			of[d.name] = o
		}
	}
	return of
}

// onDemandReq filters an instance type's offerings to those the nodepool permits so
// the cost comparison uses the SAME price basis as the oracle's cheapestOfferingPrice
// (the cheapest permitted offering). Using the cheapest permitted offering is the
// SOUND choice: underpricing a candidate replacement only makes it more likely to
// qualify as "cheaper than price(S)" ⇒ more PASS ⇒ never a false negative.
//
// NOTE: the permitted-offerings set MUST come from the candidate's nodepool. Here the
// single experiment pool is on-demand only (see genCluster / genClusterMix), so we
// filter to on-demand. A production implementation must derive this from the real
// nodepool requirements, or restricting the set could wrongly reject a replace the
// pool could actually launch cheaply (e.g. spot) — an unsound false negative.
var onDemandReq = kscheduling.NewLabelRequirements(map[string]string{
	v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand,
})

// pricePruneReject implements the combined capacity+cost gate. Returns true ⇒ REJECT
// (skip the oracle); false ⇒ PASS (fall through to the oracle).
func (h *harness) pricePruneReject(in flowInputs, priceS float64) bool {
	of := priceOverflow(in)
	if len(of) == 0 {
		// DELETE is capacity-feasible (pods fit remaining nodes with no new node) ⇒
		// we cannot reject on cost grounds; a delete may well be worthwhile. Defer.
		return false
	}
	// DELETE infeasible. Is a worthwhile REPLACE even possible? Look for ONE
	// pool-permitted instance type, cheaper than price(S), whose capacity covers the
	// overflow in every dimension.
	for _, it := range h.cloudProvider.InstanceTypes {
		o := it.Offerings.Available().Compatible(onDemandReq).Cheapest()
		if o == nil || o.Price >= priceS {
			continue // no permitted offering, or not cheaper than the removed set
		}
		fits := true
		for _, d := range dims {
			if d.get(it.Capacity) < of[d.name] { // of[d]==0 for non-overflow dims
				fits = false
				break
			}
		}
		if fits {
			return false // a cheaper single node can hold the leftover ⇒ maybe worthwhile
		}
	}
	return true // no cheaper node fits the overflow ⇒ no worthwhile replace ⇒ REJECT
}

// ---------------------------------------------------------------------------
// Experiment
// ---------------------------------------------------------------------------

type priceConfusion struct {
	total        int
	oracleConsol int
	oracleNoOp   int
	priceReject  int // combined capacity+cost gate rejects
	priceCorrect int // price reject & oracle no-op
	priceFN      int // price reject & oracle consolidatable   (MUST be 0)
	aggReject    int // capacity-only gate rejects (baseline)
	priceNotAgg  int // price rejects that the capacity check ACCEPTS (the marginal value)
	priceNanos   int64
}

func (h *harness) evalPricePair(t *testing.T, S []*disruption.Candidate, c *priceConfusion) {
	c.total++
	in := h.flowGather(t, S)
	priceS := totalPrice(S)

	tp := time.Now()
	priceOK := !h.pricePruneReject(in, priceS)
	c.priceNanos += time.Since(tp).Nanoseconds()
	aggOK := aggSolve(in)

	r := h.evalSet(t, S)
	consolidatable := r.decision != "no-op"
	if consolidatable {
		c.oracleConsol++
	} else {
		c.oracleNoOp++
	}
	if !aggOK {
		c.aggReject++
	}
	if !priceOK {
		c.priceReject++
		if consolidatable {
			c.priceFN++
			t.Errorf("FALSE NEGATIVE (price): rejected a consolidatable set (decision=%s savings=%.3f price(S)=%.3f size=%d)",
				r.decision, r.savings, priceS, len(S))
		} else {
			c.priceCorrect++
		}
		if aggOK {
			c.priceNotAgg++ // capacity check missed it; the price prune caught it
		}
	}
}

func TestPricePrune(t *testing.T) {
	type scenario struct {
		name  string
		build func(h *harness) []*disruption.Candidate
		run   func(h *harness, cands []*disruption.Candidate, c *priceConfusion)
	}

	homog := func(name string, numNodes int, it string, podsPerNode int, podCPU string) scenario {
		return scenario{
			name: name,
			build: func(h *harness) []*disruption.Candidate {
				h.genCluster(t, clusterParams{numNodes: numNodes, instanceType: it, podsPerNode: podsPerNode, podCPU: podCPU})
				return h.allCandidates(t)
			},
			run: func(h *harness, cands []*disruption.Candidate, c *priceConfusion) {
				enumerateSubsets(cands, 6, func(s []*disruption.Candidate) { h.evalPricePair(t, s, c) })
			},
		}
	}

	scenarios := []scenario{
		// Cost-loser regime: near-full medium nodes (7 of 8 CPU). Removing >=2 forces
		// overflow onto a node no cheaper than price(S) — capacity-feasible (a large
		// absorbs it) but NOT worthwhile. The capacity check PASSES all of these; the
		// price prune should REJECT them. This is the sinkhole the price prune closes.
		homog("medium-full-6", 6, "medium", 7, "1"),
		homog("medium-full-8", 8, "medium", 7, "1"),
		// Genuine worthwhile replaces/deletes: underutilized large nodes. The price
		// prune MUST PASS these (overflow is 0 — delete-feasible — so it never rejects).
		// Guards against over-eager rejection (false negatives).
		homog("large-underutil-6", 6, "large", 2, "2"),
		// Half-full mediums: a mix of worthwhile replaces and cost no-ops.
		homog("medium-half-6", 6, "medium", 4, "1"),
		// Reused capacity-doom scenarios (price prune must at least match agg here).
		homog("tight-homogeneous", 8, "medium", 1, "7"),
		{
			name: "issue-2434",
			build: func(h *harness) []*disruption.Candidate {
				h.genCluster2434(t, params2434{fillerPools: 4, fillerNodesPerPool: 2, dedicatedZone: "test-zone-1"})
				return h.allCandidates(t)
			},
			run: func(h *harness, cands []*disruption.Candidate, c *priceConfusion) {
				enumerateSubsets(cands, 6, func(s []*disruption.Candidate) { h.evalPricePair(t, s, c) })
			},
		},
		{
			// Scale, sampled: N=40 near-full mediums, deliberately oversized subsets.
			name: "scale-40-full",
			build: func(h *harness) []*disruption.Candidate {
				h.genCluster(t, clusterParams{numNodes: 40, instanceType: "medium", podsPerNode: 7, podCPU: "1"})
				return h.allCandidates(t)
			},
			run: func(h *harness, cands []*disruption.Candidate, c *priceConfusion) {
				rng := rand.New(rand.NewSource(40))
				sampleSubsets(cands, []int{2, 3, 5, 10, 20, 30, 38}, 40, rng,
					func(s []*disruption.Candidate) { h.evalPricePair(t, s, c) })
			},
		},
	}

	var b strings.Builder
	fmt.Fprintf(&b, "\nPRICE PRUNE (capacity+cost gate) vs capacity-only vs REAL SimulateScheduling\n")
	fmt.Fprintf(&b, "%-20s %-6s %-8s %-9s %-9s %-8s %-16s %-8s\n",
		"scenario", "sets", "consol.", "price%", "recall%", "agg%", "priceMarginal", "priceFN")
	fmt.Fprintln(&b, strings.Repeat("-", 92))

	totalFN := 0
	for _, sc := range scenarios {
		h := newHarness(t)
		cands := sc.build(h)
		c := &priceConfusion{}
		sc.run(h, cands, c)
		totalFN += c.priceFN
		pricePct, recallPct, aggPct, margPct := 0.0, 0.0, 0.0, 0.0
		if c.total > 0 {
			pricePct = 100 * float64(c.priceReject) / float64(c.total)
			aggPct = 100 * float64(c.aggReject) / float64(c.total)
			margPct = 100 * float64(c.priceNotAgg) / float64(c.total)
		}
		if c.oracleNoOp > 0 {
			recallPct = 100 * float64(c.priceCorrect) / float64(c.oracleNoOp)
		}
		fmt.Fprintf(&b, "%-20s %-6d %-8d %-9.1f %-9.1f %-8.1f %-16s %-8d\n",
			sc.name, c.total, c.oracleConsol, pricePct, recallPct, aggPct,
			fmt.Sprintf("%d (%.1f%%)", c.priceNotAgg, margPct), c.priceFN)
	}
	fmt.Fprintln(&b, strings.Repeat("-", 92))
	fmt.Fprintf(&b, "price%%/agg%% = rejects / all sets. recall%% = price-rejects / oracle-no-ops.\n")
	fmt.Fprintf(&b, "priceMarginal = price rejects the capacity-only check ACCEPTS (the cost prune's added value).\n")
	fmt.Fprintf(&b, "false negatives MUST be 0 -- price prune: %d\n", totalFN)
	t.Log(b.String())

	if totalFN != 0 {
		t.Fatalf("price prune unsound: %d false negatives", totalFN)
	}
}
