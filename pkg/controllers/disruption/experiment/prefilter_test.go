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

// Combined prefilter — the three validated checks (capacity/delete, cost/price,
// TSC skew) wired behind ONE gate, measured end-to-end as a front-end over an
// over-generating proposer (all-subsets enumeration = the worst case a candidate
// generator can throw at the oracle).
//
//   prefilter(S) = REJECT iff
//       pricePruneReject(S)   // (capacity-infeasible DELETE) AND (no worthwhile REPLACE)
//     OR skewInfeasible(S)    // any eligible zonal-TSC group can't keep maxSkew
//
// pricePruneReject already subsumes the pure capacity (aggregate) check: when
// overflow exceeds the largest instance, no node fits regardless of price, so it
// rejects exactly what the aggregate check would (and more). The gate is therefore
// the union of three independently-SOUND relaxations, so it is sound by construction
// (a reject from any check is a true infeasible/no-op); this test confirms it
// empirically (0 false negatives) and attributes the prune to each check.
//
// The headline metric is REJECT% = fraction of proposed sets whose expensive
// SimulateScheduling call the prefilter eliminates, at zero correctness risk.

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
)

// prefilter is the unified gate. Returns (reject, reason).
func (h *harness) prefilter(t *testing.T, S []*disruption.Candidate) (bool, string) {
	in := h.flowGather(t, S)
	if h.pricePruneReject(in, totalPrice(S)) {
		return true, "cost" // includes pure over-removal (capacity) as a sub-case
	}
	if h.skewInfeasible(t, S) {
		return true, "skew"
	}
	return false, ""
}

type combinedConfusion struct {
	total        int
	oracleNoOp   int
	oracleConsol int
	reject       int
	correct      int // reject & oracle no-op
	fn           int // reject & oracle consolidatable  (MUST be 0)
	byCapacity   int // the O(P+T) aggregate check alone would catch it
	byCostMarg   int // cost prune caught it, capacity check did not
	bySkewMarg   int // skew check caught it, price/capacity did not
	prefNanos    int64
	oracleNanos  int64
}

func (h *harness) evalCombined(t *testing.T, S []*disruption.Candidate, c *combinedConfusion) {
	c.total++
	in := h.flowGather(t, S)
	aggReject := !aggSolve(in)

	tp := time.Now()
	priceReject := h.pricePruneReject(in, totalPrice(S))
	skewReject := h.skewInfeasible(t, S)
	c.prefNanos += time.Since(tp).Nanoseconds()
	reject := priceReject || skewReject

	to := time.Now()
	r := h.evalSet(t, S)
	c.oracleNanos += time.Since(to).Nanoseconds()
	consolidatable := r.decision != "no-op"

	if consolidatable {
		c.oracleConsol++
	} else {
		c.oracleNoOp++
	}
	if reject {
		c.reject++
		if consolidatable {
			c.fn++
			t.Errorf("FALSE NEGATIVE (combined): rejected a consolidatable set (decision=%s savings=%.3f size=%d)",
				r.decision, r.savings, len(S))
		} else {
			c.correct++
		}
		// Attribution (priceReject subsumes aggReject; skew is the outermost).
		switch {
		case aggReject:
			c.byCapacity++
		case priceReject:
			c.byCostMarg++
		default: // skew only
			c.bySkewMarg++
		}
	}
}

func TestCombinedPrefilter(t *testing.T) {
	type scenario struct {
		name  string
		build func(h *harness) []*disruption.Candidate
		run   func(h *harness, cands []*disruption.Candidate, c *combinedConfusion)
	}
	enumAll := func(h *harness, cands []*disruption.Candidate, c *combinedConfusion) {
		enumerateSubsets(cands, 6, func(s []*disruption.Candidate) { h.evalCombined(t, s, c) })
	}

	scenarios := []scenario{
		// Underutilized mixed-constraint cluster: nothing doomed -> ~0 reject, 0 FN.
		{"mix-mixed", func(h *harness) []*disruption.Candidate {
			h.genClusterMix(t, mixParams{numNodes: 8, podsPerNode: 2, podCPU: "2", instanceType: "large",
				fracHostAnti: 0.15, fracZonalTSC: 0.15, fracAffinity: 0.15, groupSize: 3, constraintCPU: "2"}, rand.New(rand.NewSource(11)))
			return h.allCandidates(t)
		}, enumAll},
		// Capacity-doom (gross over-removal): capacity check carries it.
		{"tight-homogeneous", func(h *harness) []*disruption.Candidate {
			h.genCluster(t, clusterParams{numNodes: 8, instanceType: "medium", podsPerNode: 1, podCPU: "7"})
			return h.allCandidates(t)
		}, enumAll},
		{"issue-2434", func(h *harness) []*disruption.Candidate {
			h.genCluster2434(t, params2434{fillerPools: 4, fillerNodesPerPool: 2, dedicatedZone: "test-zone-1"})
			return h.allCandidates(t)
		}, enumAll},
		// Cost-doom (near-full nodes, replace not cheaper): cost prune's marginal regime.
		{"medium-full-8", func(h *harness) []*disruption.Candidate {
			h.genCluster(t, clusterParams{numNodes: 8, instanceType: "medium", podsPerNode: 7, podCPU: "1"})
			return h.allCandidates(t)
		}, enumAll},
		// Worthwhile deletes/replaces: gate must not over-reject.
		{"large-underutil-6", func(h *harness) []*disruption.Candidate {
			h.genCluster(t, clusterParams{numNodes: 6, instanceType: "large", podsPerNode: 2, podCPU: "2"})
			return h.allCandidates(t)
		}, enumAll},
		// Skew-doom (multi-zone shortage): skew check's marginal regime.
		{"zonal-skew-tight", func(h *harness) []*disruption.Candidate {
			h.genClusterZonalSkew(t, zonalSkewParams{nodesPerZone: 2, instanceType: "medium", podCPU: "5", app: "d"})
			return h.allCandidates(t)
		}, enumAll},
		{"zonal-skew-loose", func(h *harness) []*disruption.Candidate {
			h.genClusterZonalSkew(t, zonalSkewParams{nodesPerZone: 2, instanceType: "large", podCPU: "2", app: "d"})
			return h.allCandidates(t)
		}, enumAll},
		// Scale, sampled.
		{"scale-40-full", func(h *harness) []*disruption.Candidate {
			h.genCluster(t, clusterParams{numNodes: 40, instanceType: "medium", podsPerNode: 7, podCPU: "1"})
			return h.allCandidates(t)
		}, func(h *harness, cands []*disruption.Candidate, c *combinedConfusion) {
			rng := rand.New(rand.NewSource(40))
			sampleSubsets(cands, []int{2, 3, 5, 10, 20, 30, 38}, 40, rng,
				func(s []*disruption.Candidate) { h.evalCombined(t, s, c) })
		}},
	}

	var b strings.Builder
	fmt.Fprintf(&b, "\nCOMBINED PREFILTER (capacity ∪ cost ∪ skew) vs REAL SimulateScheduling\n")
	fmt.Fprintf(&b, "%-20s %-6s %-7s %-8s %-9s %-8s %-8s %-8s %-7s\n",
		"scenario", "sets", "noOp", "reject%", "recall%", "cap", "+cost", "+skew", "FN")
	fmt.Fprintln(&b, strings.Repeat("-", 90))

	var agg combinedConfusion
	totalFN := 0
	for _, sc := range scenarios {
		h := newHarness(t)
		cands := sc.build(h)
		c := &combinedConfusion{}
		sc.run(h, cands, c)
		totalFN += c.fn
		agg.total += c.total
		agg.oracleNoOp += c.oracleNoOp
		agg.reject += c.reject
		agg.correct += c.correct
		agg.byCapacity += c.byCapacity
		agg.byCostMarg += c.byCostMarg
		agg.bySkewMarg += c.bySkewMarg
		rejPct, recPct := 0.0, 0.0
		if c.total > 0 {
			rejPct = 100 * float64(c.reject) / float64(c.total)
		}
		if c.oracleNoOp > 0 {
			recPct = 100 * float64(c.correct) / float64(c.oracleNoOp)
		}
		fmt.Fprintf(&b, "%-20s %-6d %-7d %-8.1f %-9.1f %-8d %-8d %-8d %-7d\n",
			sc.name, c.total, c.oracleNoOp, rejPct, recPct, c.byCapacity, c.byCostMarg, c.bySkewMarg, c.fn)
	}
	fmt.Fprintln(&b, strings.Repeat("-", 90))
	totRej, totRec := 0.0, 0.0
	if agg.total > 0 {
		totRej = 100 * float64(agg.reject) / float64(agg.total)
	}
	if agg.oracleNoOp > 0 {
		totRec = 100 * float64(agg.correct) / float64(agg.oracleNoOp)
	}
	fmt.Fprintf(&b, "%-20s %-6d %-7d %-8.1f %-9.1f %-8d %-8d %-8d %-7d\n",
		"TOTAL", agg.total, agg.oracleNoOp, totRej, totRec, agg.byCapacity, agg.byCostMarg, agg.bySkewMarg, totalFN)
	fmt.Fprintf(&b, "\nreject%% = SimulateScheduling calls SAVED. recall%% = rejects / oracle-no-ops.\n")
	fmt.Fprintf(&b, "cap/+cost/+skew = which check first caught each reject (cost subsumes capacity; skew is disjoint).\n")
	fmt.Fprintf(&b, "false negatives MUST be 0 -- combined: %d\n", totalFN)
	t.Log(b.String())

	if totalFN != 0 {
		t.Fatalf("combined prefilter unsound: %d false negatives", totalFN)
	}
}
