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

// Skew (TSC) count-check MARGINAL-VALUE characterization across a richer scenario
// space than skewprune_test.go covers. The skew check (skewInfeasible) is already
// known SOUND (0 false negatives). This file measures WHERE it earns its keep:
//   - how much oracle (SimulateScheduling) prune it adds OVER the capacity aggregate
//     check (aggSolve) and OVER the combined capacity+cost gate (pricePruneReject);
//   - the regimes where the single m->1 replacement rescues the violation (single-zone
//     shortage, roomy cluster) so the check adds NOTHING;
//   - the effect of maxSkew k, per-zone tightness (cap_z), and #eligible-groups;
//   - the per-set wall-clock cost of skewInfeasible and its (non-)growth with N.
//
// Everything here is additive: it reuses skewInfeasible / skewCountFeasible /
// eligibleGroups / addableCount / maxReplCount (skewprune_test.go), aggSolve /
// flowGather (flowscale_test.go), pricePruneReject / priceOverflow (priceprune_test.go),
// evalSet (harness_test.go), and enumerateSubsets / sampleSubsets (flow_test.go).
// New helpers are prefixed `scl`.
//
// LIMITATION (zones): `zones` is a package-level 3-zone list wired into `offerings`
// and every synthetic instance type's offerings. Extending to >3 zones would require
// rebuilding the offering/instance-type substrate, so all scenarios stay at Z=3. The
// count check itself is O(Z*m) and zone-count-agnostic; the 3-zone restriction is a
// harness artifact, not an algorithmic one.

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/test"
)

// ---------------------------------------------------------------------------
// Generalized zonal-skew generator: N nodes over the 3 zones, one or more
// homogeneous single-zonal-TSC groups (configurable maxSkew per group), one pod
// per group per node. tightness (cap_z) is controlled by per-group podCPU vs the
// instance type's capacity; #nodes controls existing counts and remaining room.
// ---------------------------------------------------------------------------

type sclGroup struct {
	app     string
	podCPU  string
	maxSkew int
}

type sclParams struct {
	nodesPerZone int
	instanceType string
	groups       []sclGroup
}

// sclTSCPod mirrors harness.tscPod but takes a configurable maxSkew (tscPod hardcodes 1).
func (h *harness) sclTSCPod(cpu, app string, maxSkew int) *corev1.Pod {
	return test.Pod(test.PodOptions{
		ObjectMeta:           metav1.ObjectMeta{UID: uuid.NewUUID(), Labels: map[string]string{"app": app}},
		ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}},
		TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
			MaxSkew:           int32(maxSkew),
			TopologyKey:       corev1.LabelTopologyZone,
			WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": app}},
		}},
	})
}

func (h *harness) sclGenZonalSkew(t *testing.T, p sclParams) {
	h.nodePool = test.NodePool(v1.NodePool{
		Spec: v1.NodePoolSpec{
			Template: v1.NodeClaimTemplate{
				Spec: v1.NodeClaimTemplateSpec{
					Requirements: []v1.NodeSelectorRequirementWithMinValues{
						{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn,
							Values: []string{"small", "medium", "large"}},
						{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn,
							Values: []string{v1.CapacityTypeOnDemand}},
					},
				},
			},
			Disruption: v1.Disruption{
				ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
				ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
			},
			Limits: v1.Limits{corev1.ResourceCPU: resource.MustParse("1000000")},
		},
	})
	h.mustApply(t, h.nodePool)

	it := instanceType(p.instanceType)
	rs := test.ReplicaSet()
	h.mustApply(t, rs)

	idx := 0
	for _, zone := range zones {
		for n := 0; n < p.nodesPerZone; n++ {
			nc, node := test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            h.nodePool.Name,
						corev1.LabelInstanceTypeStable: it.Name,
						corev1.LabelTopologyZone:       zone,
						v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
					},
				},
				Spec: v1.NodeClaimSpec{NodeClassRef: h.nodePool.Spec.Template.Spec.NodeClassRef},
				Status: v1.NodeClaimStatus{
					ProviderID: test.RandomProviderID(),
					Allocatable: corev1.ResourceList{
						corev1.ResourceCPU:    it.Capacity[corev1.ResourceCPU],
						corev1.ResourceMemory: it.Capacity[corev1.ResourceMemory],
						corev1.ResourcePods:   resource.MustParse("110"),
					},
				},
			})
			nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
			h.mustApply(t, nc, node)
			h.nodes = append(h.nodes, node)
			h.claims = append(h.claims, nc)
			for _, g := range p.groups {
				h.bindPod(t, idx, rs, h.sclTSCPod(g.podCPU, g.app, g.maxSkew))
			}
			idx++
		}
	}
	h.makeInitializedAndStateUpdated(t)
}

// ---------------------------------------------------------------------------
// Confusion matrix: skew vs capacity-only (agg) vs capacity+cost (price) vs oracle.
// ---------------------------------------------------------------------------

type sclConfusion struct {
	total        int
	oracleConsol int
	oracleNoOp   int
	skewReject   int
	skewCorrect  int // skew reject & oracle no-op
	skewFN       int // skew reject & oracle consolidatable  (MUST be 0)
	aggReject    int
	priceReject  int
	skewNotAgg   int // skew rejects that the capacity-only check ACCEPTS
	skewNotPrice int // skew rejects that the capacity+cost gate ACCEPTS
	skewNanos    int64
	oracleNanos  int64
}

func (h *harness) sclEval(t *testing.T, S []*disruption.Candidate, c *sclConfusion) {
	c.total++
	in := h.flowGather(t, S)
	aggOK := aggSolve(in)
	priceOK := !h.pricePruneReject(in, totalPrice(S))

	ts := time.Now()
	skewReject := h.skewInfeasible(t, S)
	c.skewNanos += time.Since(ts).Nanoseconds()

	to := time.Now()
	r := h.evalSet(t, S)
	c.oracleNanos += time.Since(to).Nanoseconds()

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
	}
	if skewReject {
		c.skewReject++
		if consolidatable {
			c.skewFN++
			t.Errorf("FALSE NEGATIVE (skew-scaling): rejected a consolidatable set (decision=%s savings=%.3f size=%d)",
				r.decision, r.savings, len(S))
		} else {
			c.skewCorrect++
		}
		if aggOK {
			c.skewNotAgg++
		}
		if priceOK {
			c.skewNotPrice++
		}
	}
}

// sclPct renders count + percent-of-total.
func sclPct(n, total int) string {
	if total == 0 {
		return "0 (0.0%)"
	}
	return fmt.Sprintf("%d (%.1f%%)", n, 100*float64(n)/float64(total))
}

// ---------------------------------------------------------------------------
// The characterization
// ---------------------------------------------------------------------------

func TestSkewScaling(t *testing.T) {
	// ===== Phase 1: regime matrix (small clusters, full subset enumeration) =====
	// Each scenario varies one or more of: maxSkew k, per-zone tightness (cap_z),
	// nodesPerZone (existing counts / remaining room), and #eligible-groups. All at
	// Z=3. `partial displacement` is intrinsic: enumerating all subsets of size 2..6
	// includes sets that remove only SOME of a zone's nodes.
	type matrixScenario struct {
		name   string
		npz    int
		it     string
		groups []sclGroup
		maxSub int
	}
	one := func(cpu string, k int) []sclGroup { return []sclGroup{{"d", cpu, k}} }
	two := func(cpuD, cpuE string, kD, kE int) []sclGroup {
		return []sclGroup{{"d", cpuD, kD}, {"e", cpuE, kE}}
	}

	// cap_z per node (medium=8 CPU): cpu5->0, cpu3->1, cpu2->3, cpu1->7; (large=32): cpu2->15.
	matrix := []matrixScenario{
		// --- single group, TIGHT (cap_z=0), sweep k: value should fall as k rises ---
		{"1grp-tight-k1", 2, "medium", one("5", 1), 6},
		{"1grp-tight-k2", 2, "medium", one("5", 2), 6},
		{"1grp-tight-k3", 2, "medium", one("5", 3), 6},
		// --- single group, MID (cap_z=1), sweep k ---
		{"1grp-mid-k1", 2, "medium", one("3", 1), 6},
		{"1grp-mid-k2", 2, "medium", one("3", 2), 6},
		// --- single group, LOOSE (cap_z=15): roomy => skew should reject ~0 ---
		{"1grp-loose-k1", 2, "large", one("2", 1), 6},
		{"1grp-loose-k3", 2, "large", one("2", 3), 6},
		// --- nodesPerZone effect at TIGHT k1 (more nodes => more staying pods/room) ---
		{"1grp-tight-k1-npz3", 3, "medium", one("5", 1), 6},
		{"1grp-tight-k2-npz3", 3, "medium", one("5", 2), 6},
		// --- multiple eligible groups (2 groups, one pod each per node) ---
		{"2grp-tight-k1", 2, "medium", two("3", "3", 1, 1), 6}, // cap_z=0 both
		{"2grp-tight-mixk", 2, "medium", two("3", "3", 1, 2), 6},
		{"2grp-mid-k1", 2, "large", two("2", "2", 1, 1), 6}, // large => roomy both
	}

	var mb strings.Builder
	fmt.Fprintf(&mb, "\n=== PHASE 1: REGIME MATRIX (Z=3, full subset enum, skew vs agg vs price vs oracle) ===\n")
	fmt.Fprintf(&mb, "%-20s %-3s %-4s %-3s %-6s %-8s %-9s %-9s %-8s %-8s %-14s %-14s %-6s %-8s\n",
		"scenario", "k", "N", "grp", "sets", "consol.", "skew%", "recall%", "agg%", "price%",
		"margVsAgg", "margVsPrice", "FN", "skewUs")
	fmt.Fprintln(&mb, strings.Repeat("-", 150))

	totalFN := 0
	for _, sc := range matrix {
		h := newHarness(t)
		h.sclGenZonalSkew(t, sclParams{nodesPerZone: sc.npz, instanceType: sc.it, groups: sc.groups})
		cands := h.allCandidates(t)
		c := &sclConfusion{}
		enumerateSubsets(cands, sc.maxSub, func(s []*disruption.Candidate) { h.sclEval(t, s, c) })
		totalFN += c.skewFN

		skewPct, recallPct, aggPct, pricePct := 0.0, 0.0, 0.0, 0.0
		if c.total > 0 {
			skewPct = 100 * float64(c.skewReject) / float64(c.total)
			aggPct = 100 * float64(c.aggReject) / float64(c.total)
			pricePct = 100 * float64(c.priceReject) / float64(c.total)
		}
		if c.oracleNoOp > 0 {
			recallPct = 100 * float64(c.skewCorrect) / float64(c.oracleNoOp)
		}
		skewUs := 0.0
		if c.total > 0 {
			skewUs = float64(c.skewNanos) / float64(c.total) / 1000.0
		}
		// k reported = max group skew (the widest band present).
		k := 0
		for _, g := range sc.groups {
			if g.maxSkew > k {
				k = g.maxSkew
			}
		}
		fmt.Fprintf(&mb, "%-20s %-3d %-4d %-3d %-6d %-8d %-9.1f %-9.1f %-8.1f %-8.1f %-14s %-14s %-6d %-8.2f\n",
			sc.name, k, sc.npz*3, len(sc.groups), c.total, c.oracleConsol,
			skewPct, recallPct, aggPct, pricePct,
			sclPct(c.skewNotAgg, c.total), sclPct(c.skewNotPrice, c.total), c.skewFN, skewUs)
	}
	fmt.Fprintln(&mb, strings.Repeat("-", 150))
	fmt.Fprintf(&mb, "skew%%/agg%%/price%% = rejects / all sets. recall%% = skew-correct-rejects / oracle-no-ops.\n")
	fmt.Fprintf(&mb, "margVsAgg = skew rejects the capacity-only (agg) check ACCEPTS. margVsPrice = skew rejects the capacity+cost (price) gate ACCEPTS.\n")
	fmt.Fprintf(&mb, "skewUs = mean wall-clock per set of skewInfeasible (microseconds). FN MUST be 0.\n")
	t.Log(mb.String())

	// ===== Phase 2: cluster-size scaling (fixed tight k1 shape, sampled subsets) =====
	// Hold the scenario shape (medium, cpu5 => cap_z=0, single group, k=1) and grow N
	// via nodesPerZone {2,4,8,16} => N {6,12,24,48}. Use a FIXED sample of subsets per N
	// (same sizes, same count) so skewUs is comparable and marginal is measured on the
	// same "shape" of removal set. Confirms skew's marginal prune persists at scale and
	// its per-set cost does not grow materially with N.
	type scaleRow struct {
		npz int
	}
	scaleRows := []scaleRow{{2}, {4}, {8}, {16}}
	sampleSizes := []int{2, 3, 4, 6}
	perSize := 25

	var sb strings.Builder
	fmt.Fprintf(&sb, "\n=== PHASE 2: N-SCALING (tight cap_z=0, 1 group, k=1, %d sampled sets/size, sizes %v) ===\n", perSize, sampleSizes)
	fmt.Fprintf(&sb, "%-6s %-6s %-8s %-9s %-8s %-8s %-14s %-14s %-6s %-10s %-10s\n",
		"N", "sets", "consol.", "skew%", "agg%", "price%", "margVsAgg", "margVsPrice", "FN", "skewUs", "oracleUs")
	fmt.Fprintln(&sb, strings.Repeat("-", 120))

	for _, sr := range scaleRows {
		h := newHarness(t)
		h.sclGenZonalSkew(t, sclParams{nodesPerZone: sr.npz, instanceType: "medium", groups: one("5", 1)})
		cands := h.allCandidates(t)
		c := &sclConfusion{}
		rng := rand.New(rand.NewSource(int64(sr.npz)))
		sampleSubsets(cands, sampleSizes, perSize, rng, func(s []*disruption.Candidate) { h.sclEval(t, s, c) })
		totalFN += c.skewFN

		skewPct, aggPct, pricePct := 0.0, 0.0, 0.0
		skewUs, oracleUs := 0.0, 0.0
		if c.total > 0 {
			skewPct = 100 * float64(c.skewReject) / float64(c.total)
			aggPct = 100 * float64(c.aggReject) / float64(c.total)
			pricePct = 100 * float64(c.priceReject) / float64(c.total)
			skewUs = float64(c.skewNanos) / float64(c.total) / 1000.0
			oracleUs = float64(c.oracleNanos) / float64(c.total) / 1000.0
		}
		fmt.Fprintf(&sb, "%-6d %-6d %-8d %-9.1f %-8.1f %-8.1f %-14s %-14s %-6d %-10.2f %-10.1f\n",
			sr.npz*3, c.total, c.oracleConsol, skewPct, aggPct, pricePct,
			sclPct(c.skewNotAgg, c.total), sclPct(c.skewNotPrice, c.total), c.skewFN, skewUs, oracleUs)
	}
	fmt.Fprintln(&sb, strings.Repeat("-", 120))
	fmt.Fprintf(&sb, "skewUs/oracleUs = mean wall-clock per set (microseconds). If skewUs stays flat while N grows, cost is N-insensitive.\n")
	t.Log(sb.String())

	// ===== Phase 3: pure count-check core cost (O(Z*m), N-independent) =====
	// skewInfeasible's per-set time includes O(N) client-state gather (DeepCopyNodes,
	// allPods). The ALGORITHMIC core is skewCountFeasible: O(Z*m). Time it directly for
	// Z=3 across growing displaced counts P (which drives the m-scan upper bound) to show
	// the core is sub-microsecond and independent of cluster size N.
	var cb strings.Builder
	fmt.Fprintf(&cb, "\n=== PHASE 3: skewCountFeasible core cost (Z=3, no client access) ===\n")
	fmt.Fprintf(&cb, "%-8s %-12s\n", "P", "ns/call")
	fmt.Fprintln(&cb, strings.Repeat("-", 24))
	for _, P := range []int{2, 4, 8, 16, 32, 64} {
		existing := []int{P / 3, P / 3, P / 3}
		capacity := []int{0, 0, 0} // worst case for the m-scan: forces full replacement enumeration analogue
		reps := 200000
		ts := time.Now()
		for i := 0; i < reps; i++ {
			_ = skewCountFeasible(existing, capacity, P, 1)
		}
		nsPer := float64(time.Since(ts).Nanoseconds()) / float64(reps)
		fmt.Fprintf(&cb, "%-8d %-12.1f\n", P, nsPer)
	}
	fmt.Fprintf(&cb, "Core is O(Z*m); Z fixed at 3, m grows ~linearly with P => low-ns, N-independent.\n")
	t.Log(cb.String())

	if totalFN != 0 {
		t.Fatalf("skew check unsound: %d false negatives across scaling matrix", totalFN)
	}
}
