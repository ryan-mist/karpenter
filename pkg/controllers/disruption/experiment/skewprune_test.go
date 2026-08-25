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

// Skew (TSC) count-check experiment — prefilter-spec.md check 3. The aggregate
// capacity check is BLIND to topology skew: it accepts removal sets whose displaced
// pods cannot be re-homed without violating a DoNotSchedule zonal TopologySpread
// constraint. This file builds the per-domain COUNT check (sims/zonal-flow-analysis.md)
// and validates it against the real oracle: it must add prune the capacity check
// misses, with ZERO false negatives.
//
// Scope (fail-closed): the check applies ONLY to a cleanly-identified homogeneous
// group governed by a single zonal `DoNotSchedule` TSC — identical request signature,
// exactly one TSC (topologyKey=zone, DoNotSchedule), no (anti)affinity, no volume/zone
// pinning. Every ambiguous case (multi-TSC, TSC+affinity, ScheduleAnyway, heterogeneous
// requests, mixed owners) is EXCLUDED and falls through to the exact scheduler. The
// only soundness hazard is a false-positive "clean" group, so the gate requires
// positive cleanliness on every axis.
//
// The model (homogeneous group D, identical request r, one zonal DoNotSchedule TSC,
// maxSkew=k, domains = zones 1..Z; remove set S displaces P of D's pods):
//   existing_z = D-pods that STAY in zone z (on nodes not in S)      -- fixed
//   cap_z      = additional D-pods zone z's remaining nodes can hold  -- Σ floor(avail/r)
//   final_z    = existing_z + placed_z,  0 ≤ placed_z ≤ cap_z,  Σ placed_z = P
//   feasible ⇔ ∃ assignment with max_z final_z − min_z final_z ≤ k
// Encode maxSkew by enumerating the min level m: feasible-for-m ⇔ ∀z L_z(m) ≤ U_z(m)
// and ΣL_z(m) ≤ P ≤ ΣU_z(m), where L_z(m)=max(0, m−existing_z),
// U_z(m)=min(cap_z, m+k−existing_z). With no zone-pinning (all pods reach all zones)
// this is a pure O(Z·m) count check, no graph. The m→1 replacement adds capacity in
// ONE zone: enumerate z* (plus the no-replacement/delete case) and boost cap_{z*} by
// the most generous instance type's floor-count. REJECT S iff NO (z*, m) is feasible.
//
// Why SOUND (reject ⇒ truly infeasible ⇒ zero false negatives):
//   - cap_z is an exact-or-over count (identical items ⇒ Σ per-node floors is exact;
//     ignoring within-zone anti-affinity only over-counts) ⇒ never under-provisions.
//   - the m-equivalence for max−min≤k is exact; the replacement is modeled as the
//     LARGEST instance in whichever single zone helps most ⇒ most permissive.
//   - unmodeled constraints (minDomains, extra pods competing for the same room, soft
//     terms) only make reality STRICTER or the model MORE permissive — never a false
//     reject. A model-infeasible-for-all-(z*,m) set is therefore truly infeasible.

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/test"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// ---------------------------------------------------------------------------
// The count check
// ---------------------------------------------------------------------------

// skewCountFeasible reports whether P displaced homogeneous D-pods can be placed so
// that every zone's final count lands within a maxSkew=k band, given the fixed
// per-zone existing counts and the additional per-zone capacity `cap`.
func skewCountFeasible(existing, capacity []int, P, k int) bool {
	Z := len(existing)
	if Z == 0 {
		return P == 0 // no domains: only "feasible" if nothing needs placing
	}
	total, maxE := 0, 0
	for _, e := range existing {
		total += e
		if e > maxE {
			maxE = e
		}
	}
	// m (the target minimum level) need only range where ΣL_z(m) ≤ P is still possible;
	// beyond max(existing)+ (total+P)/Z it can't help. Small band, cheap to scan.
	hi := maxE + (total+P)/Z + k + 1
	for m := 0; m <= hi; m++ {
		sumL, sumU, ok := 0, 0, true
		for z := 0; z < Z; z++ {
			l := m - existing[z]
			if l < 0 {
				l = 0
			}
			u := capacity[z]
			if v := m + k - existing[z]; v < u {
				u = v
			}
			if u < 0 || l > u { // this zone can't be squared with band [m, m+k]
				ok = false
				break
			}
			sumL += l
			sumU += u
		}
		if ok && sumL <= P && P <= sumU {
			return true
		}
	}
	return false
}

// eligibleGroup is a cleanly-identified homogeneous single-zonal-TSC group.
type eligibleGroup struct {
	app     string
	request map[string]int64 // per-dim request signature (the group is homogeneous)
	maxSkew int
}

// zonalTSC returns the pod's single zonal DoNotSchedule TSC, or (nil,false) if the pod
// is not a clean single-zonal-TSC pod (0 or >1 TSC, non-zone key, ScheduleAnyway, or
// carries (anti)affinity — any coupling that makes a plain zonal count unsound).
func zonalTSC(p *corev1.Pod) (*corev1.TopologySpreadConstraint, bool) {
	if p.Spec.Affinity != nil &&
		(p.Spec.Affinity.PodAffinity != nil || p.Spec.Affinity.PodAntiAffinity != nil) {
		return nil, false // pod (anti)affinity coupling -> exclude
	}
	if len(p.Spec.TopologySpreadConstraints) != 1 {
		return nil, false // 0 or multiple TSCs -> exclude
	}
	tsc := &p.Spec.TopologySpreadConstraints[0]
	if tsc.TopologyKey != corev1.LabelTopologyZone {
		return nil, false // hostname/region/custom domain structure -> exclude
	}
	if tsc.WhenUnsatisfiable != corev1.DoNotSchedule {
		return nil, false // soft TSC -> must NOT model as hard -> exclude
	}
	return tsc, true
}

// eligibleGroups scans all cluster pods and returns the fail-closed set of eligible
// groups, keyed by their TSC selector's `app` value. An app is eligible ONLY if EVERY
// pod carrying that label is a clean single-zonal-TSC pod (zonalTSC ok) with an
// identical request signature and identical maxSkew. Any impurity excludes the app.
func (h *harness) eligibleGroups(t *testing.T) []eligibleGroup {
	t.Helper()
	pods := h.allPods(t)
	// Candidate apps: those named by some pod's zonal TSC selector matchLabels[app].
	candApps := map[string]bool{}
	for _, p := range pods {
		if tsc, ok := zonalTSC(p); ok && tsc.LabelSelector != nil {
			if app := tsc.LabelSelector.MatchLabels["app"]; app != "" {
				candApps[app] = true
			}
		}
	}
	var groups []eligibleGroup
	for app := range candApps {
		members := lo.Filter(pods, func(p *corev1.Pod, _ int) bool { return p.Labels["app"] == app })
		if len(members) == 0 {
			continue
		}
		clean := true
		var sig map[string]int64
		skew := -1
		for _, p := range members {
			tsc, ok := zonalTSC(p)
			if !ok || tsc.LabelSelector == nil || tsc.LabelSelector.MatchLabels["app"] != app {
				clean = false // a same-app pod that isn't a clean member of THIS group
				break
			}
			s := reqSignature(p)
			if sig == nil {
				sig, skew = s, int(tsc.MaxSkew)
			} else if !sameSignature(sig, s) || int(tsc.MaxSkew) != skew {
				clean = false // heterogeneous requests or inconsistent maxSkew
				break
			}
		}
		if clean {
			groups = append(groups, eligibleGroup{app: app, request: sig, maxSkew: skew})
		}
	}
	return groups
}

func reqSignature(p *corev1.Pod) map[string]int64 {
	req := resources.RequestsForPods(p)
	s := map[string]int64{}
	for _, d := range dims {
		s[d.name] = d.get(req)
	}
	return s
}

func sameSignature(a, b map[string]int64) bool {
	for _, d := range dims {
		if a[d.name] != b[d.name] {
			return false
		}
	}
	return true
}

// skewInfeasible reports whether removal set S provably violates a clean zonal TSC —
// i.e. some eligible group's displaced pods cannot be re-homed within maxSkew even
// with the m→1 replacement. true ⇒ REJECT (sound). false ⇒ PASS (fall through).
func (h *harness) skewInfeasible(t *testing.T, S []*disruption.Candidate) bool {
	groups := h.eligibleGroups(t)
	if len(groups) == 0 {
		return false
	}
	sNames := map[string]bool{}
	for _, c := range S {
		sNames[c.Name()] = true
	}
	// Zone of every state node, and the remaining (not-in-S) nodes per zone.
	zoneOf := map[string]string{}
	var zoneOrder []string
	remainingByZone := map[string][]*state.StateNode{}
	seenZone := map[string]bool{}
	for _, sn := range h.cluster.DeepCopyNodes() {
		if sn.Node == nil && sn.NodeClaim == nil {
			continue
		}
		z := sn.Labels()[corev1.LabelTopologyZone]
		if z == "" {
			continue
		}
		zoneOf[sn.Name()] = z
		if !seenZone[z] {
			seenZone[z] = true
			zoneOrder = append(zoneOrder, z)
		}
		if !sNames[sn.Name()] {
			remainingByZone[z] = append(remainingByZone[z], sn)
		}
	}
	if len(zoneOrder) == 0 {
		return false
	}

	pods := h.allPods(t)
	for _, g := range groups {
		members := lo.Filter(pods, func(p *corev1.Pod, _ int) bool { return p.Labels["app"] == g.app })

		// Live zones = zones with >=1 REMAINING node; ONLY these (plus a replacement's
		// zone) are TSC domains. A zone with no node is NOT a spread domain, so it must
		// NOT be modeled as a 0-count domain — doing so wrongly inflates skew and
		// causes false rejects (Kubernetes computes skew over domains that have
		// eligible nodes). Staying pods land only in live zones; displaced pods (on S)
		// must be re-homed.
		capMap := map[string]int{}   // additional D-pods each live zone can hold
		existMap := map[string]int{} // staying D-pods per live zone
		var liveZones []string
		for z, sns := range remainingByZone {
			c := 0
			for _, sn := range sns {
				c += addableCount(sn.Available(), g.request) // identical item => Σ floors is exact
			}
			capMap[z] = c
			existMap[z] = 0
			liveZones = append(liveZones, z)
		}
		displaced := 0
		for _, p := range members {
			if sNames[p.Spec.NodeName] {
				if podutils.IsReschedulable(p) {
					displaced++
				}
				continue
			}
			if z, ok := zoneOf[p.Spec.NodeName]; ok {
				existMap[z]++ // stays put (its node remains ⇒ zone is live)
			}
		}
		if displaced == 0 {
			continue // this group doesn't move -> nothing to check
		}
		replCount := h.maxReplCount(g.request) // one generous new node's floor-count

		feasibleOver := func(cap, exist map[string]int, domains []string) bool {
			E := make([]int, len(domains))
			C := make([]int, len(domains))
			for i, z := range domains {
				E[i], C[i] = exist[z], cap[z]
			}
			return skewCountFeasible(E, C, displaced, g.maxSkew)
		}

		// DELETE (no new node): domains are exactly the live zones.
		feasible := feasibleOver(capMap, existMap, liveZones)
		// REPLACE: one new node in zone z* boosts that zone's capacity — and, if z* is
		// currently empty, ADDS it as a domain. Enumerate z* over all zones the pool
		// can launch in. REJECT only if neither delete nor any z* is feasible.
		for _, zStar := range zoneOrder {
			if feasible {
				break
			}
			cap2 := map[string]int{}
			exist2 := map[string]int{}
			for z, v := range capMap {
				cap2[z] = v
			}
			for z, v := range existMap {
				exist2[z] = v
			}
			domains := append([]string(nil), liveZones...)
			if _, live := cap2[zStar]; !live {
				domains = append(domains, zStar) // replacement introduces a new domain
			}
			cap2[zStar] += replCount
			feasible = feasibleOver(cap2, exist2, domains)
		}
		if !feasible {
			if skewDebug {
				t.Logf("SKEW REJECT app=%s exist=%v cap=%v live=%v P=%d k=%d replCount=%d allZones=%v",
					g.app, existMap, capMap, liveZones, displaced, g.maxSkew, replCount, zoneOrder)
			}
			return true // this group is skew-doomed -> the whole set is doomed
		}
	}
	return false
}

var skewDebug bool

// addableCount = how many D-pods of request r fit in the available space (min over
// binding dims of floor(avail_d / r_d)); dims with zero request don't bind.
func addableCount(avail corev1.ResourceList, r map[string]int64) int {
	best := int64(math.MaxInt64)
	for _, d := range dims {
		if r[d.name] <= 0 {
			continue
		}
		if c := d.get(avail) / r[d.name]; c < best {
			best = c
		}
	}
	if best == math.MaxInt64 {
		return 0
	}
	return int(best)
}

// maxReplCount = the most D-pods any single permitted instance type could hold — the
// generous m→1 replacement (largest = most permissive = sound).
func (h *harness) maxReplCount(r map[string]int64) int {
	best := 0
	for _, it := range h.cloudProvider.InstanceTypes {
		best = max(best, addableCount(it.Capacity, r))
	}
	return best
}

func (h *harness) allPods(t *testing.T) []*corev1.Pod {
	t.Helper()
	list := &corev1.PodList{}
	if err := h.client.List(h.ctx, list); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	return lo.Map(list.Items, func(p corev1.Pod, _ int) *corev1.Pod { return &p })
}

// ---------------------------------------------------------------------------
// Zonal-skew cluster generator: one homogeneous D-group, one zonal DoNotSchedule TSC.
// ---------------------------------------------------------------------------

type zonalSkewParams struct {
	nodesPerZone int
	instanceType string
	podCPU       string // one D-pod per node; tightness controls cap_z
	app          string
}

func (h *harness) genClusterZonalSkew(t *testing.T, p zonalSkewParams) {
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
			// One D-pod (zonal DoNotSchedule TSC, maxSkew=1) per node.
			h.bindPod(t, idx, rs, h.tscPod(p.podCPU, p.app))
			idx++
		}
	}
	h.makeInitializedAndStateUpdated(t)
}

// ---------------------------------------------------------------------------
// Experiment
// ---------------------------------------------------------------------------

type skewConfusion struct {
	total        int
	oracleConsol int
	oracleNoOp   int
	skewReject   int
	skewCorrect  int // skew reject & oracle no-op
	skewFN       int // skew reject & oracle consolidatable  (MUST be 0)
	aggReject    int
	skewNotAgg   int // skew rejects the capacity check ACCEPTS (marginal value)
	skewNanos    int64
}

func (h *harness) evalSkewPair(t *testing.T, S []*disruption.Candidate, c *skewConfusion) {
	c.total++
	in := h.flowGather(t, S)
	aggOK := aggSolve(in)

	ts := time.Now()
	skewReject := h.skewInfeasible(t, S)
	c.skewNanos += time.Since(ts).Nanoseconds()

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
	if skewReject {
		c.skewReject++
		if consolidatable {
			c.skewFN++
			t.Errorf("FALSE NEGATIVE (skew): rejected a consolidatable set (decision=%s savings=%.3f size=%d)",
				r.decision, r.savings, len(S))
		} else {
			c.skewCorrect++
		}
		if aggOK {
			c.skewNotAgg++
		}
	}
}

func TestSkewPrune(t *testing.T) {
	type scenario struct {
		name  string
		build func(h *harness) []*disruption.Candidate
		run   func(h *harness, cands []*disruption.Candidate, c *skewConfusion)
	}
	enumAll := func(h *harness, cands []*disruption.Candidate, c *skewConfusion) {
		enumerateSubsets(cands, 6, func(s []*disruption.Candidate) { h.evalSkewPair(t, s, c) })
	}

	scenarios := []scenario{
		{
			// TIGHT: 2 near-full medium nodes per zone, each with one 5-CPU TSC pod
			// (3 CPU free -> cap_z per node = 0). Removing nodes in >=2 zones strands
			// their pods: one replacement (one zone) can't rebalance a multi-zone
			// shortage -> skew-infeasible. Capacity check is blind (huge headroom via
			// the generous replacement) -> skew's marginal prune.
			name:  "zonal-skew-tight",
			build: func(h *harness) []*disruption.Candidate { h.genClusterZonalSkew(t, zonalSkewParams{nodesPerZone: 2, instanceType: "medium", podCPU: "5", app: "d"}); return h.allCandidates(t) },
			run:   enumAll,
		},
		{
			// LOOSE: large nodes, tiny 2-CPU pods -> abundant per-zone room. Displaced
			// pods always re-home within skew -> skew check must reject 0 (guards FN).
			name:  "zonal-skew-loose",
			build: func(h *harness) []*disruption.Candidate { h.genClusterZonalSkew(t, zonalSkewParams{nodesPerZone: 2, instanceType: "large", podCPU: "2", app: "d"}); return h.allCandidates(t) },
			run:   enumAll,
		},
		{
			// Underutilized mixed-constraint clusters (from the race harness). TSC
			// groups here are on roomy large nodes -> nothing skew-doomed -> 0 reject,
			// 0 FN. Confirms the eligibility gate + soundness on realistic mixes.
			name: "mix-zonalTSC",
			build: func(h *harness) []*disruption.Candidate {
				h.genClusterMix(t, mixParams{numNodes: 8, podsPerNode: 2, podCPU: "2", instanceType: "large",
					fracZonalTSC: 0.3, groupSize: 3, constraintCPU: "2"}, rand.New(rand.NewSource(11)))
				return h.allCandidates(t)
			},
			run: enumAll,
		},
		{
			name: "mix-mixed",
			build: func(h *harness) []*disruption.Candidate {
				h.genClusterMix(t, mixParams{numNodes: 8, podsPerNode: 2, podCPU: "2", instanceType: "large",
					fracHostAnti: 0.15, fracZonalTSC: 0.15, fracAffinity: 0.15, groupSize: 3, constraintCPU: "2"}, rand.New(rand.NewSource(11)))
				return h.allCandidates(t)
			},
			run: enumAll,
		},
	}

	var b strings.Builder
	fmt.Fprintf(&b, "\nSKEW (TSC) COUNT CHECK vs capacity-only vs REAL SimulateScheduling\n")
	fmt.Fprintf(&b, "%-20s %-6s %-8s %-9s %-9s %-8s %-16s %-8s\n",
		"scenario", "sets", "consol.", "skew%", "recall%", "agg%", "skewMarginal", "skewFN")
	fmt.Fprintln(&b, strings.Repeat("-", 92))

	totalFN := 0
	for _, sc := range scenarios {
		h := newHarness(t)
		cands := sc.build(h)
		c := &skewConfusion{}
		sc.run(h, cands, c)
		totalFN += c.skewFN
		skewPct, recallPct, aggPct, margPct := 0.0, 0.0, 0.0, 0.0
		if c.total > 0 {
			skewPct = 100 * float64(c.skewReject) / float64(c.total)
			aggPct = 100 * float64(c.aggReject) / float64(c.total)
			margPct = 100 * float64(c.skewNotAgg) / float64(c.total)
		}
		if c.oracleNoOp > 0 {
			recallPct = 100 * float64(c.skewCorrect) / float64(c.oracleNoOp)
		}
		fmt.Fprintf(&b, "%-20s %-6d %-8d %-9.1f %-9.1f %-8.1f %-16s %-8d\n",
			sc.name, c.total, c.oracleConsol, skewPct, recallPct, aggPct,
			fmt.Sprintf("%d (%.1f%%)", c.skewNotAgg, margPct), c.skewFN)
	}
	fmt.Fprintln(&b, strings.Repeat("-", 92))
	fmt.Fprintf(&b, "skew%%/agg%% = rejects / all sets. recall%% = skew-rejects / oracle-no-ops.\n")
	fmt.Fprintf(&b, "skewMarginal = skew rejects the capacity-only check ACCEPTS (the skew check's added value).\n")
	fmt.Fprintf(&b, "false negatives MUST be 0 -- skew check: %d\n", totalFN)
	t.Log(b.String())

	if totalFN != 0 {
		t.Fatalf("skew check unsound: %d false negatives", totalFN)
	}
}
