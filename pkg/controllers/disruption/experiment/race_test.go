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

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/test"
)

// ===========================================================================
// STEP 1 — richer synthetic cluster generation with a constraint mix
// ===========================================================================

type constraintKind int

const (
	cNone constraintKind = iota
	cHostAnti
	cZonalTSC
	cAffinity
)

type mixParams struct {
	numNodes      int
	podsPerNode   int     // plain reschedulable pods per node (baseline load)
	podCPU        string  // cpu request per plain pod
	instanceType  string  // node instance type (all nodes; "large" = underutilized)
	fracHostAnti  float64 // fraction of (numNodes*podsPerNode) pods added as hostname anti-affinity groups
	fracZonalTSC  float64 // ... as zonal TSC groups
	fracAffinity  float64 // ... as zonal pod-affinity groups
	groupSize     int     // members per constraint group
	constraintCPU string  // cpu request per constrained pod
}

// genClusterMix builds an underutilized cluster with a controllable mix of
// pod-coupling constraints, placing each constraint group across nodes in a
// way that satisfies the constraint at creation time.
func (h *harness) genClusterMix(t *testing.T, p mixParams, rng *rand.Rand) {
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

	// Nodes across zones.
	nodeZone := make([]string, p.numNodes)
	for i := 0; i < p.numNodes; i++ {
		zone := zones[i%len(zones)]
		nodeZone[i] = zone
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
	}

	// Plain load pods.
	for i := 0; i < p.numNodes; i++ {
		for j := 0; j < p.podsPerNode; j++ {
			h.bindPod(t, i, rs, h.plainPod(p.podCPU))
		}
	}

	// Constrained groups.
	totalPods := p.numNodes * p.podsPerNode
	h.placeGroups(t, rs, cHostAnti, int(p.fracHostAnti*float64(totalPods)), p.groupSize, p.constraintCPU, nodeZone, rng)
	h.placeGroups(t, rs, cZonalTSC, int(p.fracZonalTSC*float64(totalPods)), p.groupSize, p.constraintCPU, nodeZone, rng)
	h.placeGroups(t, rs, cAffinity, int(p.fracAffinity*float64(totalPods)), p.groupSize, p.constraintCPU, nodeZone, rng)

	h.makeInitializedAndStateUpdated(t)
}

// placeGroups creates ceil(nPods/groupSize) constraint groups of the given kind,
// placing each group's members on nodes so the constraint holds at creation.
func (h *harness) placeGroups(t *testing.T, rs *appsv1.ReplicaSet, kind constraintKind, nPods, groupSize int, cpu string, nodeZone []string, rng *rand.Rand) {
	if nPods <= 0 || groupSize < 1 {
		return
	}
	nGroups := (nPods + groupSize - 1) / groupSize
	for g := 0; g < nGroups; g++ {
		app := fmt.Sprintf("%s-grp-%d", kindName(kind), g)
		nodeIdxs := h.pickNodes(kind, groupSize, nodeZone, rng)
		for _, ni := range nodeIdxs {
			var pod *corev1.Pod
			switch kind {
			case cHostAnti:
				pod = h.hostAntiPod(cpu, app)
			case cZonalTSC:
				pod = h.tscPod(cpu, app)
			case cAffinity:
				pod = h.affinityPod(cpu, app)
			}
			h.bindPod(t, ni, rs, pod)
		}
	}
}

// pickNodes selects groupSize node indices appropriate to the constraint:
// hostAnti/zonalTSC want distinct nodes (TSC prefers distinct zones); affinity
// wants nodes in a single zone.
func (h *harness) pickNodes(kind constraintKind, groupSize int, nodeZone []string, rng *rand.Rand) []int {
	n := len(nodeZone)
	switch kind {
	case cAffinity:
		// all members in one randomly-chosen zone
		zone := zones[rng.Intn(len(zones))]
		var pool []int
		for i := 0; i < n; i++ {
			if nodeZone[i] == zone {
				pool = append(pool, i)
			}
		}
		return sampleDistinct(pool, groupSize, rng)
	case cZonalTSC:
		// spread across zones: one node per distinct zone, cycling
		var picks []int
		used := map[int]bool{}
		for zi := 0; len(picks) < groupSize && zi < n*len(zones); zi++ {
			zone := zones[zi%len(zones)]
			for i := 0; i < n; i++ {
				if nodeZone[i] == zone && !used[i] {
					used[i] = true
					picks = append(picks, i)
					break
				}
			}
		}
		return picks
	default: // cHostAnti: any distinct nodes
		all := make([]int, n)
		for i := range all {
			all[i] = i
		}
		return sampleDistinct(all, groupSize, rng)
	}
}

func sampleDistinct(pool []int, k int, rng *rand.Rand) []int {
	cp := append([]int(nil), pool...)
	rng.Shuffle(len(cp), func(i, j int) { cp[i], cp[j] = cp[j], cp[i] })
	if k > len(cp) {
		k = len(cp)
	}
	return cp[:k]
}

func kindName(k constraintKind) string {
	switch k {
	case cHostAnti:
		return "ha"
	case cZonalTSC:
		return "tsc"
	case cAffinity:
		return "aff"
	}
	return "none"
}

// bindPod creates a pod owned by rs, binds it to nodes[idx], applies it.
func (h *harness) bindPod(t *testing.T, idx int, rs *appsv1.ReplicaSet, pod *corev1.Pod) {
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID,
		Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true),
	}}
	pod.Spec.NodeName = h.nodes[idx].Name
	h.mustApply(t, pod)
}

func (h *harness) plainPod(cpu string) *corev1.Pod {
	return test.Pod(test.PodOptions{
		ObjectMeta:           metav1.ObjectMeta{UID: uuid.NewUUID()},
		ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}},
	})
}

func (h *harness) hostAntiPod(cpu, app string) *corev1.Pod {
	return test.Pod(test.PodOptions{
		ObjectMeta:           metav1.ObjectMeta{UID: uuid.NewUUID(), Labels: map[string]string{"app": app}},
		ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}},
		PodAntiRequirements: []corev1.PodAffinityTerm{{
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": app}},
			TopologyKey:   corev1.LabelHostname,
		}},
	})
}

func (h *harness) tscPod(cpu, app string) *corev1.Pod {
	return test.Pod(test.PodOptions{
		ObjectMeta:           metav1.ObjectMeta{UID: uuid.NewUUID(), Labels: map[string]string{"app": app}},
		ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}},
		TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
			MaxSkew:           1,
			TopologyKey:       corev1.LabelTopologyZone,
			WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": app}},
		}},
	})
}

func (h *harness) affinityPod(cpu, app string) *corev1.Pod {
	return test.Pod(test.PodOptions{
		ObjectMeta:           metav1.ObjectMeta{UID: uuid.NewUUID(), Labels: map[string]string{"app": app}},
		ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}},
		PodRequirements: []corev1.PodAffinityTerm{{
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": app}},
			TopologyKey:   corev1.LabelTopologyZone,
		}},
	})
}

// ===========================================================================
// STEP 2/3 — strategies
// ===========================================================================

type stratResult struct {
	savings  float64
	decision string
	calls    int
	setSize  int
}

func subsetKey(sub []*disruption.Candidate) string {
	names := lo.Map(sub, func(c *disruption.Candidate, _ int) string { return c.Name() })
	sort.Strings(names)
	return strings.Join(names, ",")
}

// runProposals evaluates an ordered list of candidate subsets under a call budget,
// keeping the max-savings feasible result.
func (h *harness) runProposals(t *testing.T, subsets [][]*disruption.Candidate, budget int) stratResult {
	res := stratResult{decision: "no-op"}
	seen := map[string]bool{}
	for _, sub := range subsets {
		if res.calls >= budget {
			break
		}
		if len(sub) < 2 {
			continue
		}
		k := subsetKey(sub)
		if seen[k] {
			continue
		}
		seen[k] = true
		r := h.evalSet(t, sub)
		res.calls++
		if r.decision != "no-op" && r.savings > res.savings {
			res.savings = r.savings
			res.decision = r.decision
			res.setSize = len(sub)
		}
	}
	return res
}

// --- (a) BASELINE: faithful savings-ratio-desc prefix binary search ---------
func (h *harness) baselineStrategy(t *testing.T, cands []*disruption.Candidate) stratResult {
	sorted := append([]*disruption.Candidate(nil), cands...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].SavingsRatio() > sorted[j].SavingsRatio() })
	res := stratResult{decision: "no-op"}
	loI, hiI := 1, len(sorted)
	for loI <= hiI {
		mid := (loI + hiI) / 2
		r := h.evalSet(t, sorted[:mid])
		res.calls++
		if r.decision != "no-op" {
			if r.savings > res.savings {
				res.savings, res.decision, res.setSize = r.savings, r.decision, mid
			}
			loI = mid + 1
		} else {
			hiI = mid - 1
		}
	}
	return res
}

// --- (b) COUPLING GRAPH -----------------------------------------------------
// Edge weight between two candidate NODES = P(their coupled pods can't co-schedule),
// taken as the MAX over coupled pod-pairs (same "app" group + same constraint type):
//   hostname anti-affinity  -> 1.0  (never co-schedulable onto one node)
//   required pod affinity    -> 0.0  (want the same domain; co-schedulable)
//   zonal TSC maxSkew=m      -> heuristic: same-zone pair 1-1/(m+1) (=0.5 for m=1),
//                               cross-zone pair (1-1/(m+1))*0.5 (=0.25 for m=1).
//                               [This is the "unclear middle": a scalar cannot
//                               capture the global count, so we approximate the
//                               marginal difficulty of co-locating one more.]
// No coupled pod-pair -> NO edge (weight <0 sentinel). Plain nodes are isolated.
// Threshold t keeps edges with 0<=w<=t (co-schedulable-ish); connected components
// of the kept graph are the proposed "consolidatable-together" clusters.
func (h *harness) couplingSubsets(t *testing.T, cands []*disruption.Candidate) [][]*disruption.Candidate {
	n := len(cands)
	podsByNode := make([][]*corev1.Pod, n)
	for i, c := range cands {
		podsByNode[i] = h.podsOn(t, c.Name())
	}
	// pairwise weights
	W := make([][]float64, n)
	for i := range W {
		W[i] = make([]float64, n)
		for j := range W[i] {
			W[i][j] = -1
		}
	}
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			W[i][j] = pairCouplingWeight(podsByNode[i], podsByNode[j],
				cands[i].Labels()[corev1.LabelTopologyZone], cands[j].Labels()[corev1.LabelTopologyZone])
			W[j][i] = W[i][j]
		}
	}
	var proposals [][]*disruption.Candidate
	seen := map[string]bool{}
	for _, thr := range []float64{0.0, 0.5, 0.99} {
		uf := newUF(n)
		for i := 0; i < n; i++ {
			for j := i + 1; j < n; j++ {
				if W[i][j] >= 0 && W[i][j] <= thr {
					uf.union(i, j)
				}
			}
		}
		comps := map[int][]*disruption.Candidate{}
		for i := 0; i < n; i++ {
			r := uf.find(i)
			comps[r] = append(comps[r], cands[i])
		}
		var compList [][]*disruption.Candidate
		for _, m := range comps {
			if len(m) >= 2 {
				compList = append(compList, m)
			}
		}
		// order components by total price desc
		sort.SliceStable(compList, func(a, b int) bool { return totalPrice(compList[a]) > totalPrice(compList[b]) })
		for _, comp := range compList {
			members := append([]*disruption.Candidate(nil), comp...)
			sort.SliceStable(members, func(a, b int) bool { return members[a].SavingsRatio() > members[b].SavingsRatio() })
			for sz := len(members); sz >= 2; sz-- {
				sub := members[:sz]
				k := subsetKey(sub)
				if !seen[k] {
					seen[k] = true
					proposals = append(proposals, append([]*disruption.Candidate(nil), sub...))
				}
			}
		}
	}
	return proposals
}

func pairCouplingWeight(pa, pb []*corev1.Pod, za, zb string) float64 {
	w := -1.0
	for _, x := range pa {
		for _, y := range pb {
			ax, ay := x.Labels["app"], y.Labels["app"]
			if ax == "" || ax != ay {
				continue // only coupled if same constraint group
			}
			if hasHostAnti(x) && hasHostAnti(y) {
				w = maxf(w, 1.0)
			} else if hasZoneAffinity(x) && hasZoneAffinity(y) {
				w = maxf(w, 0.0)
			} else if m, ok := tscMaxSkew(x); ok {
				if _, ok2 := tscMaxSkew(y); ok2 {
					base := 1.0 - 1.0/float64(m+1)
					if za == zb {
						w = maxf(w, base)
					} else {
						w = maxf(w, base*0.5)
					}
				}
			}
		}
	}
	return w
}

// --- (c) SIMILARITY GRAPH ---------------------------------------------------
// Edge A-B iff same NodePool + same instance family + combined pod requests fit
// on ONE node of that family (mergeable) + no hostname-anti conflict (prune).
func (h *harness) similaritySubsets(t *testing.T, cands []*disruption.Candidate) [][]*disruption.Candidate {
	n := len(cands)
	podsByNode := make([][]*corev1.Pod, n)
	cpuByNode := make([]float64, n)
	for i, c := range cands {
		podsByNode[i] = h.podsOn(t, c.Name())
		cpuByNode[i] = sumCPU(podsByNode[i])
	}
	uf := newUF(n)
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			if cands[i].NodePool.Name != cands[j].NodePool.Name {
				continue
			}
			fi := cands[i].Labels()[corev1.LabelInstanceTypeStable]
			fj := cands[j].Labels()[corev1.LabelInstanceTypeStable]
			if fi != fj {
				continue
			}
			cap := instanceType(fi).Capacity[corev1.ResourceCPU]
			capCPU := float64(cap.MilliValue()) / 1000.0
			if cpuByNode[i]+cpuByNode[j] > capCPU {
				continue // combined won't fit on one node of this family
			}
			if pairCouplingWeight(podsByNode[i], podsByNode[j],
				cands[i].Labels()[corev1.LabelTopologyZone], cands[j].Labels()[corev1.LabelTopologyZone]) >= 1.0 {
				continue // hard hostname-anti conflict: can't merge
			}
			uf.union(i, j)
		}
	}
	comps := map[int][]*disruption.Candidate{}
	for i := 0; i < n; i++ {
		r := uf.find(i)
		comps[r] = append(comps[r], cands[i])
	}
	var compList [][]*disruption.Candidate
	for _, m := range comps {
		if len(m) >= 2 {
			compList = append(compList, m)
		}
	}
	sort.SliceStable(compList, func(a, b int) bool { return totalPrice(compList[a]) > totalPrice(compList[b]) })
	var proposals [][]*disruption.Candidate
	seen := map[string]bool{}
	for _, comp := range compList {
		members := append([]*disruption.Candidate(nil), comp...)
		sort.SliceStable(members, func(a, b int) bool { return members[a].SavingsRatio() > members[b].SavingsRatio() })
		for sz := len(members); sz >= 2; sz-- {
			sub := members[:sz]
			k := subsetKey(sub)
			if !seen[k] {
				seen[k] = true
				proposals = append(proposals, append([]*disruption.Candidate(nil), sub...))
			}
		}
	}
	return proposals
}

// ===========================================================================
// STEP 4 — brute-force optimum (small N)
// ===========================================================================

func (h *harness) bruteForceOptimum(t *testing.T, cands []*disruption.Candidate) (float64, int) {
	n := len(cands)
	best := 0.0
	calls := 0
	for mask := 1; mask < (1 << n); mask++ {
		var sub []*disruption.Candidate
		for i := 0; i < n; i++ {
			if mask&(1<<i) != 0 {
				sub = append(sub, cands[i])
			}
		}
		r := h.evalSet(t, sub)
		calls++
		if r.decision != "no-op" && r.savings > best {
			best = r.savings
		}
	}
	return best, calls
}

// ===========================================================================
// helpers: pods on node, constraint detection, union-find, misc
// ===========================================================================

func (h *harness) podsOn(t *testing.T, nodeName string) []*corev1.Pod {
	t.Helper()
	list := &corev1.PodList{}
	if err := h.client.List(h.ctx, list, client.MatchingFields{"spec.nodeName": nodeName}); err != nil {
		t.Fatalf("list pods on %s: %v", nodeName, err)
	}
	return lo.Map(list.Items, func(p corev1.Pod, _ int) *corev1.Pod { return &p })
}

func hasHostAnti(p *corev1.Pod) bool {
	if p.Spec.Affinity == nil || p.Spec.Affinity.PodAntiAffinity == nil {
		return false
	}
	for _, term := range p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
		if term.TopologyKey == corev1.LabelHostname {
			return true
		}
	}
	return false
}

func hasZoneAffinity(p *corev1.Pod) bool {
	if p.Spec.Affinity == nil || p.Spec.Affinity.PodAffinity == nil {
		return false
	}
	for _, term := range p.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
		if term.TopologyKey == corev1.LabelTopologyZone {
			return true
		}
	}
	return false
}

func tscMaxSkew(p *corev1.Pod) (int, bool) {
	for _, tsc := range p.Spec.TopologySpreadConstraints {
		if tsc.TopologyKey == corev1.LabelTopologyZone && tsc.WhenUnsatisfiable == corev1.DoNotSchedule {
			return int(tsc.MaxSkew), true
		}
	}
	return 0, false
}

func sumCPU(pods []*corev1.Pod) float64 {
	var s float64
	for _, p := range pods {
		for _, c := range p.Spec.Containers {
			if q, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
				s += float64(q.MilliValue()) / 1000.0
			}
		}
	}
	return s
}

func totalPrice(cs []*disruption.Candidate) float64 {
	return lo.SumBy(cs, func(c *disruption.Candidate) float64 { return c.Price })
}

func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

type unionFind struct{ p, r []int }

func newUF(n int) *unionFind {
	p := make([]int, n)
	for i := range p {
		p[i] = i
	}
	return &unionFind{p: p, r: make([]int, n)}
}
func (u *unionFind) find(x int) int {
	for u.p[x] != x {
		u.p[x] = u.p[u.p[x]]
		x = u.p[x]
	}
	return x
}
func (u *unionFind) union(a, b int) {
	ra, rb := u.find(a), u.find(b)
	if ra == rb {
		return
	}
	if u.r[ra] < u.r[rb] {
		ra, rb = rb, ra
	}
	u.p[rb] = ra
	if u.r[ra] == u.r[rb] {
		u.r[ra]++
	}
}

// ===========================================================================
// STEP 5 — the race
// ===========================================================================

type regime struct {
	name                                   string
	fracHostAnti, fracZonalTSC, fracAffin  float64
}

var regimes = []regime{
	{"none", 0, 0, 0},
	{"hostAnti", 0.3, 0, 0},
	{"zonalTSC", 0, 0.3, 0},
	{"affinity", 0, 0, 0.3},
	{"mixed", 0.15, 0.15, 0.15},
}

type cellAgg struct {
	base, coup, sim         []float64 // best savings
	baseC, coupC, simC      []float64 // calls
	baseOpt, coupOpt, simOpt []float64 // savings/OPT (small N only)
	optSavings              []float64
}

func TestRace(t *testing.T) {
	seeds := 3
	smallN := []int{8, 10} // brute-force OPT
	scaleN := []int{40}    // no OPT

	type rowKey struct {
		n      int
		regime string
	}
	rows := map[rowKey]*cellAgg{}

	run := func(n int, reg regime, withOpt bool) {
		for s := 0; s < seeds; s++ {
			rng := rand.New(rand.NewSource(int64(n*1000 + s)))
			h := newHarness(t)
			h.genClusterMix(t, mixParams{
				numNodes: n, podsPerNode: 2, podCPU: "2", instanceType: "large",
				fracHostAnti: reg.fracHostAnti, fracZonalTSC: reg.fracZonalTSC, fracAffinity: reg.fracAffin,
				groupSize: 3, constraintCPU: "2",
			}, rng)
			cands := h.allCandidates(t)
			if len(cands) == 0 {
				continue
			}
			budget := 2 * len(cands)
			base := h.baselineStrategy(t, cands)
			coup := h.runProposals(t, h.couplingSubsets(t, cands), budget)
			sim := h.runProposals(t, h.similaritySubsets(t, cands), budget)

			k := rowKey{n, reg.name}
			agg := rows[k]
			if agg == nil {
				agg = &cellAgg{}
				rows[k] = agg
			}
			agg.base = append(agg.base, base.savings)
			agg.coup = append(agg.coup, coup.savings)
			agg.sim = append(agg.sim, sim.savings)
			agg.baseC = append(agg.baseC, float64(base.calls))
			agg.coupC = append(agg.coupC, float64(coup.calls))
			agg.simC = append(agg.simC, float64(sim.calls))
			if withOpt {
				opt, _ := h.bruteForceOptimum(t, cands)
				agg.optSavings = append(agg.optSavings, opt)
				if opt > 0 {
					agg.baseOpt = append(agg.baseOpt, base.savings/opt)
					agg.coupOpt = append(agg.coupOpt, coup.savings/opt)
					agg.simOpt = append(agg.simOpt, sim.savings/opt)
				}
			}
		}
	}

	for _, reg := range regimes {
		for _, n := range smallN {
			run(n, reg, true)
		}
		for _, n := range scaleN {
			run(n, reg, false)
		}
	}

	// Report
	var b strings.Builder
	fmt.Fprintf(&b, "\n%-8s %-9s | %-22s | %-22s | %-22s | %s\n",
		"N", "regime", "baseline(save/calls)", "coupling(save/calls)", "similarity(save/calls)", "save/OPT b|c|s")
	fmt.Fprintln(&b, strings.Repeat("-", 120))
	order := []int{8, 10, 40}
	for _, n := range order {
		for _, reg := range regimes {
			agg := rows[rowKey{n, reg.name}]
			if agg == nil {
				continue
			}
			optStr := "   -"
			if len(agg.baseOpt) > 0 {
				optStr = fmt.Sprintf("%.2f|%.2f|%.2f", mean(agg.baseOpt), mean(agg.coupOpt), mean(agg.simOpt))
			}
			fmt.Fprintf(&b, "%-8d %-9s | %6.2f / %-6.1f       | %6.2f / %-6.1f       | %6.2f / %-6.1f       | %s\n",
				n, reg.name,
				mean(agg.base), mean(agg.baseC),
				mean(agg.coup), mean(agg.coupC),
				mean(agg.sim), mean(agg.simC),
				optStr)
		}
	}
	t.Log(b.String())
}

// TestBaselineSanity checks that the reimplemented prefix-binary-search baseline
// finds savings consistent with the REAL MultiNodeConsolidation.ComputeCommands.
func TestBaselineSanity(t *testing.T) {
	for _, reg := range []regime{{"none", 0, 0, 0}, {"hostAnti", 0.3, 0, 0}, {"mixed", 0.15, 0.15, 0.15}} {
		rng := rand.New(rand.NewSource(7))
		h := newHarness(t)
		h.genClusterMix(t, mixParams{
			numNodes: 10, podsPerNode: 2, podCPU: "2", instanceType: "large",
			fracHostAnti: reg.fracHostAnti, fracZonalTSC: reg.fracZonalTSC, fracAffinity: reg.fracAffin,
			groupSize: 3, constraintCPU: "2",
		}, rng)
		cands := h.allCandidates(t)
		mine := h.baselineStrategy(t, cands)
		var real float64
		for _, cmd := range h.baselineRun(t) {
			real += commandSavings(cmd)
		}
		t.Logf("regime=%-9s reimplemented-baseline=%.2f (%d calls, %s) | real ComputeCommands=%.2f",
			reg.name, mine.savings, mine.calls, mine.decision, real)
	}
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	return lo.Sum(xs) / float64(len(xs))
}
