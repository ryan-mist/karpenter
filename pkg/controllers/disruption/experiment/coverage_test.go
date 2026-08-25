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
	"sort"
	"strings"
	"testing"

	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/test"
)

// ===========================================================================
// #2434 reproduction: multi-nodepool cluster where the ONLY consolidation is a
// non-adjacent, non-monotone 2->1 memory-downsize in one dedicated NodePool,
// buried among many non-consolidatable filler nodes across other NodePools.
// ===========================================================================

// instanceTypes2434 adds a memory-optimized "membig" type that is cheaper than
// two "medium" nodes, so two memory-underutilized mediums can merge onto one
// membig for a real saving. small/medium/large mirror syntheticInstanceTypes.
func instanceTypes2434() []*cloudprovider.InstanceType {
	mk := func(name string, cpu, memGi int, base float64) *cloudprovider.InstanceType {
		vals := lo.Map(offerings(base), func(o *cloudprovider.Offering, _ int) cloudprovider.Offering { return *o })
		return fake.NewInstanceType(name,
			fake.WithResources(corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(fmt.Sprintf("%d", cpu)),
				corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", memGi)),
				corev1.ResourcePods:   resource.MustParse("110"),
			}),
			fake.WithOfferings(vals...),
		)
	}
	return []*cloudprovider.InstanceType{
		mk("small", 2, 4, 0.10),
		mk("medium", 8, 16, 0.40),
		mk("membig", 8, 48, 0.70), // memory-optimized; 0.70 < 2*0.40 so 2 medium -> 1 membig saves 0.10
		mk("large", 32, 64, 1.60),
	}
}

var allTypes2434 = []string{"small", "medium", "membig", "large"}

type params2434 struct {
	fillerPools        int    // number of filler NodePools
	fillerNodesPerPool int    // medium nodes per filler pool (kept small so exhaustive is cheap)
	dedicatedZone      string // both dedicated nodes share this zone
}

// genCluster2434 returns the target set (the 2 dedicated node names) and known OPT ($/hr).
func (h *harness) genCluster2434(t *testing.T, p params2434) (targetNames []string, knownOPT float64) {
	h.cloudProvider.InstanceTypes = instanceTypes2434()
	rs := test.ReplicaSet()
	h.mustApply(t, rs)

	medium := instanceType2434("medium")

	// Dedicated pool: 2 medium nodes, memory-underutilized (10Gi of 16Gi, 1 CPU),
	// neither can absorb the other's 10Gi (only 6Gi free) so no single-node action
	// helps; TOGETHER their 20Gi fits one membig (48Gi) -> saves 0.10. Non-monotone.
	dedPod := func() *corev1.Pod { return h.podCPUMem("1", "10Gi") }
	targetNames = h.addPool(t, "pool-dedicated", medium, 2,
		func(int) string { return p.dedicatedZone }, dedPod, rs)

	// Filler pools: medium nodes each running one NEAR-FULL pod (7 CPU / 15Gi of
	// 8 CPU / 16Gi). Low pod count => low disruption => interleaved to the front of
	// a global sort, but the pod nearly fills the node so it can't move anywhere and
	// combining fillers only ever needs an equal/pricier node => non-consolidatable.
	fillPod := func() *corev1.Pod { return h.podCPUMem("7", "15Gi") }
	for i := 0; i < p.fillerPools; i++ {
		h.addPool(t, fmt.Sprintf("pool-f%d", i), medium, p.fillerNodesPerPool,
			func(k int) string { return zones[k%len(zones)] }, fillPod, rs)
	}

	h.makeInitializedAndStateUpdated(t)
	return targetNames, 2*0.40 - 0.70 // 0.10
}

func instanceType2434(name string) *cloudprovider.InstanceType {
	it, _ := lo.Find(instanceTypes2434(), func(i *cloudprovider.InstanceType) bool { return i.Name == name })
	return it
}

// addPool creates a NodePool and `count` nodes in it, each running one pod.
func (h *harness) addPool(t *testing.T, name string, it *cloudprovider.InstanceType, count int,
	zoneAt func(i int) string, pod func() *corev1.Pod, rs *appsv1.ReplicaSet) []string {
	np := test.NodePool(v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1.NodePoolSpec{
			Template: v1.NodeClaimTemplate{
				Spec: v1.NodeClaimTemplateSpec{
					Requirements: []v1.NodeSelectorRequirementWithMinValues{
						{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn, Values: allTypes2434},
						{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeOnDemand}},
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
	h.mustApply(t, np)

	var names []string
	memQ := it.Capacity[corev1.ResourceMemory]
	cpuQ := it.Capacity[corev1.ResourceCPU]
	for i := 0; i < count; i++ {
		nc, node := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            np.Name,
				corev1.LabelInstanceTypeStable: it.Name,
				corev1.LabelTopologyZone:       zoneAt(i),
				v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
			}},
			Spec: v1.NodeClaimSpec{NodeClassRef: np.Spec.Template.Spec.NodeClassRef},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
				Allocatable: corev1.ResourceList{
					corev1.ResourceCPU:    cpuQ,
					corev1.ResourceMemory: memQ,
					corev1.ResourcePods:   resource.MustParse("110"),
				},
			},
		})
		nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		h.mustApply(t, nc, node)
		h.nodes = append(h.nodes, node)
		h.claims = append(h.claims, nc)
		h.bindPod(t, len(h.nodes)-1, rs, pod())
		names = append(names, node.Name)
	}
	return names
}

func (h *harness) podCPUMem(cpu, mem string) *corev1.Pod {
	return test.Pod(test.PodOptions{
		ObjectMeta: metav1.ObjectMeta{UID: uuid.NewUUID()},
		ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cpu),
			corev1.ResourceMemory: resource.MustParse(mem),
		}},
	})
}

// ===========================================================================
// Coverage-aware strategy runners
// ===========================================================================

type covResult struct {
	savings  float64
	decision string
	calls    int
	covered  bool // did the strategy ever EVALUATE the target set?
	setSize  int
}

// evalRec evaluates one subset, records coverage of targetKey, updates best.
func (h *harness) evalRec(t *testing.T, sub []*disruption.Candidate, targetKey string, rec *covResult) oracleResult {
	if subsetKey(sub) == targetKey {
		rec.covered = true
	}
	r := h.evalSet(t, sub)
	rec.calls++
	if r.decision != "no-op" && r.savings > rec.savings {
		rec.savings, rec.decision, rec.setSize = r.savings, r.decision, len(sub)
	}
	return r
}

// prefixBinary runs the savings-ratio prefix binary search over the given
// candidate slice, accumulating into rec.
func (h *harness) prefixBinary(t *testing.T, cands []*disruption.Candidate, targetKey string, rec *covResult) {
	sorted := append([]*disruption.Candidate(nil), cands...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].SavingsRatio() > sorted[j].SavingsRatio() })
	loI, hiI := 1, len(sorted)
	for loI <= hiI {
		mid := (loI + hiI) / 2
		r := h.evalRec(t, sorted[:mid], targetKey, rec)
		if r.decision != "no-op" {
			loI = mid + 1
		} else {
			hiI = mid - 1
		}
	}
}

// exhaustiveWithin enumerates all subsets (size>=2) of a small partition.
const exhaustiveCap = 6 // 2^6 = 64 subsets max per partition

func (h *harness) exhaustiveWithin(t *testing.T, part []*disruption.Candidate, targetKey string, rec *covResult) {
	n := len(part)
	if n > exhaustiveCap {
		h.prefixBinary(t, part, targetKey, rec) // fall back for large partitions
		return
	}
	for mask := 1; mask < (1 << n); mask++ {
		var sub []*disruption.Candidate
		for i := 0; i < n; i++ {
			if mask&(1<<i) != 0 {
				sub = append(sub, part[i])
			}
		}
		if len(sub) < 2 {
			continue
		}
		h.evalRec(t, sub, targetKey, rec)
	}
}

func partitionBy(cands []*disruption.Candidate, key func(*disruption.Candidate) string) map[string][]*disruption.Candidate {
	m := map[string][]*disruption.Candidate{}
	for _, c := range cands {
		k := key(c)
		m[k] = append(m[k], c)
	}
	return m
}

// Strategy: global prefix binary search (the real algorithm's behavior).
func (h *harness) sGlobalBinary(t *testing.T, cands []*disruption.Candidate, targetKey string) covResult {
	rec := covResult{decision: "no-op"}
	h.prefixBinary(t, cands, targetKey, &rec)
	return rec
}

// Strategy: partition by NodePool, prefix binary search within each.
func (h *harness) sPerNodePoolBinary(t *testing.T, cands []*disruption.Candidate, targetKey string) covResult {
	rec := covResult{decision: "no-op"}
	for _, part := range partitionBy(cands, func(c *disruption.Candidate) string { return c.NodePool.Name }) {
		h.prefixBinary(t, part, targetKey, &rec)
	}
	return rec
}

// Strategy: partition by NodePool, EXHAUSTIVE subset search within each small partition.
func (h *harness) sPerNodePoolExhaustive(t *testing.T, cands []*disruption.Candidate, targetKey string) covResult {
	rec := covResult{decision: "no-op"}
	for _, part := range partitionBy(cands, func(c *disruption.Candidate) string { return c.NodePool.Name }) {
		h.exhaustiveWithin(t, part, targetKey, &rec)
	}
	return rec
}

// Strategy: partition by schedulability key (nodepool, instance family, zone), exhaustive within.
func (h *harness) sPerSchedGroupExhaustive(t *testing.T, cands []*disruption.Candidate, targetKey string) covResult {
	rec := covResult{decision: "no-op"}
	key := func(c *disruption.Candidate) string {
		l := c.Labels()
		return c.NodePool.Name + "|" + l[corev1.LabelInstanceTypeStable] + "|" + l[corev1.LabelTopologyZone]
	}
	for _, part := range partitionBy(cands, key) {
		h.exhaustiveWithin(t, part, targetKey, &rec)
	}
	return rec
}

// Strategy: coupling-graph proposals (reused), coverage-aware, under a budget.
func (h *harness) sCoupling(t *testing.T, cands []*disruption.Candidate, targetKey string, budget int) covResult {
	return h.runProposalsCov(t, h.couplingSubsets(t, cands), targetKey, budget)
}

// Strategy: similarity-graph proposals (reused), coverage-aware, under a budget.
func (h *harness) sSimilarity(t *testing.T, cands []*disruption.Candidate, targetKey string, budget int) covResult {
	return h.runProposalsCov(t, h.similaritySubsets(t, cands), targetKey, budget)
}

func (h *harness) runProposalsCov(t *testing.T, subsets [][]*disruption.Candidate, targetKey string, budget int) covResult {
	rec := covResult{decision: "no-op"}
	seen := map[string]bool{}
	for _, sub := range subsets {
		if rec.calls >= budget {
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
		h.evalRec(t, sub, targetKey, &rec)
	}
	return rec
}

// ===========================================================================
// The #2434 coverage test
// ===========================================================================

func TestCoverage2434(t *testing.T) {
	// --- P1a: small N=8 (2 dedicated + 6 filler) — brute-force VERIFY the trap bites.
	{
		h := newHarness(t)
		target, knownOPT := h.genCluster2434(t, params2434{fillerPools: 3, fillerNodesPerPool: 2, dedicatedZone: "test-zone-1"})
		cands := h.allCandidates(t)
		targetKey := targetKeyOf(cands, target)
		opt, _ := h.bruteForceOptimum(t, cands)
		gb := h.sGlobalBinary(t, cands, targetKey)
		t.Logf("[verify N=%d] bruteforce OPT=%.2f (knownOPT=%.2f) | globalBinary save=%.2f covered=%v",
			len(cands), opt, knownOPT, gb.savings, gb.covered)
		if opt < knownOPT-0.001 {
			t.Fatalf("brute force (%.2f) did not find the 2->1 merge (expected >= %.2f)", opt, knownOPT)
		}
		if gb.savings >= opt-0.001 {
			t.Fatalf("scenario does not bite: global binary already achieves OPT (%.2f>=%.2f)", gb.savings, opt)
		}
	}

	// --- P1b: scale N in {80, 120}. OPT is the dedicated 2->1 merge = 0.10.
	type row struct {
		n                                                    int
		gb, pnpB, pnpE, psgE, coup, sim                      covResult
	}
	var rows []row
	for _, fp := range []int{39, 59} { // 2 dedicated + fp*2 filler = 80, 120
		h := newHarness(t)
		target, knownOPT := h.genCluster2434(t, params2434{fillerPools: fp, fillerNodesPerPool: 2, dedicatedZone: "test-zone-1"})
		cands := h.allCandidates(t)
		targetKey := targetKeyOf(cands, target)
		budget := 4 * len(cands)
		r := row{
			n:    len(cands),
			gb:   h.sGlobalBinary(t, cands, targetKey),
			pnpB: h.sPerNodePoolBinary(t, cands, targetKey),
			pnpE: h.sPerNodePoolExhaustive(t, cands, targetKey),
			psgE: h.sPerSchedGroupExhaustive(t, cands, targetKey),
			coup: h.sCoupling(t, cands, targetKey, budget),
			sim:  h.sSimilarity(t, cands, targetKey, budget),
		}
		rows = append(rows, r)
		_ = knownOPT
	}

	var b strings.Builder
	fmt.Fprintf(&b, "\n#2434 coverage reproduction (OPT = dedicated 2->1 merge = 0.10 $/hr)\n")
	fmt.Fprintf(&b, "%-5s %-22s | %-8s %-6s %-8s\n", "N", "strategy", "save", "calls", "covered")
	fmt.Fprintln(&b, strings.Repeat("-", 52))
	for _, r := range rows {
		for _, s := range []struct {
			name string
			c    covResult
		}{
			{"globalBinary", r.gb},
			{"perNodePool-binary", r.pnpB},
			{"perNodePool-exhaustive", r.pnpE},
			{"perSchedGroup-exhaustive", r.psgE},
			{"coupling", r.coup},
			{"similarity", r.sim},
		} {
			fmt.Fprintf(&b, "%-5d %-22s | %-8.2f %-6d %-8v\n", r.n, s.name, s.c.savings, s.c.calls, s.c.covered)
		}
		fmt.Fprintln(&b, strings.Repeat("-", 52))
	}
	t.Log(b.String())
}

func targetKeyOf(cands []*disruption.Candidate, names []string) string {
	set := map[string]bool{}
	for _, n := range names {
		set[n] = true
	}
	var sub []*disruption.Candidate
	for _, c := range cands {
		if set[c.Name()] {
			sub = append(sub, c)
		}
	}
	return subsetKey(sub)
}
