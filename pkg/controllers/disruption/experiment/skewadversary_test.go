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

// Adversarial soundness stress-test for the TSC skew prefilter (skewInfeasible).
// Goal: find FALSE NEGATIVES — removal sets the check REJECTS but the REAL
// SimulateScheduling oracle can actually consolidate. Zero FNs is the contract.
//
// Each probe builds a cluster, enumerates removal sets (INCLUDING size-1, since a
// single-node delete is a valid consolidation and is where several FNs live), and
// compares skewInfeasible against the oracle. Any (reject && decision != "no-op") is
// a false negative and is reported via t.Errorf.

import (
	"fmt"
	"sort"
	"testing"

	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/test"
)

// ---------------------------------------------------------------------------
// adversary helpers (all prefixed adv to avoid clashes)
// ---------------------------------------------------------------------------

// advNodePool installs the standard 3-zone {small,medium,large} on-demand pool.
// If instanceTypes is non-empty it restricts the launchable instance types (used
// by the "replacement instance the pool can't launch" probe).
func (h *harness) advNodePool(t *testing.T, instanceTypes ...string) *appsv1.ReplicaSet {
	its := instanceTypes
	if len(its) == 0 {
		its = []string{"small", "medium", "large"}
	}
	h.nodePool = test.NodePool(v1.NodePool{
		Spec: v1.NodePoolSpec{
			Template: v1.NodeClaimTemplate{
				Spec: v1.NodeClaimTemplateSpec{
					Requirements: []v1.NodeSelectorRequirementWithMinValues{
						{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn, Values: its},
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
	h.mustApply(t, h.nodePool)
	rs := test.ReplicaSet()
	h.mustApply(t, rs)
	return rs
}

// advNode creates+registers a node/nodeclaim in the given zone of the given type,
// returns it so the caller can bind pods and reference it by name.
func (h *harness) advNode(t *testing.T, zone, itName string) *corev1.Node {
	it := instanceType(itName)
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
	return node
}

// advBind attaches pod to node (rs-owned => reschedulable) and applies it.
func (h *harness) advBind(t *testing.T, node *corev1.Node, rs *appsv1.ReplicaSet, pod *corev1.Pod) {
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID,
		Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true),
	}}
	pod.Spec.NodeName = node.Name
	h.mustApply(t, pod)
}

type advPodSpec struct {
	cpu, mem       string
	app            string
	extraLabels    map[string]string
	maxSkew        int32
	minDomains     *int32
	matchLabelKeys []string
	topologyKey    string   // default: zone
	affinityZones  []string // node affinity restricting group to these zones
}

func (h *harness) advPod(o advPodSpec) *corev1.Pod {
	reqs := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(o.cpu)}
	if o.mem != "" {
		reqs[corev1.ResourceMemory] = resource.MustParse(o.mem)
	}
	labels := map[string]string{"app": o.app}
	for k, v := range o.extraLabels {
		labels[k] = v
	}
	tk := o.topologyKey
	if tk == "" {
		tk = corev1.LabelTopologyZone
	}
	skew := o.maxSkew
	if skew == 0 {
		skew = 1
	}
	po := test.PodOptions{
		ObjectMeta:           metav1.ObjectMeta{UID: uuid.NewUUID(), Labels: labels},
		ResourceRequirements: corev1.ResourceRequirements{Requests: reqs},
		TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
			MaxSkew:           skew,
			TopologyKey:       tk,
			WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": o.app}},
			MinDomains:        o.minDomains,
			MatchLabelKeys:    o.matchLabelKeys,
		}},
	}
	if len(o.affinityZones) > 0 {
		po.NodeRequirements = []corev1.NodeSelectorRequirement{{
			Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: o.affinityZones,
		}}
	}
	return test.Pod(po)
}

// advEnum yields every subset with size in [1, maxSize] (size-1 included).
func advEnum(cands []*disruption.Candidate, maxSize int, fn func([]*disruption.Candidate)) {
	n := len(cands)
	for mask := 1; mask < (1 << n); mask++ {
		pc := popcount(mask)
		if pc < 1 || pc > maxSize {
			continue
		}
		var sub []*disruption.Candidate
		for i := 0; i < n; i++ {
			if mask&(1<<i) != 0 {
				sub = append(sub, cands[i])
			}
		}
		fn(sub)
	}
}

// advProbe enumerates subsets and reports the reject/FN tallies. It records a
// t.Errorf for every false negative (reject but oracle consolidatable).
func (h *harness) advProbe(t *testing.T, name string, cands []*disruption.Candidate, maxSize int) (rejects, fns int) {
	total := 0
	advEnum(cands, maxSize, func(S []*disruption.Candidate) {
		total++
		reject := h.skewInfeasible(t, S)
		if !reject {
			return
		}
		rejects++
		r := h.evalSet(t, S)
		if r.decision != "no-op" {
			fns++
			names := lo.Map(S, func(c *disruption.Candidate, _ int) string {
				return fmt.Sprintf("%s/%s", c.Labels()[corev1.LabelTopologyZone], c.Name()[:8])
			})
			sort.Strings(names)
			t.Errorf("[%s] FALSE NEGATIVE: skew REJECTED a consolidatable set (decision=%s savings=%.3f size=%d) nodes=%v",
				name, r.decision, r.savings, len(S), names)
		}
	})
	t.Logf("[%s] sets=%d skewRejects=%d falseNegatives=%d", name, total, rejects, fns)
	return
}

// ---------------------------------------------------------------------------
// TestSkewAdversary — one subtest per probe
// ---------------------------------------------------------------------------

func TestSkewAdversary(t *testing.T) {
	// PROBE 2: matchLabelKeys. Two revisions (rev=a,rev=b) under one app=d TSC with
	// matchLabelKeys=[rev]. The REAL scheduler spreads each revision independently;
	// our model conflates them into one app group. Layout: revA {z1:2,z2:1,z3:1},
	// revB {z1:2,z2:1,z3:1} (each individually valid at maxSkew=1). Removing revA's
	// zone-2 node leaves combined counts {z1:4,z2:1,z3:2}; the model must raise
	// zone-2 to the [3,4] band with only P=1 displaced -> infeasible -> REJECT. But
	// revA alone is {z1:2,z2:0,z3:1}; its displaced pod re-homes onto the staying
	// zone-2 (revB) node within skew -> real DELETE. => FALSE NEGATIVE.
	t.Run("probe2-matchLabelKeys", func(t *testing.T) {
		h := newHarness(t)
		rs := h.advNodePool(t)
		mk := []string{"rev"}
		add := func(zone, rev string) {
			n := h.advNode(t, zone, "large")
			h.advBind(t, n, rs, h.advPod(advPodSpec{cpu: "2", app: "d", extraLabels: map[string]string{"rev": rev}, matchLabelKeys: mk}))
		}
		// revA: z1 x2, z2 x1, z3 x1
		add("test-zone-1", "a")
		add("test-zone-1", "a")
		add("test-zone-2", "a")
		add("test-zone-3", "a")
		// revB: z1 x2, z2 x1, z3 x1
		add("test-zone-1", "b")
		add("test-zone-1", "b")
		add("test-zone-2", "b")
		add("test-zone-3", "b")
		h.makeInitializedAndStateUpdated(t)
		h.advProbe(t, "probe2-matchLabelKeys", h.allCandidates(t), 2)
	})

	// PROBE 3: node-affinity pinning. app=d pods are pinned (node affinity) to
	// {z1,z2}; z3 holds an unrelated FULL node (a spread domain in the model's
	// live-zone view, but NOT in the pod's real domain set). Layout z1:3 (tight,
	// cap 0), z2:2 (room). Removing a z2 node displaces 1 D-pod. REALITY: group
	// domains are {z1,z2}, globalMin=1, re-home onto staying z2 node (2-1<=1) ->
	// DELETE. MODEL: live zones {z1,z2,z3}; z1=3 forces band floor m>=2 while z3
	// (existing 0, cap 0) forces m<=0 -> no band, replacement can't reconcile ->
	// REJECT. => FALSE NEGATIVE.
	t.Run("probe3-nodeAffinity", func(t *testing.T) {
		h := newHarness(t)
		rs := h.advNodePool(t)
		pin := []string{"test-zone-1", "test-zone-2"}
		// z1: 3 tight small nodes (cpu2 pod fills small's 2 cpu -> cap 0)
		for i := 0; i < 3; i++ {
			n := h.advNode(t, "test-zone-1", "small")
			h.advBind(t, n, rs, h.advPod(advPodSpec{cpu: "2", app: "d", affinityZones: pin}))
		}
		// z2: 2 large nodes w/ room, 1 D-pod each
		for i := 0; i < 2; i++ {
			n := h.advNode(t, "test-zone-2", "large")
			h.advBind(t, n, rs, h.advPod(advPodSpec{cpu: "2", app: "d", affinityZones: pin}))
		}
		// z3: a full large node (unrelated app) -> live domain, no room for D
		z3 := h.advNode(t, "test-zone-3", "large")
		h.advBind(t, z3, rs, test.Pod(test.PodOptions{
			ObjectMeta:           metav1.ObjectMeta{UID: uuid.NewUUID(), Labels: map[string]string{"app": "filler"}},
			ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("32")}},
		}))
		h.makeInitializedAndStateUpdated(t)
		h.advProbe(t, "probe3-nodeAffinity", h.allCandidates(t), 2)
	})

	// PROBE 1: minDomains. minDomains only makes REALITY stricter (requires more
	// occupied domains), so ignoring it should never cause an FN. Tight cluster
	// (medium nodes, 5-cpu pods => cap 0) with minDomains=3 on the TSC.
	t.Run("probe1-minDomains", func(t *testing.T) {
		h := newHarness(t)
		rs := h.advNodePool(t)
		md := lo.ToPtr(int32(3))
		for _, z := range zones {
			for i := 0; i < 2; i++ {
				n := h.advNode(t, z, "medium")
				h.advBind(t, n, rs, h.advPod(advPodSpec{cpu: "5", app: "d", minDomains: md}))
			}
		}
		h.makeInitializedAndStateUpdated(t)
		_, fns := h.advProbe(t, "probe1-minDomains", h.allCandidates(t), 4)
		if fns != 0 {
			t.Errorf("probe1-minDomains: expected 0 FN, got %d", fns)
		}
	})

	// PROBE 4: pre-skewed stayers (single clean group, no matchLabelKeys, no
	// pinning). All 3 zones seeded as domains by the pool => reality's globalMin
	// includes empty zones, which tends to make reality STRICTER, neutralizing the
	// grandfathering hazard. Build z1:3 (tight), z2:1, z3:1 (pre-skew span 2) and
	// enumerate. Expect model rejects but reality also rejects (0 FN).
	t.Run("probe4-preSkew", func(t *testing.T) {
		h := newHarness(t)
		rs := h.advNodePool(t)
		mk := func(z, it string, room bool) {
			n := h.advNode(t, z, it)
			h.advBind(t, n, rs, h.advPod(advPodSpec{cpu: "2", app: "d"}))
			_ = room
		}
		for i := 0; i < 3; i++ {
			mk("test-zone-1", "small", false) // tight cap 0
		}
		mk("test-zone-2", "large", true)
		mk("test-zone-3", "large", true)
		h.makeInitializedAndStateUpdated(t)
		h.advProbe(t, "probe4-preSkew", h.allCandidates(t), 3)
	})

	// PROBE 5: cap_z binding on memory. D-pods request cpu AND mem; mem is the
	// binding dim (medium 16Gi, 9Gi pod => cap 0 on memory). addableCount takes the
	// min over dims, so the check stays sound. Expect 0 FN.
	t.Run("probe5-memoryBinding", func(t *testing.T) {
		h := newHarness(t)
		rs := h.advNodePool(t)
		for _, z := range zones {
			for i := 0; i < 2; i++ {
				n := h.advNode(t, z, "medium")
				h.advBind(t, n, rs, h.advPod(advPodSpec{cpu: "2", mem: "9Gi", app: "d"}))
			}
		}
		h.makeInitializedAndStateUpdated(t)
		_, fns := h.advProbe(t, "probe5-memoryBinding", h.allCandidates(t), 4)
		if fns != 0 {
			t.Errorf("probe5-memoryBinding: expected 0 FN, got %d", fns)
		}
	})

	// PROBE 6: multiple eligible groups (app=d1, app=d2), both clean zonal TSC.
	// One replacement in the model serves each group independently; reality has one
	// node. Two independent tight groups across zones.
	t.Run("probe6-multiGroup", func(t *testing.T) {
		h := newHarness(t)
		rs := h.advNodePool(t)
		for _, app := range []string{"d1", "d2"} {
			for _, z := range zones {
				n := h.advNode(t, z, "medium")
				h.advBind(t, n, rs, h.advPod(advPodSpec{cpu: "5", app: app}))
			}
		}
		h.makeInitializedAndStateUpdated(t)
		h.advProbe(t, "probe6-multiGroup", h.allCandidates(t), 4)
	})

	// PROBE 7: hostname topologyKey must be EXCLUDED by eligibility (zonalTSC
	// requires topologyKey==zone). So skewInfeasible must NEVER reject a
	// hostname-TSC group => 0 rejects, 0 FN.
	t.Run("probe7-hostname", func(t *testing.T) {
		h := newHarness(t)
		rs := h.advNodePool(t)
		for _, z := range zones {
			for i := 0; i < 2; i++ {
				n := h.advNode(t, z, "medium")
				h.advBind(t, n, rs, h.advPod(advPodSpec{cpu: "5", app: "d", topologyKey: corev1.LabelHostname}))
			}
		}
		h.makeInitializedAndStateUpdated(t)
		rejects, fns := h.advProbe(t, "probe7-hostname", h.allCandidates(t), 4)
		if rejects != 0 {
			t.Errorf("probe7-hostname: expected 0 rejects (hostname must be excluded), got %d", rejects)
		}
		if fns != 0 {
			t.Errorf("probe7-hostname: expected 0 FN, got %d", fns)
		}
	})

	// PROBE 8: replacement instance the pool can't launch. The pool is small-only,
	// but maxReplCount scans ALL cloudProvider instance types (large). This
	// over-estimates replacement room => MORE permissive => cannot cause an FN
	// (only a miss). Confirm the direction: replCount reflects the large instance,
	// not the pool's small.
	t.Run("probe8-replInstance", func(t *testing.T) {
		h := newHarness(t)
		_ = h.advNodePool(t, "small") // pool restricted to small
		r := map[string]int64{"cpu": 2000, "mem": 0}
		repl := h.maxReplCount(r)
		smallFloor := int(instanceType("small").Capacity.Cpu().MilliValue() / 2000) // =1
		t.Logf("[probe8-replInstance] maxReplCount(cpu=2)=%d smallFloor=%d (model uses largest instance, ignoring pool restriction)", repl, smallFloor)
		if repl <= smallFloor {
			t.Errorf("probe8: expected replCount to reflect a larger instance than the pool allows (got %d, small=%d) -- if it equalled small the direction would need review", repl, smallFloor)
		}
	})
}

// ---------------------------------------------------------------------------
// FIX VALIDATION: a strict eligibility gate excludes the FN-causing cases.
// The real fix lives in zonalTSC/eligibleGroups (skewprune_test.go); here we
// validate the DIRECTION with a wrapper that fail-closes when any candidate
// app group contains a pod whose TSC uses matchLabelKeys/minDomains, or which
// is pinned by nodeSelector/required node affinity.
// ---------------------------------------------------------------------------

// advDisqualified reports whether a pod breaks the "clean single zonal-TSC"
// assumptions in ways the current zonalTSC gate misses: matchLabelKeys narrows
// the real spread group per-revision; minDomains adds domains reality requires;
// nodeSelector/required node affinity restricts the pod's real domain set.
func advDisqualified(p *corev1.Pod) bool {
	for i := range p.Spec.TopologySpreadConstraints {
		tsc := &p.Spec.TopologySpreadConstraints[i]
		if tsc.TopologyKey != corev1.LabelTopologyZone {
			continue
		}
		if len(tsc.MatchLabelKeys) > 0 {
			return true // per-revision spread; app-grouping conflates revisions
		}
		if tsc.MinDomains != nil {
			return true // reality is stricter; exclude to be safe
		}
	}
	if len(p.Spec.NodeSelector) > 0 {
		return true // zone pinning shrinks the real domain set
	}
	if a := p.Spec.Affinity; a != nil && a.NodeAffinity != nil &&
		a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		return true // required node affinity can shrink the real domain set
	}
	return false
}

// advStrictReject applies the current check but suppresses the reject if ANY
// pod carrying a candidate app label is disqualified (the strict gate would
// exclude that group entirely => no reject on its behalf).
func (h *harness) advStrictReject(t *testing.T, S []*disruption.Candidate) bool {
	if !h.skewInfeasible(t, S) {
		return false
	}
	pods := h.allPods(t)
	// Candidate apps named by some pod's zonal TSC selector.
	candApps := map[string]bool{}
	for _, p := range pods {
		if tsc, ok := zonalTSC(p); ok && tsc.LabelSelector != nil {
			if app := tsc.LabelSelector.MatchLabels["app"]; app != "" {
				candApps[app] = true
			}
		}
	}
	for _, p := range pods {
		if candApps[p.Labels["app"]] && advDisqualified(p) {
			return false // group excluded by strict gate => cannot reject
		}
	}
	return true
}

func TestSkewAdversaryFix(t *testing.T) {
	// Re-run the matchLabelKeys layout with the strict gate: 0 FN expected.
	t.Run("fix-matchLabelKeys", func(t *testing.T) {
		h := newHarness(t)
		rs := h.advNodePool(t)
		mk := []string{"rev"}
		add := func(zone, rev string) {
			n := h.advNode(t, zone, "large")
			h.advBind(t, n, rs, h.advPod(advPodSpec{cpu: "2", app: "d", extraLabels: map[string]string{"rev": rev}, matchLabelKeys: mk}))
		}
		add("test-zone-1", "a")
		add("test-zone-1", "a")
		add("test-zone-2", "a")
		add("test-zone-3", "a")
		add("test-zone-1", "b")
		add("test-zone-1", "b")
		add("test-zone-2", "b")
		add("test-zone-3", "b")
		h.makeInitializedAndStateUpdated(t)
		cands := h.allCandidates(t)
		rejects, fns := 0, 0
		advEnum(cands, 2, func(S []*disruption.Candidate) {
			if !h.advStrictReject(t, S) {
				return
			}
			rejects++
			if h.evalSet(t, S).decision != "no-op" {
				fns++
			}
		})
		t.Logf("[fix-matchLabelKeys] strictRejects=%d falseNegatives=%d", rejects, fns)
		if fns != 0 {
			t.Errorf("fix-matchLabelKeys: still %d FN after strict gate", fns)
		}
	})

	t.Run("fix-nodeAffinity", func(t *testing.T) {
		h := newHarness(t)
		rs := h.advNodePool(t)
		pin := []string{"test-zone-1", "test-zone-2"}
		for i := 0; i < 3; i++ {
			n := h.advNode(t, "test-zone-1", "small")
			h.advBind(t, n, rs, h.advPod(advPodSpec{cpu: "2", app: "d", affinityZones: pin}))
		}
		for i := 0; i < 2; i++ {
			n := h.advNode(t, "test-zone-2", "large")
			h.advBind(t, n, rs, h.advPod(advPodSpec{cpu: "2", app: "d", affinityZones: pin}))
		}
		z3 := h.advNode(t, "test-zone-3", "large")
		h.advBind(t, z3, rs, test.Pod(test.PodOptions{
			ObjectMeta:           metav1.ObjectMeta{UID: uuid.NewUUID(), Labels: map[string]string{"app": "filler"}},
			ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("32")}},
		}))
		h.makeInitializedAndStateUpdated(t)
		cands := h.allCandidates(t)
		rejects, fns := 0, 0
		advEnum(cands, 2, func(S []*disruption.Candidate) {
			if !h.advStrictReject(t, S) {
				return
			}
			rejects++
			if h.evalSet(t, S).decision != "no-op" {
				fns++
			}
		})
		t.Logf("[fix-nodeAffinity] strictRejects=%d falseNegatives=%d", rejects, fns)
		if fns != 0 {
			t.Errorf("fix-nodeAffinity: still %d FN after strict gate", fns)
		}
	})
}
