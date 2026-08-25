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

// Hall-violation experiment: does the max-flow prefilter EVER prune a set that the
// trivial O(P+T) aggregate capacity check accepts?
//
// Theory: flow beats the aggregate check only when aggregate capacity is sufficient
// but the displaced pods cannot REACH it (anti-affinity edge removal). Two sub-kinds:
//   (A) resource-reachability: real capacity sits behind anti-affine peers, so the
//       reachable resource < demand. Pure resource max-flow (edge removal) CATCHES this.
//   (B) cardinality: reachable resource is fine, but too many MUTUALLY anti-affine
//       pods need more distinct hosts than exist (the single m->1 replacement holds
//       only one). This is a COUNT constraint; the resource flow with its uncapped
//       generous replacement OVER-ACCEPTS => flow does NOT catch it.
//
// This test builds both and measures flow's marginal prune over the aggregate check.

import (
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/test"
)

// addNode creates one consolidatable node of the given instance type + zone and
// returns its index in h.nodes.
func (h *harness) addNode(t *testing.T, itName, zone string) int {
	it := instanceType(itName)
	nc, node := test.NodeClaimAndNode(v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			v1.NodePoolLabelKey:            h.nodePool.Name,
			corev1.LabelInstanceTypeStable: it.Name,
			corev1.LabelTopologyZone:       zone,
			v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
		}},
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
	// The test node builder does not set kubernetes.io/hostname; without it the
	// scheduler cannot pin hostname anti-affinity to a domain and does NOT enforce
	// it against existing nodes. Set it (= node name, the scheduler's own fallback)
	// so hostname anti-affinity is actually enforced in this experiment.
	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	node.Labels[corev1.LabelHostname] = node.Name
	h.mustApply(t, nc, node)
	h.nodes = append(h.nodes, node)
	h.claims = append(h.claims, nc)
	return len(h.nodes) - 1
}

type hallParams struct {
	app          string
	victimIT     string // instance type for victim (removal-candidate) nodes
	victimCount  int
	victimPodCPU string // one anti-affine pod per victim node (the displaced pods)
	keeperIT     string // "keeper" nodes host a stationary anti-affine peer (blocks reachability, holds free capacity)
	keeperCount  int
	keeperPodCPU string
	smallIT      string // small reachable nodes (no anti-affine peer)
	smallCount   int
}

// genClusterHall builds a hostname anti-affinity group "app" where:
//   - victim nodes each host one anti-affine pod (removal set candidates),
//   - keeper nodes each host a same-group peer (so victims can't re-home there,
//     yet keepers carry large free capacity the aggregate check will wrongly count),
//   - small nodes are reachable but tiny.
//
// Returns the victim node names (the removal-candidate universe to enumerate).
func (h *harness) genClusterHall(t *testing.T, p hallParams) []string {
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
	rs := test.ReplicaSet()
	h.mustApply(t, rs)

	zone := zones[0] // hostname anti-affinity is zone-independent; keep all in one zone

	// Keepers: stationary same-group peer + large free capacity (unreachable to victims).
	for i := 0; i < p.keeperCount; i++ {
		idx := h.addNode(t, p.keeperIT, zone)
		h.bindPod(t, idx, rs, h.hostAntiPod(p.keeperPodCPU, p.app))
	}
	// Small reachable nodes (no peer), tiny capacity.
	for i := 0; i < p.smallCount; i++ {
		h.addNode(t, p.smallIT, zone)
	}
	// Victims: each hosts one anti-affine pod; these are the removal candidates.
	var victimNames []string
	for i := 0; i < p.victimCount; i++ {
		idx := h.addNode(t, p.victimIT, zone)
		h.bindPod(t, idx, rs, h.hostAntiPod(p.victimPodCPU, p.app))
		victimNames = append(victimNames, h.nodes[idx].Name)
	}

	// makeInitializedAndStateUpdated now registers all pods in cluster state
	// (registerPods), so inter-pod anti-affinity is enforced centrally.
	h.makeInitializedAndStateUpdated(t)
	return victimNames
}

func TestHall(t *testing.T) {
	type scenario struct {
		name  string
		build func(h *harness) []*disruption.Candidate
	}

	victimsOnly := func(h *harness, names []string) []*disruption.Candidate {
		want := map[string]bool{}
		for _, n := range names {
			want[n] = true
		}
		var out []*disruption.Candidate
		for _, c := range h.allCandidates(t) {
			if want[c.Name()] {
				out = append(out, c)
			}
		}
		return out
	}

	scenarios := []scenario{
		{
			// RESOURCE-REACHABILITY: 6 large victims each with a 30-CPU anti-affine pod.
			// Keepers (2 large) hold a same-group peer + ~31 CPU free each (unreachable).
			// Reachable = 1 small (2 CPU) + one 32-CPU replacement = 34 < 60 (k>=2) => flow REJECTS.
			// Aggregate counts the keepers' 62 CPU => ACCEPTS. Oracle: 30-CPU anti-affine pods
			// can't reach any existing host and need one new node each => m->n => no-op.
			name: "hall-reachability",
			build: func(h *harness) []*disruption.Candidate {
				names := h.genClusterHall(t, hallParams{
					app: "hallR", victimIT: "large", victimCount: 6, victimPodCPU: "30",
					keeperIT: "large", keeperCount: 2, keeperPodCPU: "1",
					smallIT: "small", smallCount: 1,
				})
				return victimsOnly(h, names)
			},
		},
		{
			// CARDINALITY: 5 medium victims each with a small 2-CPU anti-affine pod.
			// Reachable resource is plentiful (the one replacement holds them all resource-wise),
			// but they are MUTUALLY anti-affine, so each needs a distinct host. With only a small
			// reachable node + one replacement, k>=3 needs >1 new node => oracle no-op.
			// Flow's uncapped replacement OVER-ACCEPTS (all pile on it) => flow does NOT catch it.
			name: "hall-cardinality",
			build: func(h *harness) []*disruption.Candidate {
				names := h.genClusterHall(t, hallParams{
					app: "hallC", victimIT: "medium", victimCount: 5, victimPodCPU: "2",
					keeperIT: "large", keeperCount: 1, keeperPodCPU: "1",
					smallIT: "small", smallCount: 1,
				})
				return victimsOnly(h, names)
			},
		},
	}

	var b strings.Builder
	fmt.Fprintf(&b, "\nHALL-VIOLATION: does flow ever beat the O(P+T) aggregate check?\n")
	fmt.Fprintf(&b, "%-18s %-6s %-8s %-8s %-9s %-8s %-14s %-7s %-7s\n",
		"scenario", "sets", "consol.", "flow%", "recall%", "agg%", "flowMarginal", "flowFN", "aggFN")
	fmt.Fprintln(&b, strings.Repeat("-", 96))

	totalFN, totalAggFN := 0, 0
	for _, sc := range scenarios {
		h := newHarness(t)
		cands := sc.build(h)
		if len(cands) < 2 {
			t.Fatalf("%s: expected >=2 victim candidates, got %d", sc.name, len(cands))
		}
		c := &confusion{}
		enumerateSubsets(cands, len(cands), func(s []*disruption.Candidate) { h.evalPair(t, s, c) })
		totalFN += c.falseNeg
		totalAggFN += c.aggFalseNeg
		aggPct, margPct := 0.0, 0.0
		if c.total > 0 {
			aggPct = 100 * float64(c.aggReject) / float64(c.total)
			margPct = 100 * float64(c.flowNotAgg) / float64(c.total)
		}
		fmt.Fprintf(&b, "%-18s %-6d %-8d %-8.1f %-9.1f %-8.1f %-14s %-7d %-7d\n",
			sc.name, c.total, c.oracleConsol,
			100*c.pruneRate(), 100*c.recallOnInfeasible(), aggPct,
			fmt.Sprintf("%d (%.1f%%)", c.flowNotAgg, margPct), c.falseNeg, c.aggFalseNeg)
	}
	fmt.Fprintln(&b, strings.Repeat("-", 96))
	fmt.Fprintf(&b, "flowMarginal = sets flow rejects that the aggregate check ACCEPTS (flow's added value).\n")
	fmt.Fprintf(&b, "false negatives MUST be 0 -- flow: %d, agg: %d\n", totalFN, totalAggFN)
	t.Log(b.String())

	if totalFN != 0 {
		t.Fatalf("flow model unsound: %d false negatives", totalFN)
	}
	if totalAggFN != 0 {
		t.Fatalf("aggregate check unsound: %d false negatives", totalAggFN)
	}
}
