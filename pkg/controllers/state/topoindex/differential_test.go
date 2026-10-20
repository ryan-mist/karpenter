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

package topoindex_test

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state/topoindex"
	pscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
)

// This file checks the index against Topology.countDomains: the reference below is countDomains with the informer
// List and Get replaced by reads of the objects the index was fed, and it uses the scheduler's own
// TopologyListOptions, IgnoredForTopology and TopologyNodeFilter.

// nodeFilter adapts a TopologyNodeFilter to a NodePredicate, matching nodes the way countDomains does
type nodeFilter struct {
	filter scheduling.TopologyNodeFilter
}

func (f nodeFilter) Key() string {
	return fmt.Sprint(f.filter.Hash())
}

func (f nodeFilter) Matches(labels map[string]string, taints []corev1.Taint) bool {
	return f.filter.Matches(taints, pscheduling.NewLabelRequirements(labels))
}

type group struct {
	key         string
	namespaces  sets.Set[string]
	rawSelector *metav1.LabelSelector
	filter      *scheduling.TopologyNodeFilter
}

func (g group) spec() topoindex.GroupSpec {
	spec := topoindex.GroupSpec{
		TopologyKey: g.key,
		Namespaces:  sets.List(g.namespaces),
		Selector:    scheduling.TopologyListOptions("", g.rawSelector).LabelSelector,
	}
	if g.filter != nil {
		spec.NodeFilter = nodeFilter{filter: *g.filter}
	}
	return spec
}

// countDomainsReference is the counting part of Topology.countDomains
func countDomainsReference(g group, pods map[types.UID]*corev1.Pod, nodes map[string]*corev1.Node, excluded sets.Set[string]) map[string]int32 {
	counts := map[string]int32{}
	for _, ns := range g.namespaces.UnsortedList() {
		selector := scheduling.TopologyListOptions(ns, g.rawSelector).LabelSelector
		for _, p := range pods {
			// the List
			if p.Namespace != ns || !selector.Matches(labelSet(p.Labels)) {
				continue
			}
			if scheduling.IgnoredForTopology(p) {
				continue
			}
			if excluded.Has(string(p.UID)) {
				continue
			}
			n, ok := nodes[p.Spec.NodeName]
			if !ok {
				continue
			}
			domain, ok := n.Labels[g.key]
			if !ok && g.key == corev1.LabelHostname {
				domain = n.Name
				ok = true
			}
			if !ok {
				continue
			}
			if g.filter != nil && !g.filter.Matches(n.Spec.Taints, pscheduling.NewLabelRequirements(n.Labels)) {
				continue
			}
			counts[domain]++
		}
	}
	return counts
}

type labelSet map[string]string

func (l labelSet) Has(key string) bool   { _, ok := l[key]; return ok }
func (l labelSet) Get(key string) string { return l[key] }
func (l labelSet) Lookup(key string) (string, bool) {
	v, ok := l[key]
	return v, ok
}

func randomLabelSelector(r *rand.Rand) *metav1.LabelSelector {
	switch r.IntN(8) {
	case 0:
		return nil
	case 1:
		return &metav1.LabelSelector{}
	case 2:
		// an In with no values is invalid, so it selects nothing
		return &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "app", Operator: metav1.LabelSelectorOpIn}}}
	}
	selector := &metav1.LabelSelector{}
	if r.IntN(2) == 0 {
		selector.MatchLabels = map[string]string{"app": fmt.Sprint(r.IntN(3))}
	}
	for range r.IntN(3) {
		op := []metav1.LabelSelectorOperator{metav1.LabelSelectorOpIn, metav1.LabelSelectorOpNotIn, metav1.LabelSelectorOpExists, metav1.LabelSelectorOpDoesNotExist}[r.IntN(4)]
		req := metav1.LabelSelectorRequirement{Key: []string{"app", "tier", "pod-template-hash"}[r.IntN(3)], Operator: op}
		if op == metav1.LabelSelectorOpIn || op == metav1.LabelSelectorOpNotIn {
			req.Values = []string{fmt.Sprint(r.IntN(3)), fmt.Sprint(r.IntN(3))}
		}
		selector.MatchExpressions = append(selector.MatchExpressions, req)
	}
	return selector
}

func randomFilter(r *rand.Rand) *scheduling.TopologyNodeFilter {
	if r.IntN(2) == 0 {
		return nil
	}
	pod := &corev1.Pod{}
	if r.IntN(2) == 0 {
		pod.Spec.NodeSelector = map[string]string{"pool": fmt.Sprint(r.IntN(2))}
	}
	if r.IntN(2) == 0 {
		pod.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{
				{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"zone-0", "zone-1"}},
			}}},
		}}}
	}
	if r.IntN(2) == 0 {
		pod.Spec.Tolerations = []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}}
	}
	policies := []corev1.NodeInclusionPolicy{corev1.NodeInclusionPolicyHonor, corev1.NodeInclusionPolicyIgnore}
	filter := scheduling.MakeTopologyNodeFilter(pod, policies[r.IntN(2)], policies[r.IntN(2)])
	return &filter
}

func randomGroup(r *rand.Rand) group {
	g := group{
		key:         []string{corev1.LabelTopologyZone, corev1.LabelHostname, "rack"}[r.IntN(3)],
		namespaces:  sets.New(fmt.Sprintf("ns-%d", r.IntN(3))),
		rawSelector: randomLabelSelector(r),
		filter:      randomFilter(r),
	}
	if r.IntN(3) == 0 {
		g.namespaces.Insert(fmt.Sprintf("ns-%d", r.IntN(3)))
	}
	return g
}

type cluster struct {
	r     *rand.Rand
	ix    *topoindex.Index
	pods  map[types.UID]*corev1.Pod
	nodes map[string]*corev1.Node
	rv    int
}

func (c *cluster) labels() map[string]string {
	l := map[string]string{}
	for _, key := range []string{"app", "tier", "pod-template-hash"} {
		if c.r.IntN(3) != 0 {
			l[key] = fmt.Sprint(c.r.IntN(3))
		}
	}
	return l
}

func (c *cluster) podEvent() {
	c.rv++
	uid := types.UID(fmt.Sprintf("pod-%d", c.r.IntN(80)))
	pod, ok := c.pods[uid]
	if !ok {
		pod = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: uid, Name: string(uid), Namespace: fmt.Sprintf("ns-%d", c.r.IntN(3)), Labels: c.labels()}}
		pod.Status.Phase = corev1.PodPending
	} else {
		pod = pod.DeepCopy()
	}
	pod.ResourceVersion = fmt.Sprint(c.rv)
	switch c.r.IntN(8) {
	case 0, 1, 2:
		if pod.Spec.NodeName == "" {
			pod.Spec.NodeName = fmt.Sprintf("node-%d", c.r.IntN(12))
			pod.Status.Phase = corev1.PodRunning
		}
	case 3:
		pod.Labels = c.labels()
	case 4:
		pod.DeletionTimestamp = &metav1.Time{}
	case 5:
		pod.Status.Phase = []corev1.PodPhase{corev1.PodSucceeded, corev1.PodFailed}[c.r.IntN(2)]
	case 6:
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady})
	default:
		c.ix.DeletePod(uid)
		delete(c.pods, uid)
		return
	}
	c.ix.ObservePod(pod)
	c.pods[uid] = pod
}

func (c *cluster) nodeEvent() {
	c.rv++
	name := fmt.Sprintf("node-%d", c.r.IntN(12))
	if c.r.IntN(5) == 0 {
		c.ix.DeleteNode(name)
		delete(c.nodes, name)
		return
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, ResourceVersion: fmt.Sprint(c.rv), Labels: map[string]string{"pool": fmt.Sprint(c.r.IntN(2))}}}
	if c.r.IntN(5) != 0 {
		node.Labels[corev1.LabelTopologyZone] = fmt.Sprintf("zone-%d", c.r.IntN(3))
	}
	if c.r.IntN(4) != 0 {
		// some nodes are labeled with a hostname that isn't their name
		node.Labels[corev1.LabelHostname] = fmt.Sprintf("host-%d", c.r.IntN(12))
	}
	if c.r.IntN(2) == 0 {
		node.Labels["rack"] = fmt.Sprint(c.r.IntN(2))
	}
	if c.r.IntN(3) == 0 {
		node.Spec.Taints = []corev1.Taint{{Key: "dedicated", Effect: corev1.TaintEffectNoSchedule}}
	}
	c.ix.ObserveNode(node)
	c.nodes[name] = node
}

// indexCounts are the snapshot's counts for the group minus the contributions of the excluded pods, which is what the
// scheduler's count source will hand the overlay
func indexCounts(t *testing.T, s *topoindex.Snapshot, counts *topoindex.GroupCounts, excluded sets.Set[string]) map[string]int32 {
	t.Helper()
	out := maps.Clone(counts.Counts())
	for uid := range excluded {
		f, ok := s.Pod(types.UID(uid))
		if !ok {
			continue
		}
		if domain, ok := s.Contribution(counts, f); ok {
			if out[domain]--; out[domain] == 0 {
				delete(out, domain)
			}
		}
	}
	return out
}

// TestIndexMatchesCountDomains drives random pod and node churn into the index and compares every registered group,
// both freshly registered and incrementally maintained, with countDomains' result, with random excluded pods.
func TestIndexMatchesCountDomains(t *testing.T) {
	for seed := range uint64(300) {
		runDifferential(t, seed, 400)
	}
}

func FuzzIndexMatchesCountDomains(f *testing.F) {
	f.Add(uint64(0), uint16(400))
	f.Fuzz(func(t *testing.T, seed uint64, steps uint16) {
		runDifferential(t, seed, int(steps%1000))
	})
}

func runDifferential(t *testing.T, seed uint64, steps int) {
	t.Helper()
	{
		r := rand.New(rand.NewPCG(seed, 0xd1ff)) //nolint:gosec // deterministic test input
		c := &cluster{r: r, ix: topoindex.New(), pods: map[types.UID]*corev1.Pod{}, nodes: map[string]*corev1.Node{}}
		var groups []group
		var held []func()
		for step := range steps {
			if r.IntN(3) == 0 {
				c.nodeEvent()
			} else {
				c.podEvent()
			}
			if r.IntN(20) == 0 || len(groups) == 0 {
				g := randomGroup(r)
				_, counts := c.ix.Register(g.spec())
				if want := countDomainsReference(g, c.pods, c.nodes, nil); !maps.Equal(counts.Counts(), want) {
					t.Fatalf("seed %d step %d: registered %v, countDomains %v", seed, step, counts.Counts(), want)
				}
				groups = append(groups, g)
			}
			if r.IntN(5) != 0 {
				continue
			}
			s := c.ix.Snapshot()
			excluded := sets.New[string]()
			for uid := range c.pods {
				if r.IntN(6) == 0 {
					excluded.Insert(string(uid))
				}
			}
			for _, g := range groups {
				counts, ok := s.Counts(g.spec().Key())
				if !ok {
					t.Fatalf("seed %d step %d: group not registered", seed, step)
				}
				want := countDomainsReference(g, c.pods, c.nodes, excluded)
				if got := indexCounts(t, s, counts, excluded); !maps.Equal(got, want) {
					t.Fatalf("seed %d step %d: index %v, countDomains %v for %s/%v/%v", seed, step, got, want, g.key, sets.List(g.namespaces), g.rawSelector)
				}
				// a held snapshot must keep answering for the cluster as it was
				frozen := maps.Clone(want)
				held = append(held, func() {
					if got := indexCounts(t, s, counts, excluded); !maps.Equal(got, frozen) {
						t.Fatalf("seed %d: held snapshot %d changed: %v, was %v", seed, s.Generation(), got, frozen)
					}
				})
			}
			if len(held) > 50 {
				held = slices.Delete(held, 0, len(held)-50)
			}
			for _, check := range held {
				check()
			}
		}
	}
}
