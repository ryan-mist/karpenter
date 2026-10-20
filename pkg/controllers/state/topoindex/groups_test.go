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

package topoindex

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	clock "k8s.io/utils/clock/testing"

	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
)

// taintFilter is a NodePredicate that requires a label value and rejects nodes with a taint key
type taintFilter struct {
	label, value, taint string
	calls               *int
}

func (f taintFilter) Key() string { return f.label + "=" + f.value + "!" + f.taint }

func (f taintFilter) Matches(l map[string]string, taints []corev1.Taint) bool {
	if f.calls != nil {
		*f.calls++
	}
	if l[f.label] != f.value {
		return false
	}
	for _, t := range taints {
		if t.Key == f.taint {
			return false
		}
	}
	return true
}

// referenceCounts counts the way Topology.countDomains does, from the objects the index was fed
func referenceCounts(spec GroupSpec, pods map[types.UID]*corev1.Pod, nodes map[string]*corev1.Node) map[string]int32 {
	counts := map[string]int32{}
	for ns := range sets.New(spec.Namespaces...) {
		for _, p := range pods {
			if p.Namespace != ns || !spec.Selector.Matches(labels.Set(p.Labels)) {
				continue
			}
			if !podutils.IsScheduled(p) || podutils.IsTerminal(p) || podutils.IsTerminating(p) {
				continue
			}
			n, ok := nodes[p.Spec.NodeName]
			if !ok {
				continue
			}
			domain, ok := n.Labels[spec.TopologyKey]
			if !ok && spec.TopologyKey == corev1.LabelHostname {
				domain, ok = n.Name, true
			}
			if !ok {
				continue
			}
			if spec.NodeFilter != nil && !spec.NodeFilter.Matches(n.Labels, n.Spec.Taints) {
				continue
			}
			counts[domain]++
		}
	}
	return counts
}

func snapshotCounts(t *testing.T, s *Snapshot, key GroupKey) map[string]int32 {
	t.Helper()
	c, ok := s.Counts(key)
	if !ok {
		t.Fatalf("group %q isn't registered at generation %d", key, s.Generation())
	}
	counts := map[string]int32{}
	c.All(func(domain string, n int32) { counts[domain] = n })
	return counts
}

// contributionCounts sums the contributions of every pod in the snapshot, which must equal the group's counts
func contributionCounts(s *Snapshot, c *GroupCounts) map[string]int32 {
	counts := map[string]int32{}
	s.pods.All(func(_ types.UID, f *PodFact) bool {
		if domain, ok := s.Contribution(c, *f); ok {
			counts[domain]++
		}
		return true
	})
	return counts
}

func randomSelector(r *rand.Rand) labels.Selector {
	switch r.IntN(6) {
	case 0:
		return labels.Everything()
	case 1:
		return labels.Nothing()
	}
	selector := labels.NewSelector()
	for range 1 + r.IntN(2) {
		key := fmt.Sprintf("k%d", r.IntN(3))
		var req *labels.Requirement
		var err error
		switch r.IntN(3) {
		case 0:
			req, err = labels.NewRequirement(key, selection.In, []string{fmt.Sprint(r.IntN(3)), fmt.Sprint(r.IntN(3))})
		case 1:
			req, err = labels.NewRequirement(key, selection.NotIn, []string{fmt.Sprint(r.IntN(3))})
		default:
			req, err = labels.NewRequirement(key, selection.Exists, nil)
		}
		if err != nil {
			panic(err)
		}
		selector = selector.Add(*req)
	}
	return selector
}

func randomSpec(r *rand.Rand) GroupSpec {
	spec := GroupSpec{
		TopologyKey: []string{corev1.LabelTopologyZone, corev1.LabelHostname, "rack"}[r.IntN(3)],
		Selector:    randomSelector(r),
	}
	for range 1 + r.IntN(2) {
		spec.Namespaces = append(spec.Namespaces, fmt.Sprintf("ns%d", r.IntN(3)))
	}
	if r.IntN(3) == 0 {
		spec.NodeFilter = taintFilter{label: "pool", value: fmt.Sprint(r.IntN(2)), taint: "dedicated"}
	}
	return spec
}

type churn struct {
	r     *rand.Rand
	ix    *Index
	pods  map[types.UID]*corev1.Pod
	nodes map[string]*corev1.Node
	rv    int
}

func (c *churn) nodeName() string {
	return fmt.Sprintf("node-%d", c.r.IntN(8))
}

func (c *churn) randomLabels() map[string]string {
	l := map[string]string{}
	for k := range 3 {
		if c.r.IntN(3) != 0 {
			l[fmt.Sprintf("k%d", k)] = fmt.Sprint(c.r.IntN(3))
		}
	}
	return l
}

func (c *churn) podStep(uid types.UID) {
	c.rv++
	pod, ok := c.pods[uid]
	if !ok {
		pod = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: uid, Name: string(uid), Namespace: fmt.Sprintf("ns%d", c.r.IntN(3)), Labels: c.randomLabels()}}
		pod.Status.Phase = corev1.PodRunning
	} else {
		pod = pod.DeepCopy()
	}
	pod.ResourceVersion = fmt.Sprint(c.rv)
	switch c.r.IntN(6) {
	case 0, 1:
		if pod.Spec.NodeName == "" {
			pod.Spec.NodeName = c.nodeName()
		}
	case 2:
		pod.Labels = c.randomLabels()
	case 3:
		pod.DeletionTimestamp = &metav1.Time{}
	case 4:
		pod.Status.Phase = corev1.PodSucceeded
	}
	c.ix.ObservePod(pod)
	c.pods[uid] = pod
}

func (c *churn) nodeStep(name string) {
	c.rv++
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, ResourceVersion: fmt.Sprint(c.rv), Labels: map[string]string{}}}
	if c.r.IntN(4) != 0 {
		node.Labels[corev1.LabelTopologyZone] = fmt.Sprintf("zone-%d", c.r.IntN(3))
	}
	if c.r.IntN(4) != 0 {
		node.Labels[corev1.LabelHostname] = name
	}
	if c.r.IntN(2) == 0 {
		node.Labels["rack"] = fmt.Sprint(c.r.IntN(2))
	}
	node.Labels["pool"] = fmt.Sprint(c.r.IntN(2))
	if c.r.IntN(3) == 0 {
		node.Spec.Taints = []corev1.Taint{{Key: "dedicated", Effect: corev1.TaintEffectNoSchedule}}
	}
	c.ix.ObserveNode(node)
	c.nodes[name] = node
}

// step applies one random pod or node event
func (c *churn) step() {
	switch op := c.r.IntN(20); {
	case op < 10:
		c.podStep(types.UID(fmt.Sprintf("pod-%d", c.r.IntN(60))))
	case op < 13:
		uid := types.UID(fmt.Sprintf("pod-%d", c.r.IntN(60)))
		c.ix.DeletePod(uid)
		delete(c.pods, uid)
	case op < 18:
		c.nodeStep(c.nodeName())
	default:
		name := c.nodeName()
		c.ix.DeleteNode(name)
		delete(c.nodes, name)
	}
}

type heldSnapshot struct {
	snapshot *Snapshot
	counts   map[GroupKey]map[string]int32
}

// TestRegisteredCountsMatchReference registers random groups while pods and nodes churn, and checks that the
// maintained counts, a fresh registration's counts and the sum of per-pod contributions all equal a recount, and that
// held snapshots never change.
func TestRegisteredCountsMatchReference(t *testing.T) {
	for seed := range uint64(100) {
		r := rand.New(rand.NewPCG(seed, seed^0x5eed)) //nolint:gosec // deterministic test input
		c := &churn{r: r, ix: New(), pods: map[types.UID]*corev1.Pod{}, nodes: map[string]*corev1.Node{}}
		specs := map[GroupKey]GroupSpec{}
		var held []heldSnapshot
		for step := range 300 {
			c.step()
			if r.IntN(15) == 0 || len(specs) == 0 {
				spec := randomSpec(r)
				s, counts := c.ix.Register(spec)
				if got, want := counts.Counts(), referenceCounts(spec, c.pods, c.nodes); !maps.Equal(got, want) {
					t.Fatalf("seed %d step %d: registration counts %v, want %v for %q", seed, step, got, want, spec.Key())
				}
				if s.Generation() != c.ix.Generation() {
					t.Fatalf("seed %d step %d: registration returned a stale snapshot", seed, step)
				}
				specs[spec.Key()] = spec
			}
			if r.IntN(4) != 0 {
				continue
			}
			s := c.ix.Snapshot()
			h := heldSnapshot{snapshot: s, counts: map[GroupKey]map[string]int32{}}
			for key, spec := range specs {
				got := snapshotCounts(t, s, key)
				if want := referenceCounts(spec, c.pods, c.nodes); !maps.Equal(got, want) {
					t.Fatalf("seed %d step %d: maintained counts %v, want %v for %q", seed, step, got, want, key)
				}
				gc, _ := s.Counts(key)
				if contributions := contributionCounts(s, gc); !maps.Equal(contributions, got) {
					t.Fatalf("seed %d step %d: contributions %v, want %v for %q", seed, step, contributions, got, key)
				}
				h.counts[key] = got
			}
			held = append(held, h)
			for _, h := range held {
				for key, want := range h.counts {
					if got := snapshotCounts(t, h.snapshot, key); !maps.Equal(got, want) {
						t.Fatalf("seed %d step %d: held snapshot at generation %d changed for %q", seed, step, h.snapshot.Generation(), key)
					}
				}
			}
			if len(held) > 5 {
				held = held[1:]
			}
		}
	}
}

func TestSnapshotIsReusedUntilSomethingChanges(t *testing.T) {
	ix := New()
	s1 := ix.Snapshot()
	if s2 := ix.Snapshot(); s2 != s1 {
		t.Fatal("snapshot without changes should be reused")
	}
	pod := testPod("a", "ns", "n", map[string]string{"app": "a"})
	ix.ObservePod(pod)
	s3 := ix.Snapshot()
	if s3 == s1 || s3.Generation() <= s1.Generation() {
		t.Fatal("a fact change should publish a newer snapshot")
	}
	if _, ok := s1.Pod("a"); ok {
		t.Fatal("old snapshot sees a later pod")
	}
	ix.ObservePod(pod)
	if ix.Snapshot() != s3 {
		t.Fatal("a no-op event should not publish a new snapshot")
	}
	if _, ok := ix.SnapshotAtLeast(s3.Generation()); !ok {
		t.Fatal("SnapshotAtLeast should be satisfied by a published generation")
	}
}

func TestRegisterIsIdempotent(t *testing.T) {
	ix := New()
	spec := GroupSpec{TopologyKey: corev1.LabelTopologyZone, Namespaces: []string{"b", "a", "a"}, Selector: labels.Everything()}
	_, c1 := ix.Register(spec)
	gen := ix.Generation()
	_, c2 := ix.Register(GroupSpec{TopologyKey: corev1.LabelTopologyZone, Namespaces: []string{"a", "b"}, Selector: labels.Everything()})
	if c1 != c2 || ix.Generation() != gen {
		t.Fatal("registering an equal spec should return the existing group without a new generation")
	}
	if len(ix.groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(ix.groups))
	}
}

func TestGroupKeysDistinguishSpecs(t *testing.T) {
	base := GroupSpec{TopologyKey: corev1.LabelTopologyZone, Namespaces: []string{"a"}, Selector: labels.Everything()}
	variants := []GroupSpec{
		base,
		{TopologyKey: corev1.LabelHostname, Namespaces: []string{"a"}, Selector: labels.Everything()},
		{TopologyKey: corev1.LabelTopologyZone, Namespaces: []string{"b"}, Selector: labels.Everything()},
		{TopologyKey: corev1.LabelTopologyZone, Namespaces: []string{"a", "b"}, Selector: labels.Everything()},
		{TopologyKey: corev1.LabelTopologyZone, Namespaces: []string{"a"}, Selector: labels.Nothing()},
		{TopologyKey: corev1.LabelTopologyZone, Namespaces: []string{"a"}, Selector: labels.SelectorFromSet(labels.Set{"app": "x"})},
		{TopologyKey: corev1.LabelTopologyZone, Namespaces: []string{"a"}, Selector: labels.Everything(), NodeFilter: taintFilter{label: "pool", value: "1"}},
	}
	keys := map[GroupKey]int{}
	for i, spec := range variants {
		if j, ok := keys[spec.Key()]; ok {
			t.Fatalf("specs %d and %d share key %q", i, j, spec.Key())
		}
		keys[spec.Key()] = i
	}
}

func TestNodeFilterIsMemoizedPerNode(t *testing.T) {
	ix := New()
	ix.ObserveNode(testNode("n", map[string]string{corev1.LabelTopologyZone: "a", "pool": "1"}))
	for i := range 10 {
		ix.ObservePod(testPod(fmt.Sprint(i), "ns", "n", map[string]string{"app": "a"}))
	}
	calls := 0
	_, counts := ix.Register(GroupSpec{TopologyKey: corev1.LabelTopologyZone, Namespaces: []string{"ns"}, Selector: labels.Everything(),
		NodeFilter: taintFilter{label: "pool", value: "1", calls: &calls}})
	if counts.Get("a") != 10 {
		t.Fatalf("expected 10 pods in zone a, got %d", counts.Get("a"))
	}
	if calls != 1 {
		t.Fatalf("node filter was evaluated %d times for one node", calls)
	}
}

func TestGroupsAgeOut(t *testing.T) {
	clk := clock.NewFakeClock(time.Now())
	ix := New(WithClock(clk), WithGroupLimits(10*time.Minute, 2))
	spec := func(ns string) GroupSpec {
		return GroupSpec{TopologyKey: corev1.LabelTopologyZone, Namespaces: []string{ns}, Selector: labels.Everything()}
	}
	held, _ := ix.Register(spec("a"))
	clk.Step(time.Minute)
	ix.Register(spec("b"))
	clk.Step(time.Minute)
	// over the limit: the least recently used group, a, is dropped
	s, _ := ix.Register(spec("c"))
	if _, ok := s.Counts(spec("a").Key()); ok {
		t.Fatal("least recently used group wasn't dropped at the group limit")
	}
	if _, ok := held.Counts(spec("a").Key()); !ok {
		t.Fatal("a held snapshot lost a dropped group")
	}
	// reading b keeps it alive, c ages out
	clk.Step(9 * time.Minute)
	s.Counts(spec("b").Key())
	clk.Step(2 * time.Minute)
	ix.ObservePod(testPod("p", "x", "", nil))
	s = ix.Snapshot()
	if _, ok := s.Counts(spec("b").Key()); !ok {
		t.Fatal("a recently read group was dropped")
	}
	if _, ok := s.Counts(spec("c").Key()); ok {
		t.Fatal("a group unread for longer than the TTL wasn't dropped")
	}
	if len(ix.nsGroups["c"]) != 0 {
		t.Fatal("dropped group is still maintained")
	}
}

func TestHydrateReplacesFacts(t *testing.T) {
	ix := New()
	ix.ObservePod(testPod("gone", "ns", "n1", nil))
	ix.ObserveNode(testNode("n1", nil))
	ix.ObserveNamespace(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "old"}})
	if ix.Synced() {
		t.Fatal("index without handlers reported synced")
	}
	ix.Hydrate(
		[]*corev1.Pod{testPod("kept", "ns", "n2", nil)},
		[]*corev1.Node{testNode("n2", nil)},
		[]*corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "ns"}}},
	)
	s := ix.Snapshot()
	if _, ok := s.Pod("gone"); ok {
		t.Fatal("hydrate kept a pod that's no longer listed")
	}
	if _, ok := s.Node("n1"); ok {
		t.Fatal("hydrate kept a node that's no longer listed")
	}
	if _, ok := s.Namespace("old"); ok {
		t.Fatal("hydrate kept a namespace that's no longer listed")
	}
	if _, ok := s.Pod("kept"); !ok || !ix.Synced() {
		t.Fatal("hydrate didn't add the listed pod or mark the index synced")
	}
	if len(ix.nodes) != 1 {
		t.Fatalf("expected one node record, got %d", len(ix.nodes))
	}
}

func TestNamespacesMatching(t *testing.T) {
	ix := New()
	ix.ObserveNamespace(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "a", Labels: map[string]string{"team": "x"}}})
	ix.ObserveNamespace(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "b", Labels: map[string]string{"team": "y"}}})
	got := ix.Snapshot().NamespacesMatching(labels.SelectorFromSet(labels.Set{"team": "x"}))
	if got.Len() != 1 || !got.Has("a") {
		t.Fatalf("NamespacesMatching = %v, want [a]", got.UnsortedList())
	}
}

// TestSnapshotsUnderConcurrentWrites reads snapshots from two goroutines while the writer churns. Run with -race.
func TestSnapshotsUnderConcurrentWrites(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4)) //nolint:gosec // deterministic test input
	c := &churn{r: r, ix: New(), pods: map[types.UID]*corev1.Pod{}, nodes: map[string]*corev1.Node{}}
	spec := GroupSpec{TopologyKey: corev1.LabelTopologyZone, Namespaces: []string{"ns0", "ns1", "ns2"}, Selector: labels.Everything()}
	c.ix.Register(spec)
	var wg sync.WaitGroup
	done := make(chan struct{})
	for range 2 {
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				s := c.ix.Snapshot()
				counts, ok := s.Counts(spec.Key())
				if !ok {
					t.Error("registered group missing from snapshot")
					return
				}
				if got := contributionCounts(s, counts); !maps.Equal(got, counts.Counts()) {
					t.Errorf("snapshot at generation %d is inconsistent: %v vs %v", s.Generation(), got, counts.Counts())
					return
				}
			}
		})
	}
	for range 3000 {
		c.step()
	}
	close(done)
	wg.Wait()
}
