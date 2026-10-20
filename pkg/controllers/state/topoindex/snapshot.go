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
	"cmp"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"

	"sigs.k8s.io/karpenter/pkg/controllers/state/topoindex/internal/cow"
)

const (
	// defaultGroupTTL is longer than the five minutes after which consolidation re-evaluates an unchanged cluster, so the
	// groups of a steady workload are never dropped
	defaultGroupTTL  = 10 * time.Minute
	defaultMaxGroups = 5000
	gcInterval       = time.Minute
)

// Snapshot is an immutable view of the index at one generation. It's safe for concurrent use, and holding it doesn't
// block the index's writers.
type Snapshot struct {
	gen        uint64
	groups     *cow.View[GroupKey, *GroupCounts]
	pods       *cow.View[types.UID, *PodFact]
	nodes      *cow.View[string, *Node]
	namespaces *cow.View[string, *Namespace]
	inverse    *cow.View[string, *InverseTerm]
	clock      clock.PassiveClock
}

// Generation returns the index generation the snapshot was taken at.
func (s *Snapshot) Generation() uint64 {
	return s.gen
}

// Counts returns the counts of a registered group, or false if the group wasn't registered when the snapshot was taken.
func (s *Snapshot) Counts(key GroupKey) (*GroupCounts, bool) {
	c, ok := s.groups.Get(key)
	if ok {
		c.lastUsed.Store(s.clock.Now().UnixNano())
	}
	return c, ok
}

// Pod returns the fact for the pod.
func (s *Snapshot) Pod(uid types.UID) (PodFact, bool) {
	f, ok := s.pods.Get(uid)
	if !ok {
		return PodFact{}, false
	}
	return *f, true
}

// Node returns the Node as the informer cache had it.
func (s *Snapshot) Node(name string) (*Node, bool) {
	return s.nodes.Get(name)
}

// Namespace returns the Namespace.
func (s *Snapshot) Namespace(name string) (*Namespace, bool) {
	return s.namespaces.Get(name)
}

// NamespacesMatching returns the names of the namespaces whose labels match the selector.
func (s *Snapshot) NamespacesMatching(selector labels.Selector) sets.Set[string] {
	names := sets.New[string]()
	s.namespaces.All(func(name string, ns *Namespace) bool {
		if selector.Matches(ns.Labels) {
			names.Insert(name)
		}
		return true
	})
	return names
}

// InverseTerms calls fn for every required anti-affinity term of the pods that cluster state tracks as bound to a Node
// it has, with the counts of those pods. These are the pods Cluster.ForPodsWithAntiAffinity yields.
func (s *Snapshot) InverseTerms(fn func(*InverseTerm) bool) {
	s.inverse.All(func(_ string, t *InverseTerm) bool { return fn(t) })
}

// Contribution returns the domain the counts of the group include the pod in, or false if they don't include it.
func (s *Snapshot) Contribution(c *GroupCounts, pod PodFact) (string, bool) {
	return s.contribution(c, pod, func(node *Node) bool { return c.NodeFilter.Matches(node.Labels, node.Taints) })
}

// contribution is Contribution with matches evaluating the group's node filter
func (s *Snapshot) contribution(c *GroupCounts, pod PodFact, matches func(*Node) bool) (string, bool) {
	if !pod.Countable || !c.selects(pod.Namespace, pod.Labels) {
		return "", false
	}
	node, _ := s.nodes.Get(pod.NodeName)
	return c.domain(node, matches)
}

// Snapshot returns a snapshot of the index's current contents. It's lock-free when nothing changed since the last one.
func (ix *Index) Snapshot() *Snapshot {
	if s := ix.pub.Load(); s.gen == ix.genA.Load() {
		return s
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.publishLocked()
}

// SnapshotAtLeast returns a snapshot at a generation of at least gen, or false if the index was reset since, so it
// can't tell. Since Snapshot publishes on demand, a generation seen in an earlier snapshot is always satisfied.
func (ix *Index) SnapshotAtLeast(gen uint64) (*Snapshot, bool) {
	s := ix.Snapshot()
	return s, s.gen >= gen
}

// Register starts maintaining counts for the group, if they aren't already, and returns them along with the snapshot
// they belong to. The first registration scans the pods in the group's namespaces.
func (ix *Index) Register(spec GroupSpec) (*Snapshot, *GroupCounts) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	key := spec.Key()
	g, ok := ix.groups[key]
	if !ok {
		start := ix.clock.Now()
		g = ix.registerLocked(newGroupSpec(spec))
		RegistrationDurationSeconds.Observe(ix.clock.Since(start).Seconds(), nil)
		GroupRegistrationsTotal.Inc(nil)
		ix.bumpLocked()
	}
	g.lastUsed.Store(ix.clock.Now().UnixNano())
	s := ix.publishLocked()
	c, _ := s.groups.Get(key)
	return s, c
}

func (ix *Index) registerLocked(spec *groupSpec) *group {
	g := &group{groupSpec: spec, counts: map[string]int32{}, filterMemo: map[*Node]bool{}}
	for _, ns := range g.Namespaces {
		ix.nsGroups[ns] = append(ix.nsGroups[ns], g)
		for set := range ix.nsSets[ns] {
			if !g.Selector.Matches(set.Labels) {
				continue
			}
			set.Data.groups = append(set.Data.groups, g)
			for _, rec := range set.Data.pods {
				g.add(ix.nodes[rec.nodeName], 1)
			}
		}
	}
	ix.groups[g.key] = g
	ix.frozenGroups.Set(g.key, g.freeze())
	return g
}

func (ix *Index) unregisterLocked(g *group) {
	for _, ns := range g.Namespaces {
		ix.nsGroups[ns] = slices.DeleteFunc(ix.nsGroups[ns], func(other *group) bool { return other == g })
		if len(ix.nsGroups[ns]) == 0 {
			delete(ix.nsGroups, ns)
		}
		for set := range ix.nsSets[ns] {
			set.Data.groups = slices.DeleteFunc(set.Data.groups, func(other *group) bool { return other == g })
		}
	}
	delete(ix.groups, g.key)
	ix.frozenGroups.Delete(g.key)
	GroupEvictionsTotal.Inc(nil)
}

// gcLocked drops groups that haven't been read for the TTL, and the least recently read groups over the limit.
// Snapshots that hold a dropped group's counts keep them.
func (ix *Index) gcLocked() {
	now := ix.clock.Now()
	if now.Sub(ix.lastGC) < gcInterval && len(ix.groups) <= ix.maxGroups {
		return
	}
	ix.lastGC = now
	for _, g := range ix.groups {
		if now.Sub(time.Unix(0, g.lastUsed.Load())) > ix.ttl {
			ix.unregisterLocked(g)
		}
	}
	if over := len(ix.groups) - ix.maxGroups; over > 0 {
		groups := make([]*group, 0, len(ix.groups))
		for _, g := range ix.groups {
			groups = append(groups, g)
		}
		slices.SortFunc(groups, func(a, b *group) int { return cmp.Compare(a.lastUsed.Load(), b.lastUsed.Load()) })
		for _, g := range groups[:over] {
			ix.unregisterLocked(g)
		}
	}
}

// publishLocked publishes a snapshot of the current contents, reusing the last one if nothing changed.
func (ix *Index) publishLocked() *Snapshot {
	ix.gcLocked()
	for _, g := range ix.changedGroups {
		// a group that changed and was then dropped isn't published
		if ix.groups[g.key] == g {
			ix.frozenGroups.Set(g.key, g.freeze())
		}
	}
	ix.changedGroups = ix.changedGroups[:0]
	ix.freezeInverseLocked()
	if last := ix.pub.Load(); last != nil && last.gen == ix.gen {
		return last
	}
	s := &Snapshot{
		gen:        ix.gen,
		groups:     ix.frozenGroups.Freeze(),
		pods:       ix.podFacts.Freeze(),
		nodes:      ix.nodeFacts.Freeze(),
		namespaces: ix.namespaces.Freeze(),
		inverse:    ix.inverseTerms.Freeze(),
		clock:      ix.clock,
	}
	ix.pub.Store(s)
	ix.recordMetricsLocked()
	return s
}

// SetSynced marks the index as synced, or not, regardless of its event handlers. It's used by tests and by Hydrate.
func (ix *Index) SetSynced(synced bool) {
	ix.synced.Store(synced)
}

// Synced returns true once the index holds every pod, Node and Namespace in the informer cache: its event handlers
// have been delivered the informers' initial lists, or it was hydrated.
func (ix *Index) Synced() bool {
	return ix.synced.Load() || ix.HandlersSynced()
}

// Hydrate replaces the index's facts with the objects, as an informer relist would, and marks the index synced. It's
// used by tests, which don't run informers.
func (ix *Index) Hydrate(pods []*corev1.Pod, nodes []*corev1.Node, namespaces []*corev1.Namespace) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	podUIDs := sets.New[types.UID]()
	for _, pod := range pods {
		podUIDs.Insert(pod.UID)
		ix.observePodLocked(pod)
	}
	for uid := range ix.pods {
		if !podUIDs.Has(uid) {
			ix.deletePodLocked(uid)
		}
	}
	nodeNames := sets.New[string]()
	for _, node := range nodes {
		nodeNames.Insert(node.Name)
		ix.observeNodeLocked(node)
	}
	for name, n := range ix.nodes {
		if n.node != nil && !nodeNames.Has(name) {
			ix.deleteNodeLocked(name)
		}
	}
	namespaceNames := sets.New[string]()
	for _, namespace := range namespaces {
		namespaceNames.Insert(namespace.Name)
		ix.observeNamespaceLocked(namespace)
	}
	ix.namespaces.Freeze().All(func(name string, _ *Namespace) bool {
		if !namespaceNames.Has(name) {
			ix.deleteNamespaceLocked(name)
		}
		return true
	})
	ix.synced.Store(true)
	ix.publishLocked()
}
