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
	"maps"
	"slices"
	"strings"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"sigs.k8s.io/karpenter/pkg/scheduling/topology"
)

// NodePredicate decides whether a Node takes part in a group's counts, e.g. a topology spread constraint's node
// inclusion policies. It must be immutable and safe for concurrent use.
type NodePredicate interface {
	// Key identifies the predicate. Predicates with equal keys must match the same Nodes.
	Key() string
	// Matches returns true if a Node with the labels and taints takes part in the counts.
	Matches(labels map[string]string, taints []corev1.Taint) bool
}

// GroupSpec describes a set of pod counts: for every domain of TopologyKey, the number of countable pods in
// Namespaces matched by Selector whose Node is in the informer cache and matches NodeFilter. A pod's domain is its
// Node's TopologyKey label or, for kubernetes.io/hostname, the Node's name when the label is missing. Pods whose Node
// doesn't have the label aren't counted.
type GroupSpec struct {
	TopologyKey string
	Namespaces  []string
	// Selector is matched against pod labels. It must be immutable.
	Selector labels.Selector
	// NodeFilter is nil when every Node takes part.
	NodeFilter NodePredicate
}

// GroupKey identifies a GroupSpec. Specs with equal keys have equal counts.
type GroupKey string

// Key returns the spec's GroupKey.
func (s GroupSpec) Key() GroupKey {
	var b strings.Builder
	b.WriteString(s.TopologyKey)
	b.WriteByte(0)
	namespaces := slices.Clone(s.Namespaces)
	slices.Sort(namespaces)
	for _, ns := range slices.Compact(namespaces) {
		b.WriteString(ns)
		b.WriteByte(1)
	}
	b.WriteByte(0)
	// String() is the same for Everything and Nothing, so Nothing gets its own encoding. Selectors can't contain NUL.
	if _, selectable := s.Selector.Requirements(); !selectable {
		b.WriteString("\x02nothing")
	} else {
		b.WriteString(s.Selector.String())
	}
	b.WriteByte(0)
	if s.NodeFilter != nil {
		b.WriteString(s.NodeFilter.Key())
	}
	return GroupKey(b.String())
}

// groupSpec is a GroupSpec prepared for evaluation. It's immutable and shared by the writer and snapshots.
type groupSpec struct {
	GroupSpec
	key        GroupKey
	namespaces map[string]struct{}
	hostname   bool
	// lastUsed is when the group's counts were last read or registered, in unix nanoseconds
	lastUsed *atomic.Int64
}

func newGroupSpec(spec GroupSpec) *groupSpec {
	g := &groupSpec{
		GroupSpec:  spec,
		key:        spec.Key(),
		namespaces: map[string]struct{}{},
		hostname:   spec.TopologyKey == corev1.LabelHostname,
		lastUsed:   &atomic.Int64{},
	}
	for _, ns := range spec.Namespaces {
		g.namespaces[ns] = struct{}{}
	}
	g.Namespaces = slices.Sorted(maps.Keys(g.namespaces))
	return g
}

// selects returns true if pods with the label set are counted by the group, if their Node takes part
func (g *groupSpec) selects(namespace string, l labels.Set) bool {
	_, ok := g.namespaces[namespace]
	return ok && g.Selector.Matches(l)
}

// domain returns the domain the group counts pods on the Node in, with matches evaluating the group's node filter
func (g *groupSpec) domain(node *Node, matches func(*Node) bool) (string, bool) {
	if node == nil {
		return "", false
	}
	domain, ok := node.Labels[g.TopologyKey]
	// as in Topology.countDomains, a Node without a hostname label is counted under its name
	if !ok && g.hostname {
		domain, ok = node.Name, true
	}
	if !ok || (g.NodeFilter != nil && !matches(node)) {
		return "", false
	}
	return domain, true
}

// group is the writer's state for a registered GroupSpec.
type group struct {
	*groupSpec
	// counts are the non-zero counts by domain
	counts map[string]int32
	// filterMemo memoizes NodeFilter.Matches by Node. Nodes are immutable, so a changed Node gets a new entry, and the
	// old entry is dropped when the Node changes.
	filterMemo map[*Node]bool
	// frozen is the immutable copy of counts published in the last snapshot, or nil if counts changed since
	frozen *GroupCounts
}

func (g *group) matches(node *Node) bool {
	if result, ok := g.filterMemo[node]; ok {
		return result
	}
	result := g.NodeFilter.Matches(node.Labels, node.Taints)
	g.filterMemo[node] = result
	return result
}

// add adds delta to the count of the domain the group counts pods on the node in. It returns true if this is the
// first change since the group's counts were last frozen.
func (g *group) add(node *nodeRec, delta int32) bool {
	domain, ok := g.domain(node.node, g.matches)
	if !ok {
		return false
	}
	if n := g.counts[domain] + delta; n == 0 {
		delete(g.counts, domain)
	} else {
		g.counts[domain] = n
	}
	if g.frozen == nil {
		return false
	}
	g.frozen = nil
	return true
}

// freeze returns the group's published counts, copying them if they changed since the last snapshot.
func (g *group) freeze() *GroupCounts {
	if g.frozen == nil {
		g.frozen = &GroupCounts{groupSpec: g.groupSpec, counts: maps.Clone(g.counts)}
	}
	return g.frozen
}

// GroupCounts are the counts of a registered group in one snapshot. They're immutable.
type GroupCounts struct {
	*groupSpec
	counts map[string]int32
	// base is the scheduler's form of counts, built on first use
	base atomic.Pointer[topology.BaseCounts]
}

// Key returns the group's key.
func (c *GroupCounts) Key() GroupKey {
	return c.key
}

// Get returns the count for the domain.
func (c *GroupCounts) Get(domain string) int32 {
	return c.counts[domain]
}

// Len returns the number of domains with a non-zero count.
func (c *GroupCounts) Len() int {
	return len(c.counts)
}

// All calls fn for every domain with a non-zero count, in no particular order.
func (c *GroupCounts) All(fn func(domain string, count int32)) {
	for domain, count := range c.counts {
		fn(domain, count)
	}
}

// Counts returns the non-zero counts by domain. The map is shared and must not be modified.
func (c *GroupCounts) Counts() map[string]int32 {
	return c.counts
}
