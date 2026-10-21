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
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/karpenter/pkg/scheduling/topology"
)

// CountSource is a topology.CountSource over the index. It belongs to one simulation: it pins the snapshot taken when
// it's created, and the snapshot each group's counts came from, so a group's counts and its excluded pods'
// contributions always agree. A group that isn't registered yet is registered on first use, and its counts come from
// the snapshot the registration publishes. Like the scheduler's own list source, it's called serially.
type CountSource struct {
	index    *Index
	snapshot *Snapshot
	// fallback answers the keys the index can't: those whose node filter's hash collides with another filter's
	fallback topology.CountSource
	groups   map[*topology.CountKey]pinnedGroup
	// diff is set in differential mode (see Differential)
	diff *differential
}

type pinnedGroup struct {
	snapshot *Snapshot
	counts   *GroupCounts
}

var _ topology.CountSource = (*CountSource)(nil)

// NewCountSource returns a count source over the index's current snapshot, or false if the index can't answer yet
// because it hasn't seen the informers' initial lists. fallback answers the keys the index can't.
func NewCountSource(index *Index, fallback topology.CountSource) (*CountSource, bool) {
	if index == nil {
		return nil, false
	}
	if index.beforeCountSource != nil {
		index.beforeCountSource(index)
	}
	if !index.Synced() {
		return nil, false
	}
	return &CountSource{index: index, snapshot: index.Snapshot(), fallback: fallback, groups: map[*topology.CountKey]pinnedGroup{}}, true
}

// predicate adapts the scheduler's node predicate. Its key is the predicate's hash; since keys with equal hashes may
// still differ, every lookup checks the predicate it finds with Equal.
type predicate struct {
	topology.NodePredicate
}

func (p predicate) Key() string {
	return fmt.Sprintf("%016x", p.Hash())
}

func specOf(key *topology.CountKey) GroupSpec {
	spec := GroupSpec{TopologyKey: key.TopologyKey, Namespaces: key.Namespaces, Selector: key.ListSelector()}
	if key.NodeFilter != nil {
		spec.NodeFilter = predicate{key.NodeFilter}
	}
	return spec
}

// sameFilter returns true if the group counts with the key's node filter
func sameFilter(counts *GroupCounts, key *topology.CountKey) bool {
	if counts.NodeFilter == nil || key.NodeFilter == nil {
		return counts.NodeFilter == nil && key.NodeFilter == nil
	}
	p, ok := counts.NodeFilter.(predicate)
	return ok && p.Equal(key.NodeFilter)
}

// group returns the pinned counts of the key, registering the group if the snapshot doesn't have it
func (s *CountSource) group(key *topology.CountKey) (pinnedGroup, bool) {
	if g, ok := s.groups[key]; ok {
		return g, g.counts != nil
	}
	spec := specOf(key)
	g := pinnedGroup{snapshot: s.snapshot}
	var ok bool
	if g.counts, ok = s.snapshot.Counts(spec.Key()); ok {
		lookupsTotal(resultHit)
	} else {
		g.snapshot, g.counts = s.index.Register(spec)
		lookupsTotal(resultRegistered)
	}
	if !sameFilter(g.counts, key) {
		lookupsTotal(resultCollision)
		g.counts = nil
	}
	s.groups[key] = g
	return g, g.counts != nil
}

func (s *CountSource) Counts(ctx context.Context, key *topology.CountKey) (*topology.BaseCounts, error) {
	if s.diff != nil {
		return s.diff.counts(ctx, key)
	}
	return s.indexCounts(ctx, key)
}

func (s *CountSource) indexCounts(ctx context.Context, key *topology.CountKey) (*topology.BaseCounts, error) {
	g, ok := s.group(key)
	if !ok {
		return s.fallback.Counts(ctx, key)
	}
	return g.counts.baseCounts(), nil
}

func (s *CountSource) ExcludedContributions(ctx context.Context, key *topology.CountKey, excluded *topology.ExcludedPods, f func(domain string)) error {
	if s.diff != nil {
		return s.diff.excludedContributions(ctx, key, excluded, f)
	}
	return s.indexExcludedContributions(ctx, key, excluded, f)
}

func (s *CountSource) indexExcludedContributions(ctx context.Context, key *topology.CountKey, excluded *topology.ExcludedPods, f func(domain string)) error {
	g, ok := s.group(key)
	if !ok {
		return s.fallback.ExcludedContributions(ctx, key, excluded, f)
	}
	for _, pod := range countableExcluded(g.snapshot, excluded) {
		if domain, ok := g.snapshot.Contribution(g.counts, pod); ok {
			f(domain)
		}
	}
	return nil
}

type countableExcludedKey struct {
	snapshot *Snapshot
}

// countableExcluded returns the excluded pods that the snapshot counts: most pods a simulation schedules are pending,
// so this is usually much shorter than the excluded pods
func countableExcluded(snapshot *Snapshot, excluded *topology.ExcludedPods) []PodFact {
	return excluded.Memo(countableExcludedKey{snapshot: snapshot}, func() any {
		var pods []PodFact
		for uid := range excluded.UIDs() {
			if f, ok := snapshot.Pod(uid); ok && f.Countable {
				pods = append(pods, f)
			}
		}
		return pods
	}).([]PodFact)
}

func (s *CountSource) InverseAntiAffinityTerms(ctx context.Context, excluded *topology.ExcludedPods, f func(*topology.InverseTerm)) error {
	if s.diff != nil {
		return s.diff.inverseAntiAffinityTerms(ctx, excluded, f)
	}
	return s.indexInverseAntiAffinityTerms(excluded, f)
}

func (s *CountSource) indexInverseAntiAffinityTerms(excluded *topology.ExcludedPods, f func(*topology.InverseTerm)) error {
	s.snapshot.InverseTerms(func(t *InverseTerm) bool {
		if term := t.withoutExcluded(excluded); term != nil {
			f(term)
		}
		return true
	})
	return nil
}

func (s *CountSource) Generation() uint64 {
	return s.snapshot.Generation()
}

// baseCounts returns the counts in the scheduler's form, converting them once per snapshot that changed them
func (c *GroupCounts) baseCounts() *topology.BaseCounts {
	if b := c.base.Load(); b != nil {
		return b
	}
	b := newBaseCounts(c.TopologyKey, c.counts)
	c.base.Store(b)
	return b
}

// newBaseCounts returns the counts, dense if the topology key is dense. Sparse counts share the map, which is immutable.
func newBaseCounts(topologyKey string, counts map[string]int32) *topology.BaseCounts {
	if !topology.Dense(topologyKey) {
		return &topology.BaseCounts{Sparse: counts}
	}
	dict := topology.Dict(topologyKey)
	names := make([]string, 0, len(counts))
	for domain := range counts {
		names = append(names, domain)
	}
	ids := dict.InternAll(names...)
	b := &topology.BaseCounts{View: dict.View()}
	b.Dense = make([]int32, b.View.Len())
	for i, id := range ids {
		b.Dense[id] = counts[names[i]]
	}
	return b
}

// withoutExcluded returns the term without the excluded owners, or nil if every owner is excluded
func (t *InverseTerm) withoutExcluded(excluded *topology.ExcludedPods) *topology.InverseTerm {
	var dropped []InverseOwner
	for _, o := range t.Owners {
		if excluded.Has(o.UID) {
			dropped = append(dropped, o)
		}
	}
	if len(dropped) == 0 {
		return t.converted()
	}
	if len(dropped) == len(t.Owners) {
		return nil
	}
	counts := map[string]int32{}
	owners := make([]types.UID, 0, len(t.Owners)-len(dropped))
	for _, o := range t.Owners {
		if excluded.Has(o.UID) {
			continue
		}
		owners = append(owners, o.UID)
		if o.HasDomain {
			counts[o.Domain] += o.Terms
		}
	}
	return &topology.InverseTerm{Namespace: t.Namespace, Term: t.Term, Owners: owners, Counts: &topology.BaseCounts{Sparse: counts}}
}

// converted returns the term in the scheduler's form, converting it once
func (t *InverseTerm) converted() *topology.InverseTerm {
	if c := t.scheduler.Load(); c != nil {
		return c
	}
	owners := make([]types.UID, 0, len(t.Owners))
	for _, o := range t.Owners {
		owners = append(owners, o.UID)
	}
	c := &topology.InverseTerm{Namespace: t.Namespace, Term: t.Term, Owners: owners, Counts: &topology.BaseCounts{Sparse: t.Counts}}
	t.scheduler.Store(c)
	return c
}
