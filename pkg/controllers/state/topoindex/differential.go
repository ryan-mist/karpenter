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
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/karpenter/pkg/scheduling/topology"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
)

// maxLoggedMismatches is the number of mismatches a differential source logs
const maxLoggedMismatches = 5

// differential answers from the fallback source and compares every answer with the index's
type differential struct {
	source *CountSource
	// listed and indexed are each key's base counts from the fallback and from the index
	listed, indexed map[*topology.CountKey]*topology.BaseCounts
	logged          int
}

// Differential makes the source answer every call from its fallback (listing pods), and compare the answers with the
// index's: each key's counts net of the excluded pods, and the inverse anti-affinity terms. Mismatches are counted in
// karpenter_scheduler_topology_index_differential_mismatches_total and logged, but never used. Call it before the
// source is used.
func (s *CountSource) Differential() {
	s.diff = &differential{source: s, listed: map[*topology.CountKey]*topology.BaseCounts{}, indexed: map[*topology.CountKey]*topology.BaseCounts{}}
	DifferentialChecksTotal.Inc(nil)
}

func (d *differential) counts(ctx context.Context, key *topology.CountKey) (*topology.BaseCounts, error) {
	listed, err := d.source.fallback.Counts(ctx, key)
	if err != nil {
		return nil, err
	}
	indexed, err := d.source.indexCounts(ctx, key)
	if err != nil {
		return nil, err
	}
	d.listed[key], d.indexed[key] = listed, indexed
	return listed, nil
}

// net returns the counts minus the excluded pods' contributions
func net(base *topology.BaseCounts, contributions func(f func(string)) error) (map[string]int32, error) {
	counts := map[string]int32{}
	base.ForEach(func(domain string, count int32) { counts[domain] = count })
	err := contributions(func(domain string) {
		if counts[domain]--; counts[domain] == 0 {
			delete(counts, domain)
		}
	})
	return counts, err
}

func (d *differential) excludedContributions(ctx context.Context, key *topology.CountKey, excluded *topology.ExcludedPods, f func(domain string)) error {
	if _, ok := d.listed[key]; !ok {
		if _, err := d.counts(ctx, key); err != nil {
			return err
		}
	}
	var listedDomains []string
	listed, err := net(d.listed[key], func(f func(string)) error {
		return d.source.fallback.ExcludedContributions(ctx, key, excluded, func(domain string) {
			listedDomains = append(listedDomains, domain)
			f(domain)
		})
	})
	if err != nil {
		return err
	}
	indexed, err := net(d.indexed[key], func(f func(string)) error { return d.source.indexExcludedContributions(ctx, key, excluded, f) })
	if err != nil {
		return err
	}
	if !maps.Equal(listed, indexed) {
		cause := mismatchCounts
		if d.recheckCounts(ctx, key, excluded) {
			cause = mismatchRace
		}
		d.mismatch(ctx, cause, "key", key.TopologyKey, "namespaces", key.Namespaces, "selector", key.Selector, "listed", pretty.Map(listed, 25), "index", pretty.Map(indexed, 25))
	}
	for _, domain := range listedDomains {
		f(domain)
	}
	return nil
}

type inverseTerms map[string]*struct {
	owners sets.Set[types.UID]
	counts map[string]int32
}

// mergeInverseTerms merges the terms by (namespace, term), since sources may split or group them differently
func mergeInverseTerms(terms inverseTerms, t *topology.InverseTerm) {
	encoded, err := json.Marshal(t.Term)
	if err != nil {
		encoded = []byte(t.Term.String())
	}
	key := t.Namespace + "\x00" + string(encoded)
	merged, ok := terms[key]
	if !ok {
		merged = &struct {
			owners sets.Set[types.UID]
			counts map[string]int32
		}{owners: sets.New[types.UID](), counts: map[string]int32{}}
		terms[key] = merged
	}
	merged.owners.Insert(t.Owners...)
	t.Counts.ForEach(func(domain string, count int32) { merged.counts[domain] += count })
}

func (d *differential) inverseAntiAffinityTerms(ctx context.Context, excluded *topology.ExcludedPods, f func(*topology.InverseTerm)) error {
	var terms []*topology.InverseTerm
	listed := inverseTerms{}
	if err := d.source.fallback.InverseAntiAffinityTerms(ctx, excluded, func(t *topology.InverseTerm) {
		terms = append(terms, t)
		mergeInverseTerms(listed, t)
	}); err != nil {
		return err
	}
	indexed := inverseTerms{}
	if err := d.source.indexInverseAntiAffinityTerms(excluded, func(t *topology.InverseTerm) { mergeInverseTerms(indexed, t) }); err != nil {
		return err
	}
	if !inverseTermsEqual(listed, indexed) {
		cause := mismatchInverse
		if d.recheckInverse(ctx, excluded) {
			cause = mismatchRace
		}
		for key, l := range listed {
			if i, ok := indexed[key]; !ok || !l.owners.Equal(i.owners) || !maps.Equal(l.counts, i.counts) {
				d.mismatch(ctx, cause, "term", key, "listedOwners", l.owners.Len(), "listed", pretty.Map(l.counts, 25))
			}
		}
		for key, i := range indexed {
			if _, ok := listed[key]; !ok {
				d.mismatch(ctx, cause, "term", key, "indexOwners", i.owners.Len(), "index", pretty.Map(i.counts, 25))
			}
		}
	}
	for _, t := range terms {
		f(t)
	}
	return nil
}

func inverseTermsEqual(a, b inverseTerms) bool {
	if len(a) != len(b) {
		return false
	}
	for key, x := range a {
		if y, ok := b[key]; !ok || !x.owners.Equal(y.owners) || !maps.Equal(x.counts, y.counts) {
			return false
		}
	}
	return true
}

// The index is fed by informer event handlers, which run just after the informer cache applies an event, so it can
// trail the cache that listing reads by the handler delivery latency, and the simulation's snapshot is older still. A
// mismatch is a race if the two agree again when compared at about the same moment: list first, give the handlers a
// moment to deliver what the cache already has, then read a fresh snapshot. A logic bug disagrees every time.
const (
	recheckAttempts = 3
	recheckDelay    = 20 * time.Millisecond
)

// recheckCounts returns true if the key's counts net of the excluded pods agree between listing and a fresh snapshot
func (d *differential) recheckCounts(ctx context.Context, key *topology.CountKey, excluded *topology.ExcludedPods) bool {
	for range recheckAttempts {
		listedBase, err := d.source.fallback.Counts(ctx, key)
		if err != nil {
			return false
		}
		listed, err := net(listedBase, func(f func(string)) error { return d.source.fallback.ExcludedContributions(ctx, key, excluded, f) })
		if err != nil {
			return false
		}
		time.Sleep(recheckDelay)
		fresh, ok := NewCountSource(d.source.index, d.source.fallback)
		if !ok {
			return false
		}
		indexedBase, err := fresh.indexCounts(ctx, key)
		if err != nil {
			return false
		}
		indexed, err := net(indexedBase, func(f func(string)) error { return fresh.indexExcludedContributions(ctx, key, excluded, f) })
		if err == nil && maps.Equal(listed, indexed) {
			return true
		}
	}
	return false
}

// recheckInverse returns true if the inverse anti-affinity terms agree between cluster state and a fresh snapshot
func (d *differential) recheckInverse(ctx context.Context, excluded *topology.ExcludedPods) bool {
	for range recheckAttempts {
		listed := inverseTerms{}
		if err := d.source.fallback.InverseAntiAffinityTerms(ctx, excluded, func(t *topology.InverseTerm) { mergeInverseTerms(listed, t) }); err != nil {
			return false
		}
		time.Sleep(recheckDelay)
		fresh, ok := NewCountSource(d.source.index, d.source.fallback)
		if !ok {
			return false
		}
		indexed := inverseTerms{}
		if err := fresh.indexInverseAntiAffinityTerms(excluded, func(t *topology.InverseTerm) { mergeInverseTerms(indexed, t) }); err == nil && inverseTermsEqual(listed, indexed) {
			return true
		}
	}
	return false
}

func (d *differential) mismatch(ctx context.Context, cause string, keysAndValues ...any) {
	DifferentialMismatchesTotal.Inc(mismatchLabels[cause])
	if d.logged < maxLoggedMismatches {
		d.logged++
		// disruption simulations run with a no-op logger, so mismatches go to the root logger when the context's is off
		logger := log.FromContext(ctx)
		if !logger.Enabled() {
			logger = log.Log.WithName("topology-index")
		}
		logger.Error(fmt.Errorf("topology index mismatch"), "topology counts from the index differ from listing pods",
			append([]any{"cause", cause, "generation", d.source.Generation()}, keysAndValues...)...)
	}
}
