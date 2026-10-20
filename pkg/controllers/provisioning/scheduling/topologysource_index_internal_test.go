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

package scheduling

import (
	"context"
	"encoding/json"
	"math/rand"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/controllers/state/topoindex"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling/topology"
	"sigs.k8s.io/karpenter/pkg/test"
)

// sourceCounts returns a key's counts minus its excluded pods' contributions, the way the overlay seeds a group
func sourceCounts(ctx context.Context, src topology.CountSource, key *topology.CountKey, excluded *topology.ExcludedPods) map[string]int32 {
	GinkgoHelper()
	base, err := src.Counts(ctx, key)
	Expect(err).ToNot(HaveOccurred())
	counts := map[string]int32{}
	base.ForEach(func(domain string, count int32) { counts[domain] += count })
	Expect(src.ExcludedContributions(ctx, key, excluded, func(domain string) { counts[domain]-- })).To(Succeed())
	for domain, count := range counts {
		Expect(count).To(BeNumerically(">=", 0))
		if count == 0 {
			delete(counts, domain)
		}
	}
	return counts
}

type sourceInverse struct {
	Owners sets.Set[types.UID]
	Counts map[string]int32
}

// sourceInverseTerms merges the inverse terms by (namespace, term), since sources may split or group them
func sourceInverseTerms(ctx context.Context, src topology.CountSource, excluded *topology.ExcludedPods) map[string]sourceInverse {
	GinkgoHelper()
	out := map[string]sourceInverse{}
	Expect(src.InverseAntiAffinityTerms(ctx, excluded, func(t *topology.InverseTerm) {
		encoded, err := json.Marshal(t.Term)
		Expect(err).ToNot(HaveOccurred())
		key := t.Namespace + "/" + string(encoded)
		if _, ok := out[key]; !ok {
			out[key] = sourceInverse{Owners: sets.New[types.UID](), Counts: map[string]int32{}}
		}
		out[key].Owners.Insert(t.Owners...)
		t.Counts.ForEach(func(domain string, count int32) { out[key].Counts[domain] += count })
	})).To(Succeed())
	return out
}

// The index count source must answer every count key and the inverse terms exactly as the list count source does,
// which reproduces the legacy topology (see the listCountSource spec).
var _ = Describe("index CountSource", func() {
	It("should match the list count source", func() {
		ctx := karpopts.ToContext(context.Background(), test.Options())
		for seed := range int64(400) {
			g := &identityGen{topologyIndexGen{r: rand.New(rand.NewSource(seed))}}
			f := g.fixture(ctx)
			legacy, err := newLegacyTopology(ctx, f.client, f.cluster, f.stateNodes, f.nodePools, f.instanceTypes, f.pods, &options{})
			Expect(err).ToNot(HaveOccurred(), "seed %d", seed)

			index := topoindex.New()
			f.cluster.SetTopologyIndex(index)
			pods, nodes, namespaces := &corev1.PodList{}, &corev1.NodeList{}, &corev1.NamespaceList{}
			Expect(f.client.List(ctx, pods)).To(Succeed())
			Expect(f.client.List(ctx, nodes)).To(Succeed())
			Expect(f.client.List(ctx, namespaces)).To(Succeed())
			index.Hydrate(lo.ToSlicePtr(pods.Items), lo.ToSlicePtr(nodes.Items), lo.ToSlicePtr(namespaces.Items))

			var keys []*topology.CountKey
			for _, tg := range legacy.topologyGroups {
				keys = append(keys, topology.NewCountKey(tg.Key, sets.List(tg.namespaces), tg.rawSelector, newTopologyNodePredicate(tg.nodeFilter)))
			}
			// the first simulation registers the groups, the second reads them from its snapshot
			for range 2 {
				excluded := topology.NewExcludedPods(f.pods)
				list := newListCountSource(f.client, f.cluster, excluded)
				src, ok := topoindex.NewCountSource(index, list)
				Expect(ok).To(BeTrue())
				for _, key := range keys {
					Expect(sourceCounts(ctx, src, key, excluded)).To(Equal(sourceCounts(ctx, list, key, excluded)), "seed %d, key %s %v", seed, key.TopologyKey, key.Namespaces)
				}
				Expect(sourceInverseTerms(ctx, src, excluded)).To(Equal(sourceInverseTerms(ctx, list, excluded)), "seed %d", seed)
			}
		}
	})
	It("should not answer before the index has synced", func() {
		_, ok := topoindex.NewCountSource(topoindex.New(), nil)
		Expect(ok).To(BeFalse())
		_, ok = topoindex.NewCountSource(nil, nil)
		Expect(ok).To(BeFalse())
	})
})

// differentialMismatches returns karpenter_scheduler_topology_index_differential_mismatches_total for the causes, or
// for all causes if none are given
func differentialMismatches(causes ...string) float64 {
	GinkgoHelper()
	families, err := crmetrics.Registry.Gather()
	Expect(err).ToNot(HaveOccurred())
	total := 0.0
	for _, mf := range families {
		if mf.GetName() != "karpenter_scheduler_topology_index_differential_mismatches_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "cause" && (len(causes) == 0 || lo.Contains(causes, l.GetValue())) {
					total += m.GetCounter().GetValue()
				}
			}
		}
	}
	return total
}

var _ = Describe("index CountSource differential mode", func() {
	It("should answer like the list source and only count mismatches when the index is stale", func() {
		ctx := karpopts.ToContext(context.Background(), test.Options())
		stale := 0.0
		for seed := range int64(200) {
			g := &identityGen{topologyIndexGen{r: rand.New(rand.NewSource(seed))}}
			f := g.fixture(ctx)
			legacy, err := newLegacyTopology(ctx, f.client, f.cluster, f.stateNodes, f.nodePools, f.instanceTypes, f.pods, &options{})
			Expect(err).ToNot(HaveOccurred())
			index := topoindex.New()
			f.cluster.SetTopologyIndex(index)
			pods, nodes, namespaces := &corev1.PodList{}, &corev1.NodeList{}, &corev1.NamespaceList{}
			Expect(f.client.List(ctx, pods)).To(Succeed())
			Expect(f.client.List(ctx, nodes)).To(Succeed())
			Expect(f.client.List(ctx, namespaces)).To(Succeed())
			index.Hydrate(lo.ToSlicePtr(pods.Items), lo.ToSlicePtr(nodes.Items), lo.ToSlicePtr(namespaces.Items))
			var keys []*topology.CountKey
			for _, tg := range legacy.topologyGroups {
				keys = append(keys, topology.NewCountKey(tg.Key, sets.List(tg.namespaces), tg.rawSelector, newTopologyNodePredicate(tg.nodeFilter)))
			}
			check := func() {
				excluded := topology.NewExcludedPods(f.pods)
				list := newListCountSource(f.client, f.cluster, excluded)
				src, ok := topoindex.NewCountSource(index, newListCountSource(f.client, f.cluster, excluded))
				Expect(ok).To(BeTrue())
				src.Differential()
				for _, key := range keys {
					Expect(sourceCounts(ctx, src, key, excluded)).To(Equal(sourceCounts(ctx, list, key, excluded)), "seed %d", seed)
				}
				Expect(sourceInverseTerms(ctx, src, excluded)).To(Equal(sourceInverseTerms(ctx, list, excluded)), "seed %d", seed)
			}
			before := differentialMismatches()
			check()
			Expect(differentialMismatches()).To(Equal(before), "seed %d: mismatches against a fresh index", seed)

			// the index misses a counted pod's deletion
			for i := range pods.Items {
				if pods.Items[i].Spec.NodeName != "" && !IgnoredForTopology(&pods.Items[i]) {
					Expect(f.client.Delete(ctx, &pods.Items[i])).To(Succeed())
					break
				}
			}
			// a persistent difference is a logic mismatch, not a race
			before, races := differentialMismatches(), differentialMismatches("race")
			check()
			stale += differentialMismatches() - before
			Expect(differentialMismatches("race")).To(Equal(races), "seed %d: a persistent difference was classed as a race", seed)
		}
		Expect(stale).To(BeNumerically(">", 0))
	})

	It("should class a difference that's gone when compared again as a race", func() {
		ctx := karpopts.ToContext(context.Background(), test.Options())
		races := 0.0
		for seed := range int64(100) {
			g := &identityGen{topologyIndexGen{r: rand.New(rand.NewSource(seed))}}
			f := g.fixture(ctx)
			legacy, err := newLegacyTopology(ctx, f.client, f.cluster, f.stateNodes, f.nodePools, f.instanceTypes, f.pods, &options{})
			Expect(err).ToNot(HaveOccurred())
			hydrate := func(index *topoindex.Index) {
				pods, nodes, namespaces := &corev1.PodList{}, &corev1.NodeList{}, &corev1.NamespaceList{}
				Expect(f.client.List(ctx, pods)).To(Succeed())
				Expect(f.client.List(ctx, nodes)).To(Succeed())
				Expect(f.client.List(ctx, namespaces)).To(Succeed())
				index.Hydrate(lo.ToSlicePtr(pods.Items), lo.ToSlicePtr(nodes.Items), lo.ToSlicePtr(namespaces.Items))
			}
			// the simulation's snapshot trails the cache by one pod deletion; the recheck's snapshot has caught up
			catchUp := false
			index := topoindex.New(topoindex.WithBeforeCountSource(func(index *topoindex.Index) {
				if catchUp {
					hydrate(index)
				}
			}))
			f.cluster.SetTopologyIndex(index)
			hydrate(index)
			pods := &corev1.PodList{}
			Expect(f.client.List(ctx, pods)).To(Succeed())
			for i := range pods.Items {
				if pods.Items[i].Spec.NodeName != "" && !IgnoredForTopology(&pods.Items[i]) {
					Expect(f.client.Delete(ctx, &pods.Items[i])).To(Succeed())
					break
				}
			}
			excluded := topology.NewExcludedPods(f.pods)
			src, ok := topoindex.NewCountSource(index, newListCountSource(f.client, f.cluster, excluded))
			Expect(ok).To(BeTrue())
			src.Differential()
			catchUp = true
			before, logic := differentialMismatches("race"), differentialMismatches("counts", "inverse")
			for _, tg := range legacy.topologyGroups {
				key := topology.NewCountKey(tg.Key, sets.List(tg.namespaces), tg.rawSelector, newTopologyNodePredicate(tg.nodeFilter))
				sourceCounts(ctx, src, key, excluded)
			}
			Expect(differentialMismatches("counts", "inverse")).To(Equal(logic), "seed %d: a transient difference was classed as logic", seed)
			races += differentialMismatches("race") - before
		}
		Expect(races).To(BeNumerically(">", 0))
	})
})
