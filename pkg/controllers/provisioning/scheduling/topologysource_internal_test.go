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
	"math"
	"math/rand"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling/topology"
	"sigs.k8s.io/karpenter/pkg/test"
)

// The list count source, the key universe and the presence index together must give every topology group exactly the
// domains and counts that the legacy NewTopology gives it.
var _ = Describe("listCountSource", func() {
	It("should reproduce the legacy topology groups", func() {
		ctx := karpopts.ToContext(context.Background(), test.Options())
		for seed := range int64(400) {
			g := &identityGen{topologyIndexGen{r: rand.New(rand.NewSource(seed))}}
			f := g.fixture(ctx)
			legacy, err := newLegacyTopology(ctx, f.client, f.cluster, f.stateNodes, f.nodePools, f.instanceTypes, f.pods, &options{})
			Expect(err).ToNot(HaveOccurred(), "seed %d", seed)

			excluded := topology.NewExcludedPods(f.pods)
			src := newListCountSource(f.client, f.cluster, excluded)
			sources := newDomainSources(f.nodePools, f.instanceTypes)
			universes := map[string]*keyUniverse{}
			universe := func(key string) *keyUniverse {
				if _, ok := universes[key]; !ok {
					universes[key] = newKeyUniverse(key, sources)
				}
				return universes[key]
			}
			presences := newPresenceIndex(f.stateNodes)

			for hash, tg := range legacy.topologyGroups {
				u := universe(tg.Key)
				key := topology.NewCountKey(tg.Key, sets.List(tg.namespaces), tg.rawSelector, newTopologyNodePredicate(tg.nodeFilter))
				base, err := src.Counts(ctx, key)
				Expect(err).ToNot(HaveOccurred())
				counts := map[string]int32{}
				base.ForEach(func(domain string, count int32) { counts[domain] += count })
				Expect(src.ExcludedContributions(ctx, key, excluded, func(domain string) { counts[domain]-- })).To(Succeed())
				policy, tolerations := tg.nodeFilter.TaintPolicy, tg.nodeFilter.Tolerations
				switch tg.Type {
				case TopologyTypePodAntiAffinity:
					policy = corev1.NodeInclusionPolicyIgnore
				case TopologyTypePodAffinity:
					policy, tolerations = corev1.NodeInclusionPolicyHonor, tg.seedTolerations
				}
				present := universeSeedNames(u, u.seed(policy, tolerations))
				present = present.Union(presenceNames(u, presences.get(tg.Key, u.dense, key.NodeFilter)))
				Expect(expectedDomains(present, counts)).To(Equal(tg.domains), "seed %d, group %d %s %s", seed, hash, tg.Type, tg.Key)
			}

			// inverse groups: the existing pods' terms merged by group hash, then the pods to schedule as owners
			type inverse struct {
				owners sets.Set[types.UID]
				counts map[string]int32
				key    string
			}
			inverses := map[uint64]*inverse{}
			Expect(src.InverseAntiAffinityTerms(ctx, excluded, func(term *topology.InverseTerm) {
				namespaces, err := legacy.buildNamespaceList(ctx, term.Namespace, term.Term.Namespaces, term.Term.NamespaceSelector)
				Expect(err).ToNot(HaveOccurred())
				hash := newTopologyGroupIdentity(TopologyTypePodAntiAffinity, term.Term.TopologyKey, &corev1.Pod{}, namespaces, term.Term.LabelSelector, math.MaxInt32, nil, nil, nil).Hash()
				if inverses[hash] == nil {
					inverses[hash] = &inverse{owners: sets.New[types.UID](), counts: map[string]int32{}, key: term.Term.TopologyKey}
				}
				inverses[hash].owners.Insert(term.Owners...)
				term.Counts.ForEach(func(domain string, count int32) { inverses[hash].counts[domain] += count })
			})).To(Succeed())
			for _, p := range f.pods {
				if p.Spec.Affinity == nil || p.Spec.Affinity.PodAntiAffinity == nil {
					continue
				}
				for _, term := range p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
					namespaces, err := legacy.buildNamespaceList(ctx, p.Namespace, term.Namespaces, term.NamespaceSelector)
					Expect(err).ToNot(HaveOccurred())
					hash := newTopologyGroupIdentity(TopologyTypePodAntiAffinity, term.TopologyKey, p, namespaces, term.LabelSelector, math.MaxInt32, nil, nil, nil).Hash()
					if inverses[hash] == nil {
						inverses[hash] = &inverse{owners: sets.New[types.UID](), counts: map[string]int32{}, key: term.TopologyKey}
					}
					inverses[hash].owners.Insert(p.UID)
				}
			}
			Expect(sets.KeySet(inverses)).To(Equal(sets.KeySet(legacy.inverseTopologyGroups)), "seed %d", seed)
			for hash, tg := range legacy.inverseTopologyGroups {
				u := universe(tg.Key)
				present := universeSeedNames(u, u.seed(corev1.NodeInclusionPolicyIgnore, nil))
				Expect(expectedDomains(present, inverses[hash].counts)).To(Equal(tg.domains), "seed %d, inverse group %d %s", seed, hash, tg.Key)
				Expect(inverses[hash].owners).To(Equal(sets.KeySet(tg.owners)), "seed %d", seed)
			}
		}
	})
})

// expectedDomains returns the legacy domains map of a group: every present domain, plus every domain with a non-zero
// count, with its count.
func expectedDomains(present sets.Set[string], counts map[string]int32) map[string]int32 {
	domains := map[string]int32{}
	for domain := range present {
		domains[domain] = 0
	}
	for domain, count := range counts {
		Expect(count).To(BeNumerically(">=", 0))
		if count > 0 {
			domains[domain] = count
		}
	}
	return domains
}

func presenceNames(u *keyUniverse, p *presence) sets.Set[string] {
	return universeSeedNames(u, &universeSeed{mask: p.mask, names: p.names})
}
