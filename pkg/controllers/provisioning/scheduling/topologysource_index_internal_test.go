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
