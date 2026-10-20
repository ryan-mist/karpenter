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
	"slices"
	"strings"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/scheduling/topology"
)

// keyUniverse is the universe of domains of one topology key: every domain the NodePools' instance types can launch
// into, with the taints of the NodePools that can launch into it. Topology groups are seeded with these domains, filtered
// by taints for groups that honor them. Dense keys have their domains interned (see topology.Dense); sparse keys,
// like hostname, keep the names.
type keyUniverse struct {
	key   string
	dense bool
	// domains are the universe's domains, with their taint sets
	domains []universeDomain
	// present has the DomainID of every domain, for dense keys
	present topology.Mask
	// taintSets are the distinct taint sets of the domains, indexed by universeDomain.taintSets
	taintSets []scheduling.Taints
	// all is the seed of groups that ignore taints, and seeds memoizes the seed of each toleration signature
	// (hashTolerations) for groups that honor them
	all   *universeSeed
	seeds map[uint64]*universeSeed
}

type universeDomain struct {
	name string
	id   topology.DomainID // dense keys only
	// taintSets index keyUniverse.taintSets. A pod can schedule to the domain if it tolerates any of them. A domain
	// that a NodePool without taints can launch into has only the empty set, which every pod tolerates.
	taintSets []int
}

// universeSeed is the set of domains that a toleration signature can schedule to
type universeSeed struct {
	mask  topology.Mask // dense keys
	names []string      // sparse keys
}

// newKeyUniverse builds the universe of the topology key from the NodePools. A domain's taint sets follow
// TopologyDomainGroup.Insert: a NodePool without taints replaces them with the empty set, and once a domain has the empty
// set no others are added.
func newKeyUniverse(topologyKey string, sources []domainSource) *keyUniverse {
	u := &keyUniverse{key: topologyKey, dense: topology.Dense(topologyKey), seeds: map[uint64]*universeSeed{}}
	index := map[string]int{}
	taintSetIndex := map[string]int{}
	for _, source := range sources {
		taintSet := u.internTaintSet(taintSetIndex, source.taints)
		empty := len(source.taints) == 0
		source.forEachDomain(topologyKey, func(name string) {
			i, ok := index[name]
			if !ok || empty {
				if !ok {
					i = len(u.domains)
					index[name] = i
					u.domains = append(u.domains, universeDomain{name: name})
				}
				u.domains[i].taintSets = []int{taintSet}
				return
			}
			if len(u.taintSets[u.domains[i].taintSets[0]]) == 0 {
				return
			}
			u.domains[i].taintSets = append(u.domains[i].taintSets, taintSet)
		})
	}
	if u.dense {
		ids := topology.Dict(topologyKey).InternAll(lo.Map(u.domains, func(d universeDomain, _ int) string { return d.name })...)
		for i := range u.domains {
			u.domains[i].id = ids[i]
			u.present.Set(ids[i])
		}
	}
	return u
}

func (u *keyUniverse) internTaintSet(index map[string]int, taints []corev1.Taint) int {
	keys := make([]string, 0, len(taints))
	for _, t := range taints {
		// Taints.Tolerates only reads the key, value and effect
		keys = append(keys, t.Key+"\x00"+t.Value+"\x00"+string(t.Effect))
	}
	slices.Sort(keys)
	key := strings.Join(slices.Compact(keys), "\x01")
	if i, ok := index[key]; ok {
		return i
	}
	index[key] = len(u.taintSets)
	u.taintSets = append(u.taintSets, taints)
	return index[key]
}

// seed returns the universe's domains that a group seeds with the policy and tolerations: every domain if taints are
// ignored, or the domains where the tolerations tolerate a taint set. This is the set of domains that
// TopologyDomainGroup.ForEachDomain visits for a pod with the tolerations.
func (u *keyUniverse) seed(policy corev1.NodeInclusionPolicy, tolerations []corev1.Toleration) *universeSeed {
	if policy == corev1.NodeInclusionPolicyIgnore {
		if u.all == nil {
			u.all = u.newSeed(nil)
		}
		return u.all
	}
	signature := hashTolerations(tolerations)
	if s, ok := u.seeds[signature]; ok {
		return s
	}
	tolerated := make([]bool, len(u.taintSets))
	for i, taints := range u.taintSets {
		tolerated[i] = taints.Tolerates(tolerations) == nil
	}
	s := u.newSeed(func(d universeDomain) bool {
		return slices.ContainsFunc(d.taintSets, func(i int) bool { return tolerated[i] })
	})
	u.seeds[signature] = s
	return s
}

// newSeed returns the domains that pass the filter, or every domain for a nil filter
func (u *keyUniverse) newSeed(filter func(universeDomain) bool) *universeSeed {
	s := &universeSeed{}
	for _, d := range u.domains {
		if filter != nil && !filter(d) {
			continue
		}
		if u.dense {
			s.mask.Set(d.id)
		} else {
			s.names = append(s.names, d.name)
		}
	}
	return s
}
