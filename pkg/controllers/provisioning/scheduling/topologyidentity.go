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
	"fmt"
	"hash/fnv"
	"slices"
	"strings"

	"github.com/mitchellh/hashstructure/v2"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/scheduling/topology"
)

// TopologyGroupIdentity is everything that decides which pods share a topology group and how the group behaves. Two
// groups with the same Hash are the same group, exactly as for the legacy implementation. Identities are immutable;
// the parsed selector and the CountKey are derived on first use, so that identities which turn out to be duplicates
// of a known group stay cheap.
type TopologyGroupIdentity struct {
	Type TopologyType
	Key  string
	// Namespaces are the namespaces whose pods the group selects, sorted
	Namespaces []string
	// RawSelector is the label selector as given, including any matchLabelKeys requirements. It's nil-able since the API
	// server doesn't require it on affinity terms or topology spread constraints.
	RawSelector *metav1.LabelSelector
	MaxSkew     int32
	MinDomains  *int32
	// NodeFilter decides which nodes count towards a topology spread constraint. It's the zero value, which matches
	// every node, for pod (anti-)affinity.
	NodeFilter TopologyNodeFilter
	// SeedTolerations filter the domains a pod affinity group is seeded with. They're only set for pod affinity.
	SeedTolerations []corev1.Toleration

	namespaces sets.Set[string]
	hash       uint64
	selector   labels.Selector
	countKey   *topology.CountKey
}

// newTopologyGroupIdentity returns the identity of a topology group. Its hash is the hash of the legacy group built from
// the same arguments by newLegacyTopologyGroupIdentity.
func newTopologyGroupIdentity(
	topologyType TopologyType,
	topologyKey string,
	pod *corev1.Pod,
	namespaces sets.Set[string],
	labelSelector *metav1.LabelSelector,
	maxSkew int32,
	minDomains *int32,
	taintPolicy *corev1.NodeInclusionPolicy,
	affinityPolicy *corev1.NodeInclusionPolicy,
) *TopologyGroupIdentity {
	// the zero TopologyNodeFilter always passes which is what we need for affinity/anti-affinity
	var nodeFilter TopologyNodeFilter
	if topologyType == TopologyTypeSpread {
		nodeTaintsPolicy := corev1.NodeInclusionPolicyIgnore
		if taintPolicy != nil {
			nodeTaintsPolicy = *taintPolicy
		}
		nodeAffinityPolicy := corev1.NodeInclusionPolicyHonor
		if affinityPolicy != nil {
			nodeAffinityPolicy = *affinityPolicy
		}
		nodeFilter = MakeTopologyNodeFilter(pod, nodeTaintsPolicy, nodeAffinityPolicy)
	}
	// Pod affinity groups seed their domains filtered by the creating pod's tolerations so that a self-selecting pod
	// bootstrapping its affinity doesn't pick a domain it can't schedule to. Since those tolerations then determine the
	// group's domains, they're part of the group's identity.
	var seedTolerations []corev1.Toleration
	if topologyType == TopologyTypePodAffinity {
		seedTolerations = pod.Spec.Tolerations
	}
	id := &TopologyGroupIdentity{
		Type:            topologyType,
		Key:             topologyKey,
		Namespaces:      sets.List(namespaces),
		RawSelector:     labelSelector,
		MaxSkew:         maxSkew,
		MinDomains:      minDomains,
		NodeFilter:      nodeFilter,
		SeedTolerations: seedTolerations,
		namespaces:      namespaces,
	}
	id.hash = lo.Must(hashstructure.Hash(struct {
		TopologyKey         string
		Type                TopologyType
		Namespaces          sets.Set[string]
		MaxSkew             int32
		MinDomains          *int32
		NodeFilterHash      uint64
		SeedTolerationsHash uint64
		SelectorHash        uint64
	}{
		TopologyKey:         id.Key,
		Type:                id.Type,
		Namespaces:          id.namespaces,
		MaxSkew:             id.MaxSkew,
		MinDomains:          id.MinDomains,
		NodeFilterHash:      id.NodeFilter.Hash(),
		SeedTolerationsHash: hashTolerations(id.SeedTolerations),
		SelectorHash:        hashSelector(id.RawSelector),
	}, hashstructure.FormatV2, nil))
	return id
}

// Hash identifies the group. It's the legacy TopologyGroup.Hash() of the same group.
func (id *TopologyGroupIdentity) Hash() uint64 {
	return id.hash
}

// Selector returns the selector the group selects pods with, which it records when they schedule. Unlike the selector
// that its existing pods are counted with (topology.ListSelector), a nil or invalid selector selects nothing.
func (id *TopologyGroupIdentity) Selector() labels.Selector {
	if id.selector == nil {
		selector, err := metav1.LabelSelectorAsSelector(id.RawSelector)
		if err != nil {
			selector = labels.Nothing()
		}
		id.selector = selector
	}
	return id.selector
}

// CountKey returns the key of the group's pod counts.
func (id *TopologyGroupIdentity) CountKey() *topology.CountKey {
	if id.countKey == nil {
		id.countKey = topology.NewCountKey(id.Key, id.Namespaces, id.RawSelector, newTopologyNodePredicate(id.NodeFilter))
	}
	return id.countKey
}

// SeedPolicy returns the taint policy the group's domains are seeded with: topology spread constraints seed the
// domains allowed by their NodeTaintsPolicy, pod affinity the domains the creating pod tolerates, and pod
// anti-affinity every domain, since kube-scheduler doesn't filter pod (anti-)affinity by taints. An empty domain the
// creating pod doesn't tolerate is still a valid domain for other pods sharing an anti-affinity group, or for pods
// selected by an inverse group.
func (id *TopologyGroupIdentity) SeedPolicy() corev1.NodeInclusionPolicy {
	switch id.Type {
	case TopologyTypePodAntiAffinity:
		return corev1.NodeInclusionPolicyIgnore
	case TopologyTypePodAffinity:
		return corev1.NodeInclusionPolicyHonor
	}
	return id.NodeFilter.TaintPolicy
}

// selects returns true if the group selects the pod
func (id *TopologyGroupIdentity) selects(pod *corev1.Pod) bool {
	return id.namespaces.Has(pod.Namespace) && id.Selector().Matches(labels.Set(pod.Labels))
}

// topologyNodePredicate is the topology.NodePredicate of a TopologyNodeFilter. Parts of the filter that can't change
// its result are dropped, so filters that match the same nodes compare equal: the tolerations if taints are ignored, and
// the requirements if node affinity is ignored.
type topologyNodePredicate struct {
	filter    TopologyNodeFilter
	keys      []string
	canonical string
	hash      uint64
}

// newTopologyNodePredicate returns the predicate of the filter, or nil if the filter matches every node.
func newTopologyNodePredicate(filter TopologyNodeFilter) topology.NodePredicate {
	honorAffinity := filter.AffinityPolicy == corev1.NodeInclusionPolicyHonor && len(filter.Requirements) != 0
	honorTaints := filter.TaintPolicy == corev1.NodeInclusionPolicyHonor
	if !honorAffinity && !honorTaints {
		return nil
	}
	var parts []string
	keys := sets.New[string]()
	if honorAffinity {
		terms := sets.New[string]()
		for _, requirements := range filter.Requirements {
			keys.Insert(requirements.Keys().UnsortedList()...)
			var term []string
			for _, r := range requirements.NodeSelectorRequirements() {
				term = append(term, fmt.Sprintf("%s/%s/%v/%v", r.Key, r.Operator, r.Values, lo.FromPtr(r.MinValues)))
			}
			slices.Sort(term)
			terms.Insert(strings.Join(term, ","))
		}
		parts = append(parts, "affinity", strings.Join(sets.List(terms), "|"))
	}
	if honorTaints {
		tolerations := sets.New[string]()
		for _, t := range filter.Tolerations {
			tolerations.Insert(fmt.Sprintf("%s/%s/%s/%s/%v", t.Key, t.Operator, t.Value, t.Effect, lo.FromPtr(t.TolerationSeconds)))
		}
		parts = append(parts, "taints", strings.Join(sets.List(tolerations), "|"))
	}
	p := &topologyNodePredicate{filter: filter, keys: sets.List(keys), canonical: strings.Join(parts, "\x00")}
	h := fnv.New64a()
	_, _ = h.Write([]byte(p.canonical))
	p.hash = h.Sum64()
	return p
}

func (p *topologyNodePredicate) Hash() uint64 {
	return p.hash
}

func (p *topologyNodePredicate) Equal(o topology.NodePredicate) bool {
	other, ok := o.(*topologyNodePredicate)
	return ok && p.canonical == other.canonical
}

func (p *topologyNodePredicate) Keys() []string {
	return slices.Clone(p.keys)
}

func (p *topologyNodePredicate) ReadsTaints() bool {
	return p.filter.TaintPolicy == corev1.NodeInclusionPolicyHonor
}

func (p *topologyNodePredicate) Matches(labels map[string]string, taints []corev1.Taint) bool {
	return p.filter.Matches(taints, scheduling.NewLabelRequirements(labels))
}
