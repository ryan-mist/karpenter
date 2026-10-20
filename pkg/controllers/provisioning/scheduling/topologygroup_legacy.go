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
	"maps"
	"math"
	"slices"

	"github.com/awslabs/operatorpkg/option"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// legacyTopologyGroup is used to track pod counts that match a selector by the topology domain (e.g. SELECT COUNT(*) FROM pods GROUP BY(topology_ke
type legacyTopologyGroup struct {
	// Hashed Fields
	Key        string
	Type       TopologyType
	maxSkew    int32
	minDomains *int32
	namespaces sets.Set[string]
	selector   labels.Selector

	// NOTE: This is actually nillable since there's no API server validation to require it on affinity / TSC terms. A term without it is a no-op.
	rawSelector *metav1.LabelSelector
	nodeFilter  TopologyNodeFilter
	// seedTolerations are the tolerations used to filter the domains the group is seeded with. These are only set for
	// pod affinity groups, see NewTopologyGroup.
	seedTolerations []corev1.Toleration

	// Index
	owners       map[types.UID]struct{} // Pods that have this topology as a scheduling rule
	domains      map[string]int32       // TODO(ellistarn) explore replacing with a minheap
	emptyDomains sets.Set[string]       // domains for which we know that no pod exists

	// order, if set, sorts domains in place wherever the group picks between tied domains, which otherwise follows map
	// and set iteration order. Tests set it to make the group's choices deterministic.
	order func(domains []string)
}

// newLegacyTopologyGroupIdentity returns a legacyTopologyGroup with only the fields that identify it set, which is enough to call
// Hash(). The expensive parts (parsing the selector and seeding domains from the domain group) are deferred to
// populate, so callers can dedupe by Hash() and only populate groups they haven't seen before.
func newLegacyTopologyGroupIdentity(
	topologyType TopologyType,
	topologyKey string,
	pod *corev1.Pod,
	namespaces sets.Set[string],
	labelSelector *metav1.LabelSelector,
	maxSkew int32,
	minDomains *int32,
	taintPolicy *corev1.NodeInclusionPolicy,
	affinityPolicy *corev1.NodeInclusionPolicy,
) *legacyTopologyGroup {
	// the nil *TopologyNodeFilter always passes which is what we need for affinity/anti-affinity
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
	return &legacyTopologyGroup{
		Type:            topologyType,
		Key:             topologyKey,
		namespaces:      namespaces,
		rawSelector:     labelSelector,
		nodeFilter:      nodeFilter,
		seedTolerations: seedTolerations,
		maxSkew:         maxSkew,
		minDomains:      minDomains,
	}
}

// populate parses the selector and seeds the group's domains from the domain group, filtered by the pod's tolerations
// if the group honors taints.
func (t *legacyTopologyGroup) populate(pod *corev1.Pod, domainGroup TopologyDomainGroup) {
	selector, err := metav1.LabelSelectorAsSelector(t.rawSelector)
	if err != nil {
		selector = labels.Nothing()
	}

	// Topology spread groups seed the domains allowed by their NodeTaintsPolicy. Pod (anti-)affinity isn't subject to
	// taint filtering in kube-scheduler, so anti-affinity groups seed every domain: an empty domain the creating pod
	// doesn't tolerate is still a valid domain for other pods sharing the group, or for pods selected by an inverse
	// group. Pod affinity groups keep filtering by the creating pod's tolerations (see newLegacyTopologyGroupIdentity).
	seedTaintPolicy := t.nodeFilter.TaintPolicy
	switch t.Type {
	case TopologyTypePodAntiAffinity:
		seedTaintPolicy = corev1.NodeInclusionPolicyIgnore
	case TopologyTypePodAffinity:
		seedTaintPolicy = corev1.NodeInclusionPolicyHonor
	}

	domains := map[string]int32{}
	emptyDomains := sets.New[string]()
	domainGroup.ForEachDomain(pod, seedTaintPolicy, func(domain string) {
		domains[domain] = 0
		emptyDomains.Insert(domain)
	})

	t.selector = selector
	t.domains = domains
	t.emptyDomains = emptyDomains
	t.owners = map[types.UID]struct{}{}
}

func (t *legacyTopologyGroup) Get(pod *corev1.Pod, podDomains, nodeDomains *scheduling.Requirement) (*scheduling.Requirement, sets.Set[string]) {
	switch t.Type {
	case TopologyTypeSpread:
		return t.nextDomainTopologySpread(pod, podDomains, nodeDomains)
	case TopologyTypePodAffinity:
		req := t.nextDomainAffinity(pod, podDomains, nodeDomains)
		return req, sets.New[string](req.Values()...)
	case TopologyTypePodAntiAffinity:
		req := t.nextDomainAntiAffinity(podDomains, nodeDomains)
		return req, sets.New[string](req.Values()...)
	default:
		panic(fmt.Sprintf("Unrecognized topology group type: %s", t.Type))
	}
}

func (t *legacyTopologyGroup) Record(domains ...string) {
	for _, domain := range domains {
		t.domains[domain]++
		t.emptyDomains.Delete(domain)
	}
}

// Counts returns true if the pod would count for the topology, given that it schedule to a node with the provided
// requirements
func (t *legacyTopologyGroup) Counts(pod *corev1.Pod, taints []corev1.Taint, requirements scheduling.Requirements, compatibilityOptions ...option.Function[scheduling.CompatibilityOptions]) bool {
	return t.selects(pod) && t.nodeFilter.Matches(taints, requirements, compatibilityOptions...)
}

// Register ensures that the topology is aware of the given domain names.
func (t *legacyTopologyGroup) Register(domains ...string) {
	for _, domain := range domains {
		if _, ok := t.domains[domain]; !ok {
			t.domains[domain] = 0
			t.emptyDomains.Insert(domain)
		}
	}
}

func (t *legacyTopologyGroup) Unregister(domains ...string) {
	for _, domain := range domains {
		delete(t.domains, domain)
		t.emptyDomains.Delete(domain)
	}
}

func (t *legacyTopologyGroup) AddOwner(key types.UID) {
	t.owners[key] = struct{}{}
}

func (t *legacyTopologyGroup) RemoveOwner(key types.UID) {
	delete(t.owners, key)
}

func (t *legacyTopologyGroup) IsOwnedBy(key types.UID) bool {
	_, ok := t.owners[key]
	return ok
}

// Hash is used so we can track single topologies that affect multiple groups of pods.  If a deployment has 100x pods
// with self anti-affinity, we track that as a single topology with 100 owners instead of 100x topologies.
func (t *legacyTopologyGroup) Hash() uint64 {
	return lo.Must(hashstructure.Hash(struct {
		TopologyKey         string
		Type                TopologyType
		Namespaces          sets.Set[string]
		MaxSkew             int32
		MinDomains          *int32
		NodeFilterHash      uint64
		SeedTolerationsHash uint64
		SelectorHash        uint64
	}{
		TopologyKey:         t.Key,
		Type:                t.Type,
		Namespaces:          t.namespaces,
		MaxSkew:             t.maxSkew,
		MinDomains:          t.minDomains,
		NodeFilterHash:      t.nodeFilter.Hash(),
		SeedTolerationsHash: hashTolerations(t.seedTolerations),
		SelectorHash:        hashSelector(t.rawSelector),
	}, hashstructure.FormatV2, nil))
}

// nextDomainTopologySpread returns a scheduling.Requirement that includes a node domain that a pod should be scheduled to,
// along with the number of valid domains that satisfied the maxSkew constraint.
// If there are multiple eligible domains, we return any random domain that satisfies the `maxSkew` configuration.
// If there are no eligible domains, we return a `DoesNotExist` requirement, implying that we could not satisfy the topologySpread requirement.
// nolint:gocyclo
func (t *legacyTopologyGroup) nextDomainTopologySpread(pod *corev1.Pod, podDomains, nodeDomains *scheduling.Requirement) (*scheduling.Requirement, sets.Set[string]) {
	// min count is calculated across all domains
	min := t.domainMinCount(podDomains)
	selfSelecting := t.selects(pod)

	minDomain := ""
	minCount := int32(math.MaxInt32)
	validDomains := sets.New[string]()

	// We special-case kubernetes.io/hostname primarily for new NodeClaims since their domain won't be registered until we Add() them
	if t.Key == corev1.LabelHostname && nodeDomains.Operator() == corev1.NodeSelectorOpIn && nodeDomains.Len() == 1 {
		hostName := nodeDomains.Values()[0]
		count := t.domains[hostName] // t.domains[hostName] produces a 0 value for new NodeClaims
		if selfSelecting {
			count++
		}
		// Because Karpenter can always create a new domain for hostname, we assume the global miniumum is always zero
		// This means we can just check whether our current count is less than or equal to the skew to check if the domain is valid
		if count <= t.maxSkew {
			validDomains.Insert(hostName)
			return scheduling.NewRequirement(t.Key, corev1.NodeSelectorOpIn, hostName), validDomains
		}
		return scheduling.NewRequirement(t.Key, corev1.NodeSelectorOpDoesNotExist), validDomains
	}

	// If we are explicitly selecting on specific node domains ("In" requirement),
	// this is going to be more efficient to iterate through
	// This is particularly useful when considering the hostname topology key that can have a
	// lot of t.domains but only a single nodeDomain
	if nodeDomains.Operator() == corev1.NodeSelectorOpIn {
		domains := nodeDomains.Values()
		if t.order != nil {
			domains = t.sorted(domains)
		}
		for _, domain := range domains {
			if count, ok := t.domains[domain]; ok {
				if selfSelecting {
					count++
				}
				if count-min <= t.maxSkew {
					validDomains.Insert(domain)
					if count < minCount {
						minDomain = domain
						minCount = count
					}
				}
			}
		}
	} else {
		t.forEachDomain(func(domain string) {
			// but we can only choose from the node domains
			if nodeDomains.Has(domain) {
				// comment from kube-scheduler regarding the viable choices to schedule to based on skew is:
				// 'existing matching num' + 'if self-match (1 or 0)' - 'global min matching num' <= 'maxSkew'
				count := t.domains[domain]
				if selfSelecting {
					count++
				}
				if count-min <= t.maxSkew {
					validDomains.Insert(domain)
					if count < minCount {
						minDomain = domain
						minCount = count
					}
				}
			}
		})
	}
	if minDomain == "" {
		// avoids an error message about 'zone in [""]', preferring 'zone in []'
		return scheduling.NewRequirement(t.Key, corev1.NodeSelectorOpDoesNotExist), validDomains
	}
	return scheduling.NewRequirement(t.Key, corev1.NodeSelectorOpIn, minDomain), validDomains
}

func (t *legacyTopologyGroup) domainMinCount(domains *scheduling.Requirement) int32 {
	// hostname based topologies always have a min pod count of zero since we can create one
	if t.Key == corev1.LabelHostname {
		return 0
	}

	min := int32(math.MaxInt32)
	var numPodSupportedDomains int32
	// determine our current min count
	for domain, count := range t.domains {
		if domains.Has(domain) {
			numPodSupportedDomains++
			if count < min {
				min = count
			}
		}
	}
	if t.minDomains != nil && numPodSupportedDomains < *t.minDomains {
		min = 0
	}
	return min
}

// nolint:gocyclo
func (t *legacyTopologyGroup) nextDomainAffinity(pod *corev1.Pod, podDomains *scheduling.Requirement, nodeDomains *scheduling.Requirement) *scheduling.Requirement {
	options := scheduling.NewRequirement(t.Key, corev1.NodeSelectorOpDoesNotExist)

	// We special-case kubernetes.io/hostname primarily for new NodeClaims since their domain won't be registered until we Add() them
	if t.Key == corev1.LabelHostname && nodeDomains.Operator() == corev1.NodeSelectorOpIn && nodeDomains.Len() == 1 {
		hostName := nodeDomains.Values()[0]
		if !podDomains.Has(hostName) {
			return options
		}
		if t.domains[hostName] > 0 { // t.domains[hostName] produces a 0 value for new NodeClaims
			options.Insert(hostName)
			return options
		}
		if t.selects(pod) && (len(t.domains) == len(t.emptyDomains) || !t.anyCompatiblePodDomain(podDomains)) {
			options.Insert(hostName)
			return options
		}
		return options
	}

	// If we are explicitly selecting on specific node domains ("In" requirement),
	// this is going to be more efficient to iterate through
	// This is particularly useful when considering the hostname topology key that can have a
	// lot of t.domains but only a single nodeDomain
	if nodeDomains.Operator() == corev1.NodeSelectorOpIn {
		for _, domain := range nodeDomains.Values() {
			if count, ok := t.domains[domain]; podDomains.Has(domain) && ok && count > 0 {
				options.Insert(domain)
			}
		}
	} else {
		for domain := range t.domains {
			if podDomains.Has(domain) && t.domains[domain] > 0 && nodeDomains.Has(domain) {
				options.Insert(domain)
			}
		}
	}
	if options.Len() != 0 {
		return options
	}

	// If pod is self-selecting and no pod has been scheduled yet OR the pods that have scheduled are
	// incompatible with our podDomains, we can pick a domain at random to bootstrap scheduling.
	if t.selects(pod) && (len(t.domains) == len(t.emptyDomains) || !t.anyCompatiblePodDomain(podDomains)) {
		// First try to find a domain that is within the intersection of pod/node domains. In the case of an in-flight node
		// this causes us to pick the domain that the existing in-flight node is already in if possible instead of picking
		// a random viable domain.
		intersected := podDomains.Intersection(nodeDomains)
		if domain, ok := t.firstDomain(intersected.Has); ok {
			options.Insert(domain)
		}

		// and if there are no node domains, just return the first random domain that is viable
		if domain, ok := t.firstDomain(podDomains.Has); ok {
			options.Insert(domain)
		}
	}
	return options
}

// anyCompatiblePodDomain validates whether any t.domain is compatible with our podDomains
// This is only useful in affinity checking because it tells us whether we can schedule the pod
// to the current node since it is the first pod that exists in the legacyTopologyGroup OR all other domains
// in the legacyTopologyGroup are incompatible with the podDomains
func (t *legacyTopologyGroup) anyCompatiblePodDomain(podDomains *scheduling.Requirement) bool {
	for domain := range t.domains {
		if podDomains.Has(domain) && t.domains[domain] > 0 {
			return true
		}
	}
	return false
}

// nolint:gocyclo
func (t *legacyTopologyGroup) nextDomainAntiAffinity(podDomains, nodeDomains *scheduling.Requirement) *scheduling.Requirement {
	options := scheduling.NewRequirement(t.Key, corev1.NodeSelectorOpDoesNotExist)
	// pods with anti-affinity must schedule to a domain where there are currently none of those pods (an empty
	// domain). If there are none of those domains, then the pod can't schedule and we don't need to walk this
	// list of domains.  The use case where this optimization is really great is when we are launching nodes for
	// a deployment of pods with self anti-affinity.  The domains map here continues to grow, and we continue to
	// fully scan it each iteration.

	// We special-case kubernetes.io/hostname primarily for new NodeClaims since their domain won't be registered until we Add() them
	if t.Key == corev1.LabelHostname && nodeDomains.Operator() == corev1.NodeSelectorOpIn && nodeDomains.Len() == 1 {
		hostName := nodeDomains.Values()[0]
		if t.domains[hostName] == 0 { // t.domains[hostName] produces a 0 value for new NodeClaims
			options.Insert(hostName)
		}
		return options
	}

	// If we are explicitly selecting on specific node domains ("In" requirement) and the number of node domains
	// is less than our empty domains, this is going to be more efficient to iterate through
	// This is particularly useful when considering the hostname topology key that can have a
	// lot of t.domains but only a single nodeDomain
	if nodeDomains.Operator() == corev1.NodeSelectorOpIn && nodeDomains.Len() < len(t.emptyDomains) {
		for _, domain := range nodeDomains.Values() {
			if t.emptyDomains.Has(domain) && podDomains.Has(domain) {
				options.Insert(domain)
			}
		}
	} else {
		for domain := range t.emptyDomains {
			if nodeDomains.Has(domain) && podDomains.Has(domain) {
				options.Insert(domain)
			}
		}
	}
	return options
}

// forEachDomain calls f for each of the group's domains, in map order or in the order hook's order if one is set.
func (t *legacyTopologyGroup) forEachDomain(f func(domain string)) {
	if t.order == nil {
		for domain := range t.domains {
			f(domain)
		}
		return
	}
	for _, domain := range t.sorted(slices.Collect(maps.Keys(t.domains))) {
		f(domain)
	}
}

// firstDomain returns the first of the group's domains, in map order or in the order hook's order if one is set, for
// which f returns true.
func (t *legacyTopologyGroup) firstDomain(f func(domain string) bool) (string, bool) {
	if t.order == nil {
		for domain := range t.domains {
			if f(domain) {
				return domain, true
			}
		}
		return "", false
	}
	for _, domain := range t.sorted(slices.Collect(maps.Keys(t.domains))) {
		if f(domain) {
			return domain, true
		}
	}
	return "", false
}

// sorted returns a copy of the domains sorted with the order hook
func (t *legacyTopologyGroup) sorted(domains []string) []string {
	result := slices.Clone(domains)
	t.order(result)
	return result
}

// selects returns true if the given pod is selected by this topology
func (t *legacyTopologyGroup) selects(pod *corev1.Pod) bool {
	return t.namespaces.Has(pod.Namespace) && t.selector.Matches(labels.Set(pod.Labels))
}
