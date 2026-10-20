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
	"math"
	"slices"

	"github.com/awslabs/operatorpkg/option"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/scheduling/topology"
)

// TopologyGroup tracks, for one simulation, the pods that a topology spread constraint or pod (anti-)affinity term
// selects by topology domain (e.g. SELECT COUNT(*) FROM pods GROUP BY(topology_key)). It starts from the counts of the
// existing pods that its CountSource provides, minus the pods the simulation excludes, and adds the pods the
// simulation schedules. A domain can be present with a count of 0: the universe's domains, the domains of the
// simulation's state nodes and registered domains are present even when no pod runs there.
//
// Keys that are dense (topology.Dense) keep their counts in an array by DomainID with masks of the present and
// non-zero domains. Other keys, like hostname, keep the source's counts read-only and only track changes.
type TopologyGroup struct {
	*TopologyGroupIdentity
	owners map[types.UID]struct{}
	// rot rotates the order in which the group breaks ties between domains (see order)
	rot    uint64
	dense  *denseDomains
	sparse *sparseDomains
}

type denseDomains struct {
	dict    *topology.DomainDict
	counts  []int32 // by DomainID
	present topology.Mask
	nonzero topology.Mask
}

type sparseDomains struct {
	// base are the source's counts, which may be shared with other simulations, and delta the changes to them
	base  *topology.BaseCounts
	delta map[string]int32
	// nonzero is the number of domains with a non-zero count
	nonzero int
	// seed and presence are the group's domains from the universe and from the state nodes, shared between groups
	seed     sets.Set[string]
	presence sets.Set[string]
	// registered are the domains registered with the group itself
	registered sets.Set[string]
	// hosts are the hostnames registered with the Topology, of which the group has those registered from hostFrom on
	hosts    *hostLog
	hostFrom int
	// removedAt are the unregistered domains, with the length of the host log when they were unregistered
	removedAt map[string]int
}

// hostLog records the hostnames registered with a Topology, so that registering a hostname costs O(1) instead of a
// call for every hostname group. A group has the hostnames registered after it was created.
type hostLog struct {
	n    int
	last map[string]int // hostname -> position of its latest registration
}

func (l *hostLog) register(hostname string) {
	l.last[hostname] = l.n
	l.n++
}

func newTopologyGroup(id *TopologyGroupIdentity, rot uint64) *TopologyGroup {
	return &TopologyGroup{TopologyGroupIdentity: id, owners: map[types.UID]struct{}{}, rot: rot}
}

// initDense makes the group dense, with the base counts. The caller then subtracts the excluded pods and adds the
// present domains (setPresent).
func (t *TopologyGroup) initDense(base *topology.BaseCounts) {
	d := &denseDomains{dict: topology.Dict(t.Key)}
	t.dense = d
	if base == nil {
		return
	}
	if base.Dense != nil {
		d.counts = slices.Clone(base.Dense)
	} else {
		base.ForEach(func(domain string, count int32) {
			id := d.dict.Intern(domain)
			d.grow(id)
			d.counts[id] = count
		})
	}
	for id, count := range d.counts {
		if count > 0 {
			d.nonzero.Set(topology.DomainID(id))
		}
	}
}

// subtract removes an excluded pod's count from the domain, without making the domain present. Since it's only used
// before setPresent, the domain is present if it still has a count, as the legacy countDomains, which doesn't count
// excluded pods, makes it.
func (t *TopologyGroup) subtract(domain string) {
	if d := t.dense; d != nil {
		id := d.dict.Intern(domain)
		d.grow(id)
		d.counts[id]--
		if d.counts[id] <= 0 {
			d.nonzero.Clear(id)
		}
		return
	}
	t.sparse.addCount(domain, -1)
}

// setPresent makes the dense group's present domains the domains with a count and the given domains
func (t *TopologyGroup) setPresent(present ...*topology.Mask) {
	d := t.dense
	d.present = d.nonzero.Clone()
	for _, p := range present {
		d.present.Or(p)
	}
	// every present domain has a count
	if n := d.dict.View().Len(); n > 0 {
		d.grow(topology.DomainID(n - 1))
	}
}

// initSparse makes the group sparse, with the base counts and the present domains
func (t *TopologyGroup) initSparse(base *topology.BaseCounts, seed, presence sets.Set[string], hosts *hostLog) {
	t.sparse = &sparseDomains{base: base, seed: seed, presence: presence, hosts: hosts, hostFrom: hosts.n}
	if base != nil {
		base.ForEach(func(string, int32) { t.sparse.nonzero++ })
	}
}

// count returns the count of the domain
func (t *TopologyGroup) count(domain string) int32 {
	if d := t.dense; d != nil {
		if id, ok := d.dict.View().Lookup(domain); ok && int(id) < len(d.counts) {
			return d.counts[id]
		}
		return 0
	}
	return t.sparse.count(domain)
}

func (s *sparseDomains) count(domain string) int32 {
	var count int32
	if s.base != nil {
		count = s.base.Get(domain)
	}
	return count + s.delta[domain]
}

// add adds n to the count of the domain, making it present
func (t *TopologyGroup) add(domain string, n int32) {
	if d := t.dense; d != nil {
		id := d.dict.Intern(domain)
		d.grow(id)
		d.counts[id] += n
		d.present.Set(id)
		if d.counts[id] > 0 {
			d.nonzero.Set(id)
		} else {
			d.nonzero.Clear(id)
		}
		return
	}
	t.sparse.addCount(domain, n)
}

// addCount adds n to the count of the domain. A domain with a count is present.
func (s *sparseDomains) addCount(domain string, n int32) {
	old := s.count(domain)
	if s.delta == nil {
		s.delta = map[string]int32{}
	}
	s.delta[domain] += n
	switch {
	case old <= 0 && old+n > 0:
		s.nonzero++
	case old > 0 && old+n <= 0:
		s.nonzero--
	}
}

func (d *denseDomains) grow(id topology.DomainID) {
	if int(id) >= len(d.counts) {
		d.counts = append(d.counts, make([]int32, int(id)+1-len(d.counts))...)
	}
}

// present reports whether the sparse group has the domain
func (s *sparseDomains) present(domain string) bool {
	if s.count(domain) > 0 || s.registered.Has(domain) {
		return true
	}
	removedAt, removed := s.removedAt[domain]
	if !removed && (s.seed.Has(domain) || s.presence.Has(domain)) {
		return true
	}
	last, ok := s.hosts.last[domain]
	return ok && last >= s.hostFrom && (!removed || last >= removedAt)
}

// domains returns the sparse group's domains, sorted
func (s *sparseDomains) domains() []string {
	candidates := sets.New[string]()
	candidates.Insert(s.seed.UnsortedList()...)
	candidates.Insert(s.presence.UnsortedList()...)
	candidates.Insert(s.registered.UnsortedList()...)
	for host, last := range s.hosts.last {
		if last >= s.hostFrom {
			candidates.Insert(host)
		}
	}
	if s.base != nil {
		s.base.ForEach(func(domain string, _ int32) { candidates.Insert(domain) })
	}
	for domain := range s.delta {
		candidates.Insert(domain)
	}
	return slices.Sorted(func(yield func(string) bool) {
		for domain := range candidates {
			if s.present(domain) && !yield(domain) {
				return
			}
		}
	})
}

// forEachNonZero calls f for each of the sparse group's domains with a non-zero count, until f returns false
func (s *sparseDomains) forEachNonZero(f func(domain string) bool) {
	if s.base != nil {
		done := false
		s.base.ForEach(func(domain string, count int32) {
			if !done && count+s.delta[domain] > 0 && !f(domain) {
				done = true
			}
		})
		if done {
			return
		}
	}
	for domain, delta := range s.delta {
		if s.base != nil && s.base.Get(domain) != 0 {
			continue
		}
		if delta > 0 && !f(domain) {
			return
		}
	}
}

// Record records pods scheduling to the domains
func (t *TopologyGroup) Record(domains ...string) {
	for _, domain := range domains {
		t.add(domain, 1)
	}
}

// Register ensures that the topology is aware of the given domain names.
func (t *TopologyGroup) Register(domains ...string) {
	for _, domain := range domains {
		if d := t.dense; d != nil {
			id := d.dict.Intern(domain)
			d.grow(id)
			d.present.Set(id)
			continue
		}
		if t.sparse.registered == nil {
			t.sparse.registered = sets.New[string]()
		}
		t.sparse.registered.Insert(domain)
	}
}

// Unregister removes the domains, with their counts
func (t *TopologyGroup) Unregister(domains ...string) {
	for _, domain := range domains {
		if d := t.dense; d != nil {
			if id, ok := d.dict.View().Lookup(domain); ok {
				d.grow(id)
				d.counts[id] = 0
				d.present.Clear(id)
				d.nonzero.Clear(id)
			}
			continue
		}
		s := t.sparse
		if count := s.count(domain); count != 0 {
			s.addCount(domain, -count)
		}
		s.registered.Delete(domain)
		if s.removedAt == nil {
			s.removedAt = map[string]int{}
		}
		s.removedAt[domain] = s.hosts.n
	}
}

// Counts returns true if the pod would count for the topology, given that it schedule to a node with the provided
// requirements
func (t *TopologyGroup) Counts(pod *corev1.Pod, taints []corev1.Taint, requirements scheduling.Requirements, compatibilityOptions ...option.Function[scheduling.CompatibilityOptions]) bool {
	return t.selects(pod) && t.NodeFilter.Matches(taints, requirements, compatibilityOptions...)
}

func (t *TopologyGroup) indexSelector() labels.Selector {
	return t.Selector()
}

func (t *TopologyGroup) AddOwner(key types.UID) {
	t.owners[key] = struct{}{}
}

func (t *TopologyGroup) RemoveOwner(key types.UID) {
	delete(t.owners, key)
}

func (t *TopologyGroup) IsOwnedBy(key types.UID) bool {
	_, ok := t.owners[key]
	return ok
}

// domainCounts returns the group's domains with their counts
func (t *TopologyGroup) domainCounts() map[string]int32 {
	result := map[string]int32{}
	if d := t.dense; d != nil {
		view := d.dict.View()
		d.present.ForEach(func(id topology.DomainID) bool {
			result[view.Name(id)] = d.counts[id]
			return true
		})
		return result
	}
	for _, domain := range t.sparse.domains() {
		result[domain] = t.sparse.count(domain)
	}
	return result
}

// The group breaks ties between domains in a rotated order: the present domains, by DomainID for dense keys and by
// name for sparse keys, starting at the one at position rot mod the number of present domains. The rotation is
// random per simulation, so ties are broken randomly, as the legacy topology's map iteration order does.

// start returns the DomainID that the dense group's order starts at
func (t *TopologyGroup) start() topology.DomainID {
	d := t.dense
	n := d.present.Count()
	if n == 0 {
		return 0
	}
	k := int(t.rot % uint64(n))
	var start topology.DomainID
	d.present.ForEach(func(id topology.DomainID) bool {
		if k == 0 {
			start = id
			return false
		}
		k--
		return true
	})
	return start
}

// orderedDomains returns the sparse group's domains in its order
func (t *TopologyGroup) orderedDomains() []string {
	domains := t.sparse.domains()
	if len(domains) == 0 {
		return domains
	}
	k := int(t.rot % uint64(len(domains)))
	return append(domains[k:], domains[:k]...)
}

// sortDomains sorts domains into the group's order. Domains that aren't present go last, by name.
func (t *TopologyGroup) sortDomains(domains []string) {
	rank := map[string]int{}
	if d := t.dense; d != nil {
		view := d.dict.View()
		start := t.start()
		n := d.present.Count()
		for _, domain := range domains {
			if id, ok := view.Lookup(domain); ok && d.present.Has(id) {
				// position among the present domains, rotated to start
				position := d.present.CountBelow(id) - d.present.CountBelow(start)
				if position < 0 {
					position += n
				}
				rank[domain] = position
			}
		}
	} else {
		for i, domain := range t.orderedDomains() {
			rank[domain] = i
		}
	}
	slices.SortStableFunc(domains, func(a, b string) int {
		ra, oka := rank[a]
		rb, okb := rank[b]
		switch {
		case oka && okb:
			return ra - rb
		case oka:
			return -1
		case okb:
			return 1
		}
		return compareStrings(a, b)
	})
}

func compareStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// mask returns the dense group's present domains that the requirement allows
func (t *TopologyGroup) mask(r *scheduling.Requirement) topology.Mask {
	d := t.dense
	view := d.dict.View()
	var m topology.Mask
	if r.Operator() == corev1.NodeSelectorOpIn {
		for value := range r.ValuesSeq() {
			if id, ok := view.Lookup(value); ok && d.present.Has(id) {
				m.Set(id)
			}
		}
		return m
	}
	d.present.ForEach(func(id topology.DomainID) bool {
		if r.Has(view.Name(id)) {
			m.Set(id)
		}
		return true
	})
	return m
}

func (t *TopologyGroup) Get(pod *corev1.Pod, podDomains, nodeDomains *scheduling.Requirement) (*scheduling.Requirement, sets.Set[string]) {
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

// isHostnameFastPath reports whether a lookup for a single new or existing node can skip the group's other domains
func (t *TopologyGroup) isHostnameFastPath(nodeDomains *scheduling.Requirement) bool {
	return t.Key == corev1.LabelHostname && nodeDomains.Operator() == corev1.NodeSelectorOpIn && nodeDomains.Len() == 1
}

// nextDomainTopologySpread returns a scheduling.Requirement that includes a node domain that a pod should be scheduled to,
// along with the domains that satisfied the maxSkew constraint. If there are multiple eligible domains, it returns the
// first with the lowest count in the group's order. If there are no eligible domains, it returns a `DoesNotExist`
// requirement, implying that we could not satisfy the topologySpread requirement.
func (t *TopologyGroup) nextDomainTopologySpread(pod *corev1.Pod, podDomains, nodeDomains *scheduling.Requirement) (*scheduling.Requirement, sets.Set[string]) {
	selfSelecting := t.selects(pod)
	validDomains := sets.New[string]()

	// We special-case kubernetes.io/hostname primarily for new NodeClaims since their domain won't be registered until we Add() them
	if t.isHostnameFastPath(nodeDomains) {
		hostName := nodeDomains.Values()[0]
		count := t.count(hostName) // produces a 0 value for new NodeClaims
		if selfSelecting {
			count++
		}
		// Because Karpenter can always create a new domain for hostname, we assume the global miniumum is always zero
		// This means we can just check whether our current count is less than or equal to the skew to check if the domain is valid
		if count <= t.MaxSkew {
			validDomains.Insert(hostName)
			return scheduling.NewRequirement(t.Key, corev1.NodeSelectorOpIn, hostName), validDomains
		}
		return scheduling.NewRequirement(t.Key, corev1.NodeSelectorOpDoesNotExist), validDomains
	}

	// min count is calculated across all domains
	min := t.domainMinCount(podDomains)
	minDomain := ""
	minCount := int32(math.MaxInt32)
	// comment from kube-scheduler regarding the viable choices to schedule to based on skew is:
	// 'existing matching num' + 'if self-match (1 or 0)' - 'global min matching num' <= 'maxSkew'
	consider := func(domain string, count int32) {
		if selfSelecting {
			count++
		}
		if count-min <= t.MaxSkew {
			validDomains.Insert(domain)
			if count < minCount {
				minDomain = domain
				minCount = count
			}
		}
	}
	if d := t.dense; d != nil {
		view := d.dict.View()
		// we can only choose from the node domains
		candidates := t.mask(nodeDomains)
		candidates.ForEachFrom(t.start(), func(id topology.DomainID) bool {
			consider(view.Name(id), d.counts[id])
			return true
		})
	} else {
		for _, domain := range t.orderedDomains() {
			if nodeDomains.Has(domain) {
				consider(domain, t.sparse.count(domain))
			}
		}
	}
	if minDomain == "" {
		// avoids an error message about 'zone in [""]', preferring 'zone in []'
		return scheduling.NewRequirement(t.Key, corev1.NodeSelectorOpDoesNotExist), validDomains
	}
	return scheduling.NewRequirement(t.Key, corev1.NodeSelectorOpIn, minDomain), validDomains
}

func (t *TopologyGroup) domainMinCount(domains *scheduling.Requirement) int32 {
	// hostname based topologies always have a min pod count of zero since we can create one
	if t.Key == corev1.LabelHostname {
		return 0
	}

	min := int32(math.MaxInt32)
	var numPodSupportedDomains int32
	// determine our current min count
	if d := t.dense; d != nil {
		supported := t.mask(domains)
		numPodSupportedDomains = int32(supported.Count())
		supported.ForEach(func(id topology.DomainID) bool {
			min = minInt32(min, d.counts[id])
			return true
		})
	} else {
		for _, domain := range t.sparse.domains() {
			if domains.Has(domain) {
				numPodSupportedDomains++
				min = minInt32(min, t.sparse.count(domain))
			}
		}
	}
	if t.MinDomains != nil && numPodSupportedDomains < *t.MinDomains {
		min = 0
	}
	return min
}

func minInt32(a, b int32) int32 {
	if b < a {
		return b
	}
	return a
}

// nolint:gocyclo
func (t *TopologyGroup) nextDomainAffinity(pod *corev1.Pod, podDomains *scheduling.Requirement, nodeDomains *scheduling.Requirement) *scheduling.Requirement {
	options := scheduling.NewRequirement(t.Key, corev1.NodeSelectorOpDoesNotExist)

	// We special-case kubernetes.io/hostname primarily for new NodeClaims since their domain won't be registered until we Add() them
	if t.isHostnameFastPath(nodeDomains) {
		hostName := nodeDomains.Values()[0]
		if !podDomains.Has(hostName) {
			return options
		}
		if t.count(hostName) > 0 { // produces a 0 value for new NodeClaims
			options.Insert(hostName)
			return options
		}
		if t.selects(pod) && !t.anyCompatiblePodDomain(podDomains) {
			options.Insert(hostName)
			return options
		}
		return options
	}

	// the domains with a matching pod that both the pod and node allow
	if d := t.dense; d != nil {
		view := d.dict.View()
		podMask, nodeMask := t.mask(podDomains), t.mask(nodeDomains)
		matching := podMask.And(&nodeMask)
		matching = matching.And(&d.nonzero)
		matching.ForEach(func(id topology.DomainID) bool {
			options.Insert(view.Name(id))
			return true
		})
	} else {
		for _, domain := range t.sparse.domains() {
			if podDomains.Has(domain) && t.sparse.count(domain) > 0 && nodeDomains.Has(domain) {
				options.Insert(domain)
			}
		}
	}
	if options.Len() != 0 {
		return options
	}

	// If pod is self-selecting and no pod has been scheduled yet OR the pods that have scheduled are
	// incompatible with our podDomains, we can pick a domain to bootstrap scheduling.
	if t.selects(pod) && !t.anyCompatiblePodDomain(podDomains) {
		// First try to find a domain that is within the intersection of pod/node domains. In the case of an in-flight node
		// this causes us to pick the domain that the existing in-flight node is already in if possible instead of picking
		// any viable domain.
		intersected := podDomains.Intersection(nodeDomains)
		if domain, ok := t.firstDomain(intersected.Has); ok {
			options.Insert(domain)
		}
		// and if there are no node domains, just return the first domain that is viable
		if domain, ok := t.firstDomain(podDomains.Has); ok {
			options.Insert(domain)
		}
	}
	return options
}

// firstDomain returns the first of the group's domains in its order for which f returns true
func (t *TopologyGroup) firstDomain(f func(domain string) bool) (string, bool) {
	if d := t.dense; d != nil {
		view := d.dict.View()
		var result string
		var found bool
		d.present.ForEachFrom(t.start(), func(id topology.DomainID) bool {
			if f(view.Name(id)) {
				result, found = view.Name(id), true
				return false
			}
			return true
		})
		return result, found
	}
	for _, domain := range t.orderedDomains() {
		if f(domain) {
			return domain, true
		}
	}
	return "", false
}

// anyCompatiblePodDomain validates whether any domain with a matching pod is compatible with our podDomains
// This is only useful in affinity checking because it tells us whether we can schedule the pod
// to the current node since it is the first pod that exists in the TopologyGroup OR all other domains
// in the TopologyGroup are incompatible with the podDomains
func (t *TopologyGroup) anyCompatiblePodDomain(podDomains *scheduling.Requirement) bool {
	if d := t.dense; d != nil {
		podMask := t.mask(podDomains)
		return podMask.Intersects(&d.nonzero)
	}
	if t.sparse.nonzero == 0 {
		return false
	}
	found := false
	t.sparse.forEachNonZero(func(domain string) bool {
		found = podDomains.Has(domain)
		return !found
	})
	return found
}

func (t *TopologyGroup) nextDomainAntiAffinity(podDomains, nodeDomains *scheduling.Requirement) *scheduling.Requirement {
	options := scheduling.NewRequirement(t.Key, corev1.NodeSelectorOpDoesNotExist)
	// pods with anti-affinity must schedule to a domain where there are currently none of those pods (an empty
	// domain). If there are none of those domains, then the pod can't schedule.

	// We special-case kubernetes.io/hostname primarily for new NodeClaims since their domain won't be registered until we Add() them
	if t.isHostnameFastPath(nodeDomains) {
		hostName := nodeDomains.Values()[0]
		if t.count(hostName) == 0 { // produces a 0 value for new NodeClaims
			options.Insert(hostName)
		}
		return options
	}

	if d := t.dense; d != nil {
		view := d.dict.View()
		empty := d.present.AndNot(&d.nonzero)
		podMask, nodeMask := t.mask(podDomains), t.mask(nodeDomains)
		empty = empty.And(&podMask)
		empty = empty.And(&nodeMask)
		empty.ForEach(func(id topology.DomainID) bool {
			options.Insert(view.Name(id))
			return true
		})
		return options
	}
	for _, domain := range t.sparse.domains() {
		if t.sparse.count(domain) == 0 && nodeDomains.Has(domain) && podDomains.Has(domain) {
			options.Insert(domain)
		}
	}
	return options
}
