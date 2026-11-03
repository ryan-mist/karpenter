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
	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// TopologyDomainGroup tracks the domains for a single topology. Additionally, it tracks the taints associated with
// each of these domains. This enables us to determine which domains should be considered by a pod if its
// NodeTaintPolicy is honor.
type TopologyDomainGroup map[string][][]v1.Taint

func NewTopologyDomainGroup() TopologyDomainGroup {
	return map[string][][]v1.Taint{}
}

// Insert either adds a new domain to the TopologyDomainGroup or updates an existing domain.
func (t TopologyDomainGroup) Insert(domain string, taints ...v1.Taint) {
	// If the domain is not currently tracked, insert it with the associated taints. Additionally, if there are no taints
	// provided, override the taints associated with the domain. Generally, we could remove any sets of taints for which
	// the provided set is a proper subset. This is because if a pod tolerates the supersets, it will also tolerate the
	// proper subset, and removing the superset reduces the number of taint sets we need to traverse. For now we only
	// implement the simplest case, the empty set, but we could do additional performance testing to determine if
	// implementing the general case is worth the precomputation cost.
	existing, ok := t[domain]
	if ok && len(existing[0]) == 0 {
		// This is the base case, where we're already tracking the empty set of taints for the domain. Pods will always
		// be eligible for NodeClaims with this domain (based on taints), so there is no need to track additional taints.
		return
	}
	if !ok || len(taints) == 0 {
		t[domain] = [][]v1.Taint{taints}
		return
	}
	// A domain is inserted once per (NodePool, instance type) that offers it, so without this check a domain's taint sets
	// grow with NodePools x instance types even though they hold only a handful of distinct values. A taint set that is
	// identical (element by element, every field) to one already tracked can't change the result of ForEachDomain, which
	// only asks whether any tracked set is tolerated, so it isn't tracked again.
	for _, tracked := range existing {
		if taintsIdentical(tracked, taints) {
			return
		}
	}
	t[domain] = append(existing, taints)
}

// ForEachDomain calls f on each domain tracked by the topology group. If the taintHonorPolicy is honor, only domains
// available on nodes tolerated by the provided pod will be included.
func (t TopologyDomainGroup) ForEachDomain(pod *v1.Pod, taintHonorPolicy v1.NodeInclusionPolicy, f func(domain string)) {
	for domain, taintGroups := range t {
		if taintHonorPolicy == v1.NodeInclusionPolicyIgnore {
			f(domain)
			continue
		}
		// Since the taint policy is honor, we should only call f if there is a set of taints associated with the domain which
		// the pod tolerates.
		// Perf Note: We could consider hashing the pod's tolerations and using that to look up a set of tolerated domains.
		for _, taints := range taintGroups {
			if err := scheduling.Taints(taints).ToleratesPod(pod); err == nil {
				f(domain)
				break
			}
		}
	}
}

// taintsIdentical returns true if a and b contain the same taints in the same order, comparing every field.
func taintsIdentical(a, b []v1.Taint) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) > 0 && &a[0] == &b[0] {
		// The common case: the same NodePool's taints, inserted once per instance type.
		return true
	}
	for i := range a {
		if a[i].Key != b[i].Key || a[i].Value != b[i].Value || a[i].Effect != b[i].Effect || !a[i].TimeAdded.Equal(b[i].TimeAdded) {
			return false
		}
	}
	return true
}
