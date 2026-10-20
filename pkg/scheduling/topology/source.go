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

package topology

import (
	"context"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
)

// CountSource provides the counts of existing pods that seed a scheduling simulation's topology groups. A Topology
// calls its source serially; a source that is shared between simulations must be safe for concurrent use.
type CountSource interface {
	// Counts returns the number of pods p, by domain, with
	//   p.Namespace in key.Namespaces, key.ListSelector() matching p.Labels, and p neither unscheduled, terminal nor
	//   terminating (IgnoredForTopology), whose node p.Spec.NodeName exists and passes key.NodeFilter on the node's
	//   labels and spec taints.
	// A pod's domain is its node's value for key.TopologyKey; for kubernetes.io/hostname, a node without the label is
	// its own domain, by node name. Pods whose node doesn't have the key aren't counted. Excluded pods are counted; see
	// ExcludedContributions.
	Counts(ctx context.Context, key *CountKey) (*BaseCounts, error)
	// ExcludedContributions calls f with the domain of each excluded pod that Counts(key) counts.
	ExcludedContributions(ctx context.Context, key *CountKey, excluded *ExcludedPods, f func(domain string)) error
	// InverseAntiAffinityTerms calls f for the required pod anti-affinity terms of the pods that are bound to a node in
	// cluster state, other than the excluded pods. Terminating pods are included. Each (pod, term) pair is covered by
	// exactly one call, and a call may cover the pairs of several pods in the same namespace with an equal term.
	InverseAntiAffinityTerms(ctx context.Context, excluded *ExcludedPods, f func(*InverseTerm)) error
	// Generation identifies the state that the source counts from.
	Generation() uint64
}

// InverseTerm is a required pod anti-affinity term of existing pods, which blocks the pods it selects from the domains
// that the term's owners are in.
type InverseTerm struct {
	// Namespace is the owners' namespace, which the term's namespaces default to
	Namespace string
	Term      *corev1.PodAffinityTerm
	Owners    []types.UID
	// Counts are the owners by domain: the value of Term.TopologyKey on the cluster state node each owner is bound to.
	// Unlike for Counts, there's no hostname fallback, so owners on nodes without the key aren't counted.
	Counts *BaseCounts
}

// BaseCounts are pod counts by domain. They're immutable once returned and can be shared by concurrent simulations.
// Counts are dense, indexed by the DomainIDs of View, or sparse by domain name if Dense is nil.
type BaseCounts struct {
	View  *DomainView
	Dense []int32
	// Sparse only holds non-zero counts
	Sparse map[string]int32
}

// Get returns the count of the domain.
func (b *BaseCounts) Get(domain string) int32 {
	if b.Dense == nil {
		return b.Sparse[domain]
	}
	if id, ok := b.View.Lookup(domain); ok && int(id) < len(b.Dense) {
		return b.Dense[id]
	}
	return 0
}

// ForEach calls f for each domain with a non-zero count.
func (b *BaseCounts) ForEach(f func(domain string, count int32)) {
	if b.Dense == nil {
		for domain, count := range b.Sparse {
			f(domain, count)
		}
		return
	}
	for id, count := range b.Dense {
		if count != 0 {
			f(b.View.Name(DomainID(id)), count)
		}
	}
}

// ExcludedPods are the pods that a simulation schedules, which are excluded from the counts of existing pods.
type ExcludedPods struct {
	uids sets.Set[types.UID]
	mu   sync.Mutex
	memo map[any]any
}

func NewExcludedPods(pods []*corev1.Pod) *ExcludedPods {
	e := &ExcludedPods{uids: sets.New[types.UID]()}
	for _, p := range pods {
		e.uids.Insert(p.UID)
	}
	return e
}

// Has reports whether the pod is excluded.
func (e *ExcludedPods) Has(uid types.UID) bool {
	return e.uids.Has(uid)
}

// Len returns the number of excluded pods.
func (e *ExcludedPods) Len() int {
	return len(e.uids)
}

// UIDs returns the excluded pods. It must not be modified.
func (e *ExcludedPods) UIDs() sets.Set[types.UID] {
	return e.uids
}

// Memo returns the value memoized under key, building it on first use. Sources use it to keep what they derive from
// the excluded pods, such as which of them they count, for the lifetime of the simulation.
func (e *ExcludedPods) Memo(key any, build func() any) any {
	e.mu.Lock()
	defer e.mu.Unlock()
	if v, ok := e.memo[key]; ok {
		return v
	}
	if e.memo == nil {
		e.memo = map[any]any{}
	}
	v := build()
	e.memo[key] = v
	return v
}
