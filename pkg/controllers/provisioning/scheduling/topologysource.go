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
	"fmt"

	"github.com/awslabs/operatorpkg/serrors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/scheduling/topology"
)

// listCountSource is a topology.CountSource that lists pods from the kube client and reads nodes from it, as the legacy
// countDomains does, and reads the inverse anti-affinity terms from cluster state. It belongs to one Topology: it
// memoizes nodes for the Topology's lifetime and remembers the domains of the pods that Topology excludes.
type listCountSource struct {
	kubeClient client.Client
	cluster    *state.Cluster
	excluded   *topology.ExcludedPods
	// nodes holds the nodes that existing pods are bound to, keyed by node name, with a nil entry for nodes that were
	// not found
	nodes map[string]*topologyNode
	// excludedDomains holds, by count key, the domains of the excluded pods that Counts counted
	excludedDomains map[*topology.CountKey][]string
}

var _ topology.CountSource = (*listCountSource)(nil)

func newListCountSource(kubeClient client.Client, cluster *state.Cluster, excluded *topology.ExcludedPods) *listCountSource {
	return &listCountSource{
		kubeClient:      kubeClient,
		cluster:         cluster,
		excluded:        excluded,
		nodes:           map[string]*topologyNode{},
		excludedDomains: map[*topology.CountKey][]string{},
	}
}

func (s *listCountSource) Counts(ctx context.Context, key *topology.CountKey) (*topology.BaseCounts, error) {
	counts := map[string]int32{}
	var excludedDomains []string
	nodeFilter := newNodePredicateMemo(key.NodeFilter)
	podList := &corev1.PodList{}
	for _, ns := range key.Namespaces {
		if err := s.kubeClient.List(ctx, podList, &client.ListOptions{Namespace: ns, LabelSelector: key.ListSelector()}); err != nil {
			return nil, fmt.Errorf("listing pods, %w", err)
		}
		for i := range podList.Items {
			p := &podList.Items[i]
			if IgnoredForTopology(p) {
				continue
			}
			n, err := s.getNode(ctx, p.Spec.NodeName)
			if err != nil {
				return nil, err
			}
			// Pods that cannot be evicted can be leaked in the API Server after
			// a Node is removed. Since pod bindings are immutable, these pods
			// cannot be recovered, and will be deleted by the pod lifecycle
			// garbage collector. These pods are not running, and should not
			// impact future topology calculations.
			if n == nil {
				continue
			}
			domain, ok := n.labels[key.TopologyKey]
			// Kubelet sets the hostname label, but the node may not be ready yet so there is no label.  We fall back and just
			// treat the node name as the label.  It probably is in most cases, but even if not we at least count the existence
			// of the pods in some domain, even if not in the correct one.  This is needed to handle the case of pods with
			// self-affinity only fulfilling that affinity if all domains are empty.
			if !ok && key.TopologyKey == corev1.LabelHostname {
				domain = n.name
				ok = true
			}
			if !ok {
				continue // Don't include pods if node doesn't contain domain https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/#conventions
			}
			// nodes may or may not be considered for counting purposes for topology spread constraints depending on if they
			// are selected by the pod's node selectors and required node affinities.  If these are unset, the node always counts.
			if !nodeFilter.Matches(n.labels, n.taints) {
				continue
			}
			counts[domain]++
			if s.excluded.Has(p.UID) {
				excludedDomains = append(excludedDomains, domain)
			}
		}
	}
	s.excludedDomains[key] = excludedDomains
	return newBaseCounts(key.TopologyKey, counts), nil
}

func (s *listCountSource) ExcludedContributions(ctx context.Context, key *topology.CountKey, excluded *topology.ExcludedPods, f func(domain string)) error {
	if excluded != s.excluded {
		return fmt.Errorf("excluded pods aren't the topology's")
	}
	domains, ok := s.excludedDomains[key]
	if !ok {
		if _, err := s.Counts(ctx, key); err != nil {
			return err
		}
		domains = s.excludedDomains[key]
	}
	for _, domain := range domains {
		f(domain)
	}
	return nil
}

func (s *listCountSource) InverseAntiAffinityTerms(_ context.Context, excluded *topology.ExcludedPods, f func(*topology.InverseTerm)) error {
	s.cluster.ForPodsWithAntiAffinity(func(pod *corev1.Pod, node *corev1.Node) bool {
		// don't count the pod we are excluding
		if excluded.Has(pod.UID) {
			return true
		}
		for i := range pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			term := &pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[i]
			counts := &topology.BaseCounts{Sparse: map[string]int32{}}
			if domain, ok := node.Labels[term.TopologyKey]; ok {
				counts.Sparse[domain] = 1
			}
			f(&topology.InverseTerm{Namespace: pod.Namespace, Term: term, Owners: []types.UID{pod.UID}, Counts: counts})
		}
		return true
	})
	return nil
}

func (s *listCountSource) Generation() uint64 {
	return 0
}

// getNode returns the named node, or nil if the node doesn't exist. Results are memoized for the lifetime of the
// source so that each node is fetched at most once across all topology groups.
func (s *listCountSource) getNode(ctx context.Context, name string) (*topologyNode, error) {
	if n, ok := s.nodes[name]; ok {
		return n, nil
	}
	node := &corev1.Node{}
	if err := s.kubeClient.Get(ctx, types.NamespacedName{Name: name}, node); err != nil {
		if errors.IsNotFound(err) {
			s.nodes[name] = nil
			return nil, nil
		}
		return nil, serrors.Wrap(fmt.Errorf("getting node, %w", err), "Node", klog.KRef("", name))
	}
	n := &topologyNode{name: node.Name, labels: node.Labels, taints: node.Spec.Taints}
	s.nodes[name] = n
	return n, nil
}

// newBaseCounts returns the counts, dense if the topology key is dense
func newBaseCounts(topologyKey string, counts map[string]int32) *topology.BaseCounts {
	if !topology.Dense(topologyKey) {
		return &topology.BaseCounts{Sparse: counts}
	}
	dict := topology.Dict(topologyKey)
	names := make([]string, 0, len(counts))
	for domain := range counts {
		names = append(names, domain)
	}
	ids := dict.InternAll(names...)
	b := &topology.BaseCounts{View: dict.View()}
	b.Dense = make([]int32, b.View.Len())
	for i, id := range ids {
		b.Dense[id] = counts[names[i]]
	}
	return b
}

// nodePredicateMemo memoizes a topology.NodePredicate across many nodes, like topologyNodeFilterMemo does for a
// TopologyNodeFilter: nodes that agree on the label keys the predicate reads, and on their taints if it reads them,
// get the same result. A nil predicate matches every node.
type nodePredicateMemo struct {
	predicate topology.NodePredicate
	keys      []string
	results   map[string]bool
	buf       []byte
}

func newNodePredicateMemo(predicate topology.NodePredicate) *nodePredicateMemo {
	m := &nodePredicateMemo{predicate: predicate, results: map[string]bool{}}
	if predicate != nil {
		m.keys = predicate.Keys()
	}
	return m
}

func (m *nodePredicateMemo) Matches(labels map[string]string, taints []corev1.Taint) bool {
	if m.predicate == nil {
		return true
	}
	// label keys, values and taint fields can't contain these bytes, so the signature is unambiguous
	m.buf = m.buf[:0]
	for _, key := range m.keys {
		if value, ok := labels[key]; ok {
			m.buf = append(m.buf, 1)
			m.buf = append(m.buf, value...)
		}
		m.buf = append(m.buf, 0)
	}
	if m.predicate.ReadsTaints() {
		for i := range taints {
			m.buf = append(m.buf, taints[i].Key...)
			m.buf = append(m.buf, 0)
			m.buf = append(m.buf, taints[i].Value...)
			m.buf = append(m.buf, 0)
			m.buf = append(m.buf, taints[i].Effect...)
			m.buf = append(m.buf, 0)
		}
	}
	if result, ok := m.results[string(m.buf)]; ok {
		return result
	}
	result := m.predicate.Matches(labels, taints)
	m.results[string(m.buf)] = result
	return result
}
