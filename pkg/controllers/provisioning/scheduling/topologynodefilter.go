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
	"github.com/awslabs/operatorpkg/option"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// TopologyNodeFilter is used to determine if a given actual node or scheduling node matches the pod's node selectors
// and required node affinity terms.  This is used with topology spread constraints to determine if the node should be
// included for topology counting purposes. This is only used with topology spread constraints as affinities/anti-affinities
// always count across all nodes. A nil or zero-value TopologyNodeFilter behaves well and the filter returns true for
// all nodes.
type TopologyNodeFilter struct {
	Requirements   []scheduling.Requirements
	TaintPolicy    corev1.NodeInclusionPolicy
	AffinityPolicy corev1.NodeInclusionPolicy
	Tolerations    []corev1.Toleration
}

func MakeTopologyNodeFilter(p *corev1.Pod, taintPolicy corev1.NodeInclusionPolicy, affinityPolicy corev1.NodeInclusionPolicy) TopologyNodeFilter {
	nodeSelectorRequirements := scheduling.NewLabelRequirements(p.Spec.NodeSelector)
	// if we only have a label selector, that's the only requirement that must match
	if p.Spec.Affinity == nil || p.Spec.Affinity.NodeAffinity == nil || p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return TopologyNodeFilter{
			Requirements:   []scheduling.Requirements{nodeSelectorRequirements},
			TaintPolicy:    taintPolicy,
			AffinityPolicy: affinityPolicy,
			Tolerations:    p.Spec.Tolerations,
		}
	}

	// otherwise, we need to match the combination of label selector and any term of the required node affinities since
	// those terms are OR'd together
	filter := TopologyNodeFilter{
		TaintPolicy:    taintPolicy,
		AffinityPolicy: affinityPolicy,
		Tolerations:    p.Spec.Tolerations,
	}
	for _, term := range p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		requirements := scheduling.NewRequirements()
		requirements.Add(nodeSelectorRequirements.Values()...)
		requirements.Add(scheduling.NewNodeSelectorRequirements(term.MatchExpressions...).Values()...)
		filter.Requirements = append(filter.Requirements, requirements)
	}

	return filter
}

// Matches returns true if the TopologyNodeFilter doesn't prohibit node from the participating in the topology
func (t TopologyNodeFilter) Matches(taints []corev1.Taint, requirements scheduling.Requirements, compatibilityOptions ...option.Function[scheduling.CompatibilityOptions]) bool {
	matchesAffinity := true
	if t.AffinityPolicy == corev1.NodeInclusionPolicyHonor {
		matchesAffinity = t.matchesRequirements(requirements)
	}
	matchesTaints := true
	if t.TaintPolicy == corev1.NodeInclusionPolicyHonor {
		if err := scheduling.Taints(taints).Tolerates(t.Tolerations); err != nil {
			matchesTaints = false
		}
	}
	return matchesAffinity && matchesTaints
}

// MatchesRequirements returns true if the TopologyNodeFilter doesn't prohibit a node with the requirements from
// participating in the topology. This method allows checking the requirements from a scheduling.NodeClaim to see if the
// node we will soon create participates in this topology.
func (t TopologyNodeFilter) matchesRequirements(requirements scheduling.Requirements, compatibilityOptions ...option.Function[scheduling.CompatibilityOptions]) bool {
	// no requirements, so it always matches
	if len(t.Requirements) == 0 || t.AffinityPolicy == corev1.NodeInclusionPolicyIgnore {
		return true
	}
	// these are an OR, so if any passes the filter passes
	for _, req := range t.Requirements {
		if err := requirements.Compatible(req, compatibilityOptions...); err == nil {
			return true
		}
	}
	return false
}

// topologyNodeFilterMemo memoizes TopologyNodeFilter.Matches across many nodes. Matches only reads a node's requirements
// for the keys that appear in the filter's requirements (Requirements.Compatible), and only reads the key, value and
// effect of the node's taints (Taints.Tolerates), so nodes that agree on those produce the same result. A filter that
// references a per-node label like hostname is still exact, it just doesn't collapse any nodes.
type topologyNodeFilterMemo struct {
	filter     TopologyNodeFilter
	matchesAll bool
	keys       []string
	results    map[string]bool
	buf        []byte
}

func newTopologyNodeFilterMemo(filter TopologyNodeFilter) *topologyNodeFilterMemo {
	m := &topologyNodeFilterMemo{
		filter:     filter,
		matchesAll: (filter.AffinityPolicy != corev1.NodeInclusionPolicyHonor || len(filter.Requirements) == 0) && filter.TaintPolicy != corev1.NodeInclusionPolicyHonor,
		results:    map[string]bool{},
	}
	if filter.AffinityPolicy == corev1.NodeInclusionPolicyHonor {
		keys := sets.New[string]()
		for _, reqs := range filter.Requirements {
			keys.Insert(reqs.Keys().UnsortedList()...)
		}
		m.keys = keys.UnsortedList()
	}
	return m
}

// Matches returns the same result as filter.Matches(taints, scheduling.NewLabelRequirements(labels)), only building the
// requirements if the result for a node with the same relevant labels and taints hasn't been computed yet.
func (m *topologyNodeFilterMemo) Matches(labels map[string]string, taints []corev1.Taint) bool {
	if m.matchesAll {
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
	if m.filter.TaintPolicy == corev1.NodeInclusionPolicyHonor {
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
	result := m.filter.Matches(taints, scheduling.NewLabelRequirements(labels))
	m.results[string(m.buf)] = result
	return result
}
