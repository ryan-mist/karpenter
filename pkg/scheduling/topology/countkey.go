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

// Package topology holds the types that scheduling topology shares with the sources of its pod counts, so that a
// source in cluster state can provide counts without importing the scheduler.
package topology

import (
	"hash/fnv"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
)

// NodePredicate decides whether a node participates in a topology group's counts. The scheduler implements it over the
// node filter of a topology spread constraint. Implementations are immutable.
type NodePredicate interface {
	// Hash and Equal identify the predicate; predicates that are Equal have the same Hash.
	Hash() uint64
	Equal(NodePredicate) bool
	// Keys returns the node label keys the predicate reads.
	Keys() []string
	// ReadsTaints reports whether the predicate reads the node's taints.
	ReadsTaints() bool
	// Matches reports whether a node with these labels and taints participates.
	Matches(labels map[string]string, taints []corev1.Taint) bool
}

// CountKey identifies the pod counts of a topology group: the pods in Namespaces that match ListSelector(Selector) and
// are bound to a node that passes NodeFilter, counted by the node's value for TopologyKey. Topology groups that only
// differ in their type, max skew, min domains or seed tolerations have the same counts and share a CountKey. Build
// CountKeys with NewCountKey; they're immutable.
type CountKey struct {
	TopologyKey string
	// Namespaces are sorted and unique.
	Namespaces []string
	// Selector is the group's raw label selector. Pods are counted with ListSelector(Selector), not with the selector
	// the group uses to select pods it records (see ListSelector).
	Selector *metav1.LabelSelector
	// NodeFilter is nil if every node participates.
	NodeFilter NodePredicate

	listSelector labels.Selector
	selectorKey  string
	hash         uint64
}

// NewCountKey returns the CountKey. namespaces must be sorted and unique.
func NewCountKey(topologyKey string, namespaces []string, selector *metav1.LabelSelector, nodeFilter NodePredicate) *CountKey {
	k := &CountKey{
		TopologyKey:  topologyKey,
		Namespaces:   namespaces,
		Selector:     selector,
		NodeFilter:   nodeFilter,
		listSelector: ListSelector(selector),
	}
	k.selectorKey = SelectorKey(k.listSelector)
	h := fnv.New64a()
	for _, s := range append([]string{topologyKey, k.selectorKey}, namespaces...) {
		_, _ = h.Write([]byte(s))
		_, _ = h.Write([]byte{0})
	}
	k.hash = h.Sum64()
	if nodeFilter != nil {
		k.hash = k.hash*31 + nodeFilter.Hash()
	}
	return k
}

// Hash returns a hash of the key. Keys that are Equal have the same hash.
func (k *CountKey) Hash() uint64 {
	return k.hash
}

// Equal reports whether two keys always produce the same counts.
func (k *CountKey) Equal(o *CountKey) bool {
	if k == o {
		return true
	}
	if k.hash != o.hash || k.TopologyKey != o.TopologyKey || k.selectorKey != o.selectorKey || !slices.Equal(k.Namespaces, o.Namespaces) {
		return false
	}
	if k.NodeFilter == nil || o.NodeFilter == nil {
		return k.NodeFilter == nil && o.NodeFilter == nil
	}
	return k.NodeFilter.Equal(o.NodeFilter)
}

// ListSelector returns the selector that pods are counted with, ListSelector(k.Selector).
func (k *CountKey) ListSelector() labels.Selector {
	return k.listSelector
}

// ListSelector converts the label selector of a topology group into the selector that the group's existing pods are
// counted with. Unlike metav1.LabelSelectorAsSelector, which topology groups use to select the pods they record, a nil
// selector selects every pod. Any invalid requirement selects nothing.
func ListSelector(labelSelector *metav1.LabelSelector) labels.Selector {
	selector := labels.Everything()
	if labelSelector == nil {
		return selector
	}
	for key, value := range labelSelector.MatchLabels {
		requirement, err := labels.NewRequirement(key, selection.Equals, []string{value})
		if err != nil {
			return labels.Nothing()
		}
		selector = selector.Add(*requirement)
	}
	for _, expression := range labelSelector.MatchExpressions {
		requirement, err := labels.NewRequirement(expression.Key, mapOperator(expression.Operator), expression.Values)
		if err != nil {
			return labels.Nothing()
		}
		selector = selector.Add(*requirement)
	}
	return selector
}

func mapOperator(operator metav1.LabelSelectorOperator) selection.Operator {
	switch operator {
	case metav1.LabelSelectorOpIn:
		return selection.In
	case metav1.LabelSelectorOpNotIn:
		return selection.NotIn
	case metav1.LabelSelectorOpExists:
		return selection.Exists
	case metav1.LabelSelectorOpDoesNotExist:
		return selection.DoesNotExist
	}
	// this shouldn't occur as we cover all valid cases of LabelSelectorOperator that the API allows.  If it still
	// does occur somehow we'll panic just later when the requirement throws an error.,
	return ""
}

// SelectorKey returns a canonical string for the selector: selectors with the same key select the same labels. It
// tells labels.Nothing() from labels.Everything(), and ignores the order of requirements and values, duplicates, and
// the difference between Equals and In with one value.
func SelectorKey(selector labels.Selector) string {
	requirements, selectable := selector.Requirements()
	if !selectable {
		return "\x00nothing"
	}
	keys := make([]string, 0, len(requirements))
	for i := range requirements {
		operator := requirements[i].Operator()
		switch operator {
		case selection.Equals, selection.DoubleEquals:
			operator = selection.In
		case selection.NotEquals:
			operator = selection.NotIn
		}
		values := requirements[i].ValuesUnsorted()
		slices.Sort(values)
		keys = append(keys, requirements[i].Key()+"\x00"+string(operator)+"\x00"+strings.Join(slices.Compact(values), "\x01"))
	}
	slices.Sort(keys)
	return strings.Join(slices.Compact(keys), "\x02")
}
