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

package topoindex

import (
	"encoding/json"
	"maps"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/karpenter/pkg/controllers/state/topoindex/internal/cow"
	"sigs.k8s.io/karpenter/pkg/scheduling/topology"
)

// The inverse anti-affinity view mirrors Cluster.ForPodsWithAntiAffinity, which reads cluster state rather than the
// informer cache: the pods with required anti-affinity that cluster state tracks, the Node each is bound to according
// to cluster state's bindings, and the labels of that Node in cluster state. It's fed by cluster state, under its
// lock, as those change.

// InverseOwner is a pod with a required anti-affinity term, as the inverse view counts it.
type InverseOwner struct {
	UID types.UID
	// Domain is the label of the term's topology key on the pod's Node. HasDomain is false if the Node doesn't have
	// the label, in which case the pod owns the term without being counted.
	Domain    string
	HasDomain bool
	// Terms is the number of times the pod has the term.
	Terms int32
}

// InverseTerm is a required anti-affinity term shared by the pods that have it, in one snapshot. It's immutable.
type InverseTerm struct {
	// Namespace is the owner pods' namespace.
	Namespace string
	// Term is the term as written on the pods. It must not be modified.
	Term *corev1.PodAffinityTerm
	// Owners are the pods that have the term, are bound to a Node in cluster state, and are tracked by it.
	Owners []InverseOwner
	// Counts are the owners per domain of the term's topology key, counting a pod once per time it has the term.
	Counts map[string]int32

	// scheduler is the scheduler's form of the term, built on first use
	scheduler atomic.Pointer[topology.InverseTerm]
}

type invOwner struct {
	key      types.NamespacedName
	uid      types.UID
	nodeName string
	bound    bool
	// counted is true while the owner is bound to a Node in cluster state, and so counted in its terms
	counted bool
	invNode *invNode
	// terms are the owner's required anti-affinity terms, with the number of times it has each
	terms map[*invTerm]int32
}

type invNode struct {
	// labels are the labels of the Node in cluster state, or nil if cluster state doesn't have the Node
	labels map[string]string
	owners map[*invOwner]struct{}
}

type invTerm struct {
	key       string
	namespace string
	term      *corev1.PodAffinityTerm
	// refs is the number of owners with the term, counted or not
	refs int
	// owners are the counted owners
	owners map[*invOwner]struct{}
	counts map[string]int32
}

func (ix *Index) resetInverseLocked() {
	ix.invOwners = map[types.NamespacedName]*invOwner{}
	ix.invNodes = map[string]*invNode{}
	ix.invTerms = map[string]*invTerm{}
	ix.changedTerms = map[string]struct{}{}
	ix.inverseTerms = cow.NewMap[string, *InverseTerm](inverseShards)
}

// SetAntiAffinityPod records that cluster state tracks the pod as having a required anti-affinity term, along with
// its binding and the Node it's bound to in cluster state (nil if it has none). pod is nil if cluster state stopped
// tracking it. It's called by cluster state, under its lock.
func (ix *Index) SetAntiAffinityPod(key types.NamespacedName, pod *corev1.Pod, nodeName string, bound bool, stateNode *corev1.Node) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	o, ok := ix.invOwners[key]
	if pod == nil {
		if ok {
			ix.removeOwnerLocked(o)
		}
		return
	}
	// anti-affinity terms can't change, so a pod with the same UID only needs its binding checked
	if ok && o.uid == pod.UID {
		ix.setOwnerBindingLocked(o, nodeName, bound, stateNode)
		return
	}
	if ok {
		ix.removeOwnerLocked(o)
	}
	o = &invOwner{key: key, uid: pod.UID, terms: map[*invTerm]int32{}}
	terms := pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	for i := range terms {
		t := ix.termLocked(pod.Namespace, &terms[i])
		if o.terms[t] == 0 {
			t.refs++
		}
		o.terms[t]++
	}
	ix.invOwners[key] = o
	ix.setOwnerBindingLocked(o, nodeName, bound, stateNode)
	ix.bumpLocked()
}

// SetBinding records the Node cluster state binds the pod to, and that Node in cluster state (nil if it has none).
// bound is false if cluster state has no binding for the pod. It's called by cluster state, under its lock, for every
// binding change; it's a no-op for pods without required anti-affinity.
func (ix *Index) SetBinding(key types.NamespacedName, nodeName string, bound bool, stateNode *corev1.Node) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if o, ok := ix.invOwners[key]; ok {
		ix.setOwnerBindingLocked(o, nodeName, bound, stateNode)
	}
}

// SetStateNode records the Node cluster state has under the name, or nil if it has none. It's called by cluster
// state, under its lock, whenever that changes; it's a no-op for Nodes no pod with required anti-affinity is bound to.
func (ix *Index) SetStateNode(name string, stateNode *corev1.Node) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.setStateNodeLocked(name, stateNode)
}

func (ix *Index) setStateNodeLocked(name string, stateNode *corev1.Node) {
	n, ok := ix.invNodes[name]
	if !ok || sameLabels(n.labels, stateNode) {
		return
	}
	for o := range n.owners {
		ix.uncountOwnerLocked(o)
	}
	n.labels = nodeLabels(stateNode)
	for o := range n.owners {
		ix.countOwnerLocked(o)
	}
	ix.bumpLocked()
}

// sameLabels returns true if labels are the Node's labels, where nil labels stand for no Node
func sameLabels(labels map[string]string, node *corev1.Node) bool {
	if node == nil || labels == nil {
		return node == nil && labels == nil
	}
	return maps.Equal(labels, node.Labels)
}

// nodeLabels returns a copy of the Node's labels, which is non-nil for a Node, or nil without a Node
func nodeLabels(node *corev1.Node) map[string]string {
	if node == nil {
		return nil
	}
	labels := maps.Clone(node.Labels)
	if labels == nil {
		labels = map[string]string{}
	}
	return labels
}

func (ix *Index) termLocked(namespace string, term *corev1.PodAffinityTerm) *invTerm {
	// terms marshal deterministically (map keys are sorted), so equal terms share a key
	encoded, err := json.Marshal(term)
	if err != nil {
		encoded = []byte(term.String())
	}
	key := namespace + "\x00" + string(encoded)
	t, ok := ix.invTerms[key]
	if !ok {
		t = &invTerm{key: key, namespace: namespace, term: term.DeepCopy(), owners: map[*invOwner]struct{}{}, counts: map[string]int32{}}
		ix.invTerms[key] = t
	}
	return t
}

func (ix *Index) setOwnerBindingLocked(o *invOwner, nodeName string, bound bool, stateNode *corev1.Node) {
	if !bound {
		nodeName = ""
	}
	if o.bound != bound || o.nodeName != nodeName || o.invNode == nil && bound {
		ix.uncountOwnerLocked(o)
		ix.unbindOwnerLocked(o)
		o.bound, o.nodeName = bound, nodeName
		if bound {
			n, ok := ix.invNodes[nodeName]
			if !ok {
				n = &invNode{labels: nodeLabels(stateNode), owners: map[*invOwner]struct{}{}}
				ix.invNodes[nodeName] = n
			}
			n.owners[o] = struct{}{}
			o.invNode = n
		}
		ix.countOwnerLocked(o)
		ix.bumpLocked()
	}
	if bound {
		// a no-op unless the Node changed without SetStateNode
		ix.setStateNodeLocked(nodeName, stateNode)
	}
}

func (ix *Index) unbindOwnerLocked(o *invOwner) {
	if o.invNode == nil {
		return
	}
	delete(o.invNode.owners, o)
	if len(o.invNode.owners) == 0 {
		delete(ix.invNodes, o.nodeName)
	}
	o.invNode = nil
}

func (ix *Index) removeOwnerLocked(o *invOwner) {
	ix.uncountOwnerLocked(o)
	ix.unbindOwnerLocked(o)
	for t := range o.terms {
		if t.refs--; t.refs == 0 {
			delete(ix.invTerms, t.key)
			ix.changedTerms[t.key] = struct{}{}
		}
	}
	delete(ix.invOwners, o.key)
	ix.bumpLocked()
}

// countOwnerLocked adds the owner to its terms if it's bound to a Node in cluster state, as ForPodsWithAntiAffinity
// would yield it
func (ix *Index) countOwnerLocked(o *invOwner) {
	if o.counted || o.invNode == nil || o.invNode.labels == nil {
		return
	}
	o.counted = true
	for t, n := range o.terms {
		t.owners[o] = struct{}{}
		if domain, ok := o.invNode.labels[t.term.TopologyKey]; ok {
			t.counts[domain] += n
		}
		ix.changedTerms[t.key] = struct{}{}
	}
}

func (ix *Index) uncountOwnerLocked(o *invOwner) {
	if !o.counted {
		return
	}
	o.counted = false
	for t, n := range o.terms {
		delete(t.owners, o)
		if domain, ok := o.invNode.labels[t.term.TopologyKey]; ok {
			if t.counts[domain] -= n; t.counts[domain] == 0 {
				delete(t.counts, domain)
			}
		}
		ix.changedTerms[t.key] = struct{}{}
	}
}

// freezeInverseLocked publishes the terms that changed since the last snapshot. Terms without counted owners aren't
// published, since ForPodsWithAntiAffinity wouldn't yield any pod with them.
func (ix *Index) freezeInverseLocked() {
	for key := range ix.changedTerms {
		t, ok := ix.invTerms[key]
		if !ok || len(t.owners) == 0 {
			ix.inverseTerms.Delete(key)
			continue
		}
		frozen := &InverseTerm{Namespace: t.namespace, Term: t.term, Counts: maps.Clone(t.counts), Owners: make([]InverseOwner, 0, len(t.owners))}
		for o := range t.owners {
			domain, ok := o.invNode.labels[t.term.TopologyKey]
			frozen.Owners = append(frozen.Owners, InverseOwner{UID: o.uid, Domain: domain, HasDomain: ok, Terms: o.terms[t]})
		}
		ix.inverseTerms.Set(key, frozen)
	}
	clear(ix.changedTerms)
}
