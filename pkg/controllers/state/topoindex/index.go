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

// Package topoindex is an in-memory index of the pod, node and namespace facts that scheduling topology reads. It is
// fed from informer event handlers, so it mirrors the informer cache that the scheduler would otherwise List and Get
// from for every topology group of every scheduling simulation.
package topoindex

import (
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/karpenter/pkg/controllers/state/topoindex/internal/cow"
	"sigs.k8s.io/karpenter/pkg/controllers/state/topoindex/internal/intern"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
)

const (
	podShards       = 1024
	nodeShards      = 256
	namespaceShards = 16
)

// PodFact is what the index knows about a pod.
type PodFact struct {
	Namespace string
	// Labels are the pod's labels. They're shared by every pod with the same namespace and labels, and must not be
	// modified.
	Labels   labels.Set
	NodeName string
	// Countable is true if topology counts the pod: it's scheduled, and not terminal or terminating. This is the
	// negation of scheduling.IgnoredForTopology.
	Countable bool
}

// Node is what the index knows about a Node in the informer cache. A Node and its fields must not be modified.
type Node struct {
	Name   string
	Labels map[string]string
	Taints []corev1.Taint
}

// Namespace is what the index knows about a Namespace. A Namespace and its fields must not be modified.
type Namespace struct {
	Name   string
	Labels labels.Set
}

type labelSet = intern.LabelSet[setData]

// setData is the writer's per-label-set state.
type setData struct {
	// pods are the countable pods with this label set
	pods []*podRec
}

type podRec struct {
	uid       types.UID
	rv        string
	set       *labelSet
	nodeName  string
	countable bool
	// setPos and nodePos are the pod's positions in set.Data.pods and in its node's pods while it's countable
	setPos, nodePos int
}

type nodeRec struct {
	name string
	rv   string
	// node is nil while the Node isn't in the informer cache; the record then only exists because countable pods are
	// bound to it
	node *Node
	// pods are the countable pods bound to the node
	pods []*podRec
}

// Index is the topology fact index. Writers (the informer event handlers) and readers synchronize on mu. The index
// has a generation that only increases when a fact changes, so a resync or a status-only update doesn't bump it.
type Index struct {
	mu   sync.Mutex
	gen  uint64
	genA atomic.Uint64

	sets       *intern.Table[setData]
	pods       map[types.UID]*podRec
	nodes      map[string]*nodeRec
	podFacts   *cow.Map[types.UID, PodFact]
	nodeFacts  *cow.Map[string, *Node]
	namespaces *cow.Map[string, *Namespace]
	// nsSets are the label sets in use per namespace
	nsSets map[string]map[*labelSet]struct{}

	// handlersSynced report whether the informer event handlers that feed the index have seen the informers' initial
	// lists
	handlersSynced atomic.Pointer[[]func() bool]
}

func New() *Index {
	ix := &Index{}
	ix.resetLocked()
	return ix
}

func (ix *Index) resetLocked() {
	ix.sets = intern.NewTable[setData]()
	ix.pods = map[types.UID]*podRec{}
	ix.nodes = map[string]*nodeRec{}
	ix.podFacts = cow.NewMap[types.UID, PodFact](podShards)
	ix.nodeFacts = cow.NewMap[string, *Node](nodeShards)
	ix.namespaces = cow.NewMap[string, *Namespace](namespaceShards)
	ix.nsSets = map[string]map[*labelSet]struct{}{}
}

// Generation returns the index's generation. It increases whenever a fact changes.
func (ix *Index) Generation() uint64 {
	return ix.genA.Load()
}

func (ix *Index) bumpLocked() {
	ix.gen++
	ix.genA.Store(ix.gen)
}

// SetHandlersSynced sets the functions that report whether the event handlers feeding the index have been delivered
// the initial contents of their informers.
func (ix *Index) SetHandlersSynced(fns ...func() bool) {
	ix.handlersSynced.Store(&fns)
}

// HandlersSynced returns true once every event handler set with SetHandlersSynced has synced. It's false if none were
// set.
func (ix *Index) HandlersSynced() bool {
	fns := ix.handlersSynced.Load()
	if fns == nil {
		return false
	}
	for _, fn := range *fns {
		if !fn() {
			return false
		}
	}
	return true
}

// Reset drops every fact. It's used by tests.
func (ix *Index) Reset() {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.resetLocked()
	ix.bumpLocked()
}

// ObservePod records the pod as the informer cache has it. It returns true if a fact changed.
func (ix *Index) ObservePod(pod *corev1.Pod) bool {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	changed := ix.observePodLocked(pod)
	eventsTotal(kindPod, changed)
	return changed
}

func (ix *Index) observePodLocked(pod *corev1.Pod) bool {
	rec, ok := ix.pods[pod.UID]
	if ok && rec.rv == pod.ResourceVersion && pod.ResourceVersion != "" {
		return false
	}
	countable := podutils.IsScheduled(pod) && !podutils.IsTerminal(pod) && !podutils.IsTerminating(pod)
	if ok && rec.set.Namespace == pod.Namespace && maps.Equal(rec.set.Labels, labels.Set(pod.Labels)) &&
		rec.nodeName == pod.Spec.NodeName && rec.countable == countable {
		rec.rv = pod.ResourceVersion
		return false
	}
	if !ok {
		rec = &podRec{uid: pod.UID}
		ix.pods[pod.UID] = rec
	} else {
		ix.uncountLocked(rec)
		ix.releaseSetLocked(rec.set)
	}
	rec.rv = pod.ResourceVersion
	rec.set = ix.acquireSetLocked(pod.Namespace, pod.Labels)
	rec.nodeName = pod.Spec.NodeName
	rec.countable = countable
	ix.countLocked(rec)
	ix.podFacts.Set(rec.uid, PodFact{Namespace: rec.set.Namespace, Labels: rec.set.Labels, NodeName: rec.nodeName, Countable: rec.countable})
	ix.bumpLocked()
	return true
}

// DeletePod removes the pod. It returns true if the pod was known.
func (ix *Index) DeletePod(uid types.UID) bool {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	changed := ix.deletePodLocked(uid)
	eventsTotal(kindPod, changed)
	return changed
}

func (ix *Index) deletePodLocked(uid types.UID) bool {
	rec, ok := ix.pods[uid]
	if !ok {
		return false
	}
	ix.uncountLocked(rec)
	ix.releaseSetLocked(rec.set)
	delete(ix.pods, uid)
	ix.podFacts.Delete(uid)
	ix.bumpLocked()
	return true
}

func (ix *Index) acquireSetLocked(namespace string, l map[string]string) *labelSet {
	set, created := ix.sets.Acquire(namespace, l)
	if created {
		sets, ok := ix.nsSets[namespace]
		if !ok {
			sets = map[*labelSet]struct{}{}
			ix.nsSets[namespace] = sets
		}
		sets[set] = struct{}{}
	}
	return set
}

func (ix *Index) releaseSetLocked(set *labelSet) {
	if !ix.sets.Release(set) {
		return
	}
	sets := ix.nsSets[set.Namespace]
	delete(sets, set)
	if len(sets) == 0 {
		delete(ix.nsSets, set.Namespace)
	}
}

// countLocked adds a countable pod to its label set's and node's postings
func (ix *Index) countLocked(rec *podRec) {
	if !rec.countable {
		return
	}
	rec.setPos = len(rec.set.Data.pods)
	rec.set.Data.pods = append(rec.set.Data.pods, rec)
	n := ix.nodeLocked(rec.nodeName)
	rec.nodePos = len(n.pods)
	n.pods = append(n.pods, rec)
}

// uncountLocked removes a countable pod from its label set's and node's postings
func (ix *Index) uncountLocked(rec *podRec) {
	if !rec.countable {
		return
	}
	rec.set.Data.pods = removeAt(rec.set.Data.pods, rec.setPos, func(moved *podRec) { moved.setPos = rec.setPos })
	n := ix.nodes[rec.nodeName]
	n.pods = removeAt(n.pods, rec.nodePos, func(moved *podRec) { moved.nodePos = rec.nodePos })
	ix.gcNodeLocked(n)
}

// removeAt removes s[i] by moving the last element into its place, calling moved for the element that moved.
func removeAt[T any](s []T, i int, moved func(T)) []T {
	last := len(s) - 1
	if i != last {
		s[i] = s[last]
		moved(s[i])
	}
	var zero T
	s[last] = zero
	return s[:last]
}

func (ix *Index) nodeLocked(name string) *nodeRec {
	n, ok := ix.nodes[name]
	if !ok {
		n = &nodeRec{name: name}
		ix.nodes[name] = n
	}
	return n
}

// gcNodeLocked drops the record of a node that isn't in the informer cache once no countable pods are bound to it
func (ix *Index) gcNodeLocked(n *nodeRec) {
	if n.node == nil && len(n.pods) == 0 {
		delete(ix.nodes, n.name)
	}
}

// ObserveNode records the Node as the informer cache has it. It returns true if a fact changed.
func (ix *Index) ObserveNode(node *corev1.Node) bool {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	changed := ix.observeNodeLocked(node)
	eventsTotal(kindNode, changed)
	return changed
}

func (ix *Index) observeNodeLocked(node *corev1.Node) bool {
	n := ix.nodeLocked(node.Name)
	if n.node != nil && n.rv == node.ResourceVersion && node.ResourceVersion != "" {
		return false
	}
	n.rv = node.ResourceVersion
	if n.node != nil && maps.Equal(n.node.Labels, node.Labels) && taintsEqual(n.node.Taints, node.Spec.Taints) {
		return false
	}
	n.node = &Node{Name: node.Name, Labels: maps.Clone(node.Labels), Taints: slices.Clone(node.Spec.Taints)}
	ix.nodeFacts.Set(n.name, n.node)
	ix.bumpLocked()
	return true
}

// taintsEqual compares the taint fields that topology reads: Taints.Tolerates only reads the key, value and effect.
func taintsEqual(a, b []corev1.Taint) bool {
	return slices.EqualFunc(a, b, func(x, y corev1.Taint) bool {
		return x.Key == y.Key && x.Value == y.Value && x.Effect == y.Effect
	})
}

// DeleteNode removes the Node. Pods bound to it stay, and count again if the Node comes back. It returns true if the
// Node was known.
func (ix *Index) DeleteNode(name string) bool {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	changed := ix.deleteNodeLocked(name)
	eventsTotal(kindNode, changed)
	return changed
}

func (ix *Index) deleteNodeLocked(name string) bool {
	n, ok := ix.nodes[name]
	if !ok || n.node == nil {
		return false
	}
	n.node = nil
	n.rv = ""
	ix.nodeFacts.Delete(name)
	ix.gcNodeLocked(n)
	ix.bumpLocked()
	return true
}

// ObserveNamespace records the Namespace. It returns true if a fact changed.
func (ix *Index) ObserveNamespace(namespace *corev1.Namespace) bool {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	changed := ix.observeNamespaceLocked(namespace)
	eventsTotal(kindNamespace, changed)
	return changed
}

func (ix *Index) observeNamespaceLocked(namespace *corev1.Namespace) bool {
	if ns, ok := ix.namespaces.Get(namespace.Name); ok && maps.Equal(ns.Labels, labels.Set(namespace.Labels)) {
		return false
	}
	ix.namespaces.Set(namespace.Name, &Namespace{Name: namespace.Name, Labels: labels.Set(maps.Clone(namespace.Labels))})
	ix.bumpLocked()
	return true
}

// DeleteNamespace removes the Namespace. It returns true if the Namespace was known.
func (ix *Index) DeleteNamespace(name string) bool {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	changed := ix.deleteNamespaceLocked(name)
	eventsTotal(kindNamespace, changed)
	return changed
}

func (ix *Index) deleteNamespaceLocked(name string) bool {
	if _, ok := ix.namespaces.Get(name); !ok {
		return false
	}
	ix.namespaces.Delete(name)
	ix.bumpLocked()
	return true
}
