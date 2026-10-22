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

package pdb

import (
	"context"
	"sort"
	"sync"

	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/events"
	podutil "sigs.k8s.io/karpenter/pkg/utils/pod"
)

type evictionBlocker int

const (
	zeroDisruptions evictionBlocker = iota
	fullyBlockingPDBs
)

// Limits is used to evaluate if evicting a list of pods is possible.
type Limits struct {
	byNamespace map[string]*namespacePDBs
}

// namespacePDBs are one namespace's PDBs in list order, indexed by label so that a pod is only matched against the
// PDBs whose selector it can satisfy.
type namespacePDBs struct {
	pdbs []*pdbItem
	// byLabel[key][value] are the positions in pdbs of the PDBs indexed by the requirement that label key is value.
	// Each PDB is indexed by at most one requirement (its first Equals or In requirement), which a matching pod must
	// satisfy.
	byLabel map[string]map[string][]int
	// unindexed are the positions in pdbs of the PDBs with no Equals or In requirement. They are matched against
	// every pod in the namespace.
	unindexed []int
}

// NewLimits lists the PDBs and builds Limits from them. If ctx carries an Index (WithIndex), PDBs whose UID and
// resourceVersion haven't changed since the Index last saw them aren't parsed again. Limits are the same either way:
// the PDBs are listed on every call, and a PDB is reused only if neither its spec nor its status changed.
func NewLimits(ctx context.Context, kubeClient client.Client) (Limits, error) {
	var pdbList policyv1.PodDisruptionBudgetList
	if err := kubeClient.List(ctx, &pdbList); err != nil {
		return Limits{}, err
	}
	idx, _ := ctx.Value(indexKey{}).(*Index)
	pdbs, err := idx.items(pdbList.Items)
	if err != nil {
		return Limits{}, err
	}
	return newLimits(pdbs), nil
}

func newLimits(pdbs []*pdbItem) Limits {
	l := Limits{byNamespace: map[string]*namespacePDBs{}}
	for _, pdb := range pdbs {
		ns, ok := l.byNamespace[pdb.key.Namespace]
		if !ok {
			ns = &namespacePDBs{byLabel: map[string]map[string][]int{}}
			l.byNamespace[pdb.key.Namespace] = ns
		}
		pos := len(ns.pdbs)
		ns.pdbs = append(ns.pdbs, pdb)
		if pdb.indexKey == "" {
			ns.unindexed = append(ns.unindexed, pos)
			continue
		}
		values, ok := ns.byLabel[pdb.indexKey]
		if !ok {
			values = map[string][]int{}
			ns.byLabel[pdb.indexKey] = values
		}
		for _, v := range pdb.indexValues {
			values[v] = append(values[v], pos)
		}
	}
	return l
}

// matching returns the PDBs that select pod, in list order.
func (l Limits) matching(pod *v1.Pod) []*pdbItem {
	ns, ok := l.byNamespace[pod.Namespace]
	if !ok {
		return nil
	}
	var buf [8]int
	positions := append(buf[:0], ns.unindexed...)
	for k, v := range pod.Labels {
		positions = append(positions, ns.byLabel[k][v]...)
	}
	if len(positions) > 1 {
		sort.Ints(positions)
	}
	podLabels := labels.Set(pod.Labels)
	var matched []*pdbItem
	for _, pos := range positions {
		if ns.pdbs[pos].selector.Matches(podLabels) {
			matched = append(matched, ns.pdbs[pos])
		}
	}
	return matched
}

type indexKey struct{}

// WithIndex returns a context in which NewLimits uses idx.
func WithIndex(ctx context.Context, idx *Index) context.Context {
	return context.WithValue(ctx, indexKey{}, idx)
}

// Index keeps the parsed form of the PDBs it has seen by UID and resourceVersion, so that building Limits again
// doesn't parse unchanged PDBs again. It keeps only the PDBs of the latest list.
type Index struct {
	mu      sync.Mutex
	entries map[types.UID]*pdbItem
}

func NewIndex() *Index {
	return &Index{entries: map[types.UID]*pdbItem{}}
}

// items parses pdbs, reusing the Index's items for PDBs whose UID and resourceVersion it has seen. A nil Index parses
// every PDB.
func (i *Index) items(pdbs []policyv1.PodDisruptionBudget) ([]*pdbItem, error) {
	if i == nil {
		return lo.MapErr(pdbs, func(pdb policyv1.PodDisruptionBudget, _ int) (*pdbItem, error) { return newPdb(pdb) })
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	entries := make(map[types.UID]*pdbItem, len(pdbs))
	items := make([]*pdbItem, 0, len(pdbs))
	for _, pdb := range pdbs {
		pi, ok := i.entries[pdb.UID]
		if !ok || pdb.ResourceVersion == "" || pi.resourceVersion != pdb.ResourceVersion {
			var err error
			if pi, err = newPdb(pdb); err != nil {
				return nil, err
			}
		}
		if pdb.ResourceVersion != "" {
			entries[pdb.UID] = pi
		}
		items = append(items, pi)
	}
	i.entries = entries
	return items, nil
}

// CanEvictPods returns true if every pod in the list is evictable. They may not all be evictable simultaneously, but
// for every PDB that controls the pods at least one pod can be evicted.
// nolint:gocyclo
func (l Limits) CanEvictPods(pods []*v1.Pod, clk clock.Clock, recorder events.Recorder) ([]client.ObjectKey, bool) {
	for _, pod := range pods {
		pdbs, evictable := l.isEvictable(pod, clk, recorder, zeroDisruptions)

		if !evictable {
			return pdbs, false
		}
	}
	return []client.ObjectKey{}, true
}

// isFullyBlocked returns true if the given pod is fully blocked by a PDB.
func (l Limits) isFullyBlocked(pod *v1.Pod, clk clock.Clock, recorder events.Recorder) ([]client.ObjectKey, bool) {
	pdbs, evictable := l.isEvictable(pod, clk, recorder, fullyBlockingPDBs)

	if !evictable {
		return pdbs, true
	}
	return []client.ObjectKey{}, false
}

// nolint:gocyclo
func (l Limits) isEvictable(pod *v1.Pod, clk clock.Clock, recorder events.Recorder, evictionBlocker evictionBlocker) ([]client.ObjectKey, bool) {
	// If the pod isn't eligible for being evicted, then the predicate doesn't matter
	// This is due to the fact that we won't call the eviction API on these pods when we are disrupting the node
	if !podutil.IsEvictable(pod, clk, recorder) {
		return []client.ObjectKey{}, true
	}

	matchingPDBs := l.matching(pod)

	// Regardless of whether the PDBs allow disruptions, Kubernetes doesn't support multiple PDBs on a single pod:
	// https://github.com/kubernetes/kubernetes/blob/84cacae7046df93c1f6f8ea97c912d948e1ad06a/pkg/registry/core/pod/storage/eviction.go#L226
	if len(matchingPDBs) > 1 {
		return lo.Map(matchingPDBs, func(pdb *pdbItem, _ int) client.ObjectKey {
			return pdb.key
		}), false
	}

	for _, pdb := range matchingPDBs {
		// if the PDB policy is set to allow evicting unhealthy pods, then it won't stop us from
		// evicting unhealthy pods
		if pdb.canAlwaysEvictUnhealthyPods {
			for _, c := range pod.Status.Conditions {
				if c.Type == v1.PodReady && c.Status == v1.ConditionFalse {
					return []client.ObjectKey{}, true
				}
			}
		}

		switch evictionBlocker {
		case zeroDisruptions:
			if pdb.disruptionsAllowed == 0 {
				return []client.ObjectKey{pdb.key}, false
			}
		case fullyBlockingPDBs:
			if pdb.isFullyBlocking {
				return []client.ObjectKey{pdb.key}, false
			}
		}
	}
	return []client.ObjectKey{}, true
}

// IsCurrentlyReschedulable checks if a Karpenter should consider this pod when re-scheduling to new capacity by ensuring that the pod:
// - Is reschedulable as per the checks in IsReschedulable(...)
// - Does not have an active "karpenter.sh/do-not-disrupt" annotation (https://karpenter.sh/docs/concepts/disruption/#pod-level-controls)
// - Does not have fully blocking PDBs which would prevent the pod from being evicted
// The way this is different from IsReschedulable is that this also considers non-permanent conditions which prevent a pod from being rescheduled
// to a different node like the "do-not-disrupt" annotation or fully blocking PDBs.
func (l Limits) IsCurrentlyReschedulable(pod *v1.Pod, clk clock.Clock, recorder events.Recorder) bool {
	// Don't provision capacity for pods which will not get evicted due to fully blocking PDBs.
	// Since Karpenter doesn't know when these pods will be successfully evicted, spinning up capacity until these pods are evicted is wasteful.
	_, isFullyBlocked := l.isFullyBlocked(pod, clk, recorder)

	return podutil.IsReschedulable(pod) &&
		podutil.IsDisruptable(pod, clk, recorder) &&
		!isFullyBlocked
}

type pdbItem struct {
	key             client.ObjectKey
	resourceVersion string
	selector        labels.Selector
	// indexKey and indexValues are the selector's first Equals or In requirement: a pod the selector matches has label
	// indexKey set to one of indexValues. indexKey is empty if the selector has no such requirement.
	indexKey                    string
	indexValues                 []string
	disruptionsAllowed          int32
	isFullyBlocking             bool
	canAlwaysEvictUnhealthyPods bool
}

// nolint:gocyclo
func newPdb(pdb policyv1.PodDisruptionBudget) (*pdbItem, error) {
	selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
	if err != nil {
		return nil, err
	}
	canAlwaysEvictUnhealthyPods := pdb.Spec.UnhealthyPodEvictionPolicy != nil && *pdb.Spec.UnhealthyPodEvictionPolicy == policyv1.AlwaysAllow
	var indexKey string
	var indexValues []string
	requirements, _ := selector.Requirements()
	if r, ok := lo.Find(requirements, func(r labels.Requirement) bool {
		return r.Operator() == selection.Equals || r.Operator() == selection.DoubleEquals || r.Operator() == selection.In
	}); ok {
		indexKey, indexValues = r.Key(), lo.Uniq(r.ValuesUnsorted())
	}

	return &pdbItem{
		key:                client.ObjectKeyFromObject(&pdb),
		resourceVersion:    pdb.ResourceVersion,
		selector:           selector,
		indexKey:           indexKey,
		indexValues:        indexValues,
		disruptionsAllowed: pdb.Status.DisruptionsAllowed,
		isFullyBlocking: (pdb.Spec.MaxUnavailable != nil && pdb.Spec.MaxUnavailable.Type == intstr.Int && pdb.Spec.MaxUnavailable.IntVal == 0) ||
			(pdb.Spec.MaxUnavailable != nil && pdb.Spec.MaxUnavailable.Type == intstr.String && pdb.Spec.MaxUnavailable.StrVal == "0%") ||
			(pdb.Spec.MinAvailable != nil && pdb.Spec.MinAvailable.Type == intstr.String && pdb.Spec.MinAvailable.StrVal == "100%"),
		canAlwaysEvictUnhealthyPods: canAlwaysEvictUnhealthyPods,
	}, nil
}
