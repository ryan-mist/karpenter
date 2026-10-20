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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
)

// topologyGroupIndex indexes a set of topology groups by owner and by selector so that the per-pod lookups made during
// scheduling don't need to scan every group. Groups are never removed from the index, which matches how legacyTopology only
// ever adds groups during a scheduling simulation. Mutations must be serialized; concurrent reads are safe.
type topologyGroupIndex struct {
	// owned maps a pod UID to the groups it owns. It mirrors legacyTopologyGroup.owners and is only kept consistent if owners
	// are added and removed through the index.
	owned map[types.UID][]*legacyTopologyGroup
	// Each group is filed under exactly one requirement of its selector so that a pod, which has at most one value per
	// label key, reaches each group at most once. The index is a pre-filter: callers must still check the group's
	// selector and namespaces against the pod.
	//   Equals / In                  -> byLabel[key][value] (In is filed under each of its values)
	//   Exists / Gt / Lt             -> byKey[key]
	//   only NotIn / DoesNotExist, {} -> always
	//   nothing                      -> not indexed, as it can never select a pod
	byLabel map[string]map[string][]*legacyTopologyGroup
	byKey   map[string][]*legacyTopologyGroup
	always  []*legacyTopologyGroup
}

func newTopologyGroupIndex() *topologyGroupIndex {
	return &topologyGroupIndex{
		owned:   map[types.UID][]*legacyTopologyGroup{},
		byLabel: map[string]map[string][]*legacyTopologyGroup{},
		byKey:   map[string][]*legacyTopologyGroup{},
	}
}

// add files the group in the selector index. It must be called once per group.
func (x *topologyGroupIndex) add(tg *legacyTopologyGroup) {
	requirements, selectable := tg.selector.Requirements()
	if !selectable {
		return
	}
	var keyed string
	for _, r := range requirements {
		switch r.Operator() {
		case selection.Equals, selection.DoubleEquals, selection.In:
			values, ok := x.byLabel[r.Key()]
			if !ok {
				values = map[string][]*legacyTopologyGroup{}
				x.byLabel[r.Key()] = values
			}
			for value := range r.Values() {
				values[value] = append(values[value], tg)
			}
			return
		case selection.Exists, selection.GreaterThan, selection.LessThan:
			if keyed == "" {
				keyed = r.Key()
			}
		}
	}
	if keyed != "" {
		x.byKey[keyed] = append(x.byKey[keyed], tg)
		return
	}
	x.always = append(x.always, tg)
}

// forEachCandidate calls f for every group whose selector may select the pod, each at most once. It is a superset of
// the groups that select the pod.
func (x *topologyGroupIndex) forEachCandidate(p *corev1.Pod, f func(*legacyTopologyGroup)) {
	for _, tg := range x.always {
		f(tg)
	}
	for key, value := range p.Labels {
		if values, ok := x.byLabel[key]; ok {
			for _, tg := range values[value] {
				f(tg)
			}
		}
		for _, tg := range x.byKey[key] {
			f(tg)
		}
	}
}

// addOwner registers the pod as an owner of the group.
func (x *topologyGroupIndex) addOwner(tg *legacyTopologyGroup, uid types.UID) {
	if tg.IsOwnedBy(uid) {
		return
	}
	tg.AddOwner(uid)
	x.owned[uid] = append(x.owned[uid], tg)
}

// removeOwner unregisters the pod as an owner of every group it owns.
func (x *topologyGroupIndex) removeOwner(uid types.UID) {
	for _, tg := range x.owned[uid] {
		tg.RemoveOwner(uid)
	}
	delete(x.owned, uid)
}

// ownedBy returns the groups owned by the pod. The returned slice must not be modified.
func (x *topologyGroupIndex) ownedBy(uid types.UID) []*legacyTopologyGroup {
	return x.owned[uid]
}
