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
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/scheduling/topology"
)

// presenceIndex computes which domains a simulation's state nodes give a topology group: the domains of the nodes
// that pass the group's node filter, which are present in the group even if no pod it counts runs there. This depends
// on the simulation's own state nodes (disruption leaves out its candidates), so it's computed per Topology, once per
// distinct topology key and node filter. The filter is evaluated on the Node's labels and taints, but the domain comes
// from StateNode.Labels(), as the legacy countDomains does. State nodes without a Node (in-flight NodeClaims) are
// skipped.
type presenceIndex struct {
	stateNodes []*state.StateNode
	// labels is parallel to stateNodes and holds StateNode.Labels(), populated on first use
	labels []map[string]string
	// presences are memoized by the hash of the key and node filter
	presences map[uint64][]*presence
}

// presence is the domains that the state nodes give groups with a topology key and node filter
type presence struct {
	key    string
	filter topology.NodePredicate
	// mask has the domains of a dense key, names the domains of a sparse key
	mask  topology.Mask
	names []string
}

func newPresenceIndex(stateNodes []*state.StateNode) *presenceIndex {
	return &presenceIndex{stateNodes: stateNodes, presences: map[uint64][]*presence{}}
}

// get returns the presence of the topology key and node filter, interning the domains of a dense key.
func (x *presenceIndex) get(key string, dense bool, filter topology.NodePredicate) *presence {
	hash := topology.NewCountKey(key, nil, nil, filter).Hash()
	for _, p := range x.presences[hash] {
		if p.key == key && (p.filter == nil && filter == nil || p.filter != nil && filter != nil && p.filter.Equal(filter)) {
			return p
		}
	}
	if x.labels == nil {
		x.labels = make([]map[string]string, len(x.stateNodes))
		for i, n := range x.stateNodes {
			if n.Node != nil {
				x.labels[i] = n.Labels()
			}
		}
	}
	p := &presence{key: key, filter: filter}
	nodeFilter := newNodePredicateMemo(filter)
	var names []string
	seen := map[string]struct{}{}
	for i, n := range x.stateNodes {
		// ignore state nodes which are tracking in-flight NodeClaims
		if n.Node == nil {
			continue
		}
		// ignore the node if it doesn't match the topology group
		if !nodeFilter.Matches(n.Node.Labels, n.Node.Spec.Taints) {
			continue
		}
		domain, exists := x.labels[i][key]
		if !exists {
			continue
		}
		if _, ok := seen[domain]; !ok {
			seen[domain] = struct{}{}
			names = append(names, domain)
		}
	}
	if dense {
		for _, id := range topology.Dict(key).InternAll(names...) {
			p.mask.Set(id)
		}
	} else {
		p.names = names
	}
	x.presences[hash] = append(x.presences[hash], p)
	return p
}
