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

package disruption

import (
	"context"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// Piece is one command's share of a joint simulation: its candidates and the part of the Results their pods need.
type Piece struct {
	Candidates []*Candidate
	Results    scheduling.Results
}

// Split breaks the Results of one joint simulation into independent pieces. Candidates whose pods share a new
// NodeClaim are in the same piece; sharing an existing node doesn't connect them, because the simulation already
// accounted for all of their pods on it. Each piece carries the new NodeClaims and the existing nodes its candidates'
// pods were scheduled to. New NodeClaims and existing nodes holding none of the candidates' pods (pending pods and pods
// of nodes already being disrupted) go to the first piece; with DropPendingOnly, new NodeClaims that hold only pending
// pods go nowhere. Pieces are in candidate order: a piece's position is that of its first candidate.
//
//nolint:gocyclo
func Split(candidates []*Candidate, results scheduling.Results, pending PendingClaims) []Piece {
	owner := ownerIndex(candidates)
	parent := lo.SliceToMap(candidates, func(c *Candidate) (string, string) { return c.ProviderID(), c.ProviderID() })
	var find func(string) string
	find = func(n string) string {
		if parent[n] != n {
			parent[n] = find(parent[n])
		}
		return parent[n]
	}
	owners := func(pods []*corev1.Pod) []string {
		return lo.Uniq(lo.FilterMap(pods, func(p *corev1.Pod, _ int) (string, bool) { o, ok := owner[p.UID]; return o, ok }))
	}
	claimOwner := make([]string, len(results.NewNodeClaims))
	for i, nc := range results.NewNodeClaims {
		os := owners(nc.Pods)
		for _, o := range os[min(1, len(os)):] {
			parent[find(o)] = find(os[0])
		}
		if len(os) > 0 {
			claimOwner[i] = os[0]
		}
	}
	var roots []string
	pieces := map[string]*Piece{}
	for _, c := range candidates {
		r := find(c.ProviderID())
		if _, ok := pieces[r]; !ok {
			roots = append(roots, r)
			pieces[r] = &Piece{Results: scheduling.Results{DRAClaimAllocationMetadata: results.DRAClaimAllocationMetadata}}
		}
		pieces[r].Candidates = append(pieces[r].Candidates, c)
	}
	if len(roots) == 0 {
		return nil
	}
	for i, nc := range results.NewNodeClaims {
		switch {
		case claimOwner[i] != "":
			p := pieces[find(claimOwner[i])]
			p.Results.NewNodeClaims = append(p.Results.NewNodeClaims, nc)
		case pending == KeepOnFirst || (pending == DropPendingOnly && !lo.EveryBy(nc.Pods, pod.IsProvisionable)):
			p := pieces[roots[0]]
			p.Results.NewNodeClaims = append(p.Results.NewNodeClaims, nc)
		}
	}
	// Each piece nominates the existing nodes its own pods go to, so a piece that fails to start doesn't drop the
	// nominations another piece relies on.
	for _, en := range results.ExistingNodes {
		rs := lo.Uniq(lo.Map(owners(en.Pods), func(o string, _ int) string { return find(o) }))
		if len(rs) == 0 {
			rs = roots[:1]
		}
		for _, r := range rs {
			pieces[r].Results.ExistingNodes = append(pieces[r].Results.ExistingNodes, en)
		}
	}
	return lo.Map(roots, func(r string, _ int) Piece { return *pieces[r] })
}

// CapPieces bounds the blast radius of a split batch: no piece may hold more than ComponentCap candidates. When the
// joint simulation produced a larger piece, it is not used. The batch is planned again in chunks of at most
// ComponentCap candidates, in order, each simulated on its own:
//   - every candidate of the batch and the existing nodes the earlier chunks' pods go to are excluded as destinations,
//     so no two chunks rely on the same capacity, and the candidates of later chunks stay where they are, as they will
//     if those chunks don't start;
//   - NodePool limits count the excluded nodes that stay and the earlier chunks' new NodeClaims (at their largest
//     instance type), which the simulation can't see;
//   - the first chunk also schedules the pending pods and the pods of nodes already being disrupted; later chunks
//     leave those out, so they neither plan capacity for them again nor rely on capacity the first chunk planned for
//     them.
//
// Chunking stops at the first chunk whose own pods don't all schedule, whose simulation fails or runs past ctx, or that
// shares a topology domain with an earlier chunk (see topologyCoupled): no simulation of it could see what the earlier
// chunks do to that domain. It also stops after a chunk whose new NodeClaims may launch into a capacity reservation,
// whose remaining count later simulations can't see either. The candidates of that chunk and every later one are
// returned as deferred.
//
//nolint:gocyclo
func CapPieces(ctx context.Context, sim Simulator, opts SimOptions, batch []*Candidate, pieces []Piece, pending PendingClaims) (capped []Piece, deferred []*Candidate, chunked bool) {
	if lo.EveryBy(pieces, func(p Piece) bool { return len(p.Candidates) <= ComponentCap }) {
		return pieces, nil, false
	}
	// No chunk may move pods onto a candidate of the batch: a later chunk may delete it.
	exclude := sets.New(lo.Map(batch, func(c *Candidate, _ int) string { return c.ProviderID() })...)
	destinations := map[string]*state.StateNode{}
	var newNodeClaims []*scheduling.NodeClaim
	var emitted []*corev1.Pod
	for start := 0; start < len(batch); start += ComponentCap {
		end := min(start+ComponentCap, len(batch))
		chunk := batch[start:end]
		own := lo.FlatMap(chunk, func(c *Candidate, _ int) []*corev1.Pod { return c.reschedulablePods })
		if len(capped) > 0 && topologyCoupled(own, emitted) {
			return capped, batch[start:], true
		}
		chunkOpts := opts
		chunkOpts.ExcludeDestinations = exclude.Union(opts.ExcludeDestinations)
		chunkOpts.LimitsUsed = limitsUsed(opts.LimitsUsed, batch[end:], lo.Values(destinations), newNodeClaims)
		chunkOpts.SkipDeletingNodePods = len(capped) > 0
		o, err := sim.Simulate(ctx, chunkOpts, chunk...)
		if err != nil || !o.Results.AllNonPendingPodsScheduled() {
			return capped, batch[start:], true
		}
		for _, p := range Split(chunk, o.Results, lo.Ternary(len(capped) == 0, pending, dropUnowned)) {
			capped = append(capped, p)
			for _, en := range p.Results.ExistingNodes {
				exclude.Insert(en.ProviderID())
				destinations[en.ProviderID()] = en.StateNode
			}
			newNodeClaims = append(newNodeClaims, p.Results.NewNodeClaims...)
		}
		emitted = append(emitted, own...)
		if start == 0 {
			// The pods of nodes already being disrupted move with the first chunk.
			emitted = append(emitted, lo.Filter(scheduledPodsOf(o.Results), func(p *corev1.Pod, _ int) bool { return !pod.IsProvisionable(p) })...)
		}
		if lo.SomeBy(newNodeClaims, mayLaunchReserved) {
			return capped, batch[end:], true
		}
	}
	return capped, nil, true
}

// limitsUsed is base plus, by NodePool, the capacity of nodes that stay and of new NodeClaims at their largest instance
// type (as the scheduler counts its own new NodeClaims).
func limitsUsed(base map[string]corev1.ResourceList, staying []*Candidate, destinations []*state.StateNode, newNodeClaims []*scheduling.NodeClaim) map[string]corev1.ResourceList {
	used := lo.MapValues(base, func(rl corev1.ResourceList, _ string) corev1.ResourceList { return rl.DeepCopy() })
	add := func(np string, rl corev1.ResourceList) { used[np] = resources.Merge(used[np], rl) }
	for _, c := range staying {
		add(c.NodePool.Name, c.Capacity())
	}
	for _, n := range destinations {
		if np, ok := n.Labels()[v1.NodePoolLabelKey]; ok {
			add(np, n.Capacity())
		}
	}
	for _, nc := range newNodeClaims {
		add(nc.NodePoolName, resources.MaxResources(lo.Map(nc.InstanceTypeOptions, func(it *cloudprovider.InstanceType, _ int) corev1.ResourceList { return it.Capacity })...))
	}
	return used
}

// mayLaunchReserved reports whether a new NodeClaim may launch into a capacity reservation.
func mayLaunchReserved(nc *scheduling.NodeClaim) bool {
	return lo.SomeBy(nc.InstanceTypeOptions, func(it *cloudprovider.InstanceType) bool {
		return lo.SomeBy(it.Offerings.Available().Compatible(nc.Requirements), func(o *cloudprovider.Offering) bool {
			return o.CapacityType() == v1.CapacityTypeReserved
		})
	})
}

// scheduledPodsOf returns the pods the Results place on existing nodes and new NodeClaims.
func scheduledPodsOf(r scheduling.Results) []*corev1.Pod {
	return append(lo.FlatMap(r.ExistingNodes, func(en *scheduling.ExistingNode, _ int) []*corev1.Pod { return en.Pods }),
		lo.FlatMap(r.NewNodeClaims, func(nc *scheduling.NodeClaim, _ int) []*corev1.Pod { return nc.Pods })...)
}

func ownerIndex(candidates []*Candidate) map[types.UID]string {
	owner := map[types.UID]string{}
	for _, c := range candidates {
		for _, p := range c.reschedulablePods {
			owner[p.UID] = c.ProviderID()
		}
	}
	return owner
}

// topologyCoupled reports whether moving the pods of a and of b can affect each other's placement through a
// topology domain other than the node: a topology spread constraint or a pod (anti-)affinity term of a pod in one set,
// with a topology key other than the hostname, selects a pod in the other set. Pods using DRA claims are coupled to
// any other pod using DRA claims, because they may share devices. It over-approximates: it ignores matchLabelKeys
// and treats terms with a namespace selector as selecting every namespace. Hostname domains need no check, because
// pieces never share a node.
func topologyCoupled(a, b []*corev1.Pod) bool {
	if lo.SomeBy(a, usesClaims) && lo.SomeBy(b, usesClaims) {
		return true
	}
	return selectsAny(a, b) || selectsAny(b, a)
}

func usesClaims(p *corev1.Pod) bool { return len(p.Spec.ResourceClaims) > 0 }

// selectsAny reports whether a topology term (other than hostname-keyed) of a pod in from selects a pod in to.
func selectsAny(from, to []*corev1.Pod) bool {
	for _, p := range from {
		for _, t := range domainTerms(p) {
			if lo.SomeBy(to, t.selects) {
				return true
			}
		}
	}
	return false
}

type domainTerm struct {
	selector   labels.Selector
	namespaces sets.Set[string] // nil: every namespace
}

func (t domainTerm) selects(p *corev1.Pod) bool {
	return (t.namespaces == nil || t.namespaces.Has(p.Namespace)) && t.selector.Matches(labels.Set(p.Labels))
}

func domainTerms(p *corev1.Pod) []domainTerm {
	var terms []domainTerm
	add := func(key string, sel *metav1.LabelSelector, namespaces sets.Set[string]) {
		if key == corev1.LabelHostname {
			return
		}
		s, err := metav1.LabelSelectorAsSelector(sel)
		if err != nil {
			s = labels.Everything()
		}
		terms = append(terms, domainTerm{selector: s, namespaces: namespaces})
	}
	for _, tsc := range p.Spec.TopologySpreadConstraints {
		add(tsc.TopologyKey, tsc.LabelSelector, sets.New(p.Namespace))
	}
	affinityTerm := func(t corev1.PodAffinityTerm) {
		var namespaces sets.Set[string]
		if t.NamespaceSelector == nil {
			namespaces = sets.New(t.Namespaces...)
			if len(t.Namespaces) == 0 {
				namespaces.Insert(p.Namespace)
			}
		}
		add(t.TopologyKey, t.LabelSelector, namespaces)
	}
	if aff := p.Spec.Affinity; aff != nil {
		if aff.PodAffinity != nil {
			lo.ForEach(aff.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution, func(t corev1.PodAffinityTerm, _ int) { affinityTerm(t) })
			lo.ForEach(aff.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution, func(t corev1.WeightedPodAffinityTerm, _ int) { affinityTerm(t.PodAffinityTerm) })
		}
		if aff.PodAntiAffinity != nil {
			lo.ForEach(aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, func(t corev1.PodAffinityTerm, _ int) { affinityTerm(t) })
			lo.ForEach(aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, func(t corev1.WeightedPodAffinityTerm, _ int) { affinityTerm(t.PodAffinityTerm) })
		}
	}
	return terms
}
