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

package pipeline

import (
	"hash/fnv"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// FootprintOf derives a command's footprint from its candidates, its replacements and their simulation results. Only
// the candidates' own pods count for destinations: the pods of nodes already being disrupted and pending pods in the
// same results don't. The pending pods that share a replacement with the candidates' pods are recorded. computedAt is the Ledger version read before the
// simulation read cluster state.
//
// Not derived yet: NewNodeClaims (the method cores split joint simulations themselves), Devices (needs a DRA allocator
// export) and Reservations (needs a ReservationManager export); Relate and the Ledger treat them as empty.
func FootprintOf(cmd disruption.Command, computedAt uint64) types.Footprint {
	own := sets.New[k8stypes.UID]()
	for _, c := range cmd.Candidates {
		for _, p := range c.ReschedulablePods() {
			own.Insert(p.UID)
		}
	}
	fp := types.Footprint{
		Removed:        sets.New[string](),
		Destinations:   sets.New[string](),
		TopologyGroups: sets.New[uint64](),
		BudgetNodes:    map[string]sets.Set[string]{},
		ComputedAt:     computedAt,
	}
	for _, c := range cmd.Candidates {
		fp.Removed.Insert(c.ProviderID())
		// Nodes that aren't initialized don't count against budgets (repair is the only method that disrupts them).
		if c.Initialized() {
			fp.BudgetNodes[c.NodePool.Name] = fp.BudgetNodes[c.NodePool.Name].Union(sets.New(c.ProviderID()))
		}
		for _, p := range c.ReschedulablePods() {
			fp.TopologyGroups.Insert(CouplingKeys(p)...)
		}
	}
	for _, n := range cmd.Results.ExistingNodes {
		if lo.SomeBy(n.Pods, func(p *corev1.Pod) bool { return own.Has(p.UID) }) {
			fp.Destinations.Insert(n.ProviderID())
		}
	}
	if len(cmd.Dependencies) > 0 {
		fp.Dependencies = sets.New(cmd.Dependencies...)
		fp.Destinations = fp.Destinations.Union(fp.Dependencies)
	}
	// The command's replacements are its new NodeClaims (a core has already dropped the ones that aren't its own).
	for _, r := range cmd.Replacements {
		if fp.LimitCost == nil {
			fp.LimitCost = map[string]corev1.ResourceList{}
		}
		// The most the NodeClaim can use of its NodePool's limits: its largest instance type option, and one node.
		capacity := resources.MaxResources(lo.Map(r.InstanceTypeOptions, func(it *cloudprovider.InstanceType, _ int) corev1.ResourceList { return it.Capacity })...)
		capacity[resources.Node] = *resources.Quantity("1")
		fp.LimitCost[r.NodePoolName] = resources.Merge(fp.LimitCost[r.NodePoolName], capacity)
		if r.IsStaticNodeClaim {
			if fp.StaticNodeCount == nil {
				fp.StaticNodeCount = map[string]int64{}
			}
			fp.StaticNodeCount[r.NodePoolName]++
		}
		if !lo.SomeBy(r.Pods, func(p *corev1.Pod) bool { return own.Has(p.UID) }) {
			continue
		}
		for _, p := range r.Pods {
			if !own.Has(p.UID) && p.Spec.NodeName == "" {
				if fp.PendingPods == nil {
					fp.PendingPods = sets.New[k8stypes.UID]()
				}
				fp.PendingPods.Insert(p.UID)
			}
		}
	}
	return fp
}

// AnyNamespace is the coupling key of a pod whose pod (anti-)affinity term selects pods by a namespaceSelector: any
// moved pod may be in a namespace it selects. Relate couples it with every footprint that moves pods.
var AnyNamespace = couplingKey("*")

func couplingKey(namespace string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(namespace))
	return h.Sum64()
}

// CouplingKeys are the hashes of the namespaces whose pods a pod's move can affect, or be affected by, through a
// non-hostname topology spread constraint or pod (anti-)affinity term: its own namespace (a spread constraint or term
// selects only pods of its namespace by default, and another pod's term may select it), the namespaces its terms list,
// and AnyNamespace for a term with a namespaceSelector. Hostname-keyed constraints add nothing beyond the own namespace:
// their conflicts are overlaps of Removed and Destinations.
//
// This over-approximates the scheduler's topology groups: it ignores label selectors and topology keys, so plans that
// move pods of the same namespace always couple and may be re-simulated more often than needed, never less.
func CouplingKeys(p *corev1.Pod) []uint64 {
	keys := sets.New(couplingKey(p.Namespace))
	if a := p.Spec.Affinity; a != nil {
		var terms []corev1.PodAffinityTerm
		if a.PodAffinity != nil {
			terms = append(terms, a.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution...)
			terms = append(terms, lo.Map(a.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution, func(t corev1.WeightedPodAffinityTerm, _ int) corev1.PodAffinityTerm { return t.PodAffinityTerm })...)
		}
		if a.PodAntiAffinity != nil {
			terms = append(terms, a.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution...)
			terms = append(terms, lo.Map(a.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, func(t corev1.WeightedPodAffinityTerm, _ int) corev1.PodAffinityTerm { return t.PodAffinityTerm })...)
		}
		for _, t := range terms {
			if t.TopologyKey == corev1.LabelHostname {
				continue
			}
			if t.NamespaceSelector != nil {
				keys.Insert(AnyNamespace)
			}
			keys.Insert(lo.Map(t.Namespaces, func(ns string, _ int) uint64 { return couplingKey(ns) })...)
		}
	}
	return keys.UnsortedList()
}

// PlanFrom is the plan for a command a method core computed. The planner sets the batch fields and ValidateAt.
// computedAt is the Ledger version read before the core simulated; now is the round's time; catalog gives a NodePool's
// catalog version (Snapshot.CatalogVersion).
func PlanFrom(cmd disruption.Command, computedAt uint64, now time.Time, catalog func(nodePool string) string) *Plan {
	id := uuid.New()
	p := &Plan{
		ID:                  id,
		BatchID:             id,
		Siblings:            1,
		Method:              cmd.Method,
		TerminateFirst:      cmd.TerminateFirstReason,
		Candidates:          cmd.Candidates,
		Replacements:        cmd.Replacements,
		Results:             cmd.Results,
		PoolDisruptionCosts: cmd.PoolDisruptionCosts,
		Footprint:           FootprintOf(cmd, computedAt),
		PodSetHash:          map[string]uint64{},
		CatalogVersions:     map[string]string{},
		ComputedAt:          now,
	}
	switch {
	case cmd.Reboot:
		p.Action = types.ActionReboot
	case len(cmd.Replacements) > 0:
		p.Action = types.ActionReplace
	default:
		p.Action = types.ActionDelete
	}
	for _, c := range cmd.Candidates {
		p.PodSetHash[c.ProviderID()] = PodSetHash(c)
		p.CatalogVersions[c.NodePool.Name] = catalog(c.NodePool.Name)
	}
	for _, r := range cmd.Replacements {
		p.CatalogVersions[r.NodePoolName] = catalog(r.NodePoolName)
	}
	return p
}
