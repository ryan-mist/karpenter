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
	"math/rand/v2"
	"os"

	"github.com/awslabs/operatorpkg/option"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/scheduling/topology"
	"sigs.k8s.io/karpenter/pkg/utils/pod"
)

// Topology tracks the topology spread constraints and pod (anti-)affinities of the pods being scheduled, along with
// the counts of the existing pods they select, and tightens the requirements of the nodes the pods schedule to so the
// constraints are met. It delegates to an implementation so that an implementation can be checked against the legacy
// one (topology_legacy.go), which is kept as a reference.
type Topology struct {
	impl topologyImpl
}

// topologyImpl is a topology implementation. AddRequirements is called concurrently from the scheduler's parallel
// rounds and must not mutate state; every other method is only called serially.
type topologyImpl interface {
	Update(ctx context.Context, p *corev1.Pod) error
	Record(p *corev1.Pod, taints []corev1.Taint, requirements scheduling.Requirements, compatibilityOptions ...option.Function[scheduling.CompatibilityOptions])
	AddRequirements(p *corev1.Pod, taints []corev1.Taint, podRequirements, nodeRequirements scheduling.Requirements, compatibilityOptions ...option.Function[scheduling.CompatibilityOptions]) (scheduling.Requirements, error)
	GetTopologyZoneConstraints(p *corev1.Pod, podRequirements scheduling.Requirements) (sets.Set[string], bool)
	Register(topologyKey string, domain string)
	Unregister(topologyKey string, domain string)
	BeginPod(p *corev1.Pod, podRequirements scheduling.Requirements)
	// seal ends the simulation: afterwards only errors from AddRequirements are rendered (topology errors are lazy), so
	// an implementation can release what it only needs while scheduling.
	seal()
}

func NewTopology(
	ctx context.Context,
	kubeClient client.Client,
	cluster *state.Cluster,
	stateNodes []*state.StateNode,
	nodePools []*v1.NodePool,
	instanceTypes map[string][]*cloudprovider.InstanceType,
	pods []*corev1.Pod,
	opts ...Options,
) (*Topology, error) {
	o := option.Resolve(opts...)
	report := shadowReporter(ctx)
	// tests compare after every call; log mode and sampled simulations on real clusters only compare decisions after
	// NewTopology (see shadowTopology.compareAll)
	compareAll := os.Getenv(TopologyShadowEnv) == "1"
	if report == nil && o.topologyShadowSample > 0 && rand.Float64() < o.topologyShadowSample {
		report = newShadowLogReporter(ctx)
	}
	var impl topologyImpl
	var err error
	switch {
	case report != nil:
		var shadow *shadowTopology
		if shadow, err = newShadowTopology(ctx, kubeClient, cluster, stateNodes, nodePools, instanceTypes, pods, o, report); err == nil {
			shadow.compareAll = compareAll
			impl = shadow
		}
	case o.topologyEngine == karpopts.TopologyEngineLegacy:
		impl, err = newLegacyTopology(ctx, kubeClient, cluster, stateNodes, nodePools, instanceTypes, pods, o)
	default:
		impl, err = newOverlayTopology(ctx, kubeClient, cluster, stateNodes, nodePools, instanceTypes, pods, o)
	}
	if err != nil {
		return nil, err
	}
	return &Topology{impl: impl}, nil
}

// Update unregisters the pod as the owner of all affinities and then creates any new topologies based on the pod spec
// registered the pod as the owner of all associated affinities, new or old.  This allows Update() to be called after
// relaxation of a preference to properly break the topology <-> owner relationship so that the preferred topology will
// no longer influence scheduling.
func (t *Topology) Update(ctx context.Context, p *corev1.Pod) error {
	return t.impl.Update(ctx, p)
}

// Record records the topology changes given that pod p schedule on a node with the given requirements
func (t *Topology) Record(p *corev1.Pod, taints []corev1.Taint, requirements scheduling.Requirements, compatibilityOptions ...option.Function[scheduling.CompatibilityOptions]) {
	t.impl.Record(p, taints, requirements, compatibilityOptions...)
}

// AddRequirements tightens the input requirements by adding additional requirements that are being enforced by topology spreads
// affinities, anti-affinities or inverse anti-affinities.  The nodeHostname is the hostname that we are currently considering
// placing the pod on.  It returns these newly tightened requirements, or an error in the case of a set of requirements that
// cannot be satisfied.
func (t *Topology) AddRequirements(p *corev1.Pod, taints []corev1.Taint, podRequirements, nodeRequirements scheduling.Requirements, compatibilityOptions ...option.Function[scheduling.CompatibilityOptions]) (scheduling.Requirements, error) {
	return t.impl.AddRequirements(p, taints, podRequirements, nodeRequirements, compatibilityOptions...)
}

// GetTopologyZoneConstraints returns the set of valid zones from all topology constraints
// that use the zone topology key for the given pod, along with whether the constraints are satisfiable.
func (t *Topology) GetTopologyZoneConstraints(p *corev1.Pod, podRequirements scheduling.Requirements) (sets.Set[string], bool) {
	return t.impl.GetTopologyZoneConstraints(p, podRequirements)
}

// Register is used to register a domain as available across topologies for the given topology key.
func (t *Topology) Register(topologyKey string, domain string) {
	t.impl.Register(topologyKey, domain)
}

// Unregister is used to unregister a domain as available across topologies for the given topology key.
func (t *Topology) Unregister(topologyKey string, domain string) {
	t.impl.Unregister(topologyKey, domain)
}

// seal releases what the topology only needs while scheduling, once the scheduler's Solve is done. The topology's
// errors can still be rendered; any other call is a bug, which panics in tests and is ignored otherwise.
func (t *Topology) seal() {
	t.impl.seal()
}

// BeginPod prepares the topology for the scheduler to try the pod, with its strict requirements, on candidate nodes
// in parallel. AddRequirements calls with the same requirements are cheaper afterwards; it isn't required.
func (t *Topology) BeginPod(p *corev1.Pod, podRequirements scheduling.Requirements) {
	t.impl.BeginPod(p, podRequirements)
}

// domainSource is a NodePool's contribution to the universe of domains: the domains its instance types can launch
// into, given the NodePool's requirements, along with the NodePool's taints.
type domainSource struct {
	// requirements are the NodePool's requirements and template labels
	requirements  scheduling.Requirements
	instanceTypes []*cloudprovider.InstanceType
	taints        []corev1.Taint
}

func newDomainSources(nodePools []*v1.NodePool, instanceTypes map[string][]*cloudprovider.InstanceType) []domainSource {
	nodePoolIndex := lo.SliceToMap(nodePools, func(np *v1.NodePool) (string, *v1.NodePool) {
		return np.Name, np
	})
	sources := make([]domainSource, 0, len(instanceTypes))
	for npName, its := range instanceTypes {
		np := nodePoolIndex[npName]
		requirements := scheduling.NewNodeSelectorRequirementsWithMinValues(np.Spec.Template.Spec.Requirements...)
		requirements.Add(scheduling.NewLabelRequirements(np.Spec.Template.Labels).Values()...)
		sources = append(sources, domainSource{requirements: requirements, instanceTypes: its, taints: np.Spec.Template.Spec.Taints})
	}
	return sources
}

// insertDomains inserts the NodePool's domains for the topology key into the domain group, each once with the
// NodePool's taints.
func (s domainSource) insertDomains(domainGroup TopologyDomainGroup, topologyKey string) {
	s.forEachDomain(topologyKey, func(domain string) {
		domainGroup.Insert(domain, s.taints...)
	})
}

// forEachDomain calls f once for each of the NodePool's domains for the topology key. For each instance type, the
// domains are the values of the instance type's requirement intersected with the NodePool's, or the values of whichever
// requirement exists if only one does. The values of a NodePool In requirement are always domains, even if the
// NodePool has no instance types.
func (s domainSource) forEachDomain(topologyKey string, f func(domain string)) {
	requirement := s.requirements[topologyKey]
	// If the NodePool's requirement is concrete, every instance type's domains are a subset of its values
	if requirement != nil && (requirement.Operator() == corev1.NodeSelectorOpIn || requirement.Operator() == corev1.NodeSelectorOpDoesNotExist) {
		for domain := range requirement.ValuesSeq() {
			f(domain)
		}
		return
	}
	domains := sets.New[string]()
	requirementOnly := false
	for _, it := range s.instanceTypes {
		itRequirement := it.Requirements[topologyKey]
		switch {
		case itRequirement == nil:
			requirementOnly = requirement != nil
		case requirement == nil:
			for domain := range itRequirement.ValuesSeq() {
				domains.Insert(domain)
			}
		default:
			for domain := range itRequirement.IntersectionValuesSeq(requirement) {
				domains.Insert(domain)
			}
		}
	}
	if requirementOnly {
		for domain := range requirement.ValuesSeq() {
			domains.Insert(domain)
		}
	}
	for domain := range domains {
		f(domain)
	}
}

// TopologyListOptions lists the pods in the namespace that a topology group with the label selector counts.
func TopologyListOptions(namespace string, labelSelector *metav1.LabelSelector) *client.ListOptions {
	return &client.ListOptions{Namespace: namespace, LabelSelector: topology.ListSelector(labelSelector)}
}

func IgnoredForTopology(p *corev1.Pod) bool {
	return !pod.IsScheduled(p) || pod.IsTerminal(p) || pod.IsTerminating(p)
}
