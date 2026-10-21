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
	"fmt"
	"math"
	"slices"

	"github.com/awslabs/operatorpkg/option"
	"github.com/awslabs/operatorpkg/serrors"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
)

type legacyTopology struct {
	kubeClient       client.Client
	preferencePolicy PreferencePolicy
	// Both the topologyGroups and inverseTopologies are maps of the hash from legacyTopologyGroup.Hash() to the topology group
	// itself. This is used to allow us to store one topology group that tracks the topology of many pods instead of
	// having a 1<->1 mapping between topology groups and pods owned/selected by that group.
	topologyGroups map[uint64]*legacyTopologyGroup
	// Anti-affinity works both ways (if a zone has a pod foo with anti-affinity to a pod bar, we can't schedule bar to
	// that zone, even though bar has no anti affinity terms on it. For this to work, we need to separately track the
	// topologies of pods with anti-affinity terms, so we can prevent scheduling the pods they have anti-affinity to
	// in some cases.
	inverseTopologyGroups map[uint64]*legacyTopologyGroup
	// topologyIndex and inverseTopologyIndex index the groups in topologyGroups and inverseTopologyGroups by owner and
	// by selector, so per-pod lookups don't need to scan every group.
	topologyIndex        *topologyGroupIndex[*legacyTopologyGroup]
	inverseTopologyIndex *topologyGroupIndex[*legacyTopologyGroup]
	// The universe of domains by topology key. Only the keys of topology spread constraints and pod (anti-)affinity
	// terms are ever read, so each key's domains are built from the domainSources the first time it is read.
	domainGroups  map[string]TopologyDomainGroup
	domainSources []domainSource
	// excludedPods are the pod UIDs of pods that are excluded from counting.  This is used so we can simulate
	// moving pods to prevent them from being double counted.
	excludedPods sets.Set[string]
	cluster      *state.Cluster
	stateNodes   []*state.StateNode
	// stateNodeLabels and nodes memoize the per-node work that countDomains would otherwise repeat for every topology
	// group. stateNodeLabels is parallel to stateNodes and holds StateNode.Labels(), populated on first use. nodes holds
	// the nodes that existing pods are bound to, keyed by node name, with a nil entry for nodes that were not found. They
	// are only accessed from countDomains, which runs from Update and is never called concurrently.
	stateNodeLabels []map[string]string
	nodes           map[string]*topologyNode
	// domainOrder, if set, returns the order hook of each new topology group (see legacyTopologyGroup.order)
	domainOrder func(key string, hash uint64, inverse bool) func(domains []string)
}

// topologyNode is the part of a node that countDomains reads
type topologyNode struct {
	name   string
	labels map[string]string
	taints []corev1.Taint
}

func newLegacyTopology(
	ctx context.Context,
	kubeClient client.Client,
	cluster *state.Cluster,
	stateNodes []*state.StateNode,
	nodePools []*v1.NodePool,
	instanceTypes map[string][]*cloudprovider.InstanceType,
	pods []*corev1.Pod,
	opts *options,
) (*legacyTopology, error) {
	t := &legacyTopology{
		kubeClient:            kubeClient,
		preferencePolicy:      opts.preferencePolicy,
		domainOrder:           opts.domainOrder,
		cluster:               cluster,
		stateNodes:            stateNodes,
		domainGroups:          map[string]TopologyDomainGroup{},
		domainSources:         newDomainSources(nodePools, instanceTypes),
		topologyGroups:        map[uint64]*legacyTopologyGroup{},
		inverseTopologyGroups: map[uint64]*legacyTopologyGroup{},
		topologyIndex:         newTopologyGroupIndex[*legacyTopologyGroup](),
		inverseTopologyIndex:  newTopologyGroupIndex[*legacyTopologyGroup](),
		excludedPods:          sets.New[string](),
		nodes:                 map[string]*topologyNode{},
	}

	// these are the pods that we intend to schedule, so if they are currently in the cluster we shouldn't count them for
	// topology purposes
	for _, p := range pods {
		t.excludedPods.Insert(string(p.UID))
	}

	errs := t.updateInverseAffinities(ctx)
	for i := range pods {
		errs = multierr.Append(errs, t.Update(ctx, pods[i]))
	}
	if errs != nil {
		return nil, errs
	}
	return t, nil
}

// domainGroup returns the universe of domains for the topology key, building it on first use. Like the rest of the
// legacyTopology's state, this isn't safe for concurrent use; it's only reached from NewTopology and Update.
func (t *legacyTopology) domainGroup(topologyKey string) TopologyDomainGroup {
	if domainGroup, ok := t.domainGroups[topologyKey]; ok {
		return domainGroup
	}
	domainGroup := NewTopologyDomainGroup()
	for _, source := range t.domainSources {
		source.insertDomains(domainGroup, topologyKey)
	}
	t.domainGroups[topologyKey] = domainGroup
	return domainGroup
}

// setOrder sets the order hook of a new topology group or inverse topology group
func (t *legacyTopology) setOrder(tg *legacyTopologyGroup, hash uint64, inverse bool) {
	if t.domainOrder != nil {
		tg.order = t.domainOrder(tg.Key, hash, inverse)
	}
}

// topologyError allows lazily generating the error string in the topology error.  If a pod fails to schedule, most often
// we are only interested in the fact that it failed to schedule and not why.
type topologyError struct {
	topology    *legacyTopologyGroup
	podDomains  *scheduling.Requirement
	nodeDomains *scheduling.Requirement
}

func (t topologyError) Error() string {
	return fmt.Sprintf("unsatisfiable topology constraint for %s, key=%s (counts = %s, podDomains = %v, nodeDomains = %v", t.topology.Type, t.topology.Key,
		pretty.Map(t.topology.domains, 25), t.podDomains, t.nodeDomains)
}

// Update unregisters the pod as the owner of all affinities and then creates any new topologies based on the pod spec
// registered the pod as the owner of all associated affinities, new or old.  This allows Update() to be called after
// relaxation of a preference to properly break the topology <-> owner relationship so that the preferred topology will
// no longer influence scheduling.
func (t *legacyTopology) Update(ctx context.Context, p *corev1.Pod) error {
	t.topologyIndex.removeOwner(p.UID)

	if (t.preferencePolicy == PreferencePolicyIgnore && pod.HasRequiredPodAntiAffinity(p)) ||
		(t.preferencePolicy == PreferencePolicyRespect && pod.HasPodAntiAffinity(p)) {
		if err := t.updateInverseAntiAffinity(ctx, p, nil); err != nil {
			return fmt.Errorf("updating inverse anti-affinities, %w", err)
		}
	}

	topologies := t.newForTopologies(p)
	affinities, err := t.newForAffinities(ctx, p)
	if err != nil {
		return fmt.Errorf("updating affinities, %w", err)
	}

	for _, tg := range append(topologies, affinities...) {
		hash := tg.Hash()
		// Avoid populating and recomputing topology counts if we've already seen this group
		if existing, ok := t.topologyGroups[hash]; !ok {
			tg.populate(p, t.domainGroup(tg.Key))
			t.setOrder(tg, hash, false)
			if err := t.countDomains(ctx, tg); err != nil {
				return err
			}
			t.topologyGroups[hash] = tg
			t.topologyIndex.add(tg)
		} else {
			tg = existing
		}
		t.topologyIndex.addOwner(tg, p.UID)
	}
	return nil
}

// Record records the topology changes given that pod p schedule on a node with the given requirements
func (t *legacyTopology) Record(p *corev1.Pod, taints []corev1.Taint, requirements scheduling.Requirements, compatibilityOptions ...option.Function[scheduling.CompatibilityOptions]) {
	// once we've committed to a domain, we record the usage in every topology that cares about it
	t.topologyIndex.forEachCandidate(p, func(tg *legacyTopologyGroup) {
		if tg.Counts(p, taints, requirements, compatibilityOptions...) {
			domains := requirements.Get(tg.Key)
			if tg.Type == TopologyTypePodAntiAffinity {
				// for anti-affinity topologies we need to block out all possible domains that the pod could land in
				tg.Record(domains.Values()...)
			} else {
				// but for affinity & topology spread, we can only record the domain if we know the specific domain we land in
				if domains.Len() == 1 {
					tg.Record(domains.Values()[0])
				}
			}
		}
	})
	// for anti-affinities, we record where the pods could be, even if
	// requirements haven't collapsed to a single value.
	for _, tg := range t.inverseTopologyIndex.ownedBy(p.UID) {
		tg.Record(requirements.Get(tg.Key).Values()...)
	}
}

// AddRequirements tightens the input requirements by adding additional requirements that are being enforced by topology spreads
// affinities, anti-affinities or inverse anti-affinities.  The nodeHostname is the hostname that we are currently considering
// placing the pod on.  It returns these newly tightened requirements, or an error in the case of a set of requirements that
// cannot be satisfied.
func (t *legacyTopology) AddRequirements(p *corev1.Pod, taints []corev1.Taint, podRequirements, nodeRequirements scheduling.Requirements, compatibilityOptions ...option.Function[scheduling.CompatibilityOptions]) (scheduling.Requirements, error) {
	requirements := scheduling.NewRequirements(nodeRequirements.Values()...)
	for _, topology := range t.getMatchingTopologies(p, taints, nodeRequirements, compatibilityOptions...) {
		podDomains := scheduling.NewRequirement(topology.Key, corev1.NodeSelectorOpExists)
		if podRequirements.Has(topology.Key) {
			podDomains = podRequirements.Get(topology.Key)
		}
		nodeDomains := scheduling.NewRequirement(topology.Key, corev1.NodeSelectorOpExists)
		if nodeRequirements.Has(topology.Key) {
			nodeDomains = nodeRequirements.Get(topology.Key)
		}
		domains, _ := topology.Get(p, podDomains, nodeDomains)
		if domains.Len() == 0 {
			return nil, topologyError{
				topology:    topology,
				podDomains:  podDomains,
				nodeDomains: nodeDomains,
			}
		}
		requirements.Add(domains)
	}
	return requirements, nil
}

// GetTopologyZoneConstraints returns the set of valid zones from all topology constraints
// that use the zone topology key for the given pod, along with whether the constraints are satisfiable.
func (t *legacyTopology) GetTopologyZoneConstraints(p *corev1.Pod, podRequirements scheduling.Requirements) (sets.Set[string], bool) {
	var result sets.Set[string]

	for _, topology := range t.topologyIndex.ownedBy(p.UID) {
		if topology.Key != corev1.LabelTopologyZone {
			continue
		}

		podDomains := scheduling.NewRequirement(topology.Key, corev1.NodeSelectorOpExists)
		if podRequirements.Has(topology.Key) {
			podDomains = podRequirements.Get(topology.Key)
		}
		nodeDomains := scheduling.NewRequirement(topology.Key, corev1.NodeSelectorOpExists)
		_, validDomains := topology.Get(p, podDomains, nodeDomains)

		if validDomains.Len() == 0 {
			return nil, false
		}
		if result == nil {
			result = validDomains
		} else {
			for zone := range result {
				if !validDomains.Has(zone) {
					delete(result, zone)
				}
			}
		}
	}
	return result, true
}

// Register is used to register a domain as available across topologies for the given topology key.
func (t *legacyTopology) Register(topologyKey string, domain string) {
	for _, tg := range t.topologyGroups {
		if tg.Key == topologyKey {
			tg.Register(domain)
		}
	}
	for _, tg := range t.inverseTopologyGroups {
		if tg.Key == topologyKey {
			tg.Register(domain)
		}
	}
}

// BeginPod does nothing; the legacy topology has no per-pod plan.
func (t *legacyTopology) BeginPod(*corev1.Pod, scheduling.Requirements) {}

// seal does nothing; the legacy topology keeps its state.
func (t *legacyTopology) seal() {}

// Unregister is used to unregister a domain as available across topologies for the given topology key.
func (t *legacyTopology) Unregister(topologyKey string, domain string) {
	for _, topology := range t.topologyGroups {
		if topology.Key == topologyKey {
			topology.Unregister(domain)
		}
	}
	for _, topology := range t.inverseTopologyGroups {
		if topology.Key == topologyKey {
			topology.Unregister(domain)
		}
	}
}

// updateInverseAffinities is used to identify pods with anti-affinity terms so we can track those topologies.  We
// have to look at every pod in the cluster as there is no way to query for a pod with anti-affinity terms.
func (t *legacyTopology) updateInverseAffinities(ctx context.Context) error {
	var errs error
	t.cluster.ForPodsWithAntiAffinity(func(pod *corev1.Pod, node *corev1.Node) bool {
		// don't count the pod we are excluding
		if t.excludedPods.Has(string(pod.UID)) {
			return true
		}
		if err := t.updateInverseAntiAffinity(ctx, pod, node.Labels); err != nil {
			errs = multierr.Append(errs, fmt.Errorf("tracking existing pod anti-affinity, %w", err))
		}
		return true
	})
	return errs
}

// updateInverseAntiAffinity is used to track topologies of inverse anti-affinities. Here the domains & counts track the
// pods with the anti-affinity.
func (t *legacyTopology) updateInverseAntiAffinity(ctx context.Context, pod *corev1.Pod, domains map[string]string) error {
	// We intentionally don't track inverse anti-affinity preferences. We're not
	// required to enforce them so it just adds complexity for very little
	// value.  The problem with them comes from the relaxation process, the pod
	// we are relaxing is not the pod with the anti-affinity term.
	for _, term := range pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
		namespaces, err := t.buildNamespaceList(ctx, pod.Namespace, term.Namespaces, term.NamespaceSelector)
		if err != nil {
			return err
		}

		tg := newLegacyTopologyGroupIdentity(TopologyTypePodAntiAffinity, term.TopologyKey, pod, namespaces, term.LabelSelector, math.MaxInt32, nil, nil, nil)

		hash := tg.Hash()
		if existing, ok := t.inverseTopologyGroups[hash]; !ok {
			tg.populate(pod, t.domainGroup(tg.Key))
			t.setOrder(tg, hash, true)
			t.inverseTopologyGroups[hash] = tg
			t.inverseTopologyIndex.add(tg)
		} else {
			tg = existing
		}
		if domain, ok := domains[tg.Key]; ok {
			tg.Record(domain)
		}
		t.inverseTopologyIndex.addOwner(tg, pod.UID)
	}
	return nil
}

// countDomains initializes the topology group by registereding any well known domains and performing pod counts
// against the cluster for any existing pods.
//
//nolint:gocyclo
func (t *legacyTopology) countDomains(ctx context.Context, tg *legacyTopologyGroup) error {
	podList := &corev1.PodList{}

	// collect the pods from all the specified namespaces (don't see a way to query multiple namespaces
	// simultaneously)
	var pods []corev1.Pod
	for _, ns := range tg.namespaces.UnsortedList() {
		if err := t.kubeClient.List(ctx, podList, TopologyListOptions(ns, tg.rawSelector)); err != nil {
			return fmt.Errorf("listing pods, %w", err)
		}
		pods = append(pods, podList.Items...)
	}

	// capture new domain values from existing nodes that may not have any pods selected by the topology group
	// scheduled to them already
	// Note: long term we should handle this when constructing the domain groups, but that would require domain groups
	// to handle affinity in addition to taints / tolerations.
	if t.stateNodeLabels == nil {
		t.stateNodeLabels = make([]map[string]string, len(t.stateNodes))
		for i, n := range t.stateNodes {
			if n.Node != nil {
				t.stateNodeLabels[i] = n.Labels()
			}
		}
	}
	nodeFilter := newTopologyNodeFilterMemo(tg.nodeFilter)
	for i, n := range t.stateNodes {
		// ignore state nodes which are tracking in-flight NodeClaims
		if n.Node == nil {
			continue
		}
		// ignore the node if it doesn't match the topology group
		if !nodeFilter.Matches(n.Node.Labels, n.Node.Spec.Taints) {
			continue
		}
		domain, exists := t.stateNodeLabels[i][tg.Key]
		if !exists {
			continue
		}
		if _, ok := tg.domains[domain]; !ok {
			tg.domains[domain] = 0
			tg.emptyDomains.Insert(domain)
		}
	}

	for i, p := range pods {
		if IgnoredForTopology(&pods[i]) {
			continue
		}
		// pod is excluded for counting purposes
		if t.excludedPods.Has(string(p.UID)) {
			continue
		}
		n, err := t.getNode(ctx, p.Spec.NodeName)
		if err != nil {
			return err
		}
		// Pods that cannot be evicted can be leaked in the API Server after
		// a Node is removed. Since pod bindings are immutable, these pods
		// cannot be recovered, and will be deleted by the pod lifecycle
		// garbage collector. These pods are not running, and should not
		// impact future topology calculations.
		if n == nil {
			continue
		}
		domain, ok := n.labels[tg.Key]
		// Kubelet sets the hostname label, but the node may not be ready yet so there is no label.  We fall back and just
		// treat the node name as the label.  It probably is in most cases, but even if not we at least count the existence
		// of the pods in some domain, even if not in the correct one.  This is needed to handle the case of pods with
		// self-affinity only fulfilling that affinity if all domains are empty.
		if !ok && tg.Key == corev1.LabelHostname {
			domain = n.name
			ok = true
		}
		if !ok {
			continue // Don't include pods if node doesn't contain domain https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/#conventions
		}

		// nodes may or may not be considered for counting purposes for topology spread constraints depending on if they
		// are selected by the pod's node selectors and required node affinities.  If these are unset, the node always counts.
		if !nodeFilter.Matches(n.labels, n.taints) {
			continue
		}
		tg.Record(domain)
	}
	return nil
}

// getNode returns the named node, or nil if the node doesn't exist. Results are memoized for the lifetime of the legacyTopology
// so that each node is fetched at most once across all topology groups.
func (t *legacyTopology) getNode(ctx context.Context, name string) (*topologyNode, error) {
	if n, ok := t.nodes[name]; ok {
		return n, nil
	}
	node := &corev1.Node{}
	if err := t.kubeClient.Get(ctx, types.NamespacedName{Name: name}, node); err != nil {
		if errors.IsNotFound(err) {
			t.nodes[name] = nil
			return nil, nil
		}
		return nil, serrors.Wrap(fmt.Errorf("getting node, %w", err), "Node", klog.KRef("", name))
	}
	n := &topologyNode{name: node.Name, labels: node.Labels, taints: node.Spec.Taints}
	t.nodes[name] = n
	return n, nil
}

// newForTopologies returns the identities of the topology groups for the input pod's topology spread constraints. They
// must be populated before use.
func (t *legacyTopology) newForTopologies(p *corev1.Pod) []*legacyTopologyGroup {
	var topologyGroups []*legacyTopologyGroup
	for _, tsc := range p.Spec.TopologySpreadConstraints {
		if t.preferencePolicy == PreferencePolicyIgnore && tsc.WhenUnsatisfiable != corev1.DoNotSchedule {
			continue
		}
		// tsc.LabelSelector points into the pod spec, which may be shared (e.g. across virtual pods) and is re-read on
		// every Update, so the matchLabelKeys requirements are added to a copy rather than appended in place.
		selector := tsc.LabelSelector
		for _, key := range tsc.MatchLabelKeys {
			if value, ok := p.Labels[key]; ok {
				if selector == tsc.LabelSelector {
					selector = tsc.LabelSelector.DeepCopy()
				}
				selector.MatchExpressions = append(selector.MatchExpressions, metav1.LabelSelectorRequirement{
					Key:      key,
					Operator: metav1.LabelSelectorOpIn,
					Values:   []string{value},
				})
			}
		}
		topologyGroups = append(topologyGroups, newLegacyTopologyGroupIdentity(
			TopologyTypeSpread,
			tsc.TopologyKey,
			p,
			sets.New(p.Namespace),
			selector,
			tsc.MaxSkew,
			tsc.MinDomains,
			tsc.NodeTaintsPolicy,
			tsc.NodeAffinityPolicy,
		))
	}
	return topologyGroups
}

// newForAffinities returns the identities of the topology groups for the input pod's required/preferred affinity terms.
// They must be populated before use.
func (t *legacyTopology) newForAffinities(ctx context.Context, p *corev1.Pod) ([]*legacyTopologyGroup, error) {
	var topologyGroups []*legacyTopologyGroup
	// No affinity defined
	if p.Spec.Affinity == nil {
		return topologyGroups, nil
	}
	affinityTerms := map[TopologyType][]corev1.PodAffinityTerm{}

	// include both soft and hard affinity terms
	if p.Spec.Affinity.PodAffinity != nil {
		affinityTerms[TopologyTypePodAffinity] = append(affinityTerms[TopologyTypePodAffinity], p.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution...)
		if t.preferencePolicy == PreferencePolicyRespect {
			for _, term := range p.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
				affinityTerms[TopologyTypePodAffinity] = append(affinityTerms[TopologyTypePodAffinity], term.PodAffinityTerm)
			}
		}
	}

	// include both soft and hard antiaffinity terms
	if p.Spec.Affinity.PodAntiAffinity != nil {
		affinityTerms[TopologyTypePodAntiAffinity] = append(affinityTerms[TopologyTypePodAntiAffinity], p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution...)
		if t.preferencePolicy == PreferencePolicyRespect {
			for _, term := range p.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
				affinityTerms[TopologyTypePodAntiAffinity] = append(affinityTerms[TopologyTypePodAntiAffinity], term.PodAffinityTerm)
			}
		}
	}

	// build topologies
	for topologyType, terms := range affinityTerms {
		for _, term := range terms {
			namespaces, err := t.buildNamespaceList(ctx, p.Namespace, term.Namespaces, term.NamespaceSelector)
			if err != nil {
				return nil, err
			}
			topologyGroups = append(topologyGroups, newLegacyTopologyGroupIdentity(topologyType, term.TopologyKey, p, namespaces, term.LabelSelector, math.MaxInt32, nil, nil, nil))
		}
	}
	return topologyGroups, nil
}

// buildNamespaceList constructs a unique list of namespaces consisting of the pod's namespace and the optional list of
// namespaces and those selected by the namespace selector
func (t *legacyTopology) buildNamespaceList(ctx context.Context, namespace string, namespaces []string, selector *metav1.LabelSelector) (sets.Set[string], error) {
	if len(namespaces) == 0 && selector == nil {
		return sets.New(namespace), nil
	}
	if selector == nil {
		return sets.New(namespaces...), nil
	}
	var namespaceList corev1.NamespaceList
	labelSelector, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return nil, fmt.Errorf("parsing selector, %w", err)
	}
	if err := t.kubeClient.List(ctx, &namespaceList, &client.ListOptions{LabelSelector: labelSelector}); err != nil {
		return nil, fmt.Errorf("listing namespaces, %w", err)
	}
	selected := sets.New[string]()
	for _, namespace := range namespaceList.Items {
		selected.Insert(namespace.Name)
	}
	selected.Insert(namespaces...)
	return selected, nil
}

// getMatchingTopologies returns a sorted list of topologies that either control the scheduling of pod p, or for which
// the topology selects pod p and the scheduling of p affects the count per topology domain
func (t *legacyTopology) getMatchingTopologies(p *corev1.Pod, taints []corev1.Taint, requirements scheduling.Requirements, compatibilityOptions ...option.Function[scheduling.CompatibilityOptions]) []*legacyTopologyGroup {
	// clone so that appending can't write into the index's backing array, which is shared by concurrent callers
	matchingTopologies := slices.Clone(t.topologyIndex.ownedBy(p.UID))
	t.inverseTopologyIndex.forEachCandidate(p, func(tg *legacyTopologyGroup) {
		if tg.Counts(p, taints, requirements, compatibilityOptions...) {
			matchingTopologies = append(matchingTopologies, tg)
		}
	})
	return matchingTopologies
}
