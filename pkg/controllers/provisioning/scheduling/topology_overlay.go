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
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"

	"github.com/awslabs/operatorpkg/option"
	"github.com/go-logr/logr"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/topoindex"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/scheduling/topology"
	"sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
)

// overlayTopology is the topology implementation that replaces legacyTopology. Its decisions are the same, except for
// how ties between domains with equal counts are broken. The counts of existing pods come from a topology.CountSource
// and are only read: each group overlays them with the pods the simulation excludes and the pods it schedules.
type overlayTopology struct {
	kubeClient       client.Client
	preferencePolicy PreferencePolicy
	src              topology.CountSource
	excluded         *topology.ExcludedPods
	// seed seeds the order in which groups break ties between domains
	seed uint64
	// groups and inverseGroups are keyed by the group's hash, which identifies it. See legacyTopology.
	groups        map[uint64]*TopologyGroup
	inverseGroups map[uint64]*TopologyGroup
	index         *topologyGroupIndex[*TopologyGroup]
	inverseIndex  *topologyGroupIndex[*TopologyGroup]
	// universes are the domains of each topology key, built on first use from the domainSources
	universes     map[string]*keyUniverse
	domainSources []domainSource
	presence      *presenceIndex
	hosts         *hostLog
	// plans hold each pod's candidate-independent part of AddRequirements, built by BeginPod. version counts the
	// changes to the groups, so that a plan built before one isn't used.
	plans   map[types.UID]*podPlan
	version uint64
	// sealed is set once the simulation is done (see seal)
	sealed bool
	log    logr.Logger
}

// podPlan is the part of AddRequirements for a pod that doesn't depend on the candidate node: the groups that
// constrain the pod and their plan entries.
type podPlan struct {
	version         uint64
	podRequirements scheduling.Requirements
	entries         []planEntry
}

func newOverlayTopology(
	ctx context.Context,
	kubeClient client.Client,
	cluster *state.Cluster,
	stateNodes []*state.StateNode,
	nodePools []*v1.NodePool,
	instanceTypes map[string][]*cloudprovider.InstanceType,
	pods []*corev1.Pod,
	opts *options,
) (*overlayTopology, error) {
	t := &overlayTopology{
		kubeClient:       kubeClient,
		preferencePolicy: opts.preferencePolicy,
		// these are the pods that we intend to schedule, so if they are currently in the cluster we shouldn't count them for
		// topology purposes
		excluded:      topology.NewExcludedPods(pods),
		seed:          rand.Uint64(),
		groups:        map[uint64]*TopologyGroup{},
		inverseGroups: map[uint64]*TopologyGroup{},
		index:         newTopologyGroupIndex[*TopologyGroup](),
		inverseIndex:  newTopologyGroupIndex[*TopologyGroup](),
		universes:     map[string]*keyUniverse{},
		domainSources: newDomainSources(nodePools, instanceTypes),
		presence:      newPresenceIndex(stateNodes),
		hosts:         &hostLog{last: map[string]int{}},
		plans:         map[types.UID]*podPlan{},
		log:           log.FromContext(ctx),
	}
	if opts.topologySeed != nil {
		t.seed = *opts.topologySeed
	}
	t.src = newListCountSource(kubeClient, cluster, t.excluded)
	// with the TopologyIndex gate on, existing pods are counted from cluster state's topology index once it has synced
	if cluster != nil {
		if src, ok := topoindex.NewCountSource(cluster.TopologyIndex(), t.src); ok {
			if rate := karpopts.FromContext(ctx).TopologyIndexDifferentialSampleRate; rate > 0 && rand.Float64() < rate {
				src.Differential()
			}
			t.src = src
		}
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

// seal drops everything the groups don't need to render errors: the count source and the snapshot it may pin, the
// plans, the universes, the state node presence and the excluded pods. The groups keep their own counts and domains.
func (t *overlayTopology) seal() {
	t.sealed = true
	t.src, t.excluded, t.kubeClient = nil, nil, nil
	t.plans, t.universes, t.presence, t.domainSources = nil, nil, nil, nil
}

// checkSealed reports a call after seal: it panics in tests, and is logged and ignored otherwise.
func (t *overlayTopology) checkSealed(call string) bool {
	if !t.sealed {
		return false
	}
	if testing.Testing() {
		panic(fmt.Sprintf("topology %s called after the simulation ended", call))
	}
	t.log.Error(nil, "topology called after the simulation ended", "call", call)
	return true
}

// universe returns the universe of domains of the topology key. Like the rest of the Topology's state, it isn't safe
// for concurrent use; it's only reached from NewTopology and Update.
func (t *overlayTopology) universe(topologyKey string) *keyUniverse {
	if u, ok := t.universes[topologyKey]; ok {
		return u
	}
	u := newKeyUniverse(topologyKey, t.domainSources)
	t.universes[topologyKey] = u
	return u
}

// rotation returns the rotation of a group's tie-breaking order. Groups have their own, so that groups that tie in the
// same domains don't all prefer the same one.
func (t *overlayTopology) rotation(hash uint64) uint64 {
	// splitmix64
	z := t.seed ^ hash + 0x9e3779b97f4a7c15
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// overlayTopologyError allows lazily generating the error string in the topology error.  If a pod fails to schedule,
// most often we are only interested in the fact that it failed to schedule and not why.
type overlayTopologyError struct {
	topology    *TopologyGroup
	podDomains  *scheduling.Requirement
	nodeDomains *scheduling.Requirement
}

func (t overlayTopologyError) Error() string {
	return fmt.Sprintf("unsatisfiable topology constraint for %s, key=%s (counts = %s, podDomains = %v, nodeDomains = %v", t.topology.Type, t.topology.Key,
		pretty.Map(t.topology.domainCounts(), 25), t.podDomains, t.nodeDomains)
}

// Update unregisters the pod as the owner of all affinities and then creates any new topologies based on the pod spec
// registered the pod as the owner of all associated affinities, new or old.  This allows Update() to be called after
// relaxation of a preference to properly break the topology <-> owner relationship so that the preferred topology will
// no longer influence scheduling.
func (t *overlayTopology) Update(ctx context.Context, p *corev1.Pod) error {
	if t.checkSealed("Update") {
		return nil
	}
	t.version++
	delete(t.plans, p.UID)
	t.index.removeOwner(p.UID)

	if (t.preferencePolicy == PreferencePolicyIgnore && pod.HasRequiredPodAntiAffinity(p)) ||
		(t.preferencePolicy == PreferencePolicyRespect && pod.HasPodAntiAffinity(p)) {
		if err := t.updateInverseAntiAffinity(ctx, p); err != nil {
			return fmt.Errorf("updating inverse anti-affinities, %w", err)
		}
	}

	identities := t.newForTopologies(p)
	affinities, err := t.newForAffinities(ctx, p)
	if err != nil {
		return fmt.Errorf("updating affinities, %w", err)
	}

	for _, id := range append(identities, affinities...) {
		hash := id.Hash()
		// Avoid recomputing topology counts if we've already seen this group
		tg, ok := t.groups[hash]
		if !ok {
			if tg, err = t.newGroup(ctx, id, p); err != nil {
				return err
			}
			t.groups[hash] = tg
			t.index.add(tg)
		}
		t.index.addOwner(tg, p.UID)
	}
	return nil
}

// newGroup creates the group: the universe's domains that it seeds with, the domains of the state nodes that pass its
// node filter, and the counts of the existing pods it selects, other than the excluded pods. A group created after
// pods were recorded doesn't count them, as for the legacy topology.
func (t *overlayTopology) newGroup(ctx context.Context, id *TopologyGroupIdentity, p *corev1.Pod) (*TopologyGroup, error) {
	u := t.universe(id.Key)
	// the seed's taints are tolerated with the creating pod's tolerations, which are part of the identity of the groups
	// that honor taints
	seed := u.seed(id.SeedPolicy(), p.Spec.Tolerations)
	key := id.CountKey()
	base, err := t.src.Counts(ctx, key)
	if err != nil {
		return nil, err
	}
	tg := newTopologyGroup(id, t.rotation(id.Hash()))
	presence := t.presence.get(id.Key, u.dense, key.NodeFilter)
	if u.dense {
		tg.initDense(base)
	} else {
		tg.initSparse(base, seed.nameSet(), presence.nameSet(), t.hosts)
	}
	if err := t.src.ExcludedContributions(ctx, key, t.excluded, tg.subtract); err != nil {
		return nil, err
	}
	if u.dense {
		tg.setPresent(&seed.mask, &presence.mask)
	}
	return tg, nil
}

// newInverseGroup creates an inverse anti-affinity group. It's seeded with every domain in the universe, and its counts
// come from the terms of its owners.
func (t *overlayTopology) newInverseGroup(id *TopologyGroupIdentity) *TopologyGroup {
	u := t.universe(id.Key)
	seed := u.seed(corev1.NodeInclusionPolicyIgnore, nil)
	tg := newTopologyGroup(id, t.rotation(id.Hash()))
	if u.dense {
		tg.initDense(nil)
		tg.setPresent(&seed.mask)
	} else {
		tg.initSparse(nil, seed.nameSet(), nil, t.hosts)
	}
	return tg
}

// Record records the topology changes given that pod p schedule on a node with the given requirements
func (t *overlayTopology) Record(p *corev1.Pod, taints []corev1.Taint, requirements scheduling.Requirements, compatibilityOptions ...option.Function[scheduling.CompatibilityOptions]) {
	if t.checkSealed("Record") {
		return
	}
	t.version++
	// once we've committed to a domain, we record the usage in every topology that cares about it
	t.index.forEachCandidate(p, func(tg *TopologyGroup) {
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
	for _, tg := range t.inverseIndex.ownedBy(p.UID) {
		tg.Record(requirements.Get(tg.Key).Values()...)
	}
}

// BeginPod computes the part of AddRequirements for the pod that doesn't depend on the candidate node, before the
// scheduler tries the candidates in parallel. It must be called serially. AddRequirements uses the plan as long as it
// gets the same pod requirements and no group changed since.
func (t *overlayTopology) BeginPod(p *corev1.Pod, podRequirements scheduling.Requirements) {
	if t.checkSealed("BeginPod") {
		return
	}
	t.plans[p.UID] = &podPlan{version: t.version, podRequirements: podRequirements, entries: t.planEntries(p, podRequirements)}
}

// planEntries returns the entries of the groups that either control the scheduling of the pod, or that select the pod
// so that its scheduling affects their counts: the groups it owns, then the inverse groups that select it. Inverse
// groups have no node filter, so which of them select the pod doesn't depend on the node.
func (t *overlayTopology) planEntries(p *corev1.Pod, podRequirements scheduling.Requirements) []planEntry {
	owned := t.index.ownedBy(p.UID)
	entries := make([]planEntry, 0, len(owned))
	add := func(tg *TopologyGroup) {
		podDomains := scheduling.NewRequirement(tg.Key, corev1.NodeSelectorOpExists)
		if podRequirements.Has(tg.Key) {
			podDomains = podRequirements.Get(tg.Key)
		}
		entries = append(entries, tg.newPlanEntry(p, podDomains))
	}
	for _, tg := range owned {
		add(tg)
	}
	t.inverseIndex.forEachCandidate(p, func(tg *TopologyGroup) {
		if tg.selects(p) {
			add(tg)
		}
	})
	return entries
}

// plan returns the pod's plan if it's current for the pod requirements
func (t *overlayTopology) plan(p *corev1.Pod, podRequirements scheduling.Requirements) []planEntry {
	if plan, ok := t.plans[p.UID]; ok && plan.version == t.version && sameRequirements(plan.podRequirements, podRequirements) {
		return plan.entries
	}
	return t.planEntries(p, podRequirements)
}

// sameRequirements reports whether a and b are the same requirements (not just equal ones)
func sameRequirements(a, b scheduling.Requirements) bool {
	return reflect.ValueOf(a).UnsafePointer() == reflect.ValueOf(b).UnsafePointer()
}

// AddRequirements tightens the input requirements by adding additional requirements that are being enforced by topology spreads
// affinities, anti-affinities or inverse anti-affinities.  The nodeHostname is the hostname that we are currently considering
// placing the pod on.  It returns these newly tightened requirements, or an error in the case of a set of requirements that
// cannot be satisfied.
func (t *overlayTopology) AddRequirements(p *corev1.Pod, _ []corev1.Taint, podRequirements, nodeRequirements scheduling.Requirements, _ ...option.Function[scheduling.CompatibilityOptions]) (scheduling.Requirements, error) {
	if t.checkSealed("AddRequirements") {
		return nil, fmt.Errorf("topology called after the simulation ended")
	}
	requirements := scheduling.NewRequirements(nodeRequirements.Values()...)
	entries := t.plan(p, podRequirements)
	for i := range entries {
		e := &entries[i]
		nodeDomains := scheduling.NewRequirement(e.tg.Key, corev1.NodeSelectorOpExists)
		if nodeRequirements.Has(e.tg.Key) {
			nodeDomains = nodeRequirements.Get(e.tg.Key)
		}
		domains := e.tg.next(e, nodeDomains, nil)
		if domains.Len() == 0 {
			return nil, overlayTopologyError{
				topology:    e.tg,
				podDomains:  e.podDomains,
				nodeDomains: nodeDomains,
			}
		}
		requirements.Add(domains)
	}
	return requirements, nil
}

// GetTopologyZoneConstraints returns the set of valid zones from all topology constraints
// that use the zone topology key for the given pod, along with whether the constraints are satisfiable.
func (t *overlayTopology) GetTopologyZoneConstraints(p *corev1.Pod, podRequirements scheduling.Requirements) (sets.Set[string], bool) {
	if t.checkSealed("GetTopologyZoneConstraints") {
		return nil, true
	}
	var result sets.Set[string]

	for _, topology := range t.index.ownedBy(p.UID) {
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

// Register is used to register a domain as available across topologies for the given topology key. Hostnames are
// logged once for all groups instead (hostLog).
func (t *overlayTopology) Register(topologyKey string, domain string) {
	if t.checkSealed("Register") {
		return
	}
	t.version++
	if topologyKey == corev1.LabelHostname {
		t.hosts.register(domain)
		return
	}
	for _, groups := range []map[uint64]*TopologyGroup{t.groups, t.inverseGroups} {
		for _, tg := range groups {
			if tg.Key == topologyKey {
				tg.Register(domain)
			}
		}
	}
}

// Unregister is used to unregister a domain as available across topologies for the given topology key.
func (t *overlayTopology) Unregister(topologyKey string, domain string) {
	if t.checkSealed("Unregister") {
		return
	}
	t.version++
	for _, groups := range []map[uint64]*TopologyGroup{t.groups, t.inverseGroups} {
		for _, tg := range groups {
			if tg.Key == topologyKey {
				tg.Unregister(domain)
			}
		}
	}
}

// updateInverseAffinities tracks the topologies of the required anti-affinity terms of the existing pods, so that the
// pods they select can't schedule into the domains the existing pods are in.
func (t *overlayTopology) updateInverseAffinities(ctx context.Context) error {
	var errs error
	if err := t.src.InverseAntiAffinityTerms(ctx, t.excluded, func(term *topology.InverseTerm) {
		tg, err := t.inverseGroup(ctx, term.Namespace, term.Term)
		if err != nil {
			errs = multierr.Append(errs, fmt.Errorf("tracking existing pod anti-affinity, %w", err))
			return
		}
		term.Counts.ForEach(func(domain string, count int32) { tg.add(domain, count) })
		for _, uid := range term.Owners {
			t.inverseIndex.addOwner(tg, uid)
		}
	}); err != nil {
		return err
	}
	return errs
}

// updateInverseAntiAffinity tracks the topologies of a pod's required anti-affinity terms, with the pod as an owner
// that doesn't count towards their domains.
func (t *overlayTopology) updateInverseAntiAffinity(ctx context.Context, p *corev1.Pod) error {
	// We intentionally don't track inverse anti-affinity preferences. We're not
	// required to enforce them so it just adds complexity for very little
	// value.  The problem with them comes from the relaxation process, the pod
	// we are relaxing is not the pod with the anti-affinity term.
	for i := range p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
		tg, err := t.inverseGroup(ctx, p.Namespace, &p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[i])
		if err != nil {
			return err
		}
		t.inverseIndex.addOwner(tg, p.UID)
	}
	return nil
}

// inverseGroup returns the inverse group of an anti-affinity term of a pod in the namespace, creating it if needed
func (t *overlayTopology) inverseGroup(ctx context.Context, namespace string, term *corev1.PodAffinityTerm) (*TopologyGroup, error) {
	namespaces, err := t.buildNamespaceList(ctx, namespace, term.Namespaces, term.NamespaceSelector)
	if err != nil {
		return nil, err
	}
	id := newTopologyGroupIdentity(TopologyTypePodAntiAffinity, term.TopologyKey, nil, namespaces, term.LabelSelector, math.MaxInt32, nil, nil, nil)
	hash := id.Hash()
	tg, ok := t.inverseGroups[hash]
	if !ok {
		tg = t.newInverseGroup(id)
		t.inverseGroups[hash] = tg
		t.inverseIndex.add(tg)
	}
	return tg, nil
}

// newForTopologies returns the identities of the topology groups for the input pod's topology spread constraints.
func (t *overlayTopology) newForTopologies(p *corev1.Pod) []*TopologyGroupIdentity {
	var identities []*TopologyGroupIdentity
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
		identities = append(identities, newTopologyGroupIdentity(
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
	return identities
}

// newForAffinities returns the identities of the topology groups for the input pod's required/preferred affinity terms.
func (t *overlayTopology) newForAffinities(ctx context.Context, p *corev1.Pod) ([]*TopologyGroupIdentity, error) {
	var identities []*TopologyGroupIdentity
	// No affinity defined
	if p.Spec.Affinity == nil {
		return identities, nil
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
	for _, topologyType := range []TopologyType{TopologyTypePodAffinity, TopologyTypePodAntiAffinity} {
		for _, term := range affinityTerms[topologyType] {
			namespaces, err := t.buildNamespaceList(ctx, p.Namespace, term.Namespaces, term.NamespaceSelector)
			if err != nil {
				return nil, err
			}
			identities = append(identities, newTopologyGroupIdentity(topologyType, term.TopologyKey, p, namespaces, term.LabelSelector, math.MaxInt32, nil, nil, nil))
		}
	}
	return identities, nil
}

// buildNamespaceList constructs a unique list of namespaces consisting of the pod's namespace and the optional list of
// namespaces and those selected by the namespace selector
func (t *overlayTopology) buildNamespaceList(ctx context.Context, namespace string, namespaces []string, selector *metav1.LabelSelector) (sets.Set[string], error) {
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

// sortDomains sorts the domains into the order in which the group with the hash breaks ties. The shadow topology uses
// it as the legacy topology's order hook, so both implementations make the same choices.
func (t *overlayTopology) sortDomains(hash uint64, inverse bool, domains []string) {
	tg, ok := lo.Ternary(inverse, t.inverseGroups, t.groups)[hash]
	if !ok {
		slices.Sort(domains)
		return
	}
	tg.sortDomains(domains)
}
