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
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/awslabs/operatorpkg/option"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// TopologyShadowEnv names the environment variable that makes every Topology a shadowTopology, which compares the
// overlay and legacy implementations. "1" panics on any difference, for tests. "log" logs the first differences of
// each Topology and counts them all (TopologyShadowMismatchesTotal), for test clusters. Shadowing roughly doubles the
// cost of topology.
const TopologyShadowEnv = "KARPENTER_TOPOLOGY_SHADOW"

// maxLoggedShadowMismatches is the number of differences a shadow topology logs in "log" mode
const maxLoggedShadowMismatches = 5

// shadowReporter returns how differences are reported in the mode the environment selects, or nil if shadowing is off.
func shadowReporter(ctx context.Context) func(call, mismatch string) {
	switch os.Getenv(TopologyShadowEnv) {
	case "1":
		return func(_, mismatch string) {
			panic(fmt.Sprintf("topology shadow mismatch: %s", mismatch))
		}
	case "log":
		return newShadowLogReporter(ctx)
	}
	return nil
}

// newShadowLogReporter returns a reporter that logs the first differences and counts all of them
func newShadowLogReporter(ctx context.Context) func(call, mismatch string) {
	logged := 0
	return func(call, mismatch string) {
		TopologyShadowMismatchesTotal.Inc(map[string]string{callLabel: call})
		if logged < maxLoggedShadowMismatches {
			logged++
			log.FromContext(ctx).Error(nil, "topology shadow mismatch", "mismatch", mismatch)
		}
	}
}

// shadowTopology runs the overlay topology and the legacy topology side by side and compares them after every call.
// The legacy topology breaks ties in the overlay topology's order (legacyTopology.domainOrder), so both must make
// exactly the same decisions. The overlay topology's results are returned.
type shadowTopology struct {
	overlay *overlayTopology
	legacy  *legacyTopology
	// report is called with each difference, with the name of the call that found it
	report func(call, mismatch string)
	mu     sync.Mutex
	// ordered counts the legacy topology's calls to its order hook with more than one domain
	ordered atomic.Int64
}

func newShadowTopology(
	ctx context.Context,
	kubeClient client.Client,
	cluster *state.Cluster,
	stateNodes []*state.StateNode,
	nodePools []*v1.NodePool,
	instanceTypes map[string][]*cloudprovider.InstanceType,
	pods []*corev1.Pod,
	opts *options,
	report func(call, mismatch string),
) (*shadowTopology, error) {
	s := &shadowTopology{report: report}
	overlay, overlayErr := newOverlayTopology(ctx, kubeClient, cluster, stateNodes, nodePools, instanceTypes, pods, opts)
	s.overlay = overlay
	legacyOpts := *opts
	legacyOpts.domainOrder = func(_ string, hash uint64, inverse bool) func([]string) {
		return func(domains []string) {
			if len(domains) > 1 {
				s.ordered.Add(1)
			}
			if s.overlay != nil {
				s.overlay.sortDomains(hash, inverse, domains)
			}
		}
	}
	legacy, legacyErr := newLegacyTopology(ctx, kubeClient, cluster, stateNodes, nodePools, instanceTypes, pods, &legacyOpts)
	s.compareErrors("NewTopology", overlayErr, legacyErr)
	if overlayErr != nil {
		return nil, overlayErr
	}
	if legacyErr != nil {
		return nil, legacyErr
	}
	s.legacy = legacy
	s.compareGroups("NewTopology")
	return s, nil
}

func (s *shadowTopology) mismatch(call, format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.report(call, call+": "+fmt.Sprintf(format, args...))
}

func (s *shadowTopology) compareErrors(call string, overlayErr, legacyErr error) {
	if (overlayErr == nil) != (legacyErr == nil) {
		s.mismatch(call, "overlay error %v, legacy error %v", overlayErr, legacyErr)
	}
}

// compareGroups compares every group's domains, counts and owners
func (s *shadowTopology) compareGroups(call string) {
	s.compareGroupMaps(call, "group", s.overlay.groups, s.legacy.topologyGroups)
	s.compareGroupMaps(call, "inverse group", s.overlay.inverseGroups, s.legacy.inverseTopologyGroups)
}

func (s *shadowTopology) compareGroupMaps(call, kind string, overlay map[uint64]*TopologyGroup, legacy map[uint64]*legacyTopologyGroup) {
	if !sets.KeySet(overlay).Equal(sets.KeySet(legacy)) {
		s.mismatch(call, "%s hashes differ: overlay %v, legacy %v", kind, sets.List(sets.KeySet(overlay)), sets.List(sets.KeySet(legacy)))
		return
	}
	for hash, tg := range overlay {
		lg := legacy[hash]
		if counts := tg.domainCounts(); !maps.Equal(counts, lg.domains) {
			s.mismatch(call, "%s %d (%s %s) domains differ: overlay %v, legacy %v", kind, hash, tg.Type, tg.Key, counts, lg.domains)
		}
		if !maps.Equal(tg.owners, lg.owners) {
			s.mismatch(call, "%s %d (%s %s) owners differ: overlay %v, legacy %v", kind, hash, tg.Type, tg.Key, owners(tg.owners), owners(lg.owners))
		}
	}
}

func owners(m map[types.UID]struct{}) []types.UID {
	return slices.Sorted(maps.Keys(m))
}

func (s *shadowTopology) Update(ctx context.Context, p *corev1.Pod) error {
	overlayErr := s.overlay.Update(ctx, p)
	legacyErr := s.legacy.Update(ctx, p)
	s.compareErrors("Update", overlayErr, legacyErr)
	s.compareGroups("Update")
	return overlayErr
}

func (s *shadowTopology) Record(p *corev1.Pod, taints []corev1.Taint, requirements scheduling.Requirements, compatibilityOptions ...option.Function[scheduling.CompatibilityOptions]) {
	s.overlay.Record(p, taints, requirements, compatibilityOptions...)
	s.legacy.Record(p, taints, requirements, compatibilityOptions...)
	s.compareGroups("Record")
}

func (s *shadowTopology) AddRequirements(p *corev1.Pod, taints []corev1.Taint, podRequirements, nodeRequirements scheduling.Requirements, compatibilityOptions ...option.Function[scheduling.CompatibilityOptions]) (scheduling.Requirements, error) {
	overlay, overlayErr := s.overlay.AddRequirements(p, taints, podRequirements, nodeRequirements, compatibilityOptions...)
	legacy, legacyErr := s.legacy.AddRequirements(p, taints, podRequirements, nodeRequirements, compatibilityOptions...)
	s.compareErrors("AddRequirements", overlayErr, legacyErr)
	if overlayErr == nil && legacyErr == nil {
		if o, l := requirementsString(overlay), requirementsString(legacy); o != l {
			s.mismatch("AddRequirements", "pod %s: overlay %s, legacy %s", p.Name, o, l)
		}
	}
	return overlay, overlayErr
}

func (s *shadowTopology) GetTopologyZoneConstraints(p *corev1.Pod, podRequirements scheduling.Requirements) (sets.Set[string], bool) {
	overlay, overlayOK := s.overlay.GetTopologyZoneConstraints(p, podRequirements)
	legacy, legacyOK := s.legacy.GetTopologyZoneConstraints(p, podRequirements)
	if overlayOK != legacyOK || !overlay.Equal(legacy) {
		s.mismatch("GetTopologyZoneConstraints", "pod %s: overlay %v %v, legacy %v %v", p.Name, sets.List(overlay), overlayOK, sets.List(legacy), legacyOK)
	}
	return overlay, overlayOK
}

func (s *shadowTopology) seal() {
	s.overlay.seal()
	s.legacy.seal()
}

func (s *shadowTopology) BeginPod(p *corev1.Pod, podRequirements scheduling.Requirements) {
	s.overlay.BeginPod(p, podRequirements)
	s.legacy.BeginPod(p, podRequirements)
}

func (s *shadowTopology) Register(topologyKey string, domain string) {
	s.overlay.Register(topologyKey, domain)
	s.legacy.Register(topologyKey, domain)
	s.compareGroups("Register")
}

func (s *shadowTopology) Unregister(topologyKey string, domain string) {
	s.overlay.Unregister(topologyKey, domain)
	s.legacy.Unregister(topologyKey, domain)
	s.compareGroups("Unregister")
}

// requirementsString renders requirements canonically
func requirementsString(requirements scheduling.Requirements) string {
	var parts []string
	for _, r := range requirements.NodeSelectorRequirements() {
		values := slices.Clone(r.Values)
		slices.Sort(values)
		parts = append(parts, fmt.Sprintf("%s %s %v minValues=%d", r.Key, r.Operator, values, lo.FromPtr(r.MinValues)))
	}
	slices.Sort(parts)
	return strings.Join(parts, "; ")
}
