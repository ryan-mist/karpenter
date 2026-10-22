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
	"context"
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

// CandidateRebuilder rebuilds a plan's candidates from live state for admission.
type CandidateRebuilder interface {
	Rebuild(ctx context.Context, class string, providerIDs ...string) (*Rebuilt, error)
}

// Rebuilt is a plan's candidates rebuilt from live state, as the disruption controller's validation rebuilds them.
type Rebuilt struct {
	// Candidates are the rebuilt candidates that are still candidates of the class, by providerID.
	Candidates map[string]*disruption.Candidate
	// Rejected are the candidates that are no longer candidates of the class, by providerID.
	Rejected map[string]types.RejectReason
	// Events are the events the rebuild published (why a candidate is blocked).
	Events *RoundEvents

	instanceTypes map[string]map[string]*cloudprovider.InstanceType
}

// CatalogVersion is the live catalog version of nodePool, as Snapshot.CatalogVersion.
func (r *Rebuilt) CatalogVersion(nodePool string) string {
	return CatalogVersion(r.instanceTypes[nodePool])
}

type rebuilder struct {
	kubeClient    client.Client
	cluster       *state.Cluster
	clock         clock.Clock
	cloudProvider cloudprovider.CloudProvider
	queue         *disruption.Queue
}

// NewCandidateRebuilder rebuilds candidates with the disruption controller's rules, copying only their state nodes.
func NewCandidateRebuilder(kubeClient client.Client, cluster *state.Cluster, clk clock.Clock, cp cloudprovider.CloudProvider, queue *disruption.Queue) CandidateRebuilder {
	return &rebuilder{kubeClient: kubeClient, cluster: cluster, clock: clk, cloudProvider: cp, queue: queue}
}

func (r *rebuilder) Rebuild(ctx context.Context, class string, providerIDs ...string) (*Rebuilt, error) {
	want := sets.New(providerIDs...)
	ev := &RoundEvents{}
	s, err := disruption.NewCandidateSnapshotFromNodes(ctx, func() state.StateNodes {
		var out state.StateNodes
		for n := range r.cluster.Nodes() {
			if want.Has(n.ProviderID()) {
				out = append(out, n.DeepCopy())
			}
		}
		return out
	}, r.kubeClient, ev, r.clock, r.cloudProvider, r.queue)
	if err != nil {
		return nil, err
	}
	out := &Rebuilt{
		Candidates:    lo.SliceToMap(s.Candidates(ctx, class), func(c *disruption.Candidate) (string, *disruption.Candidate) { return c.ProviderID(), c }),
		Rejected:      map[string]types.RejectReason{},
		Events:        ev,
		instanceTypes: s.InstanceTypes(),
	}
	nodes := lo.SliceToMap(s.Nodes(), func(n *state.StateNode) (string, *state.StateNode) { return n.ProviderID(), n })
	for _, id := range providerIDs {
		if _, ok := out.Candidates[id]; ok {
			continue
		}
		out.Rejected[id] = r.classify(nodes[id])
	}
	return out, nil
}

// classify says why a node that was a candidate no longer is.
func (r *rebuilder) classify(n *state.StateNode) types.RejectReason {
	switch {
	case n == nil || n.Node == nil || n.NodeClaim == nil, n.MarkedForDeletion(), n.RebootInProgress(), r.queue.HasAny(n.ProviderID()):
		return types.ReasonCandidateInvalid
	case n.Nominated(r.clock):
		return types.ReasonCandidateNominated
	default:
		return types.ReasonCandidateBlocked
	}
}

// PodSetHash identifies a candidate's reschedulable pods. A planner records it per candidate in Plan.PodSetHash, and
// admission compares it with the rebuilt candidate's.
func PodSetHash(c *disruption.Candidate) uint64 {
	uids := lo.Map(c.ReschedulablePods(), func(p *corev1.Pod, _ int) string { return string(p.UID) })
	sort.Strings(uids)
	h := fnv.New64a()
	for _, uid := range uids {
		fmt.Fprintf(h, "%s;", uid)
	}
	return h.Sum64()
}
