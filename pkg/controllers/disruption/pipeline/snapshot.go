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
	"slices"
	"sort"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
)

// Scope is what a lane's planners need from a snapshot. A lane computes it once from its planners.
type Scope struct {
	Lane types.LaneID
	// Classes are the disruption classes the lane's planners use.
	Classes []string
	// NeedTotals are the classes whose NodePool totals a planner consumes (TotalsConsumer). They are computed from all
	// of the class's candidates the first time a planner of that class gets its candidates.
	NeedTotals []string
}

// ScopeFor is the Scope of a lane with these planners.
func ScopeFor(lane types.LaneID, planners ...Planner) Scope {
	s := Scope{Lane: lane}
	for _, p := range planners {
		if !slices.Contains(s.Classes, p.Class()) {
			s.Classes = append(s.Classes, p.Class())
		}
		if tc, ok := p.(TotalsConsumer); ok && tc.NeedsTotals() && !slices.Contains(s.NeedTotals, p.Class()) {
			s.NeedTotals = append(s.NeedTotals, p.Class())
		}
	}
	return s
}

// SnapshotBuilder builds the read-only view of the cluster that one lane round plans on.
type SnapshotBuilder interface {
	Build(ctx context.Context, scope Scope) (*Snapshot, error)
}

type SnapshotOptions struct {
	nodes func(*state.Cluster) state.StateNodes
}

// WithNodeSource replaces the snapshot's copy of the state nodes (cluster.DeepCopyNodes), e.g. to fix their order.
func WithNodeSource(nodes func(*state.Cluster) state.StateNodes) option.Function[SnapshotOptions] {
	return func(o *SnapshotOptions) { o.nodes = nodes }
}

type snapshotBuilder struct {
	kubeClient    client.Client
	cluster       *state.Cluster
	clock         clock.Clock
	cloudProvider cloudprovider.CloudProvider
	queue         *disruption.Queue
	clusterCost   *cost.ClusterCost
	reserved      ReservationView
	nodes         func(*state.Cluster) state.StateNodes
}

// NewSnapshotBuilder returns the v1 snapshot builder. A round builds the class-independent part of candidate
// construction once (NodePools and instance types, PDBs, one copy of the state nodes, one fixed now, each node's pods
// listed without a deep copy), and each planner gets its own Candidates from it (Snapshot.CandidatesFor). reserved may
// be nil, in which case nothing is reserved.
func NewSnapshotBuilder(kubeClient client.Client, cluster *state.Cluster, clk clock.Clock, cp cloudprovider.CloudProvider,
	queue *disruption.Queue, clusterCost *cost.ClusterCost, reserved ReservationView, opts ...option.Function[SnapshotOptions],
) SnapshotBuilder {
	o := option.Resolve(opts...)
	return &snapshotBuilder{
		kubeClient:    kubeClient,
		cluster:       cluster,
		clock:         clk,
		cloudProvider: cp,
		queue:         queue,
		clusterCost:   clusterCost,
		reserved:      reserved,
		nodes:         lo.Ternary(o.nodes != nil, o.nodes, (*state.Cluster).DeepCopyNodes),
	}
}

func (b *snapshotBuilder) Build(ctx context.Context, scope Scope) (*Snapshot, error) {
	ev := &RoundEvents{}
	cs, err := disruption.NewCandidateSnapshotFromNodes(ctx, func() state.StateNodes { return b.nodes(b.cluster) }, b.kubeClient, ev, b.clock, b.cloudProvider, b.queue)
	if err != nil {
		return nil, err
	}
	return &Snapshot{
		BuiltAt:       cs.Now(),
		NodePools:     cs.NodePools(),
		InstanceTypes: cs.InstanceTypes(),
		PDBs:          cs.PDBs(),
		Nodes:         cs.Nodes(),
		Reserved:      b.reserved,
		Events:        ev,
		scope:         scope,
		candidates:    cs,
		clusterCost:   b.clusterCost,
		totals:        map[string]map[string]disruption.NodePoolTotals{},
		catalog:       map[string]string{},
	}, nil
}

// Snapshot is the read-only view one lane round plans on. It holds the class-independent inputs of candidate
// construction, never shared Candidates: every CandidatesFor call materializes new Candidates that only its caller
// owns, so a planner that writes to its Candidates (repair stamps a drain bound) can't affect another planner. Its
// maps, nodes, pods and objects are shared and must not be modified. A Snapshot belongs to one lane round and is not
// safe for concurrent use.
type Snapshot struct {
	// BuiltAt is the round's fixed now. Every candidate is evaluated at it.
	BuiltAt       time.Time
	NodePools     map[string]*v1.NodePool
	InstanceTypes map[string]map[string]*cloudprovider.InstanceType
	PDBs          pdb.Limits
	// Nodes is the round's one copy of the state nodes.
	Nodes state.StateNodes
	// Reserved is the live reservation view (the Ledger's), not a copy. It may be nil.
	Reserved ReservationView
	// Events buffers the round's events: candidate rejections and everything the planners publish.
	Events *RoundEvents

	scope       Scope
	candidates  *disruption.CandidateSnapshot
	clusterCost *cost.ClusterCost
	totals      map[string]map[string]disruption.NodePoolTotals
	catalog     map[string]string
}

// CandidatesFor returns the planner's eligible candidates, in snapshot order: the nodes that are candidates for the
// planner's class (the disruption controller's rules, evaluated at BuiltAt), that no reserved plan disrupts and no
// started command is moving pods onto, and that pass the planner's Filter. The Candidates are new and owned by the
// caller. Rejection events and Filter verdicts go to Events.
func (s *Snapshot) CandidatesFor(ctx context.Context, p Planner) []*disruption.Candidate {
	all := s.candidates.Candidates(ctx, p.Class())
	if slices.Contains(s.scope.NeedTotals, p.Class()) {
		if _, ok := s.totals[p.Class()]; !ok {
			s.totals[p.Class()] = s.candidates.NodePoolTotals(ctx, all, s.clusterCost)
		}
	}
	inFlight := sets.New[string]()
	if s.Reserved != nil {
		inFlight = s.Reserved.InFlightDestinations()
	}
	return lo.Filter(all, func(c *disruption.Candidate, _ int) bool {
		if s.Reserved != nil && s.Reserved.IsReserved(c.ProviderID()) {
			return false
		}
		if inFlight.Has(c.ProviderID()) {
			return false
		}
		ok, v := p.Filter(ctx, c, s.BuiltAt)
		if v != nil {
			s.Events.Record(*v)
		}
		return ok
	})
}

// Totals are the NodePool totals of class for balanced scoring, computed from all of the class's candidates and all
// nodes, as the disruption controller passes them to a consolidation method.
func (s *Snapshot) Totals(ctx context.Context, class string) map[string]disruption.NodePoolTotals {
	if t, ok := s.totals[class]; ok {
		return t
	}
	s.totals[class] = s.candidates.NodePoolTotals(ctx, s.candidates.Candidates(ctx, class), s.clusterCost)
	return s.totals[class]
}

// Explain returns why the node is not a candidate for class (nil if it is one), as NewCandidate would. The error is
// nil if the snapshot doesn't have the node.
func (s *Snapshot) Explain(ctx context.Context, providerID, class string) error {
	err, _ := s.candidates.Explain(ctx, providerID, class)
	return err
}

// CatalogVersion identifies the instance types and offerings of nodePool at BuiltAt. It changes when an instance type,
// an offering's availability, price or capacity reservation changes, or the NodePool's instance types can't be listed.
func (s *Snapshot) CatalogVersion(nodePool string) string {
	if v, ok := s.catalog[nodePool]; ok {
		return v
	}
	s.catalog[nodePool] = CatalogVersion(s.InstanceTypes[nodePool])
	return s.catalog[nodePool]
}

// CatalogVersion is a fingerprint of instance types and their offerings: names, offering zone, capacity type,
// reservation ID and capacity, availability and price. It doesn't depend on the order of the offerings: cloud providers
// build them from sets (the AWS provider iterates its zones and capacity types), so a rebuilt offering cache lists the
// same offerings in another order (KWOK run 2: 95 of 98 admission re-simulations). A nil map (no instance types) has
// its own version.
func CatalogVersion(its map[string]*cloudprovider.InstanceType) string {
	if its == nil {
		return "unavailable"
	}
	h := fnv.New64a()
	names := lo.Keys(its)
	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintf(h, "%s;", name)
		offerings := lo.Map(its[name].Offerings, func(o *cloudprovider.Offering, _ int) string {
			// Requirement.String, not Offering.Zone and friends: Any picks a random value from a multi-valued requirement.
			return fmt.Sprintf("%s/%s/%s/%d/%t/%g;", o.Requirements.Get(corev1.LabelTopologyZone), o.Requirements.Get(v1.CapacityTypeLabelKey),
				o.Requirements.Get(cloudprovider.ReservationIDLabel), o.ReservationCapacity, o.Available, o.Price)
		})
		sort.Strings(offerings)
		for _, o := range offerings {
			fmt.Fprint(h, o)
		}
	}
	return fmt.Sprintf("%016x", h.Sum64())
}
