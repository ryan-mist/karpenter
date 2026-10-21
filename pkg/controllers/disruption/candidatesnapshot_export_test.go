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

// Test-only hooks for the candidate snapshot differential test (candidatesnapshot_test.go).

import (
	"context"
	"fmt"
	"time"

	"github.com/awslabs/operatorpkg/serrors"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/node/health"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	disruptionutils "sigs.k8s.io/karpenter/pkg/utils/disruption"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
	"sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
)

// NewCandidateSnapshotFromNodes is NewCandidateSnapshot with the state-node deep copy supplied by the caller, so a
// test can give it and the legacy oracle the same node order (DeepCopyNodes returns map order).
func NewCandidateSnapshotFromNodes(ctx context.Context, deepCopyNodes func() state.StateNodes, kubeClient client.Client, recorder events.Recorder,
	clk clock.Clock, cloudProvider cloudprovider.CloudProvider, queue *Queue,
) (*CandidateSnapshot, error) {
	return newCandidateSnapshot(ctx, deepCopyNodes, kubeClient, recorder, clk, cloudProvider, queue)
}

// MethodCandidates is the candidate step of Controller.disrupt.
func MethodCandidates(ctx context.Context, m Method, snapshot **CandidateSnapshot, build func() (*CandidateSnapshot, error)) (candidates, allCandidates []*Candidate, s *CandidateSnapshot, err error) {
	return methodCandidates(ctx, m, snapshot, build)
}

// Explain returns why the snapshot node with providerID is not a candidate for disruptionClass (nil if it is one).
func (s *CandidateSnapshot) Explain(ctx context.Context, providerID string, disruptionClass string) (error, bool) {
	for i, n := range s.nodes {
		if n.ProviderID() == providerID {
			_, err := s.candidate(ctx, i, disruptionClass)
			return err, true
		}
	}
	return nil, false
}

// CandidateFields is every Candidate field, in comparable form.
type CandidateFields struct {
	ProviderID               string
	NodePool                 string
	NodePoolResourceVersion  string
	InstanceType             *cloudprovider.InstanceType
	CapacityType             string
	Zone                     string
	ReschedulablePods        []string // namespace/name/uid/resourceVersion, in order
	HasPodBlockers           bool
	DisruptionCost           float64
	Price                    float64
	RescheduleDisruptionCost float64
	TerminationGracePeriod   *time.Duration
	RepairPolicyResult       health.RepairResult
	RebootEscalated          bool
}

func (c *Candidate) Fields() CandidateFields {
	return CandidateFields{
		ProviderID:              c.ProviderID(),
		NodePool:                c.NodePool.Name,
		NodePoolResourceVersion: c.NodePool.ResourceVersion,
		InstanceType:            c.instanceType,
		CapacityType:            c.capacityType,
		Zone:                    c.zone,
		ReschedulablePods: lo.Map(c.reschedulablePods, func(p *corev1.Pod, _ int) string {
			return fmt.Sprintf("%s/%s/%s/%s", p.Namespace, p.Name, p.UID, p.ResourceVersion)
		}),
		HasPodBlockers:           c.hasPodBlockers,
		DisruptionCost:           c.DisruptionCost,
		Price:                    c.Price,
		RescheduleDisruptionCost: c.RescheduleDisruptionCost,
		TerminationGracePeriod:   c.TerminationGracePeriod,
		RepairPolicyResult:       c.RepairPolicyResult,
		RebootEscalated:          c.RebootEscalated,
	}
}

// Scribble overwrites every field a Candidate owns, including the fields of its StateNode copy, as a method or the
// queue writing to its Candidates would (Repair stamps TerminationGracePeriod on candidates it then skips and sets
// RebootEscalated; the queue replaces the NodeClaim, queue.go:249). Another method's Candidates must not change.
func (c *Candidate) Scribble() {
	c.StateNode.NodeClaim = &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "scribbled"}}
	c.StateNode.Node = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "scribbled"}}
	*c = Candidate{
		StateNode:                c.StateNode,
		NodePool:                 &v1.NodePool{},
		capacityType:             "scribbled",
		zone:                     "scribbled",
		hasPodBlockers:           !c.hasPodBlockers,
		DisruptionCost:           -1,
		Price:                    -1,
		RescheduleDisruptionCost: -1,
		TerminationGracePeriod:   lo.ToPtr(time.Nanosecond),
		RepairPolicyResult:       health.RepairResult{Action: "scribbled", TerminationGracePeriod: lo.ToPtr(time.Nanosecond)},
		RebootEscalated:          true,
	}
}

// ScribbleShared modifies the StateNode and the pods a Candidate points to, as ComputeCommands may (scheduling
// injects topology spread constraints and relaxes preferences on candidate pods in place). Only a later snapshot is
// guaranteed not to see this.
func (c *Candidate) ScribbleShared() {
	c.Node.Labels["scribbled"] = "true"
	c.NodeClaim.Labels["scribbled"] = "true"
	c.NodeClaim.Spec.TerminationGracePeriod = &metav1.Duration{Duration: time.Nanosecond}
	for _, p := range c.reschedulablePods {
		p.Spec.TopologySpreadConstraints = append(p.Spec.TopologySpreadConstraints, corev1.TopologySpreadConstraint{TopologyKey: "scribbled"})
		p.Annotations = lo.Assign(p.Annotations, map[string]string{v1.DoNotDisruptAnnotationKey: "true"})
	}
}

// ComputeNodePoolTotals is the NodePoolTotals Controller.disrupt passes to a NodePoolTotalsSetter.
func (s *CandidateSnapshot) ComputeNodePoolTotals(ctx context.Context, allCandidates []*Candidate, clusterCost *cost.ClusterCost) map[string]NodePoolTotals {
	return s.NodePoolTotals(ctx, allCandidates, clusterCost)
}

// LegacyNewCandidate is the per-node oracle: NewCandidate as it was before CandidateSnapshot.
func LegacyNewCandidate(ctx context.Context, kubeClient client.Client, recorder events.Recorder, clk clock.Clock, node *state.StateNode, pdbs pdb.Limits,
	nodePoolMap map[string]*v1.NodePool, nodePoolToInstanceTypesMap map[string]map[string]*cloudprovider.InstanceType, queue *Queue, disruptionClass string,
) (*Candidate, error) {
	return legacyNewCandidate(ctx, kubeClient, recorder, clk, node, pdbs, nodePoolMap, nodePoolToInstanceTypesMap, queue, disruptionClass)
}

// The oracle below is GetCandidatesWithTotals and NewCandidate verbatim from before CandidateSnapshot (one full build
// per call), except that GetCandidatesWithTotals takes the state-node deep copy as deepCopyNodes.

//nolint:gocyclo
func legacyNewCandidate(ctx context.Context, kubeClient client.Client, recorder events.Recorder, clk clock.Clock, node *state.StateNode, pdbs pdb.Limits,
	nodePoolMap map[string]*v1.NodePool, nodePoolToInstanceTypesMap map[string]map[string]*cloudprovider.InstanceType, queue *Queue, disruptionClass string,
) (*Candidate, error) {
	// If the orchestration queue is already considering a candidate we want to disrupt, don't consider it a candidate.
	if queue.HasAny(node.ProviderID()) {
		return nil, fmt.Errorf("candidate is already being disrupted")
	}
	if disruptionClass == RepairDisruptionClass && node.NodeClaim != nil && node.Labels()[v1.NodePoolLabelKey] == "" {
		recorder.Publish(disruptionevents.Blocked(node.Node, node.NodeClaim,
			"repair requires a NodePool to construct and budget a safe replacement")...)
		return nil, serrors.Wrap(fmt.Errorf("repair requires a nodepool"), "NodeClaim", klog.KObj(node.NodeClaim))
	}

	err := node.ValidateNodeDisruptable(clk)
	if err != nil {
		// Repair is voluntary but is NOT discretionary: do-not-disrupt was never intended to strand a broken node.
		// Nor is a node that never initializes left stranded, since nothing else replaces it.
		if disruptionClass == RepairDisruptionClass {
			err = state.IgnoreNodeUninitializedError(state.IgnoreNodeDoNotDisruptError(err))
		}
		if err != nil {
			// Only emit an event for Karpenter-managed nodes.
			if node.NodeClaim != nil {
				recorder.Publish(disruptionevents.Blocked(node.Node, node.NodeClaim, pretty.Sentence(err.Error()))...)
			}
			err = fmt.Errorf("validating node for disruption, %w", err)
			if node.Node == nil || !node.Registered() {
				return nil, serrors.Wrap(err, "NodeClaim", klog.KObj(node.NodeClaim))
			}
			return nil, serrors.Wrap(err, "Node", klog.KObj(node.Node))
		}
	}
	// We know that the node will have the label key because of the node.IsDisruptable check above
	nodePoolName := node.Labels()[v1.NodePoolLabelKey]
	// nodePool is a shared cache-backed pointer; treat as read-only. DeepCopy before mutating.
	nodePool := nodePoolMap[nodePoolName]
	instanceTypeMap := nodePoolToInstanceTypesMap[nodePoolName]
	// skip any candidates where we can't determine the nodePool
	if nodePool == nil || instanceTypeMap == nil {
		recorder.Publish(disruptionevents.Blocked(node.Node, node.NodeClaim, fmt.Sprintf("NodePool not found (NodePool=%s)", nodePoolName))...)
		return nil, serrors.Wrap(fmt.Errorf("nodepool not found"), "NodePool", klog.KRef("", nodePoolName))
	}
	// We only care if instanceType in non-empty consolidation to do price-comparison.
	instanceType := instanceTypeMap[node.Labels()[corev1.LabelInstanceTypeStable]]
	pods, err := node.ValidatePodsDisruptable(ctx, kubeClient, pdbs, clk, recorder)
	hasPodBlockers := false
	if err != nil {
		// Repair resolves its policy deadline after candidate construction, so retain whether pod blockers were ignored
		// and let Repair reject an unbounded candidate. Eventual disruption already knows its NodeClaim drain bound.
		validationErr := err
		switch {
		case disruptionClass == RepairDisruptionClass:
			validationErr = state.IgnorePodBlockEvictionError(err)
			hasPodBlockers = validationErr == nil
		case disruptionClass == EventualDisruptionClass && node.NodeClaim.Spec.TerminationGracePeriod != nil:
			validationErr = state.IgnorePodBlockEvictionError(err)
		}
		if validationErr != nil {
			recorder.Publish(disruptionevents.Blocked(node.Node, node.NodeClaim, pretty.Sentence(err.Error()))...)
			return nil, serrors.Wrap(fmt.Errorf("validating pod disruption, %w", err), "Node", klog.KObj(node.Node))
		}
	}
	reschedulable := lo.Filter(pods, func(p *corev1.Pod, _ int) bool { return pod.IsReschedulable(p) })
	return &Candidate{
		StateNode:         node,
		instanceType:      instanceType,
		NodePool:          nodePool,
		capacityType:      node.Labels()[v1.CapacityTypeLabelKey],
		zone:              node.Labels()[corev1.LabelTopologyZone],
		reschedulablePods: reschedulable,
		hasPodBlockers:    hasPodBlockers,
		// We get the disruption cost from all pods in the candidate, not just the reschedulable pods
		DisruptionCost:           disruptionutils.ReschedulingCost(ctx, pods) * disruptionutils.LifetimeRemaining(clk, nodePool, node.NodeClaim),
		Price:                    disruptionutils.ResolveOfferingPrice(node.Labels(), instanceType),
		RescheduleDisruptionCost: disruptionutils.ComputeRescheduleDisruptionCost(ctx, reschedulable),
	}, nil
}

func LegacyGetCandidatesWithTotals(ctx context.Context, deepCopyNodes func() state.StateNodes, kubeClient client.Client, recorder events.Recorder, clk clock.Clock,
	cloudProvider cloudprovider.CloudProvider, shouldDisrupt CandidateFilter, disruptionClass string, queue *Queue, clusterCost *cost.ClusterCost,
) ([]*Candidate, map[string]NodePoolTotals, error) {
	nodePoolMap, nodePoolToInstanceTypesMap, err := BuildNodePoolMap(ctx, kubeClient, cloudProvider)
	if err != nil {
		return nil, nil, err
	}
	pdbs, err := pdb.NewLimits(ctx, kubeClient)
	if err != nil {
		return nil, nil, fmt.Errorf("tracking PodDisruptionBudgets, %w", err)
	}
	// We use a clock which has a fixed value for now so all candidates get the same value.
	// now is used in the calculation of DisruptionCost.
	// Using a fixed value is important for repair which tie-breaks candidates based on their DisruptionCost.
	// We don't want the order in which candidates are evaluated to affect their DisruptionCost.
	var fixedNowClk clock.Clock = fixedNowClock{Clock: clk, now: clk.Now()}
	allNodes := deepCopyNodes()
	allCandidates := lo.FilterMap(allNodes, func(n *state.StateNode, _ int) (*Candidate, bool) {
		cn, e := legacyNewCandidate(ctx, kubeClient, recorder, fixedNowClk, n, pdbs, nodePoolMap, nodePoolToInstanceTypesMap, queue, disruptionClass)
		return cn, e == nil
	})
	// Compute totals using ALL nodes for disruption cost denominator (RFC requirement:
	// "Non-candidate nodes still contribute to the denominators").
	nodePoolTotals := computeNodePoolTotals(ctx, allCandidates, []*state.StateNode(allNodes), clusterCost)
	filtered := lo.Filter(allCandidates, func(c *Candidate, _ int) bool { return shouldDisrupt(ctx, c) })
	return filtered, nodePoolTotals, nil
}
