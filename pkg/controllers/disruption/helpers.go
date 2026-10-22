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
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/awslabs/operatorpkg/serrors"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/karpenter/pkg/operator/options"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	operatorlogging "sigs.k8s.io/karpenter/pkg/operator/logging"
	pkgscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	nodepoolutils "sigs.k8s.io/karpenter/pkg/utils/nodepool"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
)

var errCandidateDeleting = fmt.Errorf("candidate is deleting")

// SimulationOptions configures disruption-specific scheduling behavior.
type SimulationOptions struct {
	// IncludeBlockedCandidatePods includes candidate pods even when they cannot currently be evicted.
	IncludeBlockedCandidatePods bool
	// ExcludeDestinations are providerIDs of nodes that may not receive pods in this simulation. Their own pods are not
	// moved. Batched drift uses it so each chunk of a split batch plans around the capacity earlier chunks rely on.
	// Empty for every single-candidate simulation.
	ExcludeDestinations sets.Set[string]
	// LimitsUsed counts resources against NodePool limits on top of the cluster's nodes, by NodePool name: excluded
	// destinations that stay, and replacements that other commands planned but haven't launched.
	LimitsUsed map[string]corev1.ResourceList
	// SkipDeletingNodePods leaves out the pods of nodes that are already being disrupted, for a simulation whose plan
	// already placed them in an earlier simulation.
	SkipDeletingNodePods bool
	// TrackUninitialized lets the candidates' pods use nodes that aren't initialized yet (in-flight replacements)
	// without failing them with UninitializedNodeError: the caller records those nodes as the command's Dependencies
	// (UninitializedDependencies) and waits for them before deleting the candidates. When a destination isn't
	// initialized, the candidates' pods are scheduled after pending pods and pods of deleting nodes, so those keep the
	// capacity launched for them and nothing is provisioned twice for them.
	TrackUninitialized bool
	// CandidateGroupSize, if set, splits the candidates, in order, into groups of that size whose pods never share a
	// node, so no part of the plan that one group's pods need (an existing node or a new NodeClaim) is shared with
	// another group.
	CandidateGroupSize int
	// WithoutPendingPodSideEffects lists the pending pods without recording scheduling decisions or publishing events
	// about them (provisioning.WithoutSideEffects). The disruption pipeline sets it.
	WithoutPendingPodSideEffects bool
}

// SimulateScheduling determines whether candidate workloads can be rescheduled after disruption.
//
//nolint:gocyclo
func SimulateScheduling(ctx context.Context, kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner, clk clock.Clock, recorder events.Recorder,
	schedulerOpts []scheduling.Options, simulationOpts SimulationOptions, candidates ...*Candidate,
) (scheduling.Results, error) {
	recordSimulation(ctx)
	candidateNames := sets.NewString(lo.Map(candidates, func(t *Candidate, i int) string { return t.Name() })...)
	nodes := cluster.DeepCopyNodes()
	deletingNodes := nodes.Deleting()
	stateNodes := lo.Filter(nodes.Active(), func(n *state.StateNode, _ int) bool {
		return !candidateNames.Has(n.Name()) && !simulationOpts.ExcludeDestinations.Has(n.ProviderID())
	})

	// We do one final check to ensure that the node that we are attempting to consolidate isn't
	// already handled for deletion by some other controller. This could happen if the node was markedForDeletion
	// between returning the candidates and getting the stateNodes above
	if _, ok := lo.Find(deletingNodes, func(n *state.StateNode) bool {
		return candidateNames.Has(n.Name())
	}); ok {
		return scheduling.Results{}, errCandidateDeleting
	}

	var pendingOpts []option.Function[provisioning.PendingPodsOptions]
	if simulationOpts.WithoutPendingPodSideEffects {
		pendingOpts = append(pendingOpts, provisioning.WithoutSideEffects)
	}
	pods, err := provisioner.GetPendingPods(ctx, pendingOpts...)
	if err != nil {
		return scheduling.Results{}, fmt.Errorf("determining pending pods, %w", err)
	}

	// candidatePods are the pods on consolidation candidate nodes that we will reschedule. Their UIDs feed
	// deletingPodUIDs below so the DRA allocator frees the devices they hold and re-allocates their claims onto the
	// replacement capacity, mirroring how pods on already-deleting nodes are treated.
	var candidatePods []*corev1.Pod
	if simulationOpts.IncludeBlockedCandidatePods {
		candidatePods = lo.FlatMap(candidates, func(candidate *Candidate, _ int) []*corev1.Pod {
			return candidate.reschedulablePods
		})
	} else {
		// Don't provision capacity for pods which will not get evicted due to fully blocking PDBs.
		// Since Karpenter doesn't know when these pods will be successfully evicted, spinning up capacity until
		// these pods are evicted is wasteful. Repair opts out because its bounded drain owns when those pods leave,
		// while replacement planning must still account for their demand.
		pdbs, err := pdb.NewLimits(ctx, kubeClient)
		if err != nil {
			return scheduling.Results{}, fmt.Errorf("tracking PodDisruptionBudgets, %w", err)
		}
		candidatePods = lo.FlatMap(candidates, func(candidate *Candidate, _ int) []*corev1.Pod {
			return lo.Filter(candidate.reschedulablePods, func(p *corev1.Pod, _ int) bool {
				return pdbs.IsCurrentlyReschedulable(p, clk, recorder)
			})
		})
	}
	// Candidates hold the informer cache's pods (StateNode.ValidatePodsDisruptable lists them without a deep copy), and
	// the scheduler modifies the pods it is given in place (DefaultTopologySpreadInjector.Inject). Give it copies.
	candidatePods = lo.Map(candidatePods, func(p *corev1.Pod, _ int) *corev1.Pod { return p.DeepCopy() })
	pods = append(pods, candidatePods...)

	var deletingNodePods []*corev1.Pod
	if !simulationOpts.SkipDeletingNodePods {
		if deletingNodePods, err = deletingNodes.CurrentlyReschedulablePods(ctx, kubeClient, clk, recorder); err != nil {
			return scheduling.Results{}, fmt.Errorf("failed to get pods from deleting nodes, %w", err)
		}
	}
	pods = append(pods, deletingNodePods...)

	var opts []scheduling.Options
	if options.FromContext(ctx).PreferencePolicy == options.PreferencePolicyIgnore {
		opts = append(opts, scheduling.IgnorePreferences)
	}
	opts = append(opts, scheduling.MinValuesPolicy(options.FromContext(ctx).MinValuesPolicy))
	opts = append(opts, schedulerOpts...)
	for np, used := range simulationOpts.LimitsUsed {
		opts = append(opts, scheduling.CountAgainstLimits(np, used))
	}
	// Only uninitialized nodes need the order: without one, the scheduler places pods exactly as it always has.
	if simulationOpts.TrackUninitialized && lo.SomeBy(stateNodes, func(n *state.StateNode) bool { return !n.Initialized() }) {
		opts = append(opts, scheduling.CandidatePodsLast)
	}
	if size := simulationOpts.CandidateGroupSize; size > 0 && len(candidates) > size {
		groups := map[types.UID]int{}
		for i, c := range candidates {
			for _, p := range c.reschedulablePods {
				groups[p.UID] = i / size
			}
		}
		opts = append(opts, scheduling.PodGroups(groups))
	}
	// Both consolidation candidate pods and pods on already-deleting nodes are migrating off their current nodes, so
	// the DRA allocator should treat the devices they hold as available for reallocation (and re-allocate their claims).
	deletingPodUIDs := sets.New(lo.Map(append(candidatePods, deletingNodePods...), func(p *corev1.Pod, _ int) types.UID { return p.UID })...)
	scheduler, err := provisioner.NewScheduler(
		log.IntoContext(ctx, operatorlogging.NopLogger),
		pods,
		stateNodes,
		deletingPodUIDs,
		opts...,
	)
	if err != nil {
		return scheduling.Results{}, fmt.Errorf("creating scheduler, %w", err)
	}

	results, err := scheduler.Solve(log.IntoContext(ctx, operatorlogging.NopLogger), pods)
	if err != nil {
		return scheduling.Results{}, fmt.Errorf("scheduling pods, %w", err)
	}
	results = results.TruncateInstanceTypes(ctx, scheduling.MaxInstanceTypes)
	deletingNodePodKeys := lo.SliceToMap(deletingNodePods, func(p *corev1.Pod) (client.ObjectKey, any) {
		return client.ObjectKeyFromObject(p), nil
	})
	for _, n := range results.ExistingNodes {
		// We consider existing nodes for scheduling. When these nodes are unmanaged, their taint logic should
		// tell us if we can schedule to them or not; however, if these nodes are managed, we will still schedule to them
		// even if they are still in the middle of their initialization loop. In the case of disruption, we don't want
		// to proceed disrupting if our scheduling decision relies on nodes that haven't entered a terminal state.
		if !n.Initialized() && !simulationOpts.TrackUninitialized {
			for _, p := range n.Pods {
				// Only add a pod scheduling error if it isn't on an already deleting node.
				// If the pod is on a deleting node, we assume one of two things has already happened:
				// 1. The node was manually terminated, at which the provisioning controller has scheduled or is scheduling a node
				//    for the pod.
				// 2. The node was chosen for a previous disruption command, we assume that the uninitialized node will come up
				//    for this command, and we assume it will be successful. If it is not successful, the node will become
				//    not terminating, and we will no longer need to consider these pods.
				if _, ok := deletingNodePodKeys[client.ObjectKeyFromObject(p)]; !ok {
					results.PodErrors[p] = NewUninitializedNodeError(n)
				}
			}
		}
	}
	return results, nil
}

// SimulateSchedulingWithReservedFallback runs the candidate-gone scheduling simulation for a single voluntary-disruption
// candidate and reports whether the candidate must be terminated before a replacement can be provisioned
// (Terminate-First Disruption, RFC kubernetes-sigs/karpenter#3203). When terminateFirst is true the caller should issue
// a delete-only command and let reactive provisioning refill the freed reservation slot instead of staging a
// replacement; the returned Results are the credit-back (pass 2) simulation so it can nominate the existing nodes that
// absorb the freed pods. When terminateFirst is false the Results are the pass-1 (replace-first / Blocked) simulation.
//
// It simulates up to twice:
//
//  1. The normal fallback simulation. In fallback mode a full reservation (Available=true, ReservationCapacity=0) does
//     not satisfy a pod, so the scheduler falls through to a lower-weight NodePool (e.g. on-demand) or a different
//     reservation that still has capacity. If every reschedulable pod places, terminateFirst is false and the caller
//     replaces-first with these Results (as it always has).
//
//  2. Only if pass 1 leaves pods pending AND terminateFirstEnabled is set by the caller AND the candidate itself holds a
//     reservation: re-simulate in strict mode with the candidate's reservation slot credited back (modeling the slot it
//     will free on termination). If every pod then places, terminateFirst is true — deleting the candidate is exactly
//     what unblocks the reschedule. Strict mode makes surplus pods that wouldn't fit the freed slot fail rather than
//     fall back, and a reservation that is unavailable for another reason stays unschedulable, so we don't terminate
//     uselessly.
//
// A false terminateFirst with pods still pending in the returned Results is the caller's Blocked signal, unchanged.
func SimulateSchedulingWithReservedFallback(
	ctx context.Context,
	kubeClient client.Client,
	cluster *state.Cluster,
	provisioner *provisioning.Provisioner,
	clk clock.Clock,
	recorder events.Recorder,
	candidate *Candidate,
	terminateFirstEnabled bool,
	simulationOpts SimulationOptions,
) (results scheduling.Results, terminateFirst bool, err error) {
	// Pass 1: replace-first feasibility.
	results, err = SimulateScheduling(ctx, kubeClient, cluster, provisioner, clk, recorder, nil, simulationOpts, candidate)
	if err != nil || results.AllNonPendingPodsScheduled() {
		return results, false, err
	}

	// The gate is caller-supplied so each method controls terminate-first independently (drift: TerminateFirstDrift,
	// repair: TerminateFirstRepair). Only a reserved candidate is eligible: its slot is finite and exclusive, so a
	// replacement can't be staged until the old node frees it — on-demand/spot can just add a node (replace-first).
	reservationID := candidate.Labels()[cloudprovider.ReservationIDLabel]
	if !terminateFirstEnabled || candidate.capacityType != v1.CapacityTypeReserved || reservationID == "" {
		return results, false, nil
	}

	// Pass 2: terminate-first feasibility — credit the candidate's reservation slot back and require every pod to place
	// under strict mode.
	tfResults, err := SimulateScheduling(ctx, kubeClient, cluster, provisioner, clk, recorder,
		[]scheduling.Options{scheduling.DisableReservedCapacityFallback, scheduling.CreditReservationCapacity(reservationID, 1)}, simulationOpts, candidate)
	if err != nil {
		return results, false, err
	}
	if tfResults.AllNonPendingPodsScheduled() {
		// Return the credit-back Results, not pass 1's: this is the accurate post-termination picture (reactive
		// provisioning runs strict with the freed slot available), so the caller nominates the existing nodes that
		// absorb the freed pods without spuriously reporting the reserved-bound pods as unschedulable.
		return tfResults, true, nil
	}
	return results, false, nil
}

// UninitializedNodeError tracks a special pod error for disruption where pods schedule to a node
// that hasn't been initialized yet, meaning that we can't be confident to make a disruption decision based off of it
type UninitializedNodeError struct {
	*scheduling.ExistingNode
}

func NewUninitializedNodeError(node *scheduling.ExistingNode) *UninitializedNodeError {
	return &UninitializedNodeError{ExistingNode: node}
}

func (u *UninitializedNodeError) Error() string {
	var info []string
	if u.NodeClaim != nil {
		info = append(info, fmt.Sprintf("nodeclaim/%s", u.NodeClaim.Name))
	}
	if u.Node != nil {
		info = append(info, fmt.Sprintf("node/%s", u.Node.Name))
	}
	return fmt.Sprintf("would schedule against uninitialized %s", strings.Join(info, ", "))
}

// UninitializedDependencies returns the providerIDs, sorted, of the nodes that aren't initialized yet (and aren't
// being deleted) to which the results schedule a pod of the candidates: a command of these candidates can delete them
// only once those nodes are initialized. Pending pods and pods of deleting nodes don't count, as for
// UninitializedNodeError.
func UninitializedDependencies(candidates []*Candidate, results scheduling.Results) []string {
	owned := sets.New[types.UID]()
	for _, c := range candidates {
		for _, p := range c.reschedulablePods {
			owned.Insert(p.UID)
		}
	}
	var deps []string
	for _, en := range results.ExistingNodes {
		if en.Initialized() || en.MarkedForDeletion() {
			continue
		}
		if lo.SomeBy(en.Pods, func(p *corev1.Pod) bool { return owned.Has(p.UID) }) {
			deps = append(deps, en.ProviderID())
		}
	}
	slices.Sort(deps)
	return deps
}

// instanceTypesAreSubset returns true if the lhs slice of instance types are a subset of the rhs.
func instanceTypesAreSubset(lhs []*cloudprovider.InstanceType, rhs []*cloudprovider.InstanceType) bool {
	rhsNames := sets.NewString(lo.Map(rhs, func(t *cloudprovider.InstanceType, i int) string { return t.Name })...)
	lhsNames := sets.NewString(lo.Map(lhs, func(t *cloudprovider.InstanceType, i int) string { return t.Name })...)
	return len(rhsNames.Intersection(lhsNames)) == len(lhsNames)
}

// GetCandidates returns nodes that appear to be currently deprovisionable based off of their nodePool.
func GetCandidates(ctx context.Context, cluster *state.Cluster, kubeClient client.Client, recorder events.Recorder, clk clock.Clock,
	cloudProvider cloudprovider.CloudProvider, shouldDisrupt CandidateFilter, disruptionClass string, queue *Queue,
) ([]*Candidate, error) {
	candidates, _, err := GetCandidatesWithTotals(ctx, cluster, kubeClient, recorder, clk, cloudProvider, shouldDisrupt, disruptionClass, queue, nil)
	return candidates, err
}

// GetCandidatesWithTotals returns candidates and NodePoolTotals computed from all
// candidates before filtering, so balanced scoring normalizes against the full pool.
// When clusterCost is non-nil, TotalCost is read from precomputed cluster state
// rather than re-summed from candidates.
func GetCandidatesWithTotals(ctx context.Context, cluster *state.Cluster, kubeClient client.Client, recorder events.Recorder, clk clock.Clock,
	cloudProvider cloudprovider.CloudProvider, shouldDisrupt CandidateFilter, disruptionClass string, queue *Queue, clusterCost *cost.ClusterCost,
) ([]*Candidate, map[string]NodePoolTotals, error) {
	snapshot, err := NewCandidateSnapshot(ctx, cluster, kubeClient, recorder, clk, cloudProvider, queue)
	if err != nil {
		return nil, nil, err
	}
	allCandidates := snapshot.Candidates(ctx, disruptionClass)
	nodePoolTotals := snapshot.NodePoolTotals(ctx, allCandidates, clusterCost)
	filtered := lo.Filter(allCandidates, func(c *Candidate, _ int) bool { return shouldDisrupt(ctx, c) })
	return filtered, nodePoolTotals, nil
}

// CandidateSnapshot holds the class-independent inputs of candidate construction: the NodePools and their instance
// types, the PDBs, a deep copy of the state nodes, one fixed now, and each node's pod list and pod checks. These are
// nearly all of the cost of GetCandidatesWithTotals, so consecutive disruption methods share one snapshot, and each
// method applies its own class's NewCandidate rules to it (Candidates).
type CandidateSnapshot struct {
	kubeClient                 client.Client
	recorder                   events.Recorder
	clk                        clock.Clock
	queue                      *Queue
	nodePoolMap                map[string]*v1.NodePool
	nodePoolToInstanceTypesMap map[string]map[string]*cloudprovider.InstanceType
	pdbs                       pdb.Limits
	nodes                      state.StateNodes
	// pods[i] memoizes nodes[i]'s StateNode.ValidatePodsDisruptable. It is filled the first time a class gets that
	// far for the node, which is when NewCandidate would have run it.
	pods []*validatedPods
}

// validatedPods is the result of StateNode.ValidatePodsDisruptable, and the events it published, so every later class
// publishes them again exactly as if it had run the checks itself.
type validatedPods struct {
	pods   []*corev1.Pod
	err    error
	events []events.Event
}

// NewCandidateSnapshot does the class-independent work of GetCandidatesWithTotals, in the same order.
func NewCandidateSnapshot(ctx context.Context, cluster *state.Cluster, kubeClient client.Client, recorder events.Recorder, clk clock.Clock,
	cloudProvider cloudprovider.CloudProvider, queue *Queue,
) (*CandidateSnapshot, error) {
	return newCandidateSnapshot(ctx, cluster.DeepCopyNodes, kubeClient, recorder, clk, cloudProvider, queue)
}

func newCandidateSnapshot(ctx context.Context, deepCopyNodes func() state.StateNodes, kubeClient client.Client, recorder events.Recorder, clk clock.Clock,
	cloudProvider cloudprovider.CloudProvider, queue *Queue,
) (*CandidateSnapshot, error) {
	nodePoolMap, nodePoolToInstanceTypesMap, err := BuildNodePoolMap(ctx, kubeClient, cloudProvider)
	if err != nil {
		return nil, err
	}
	pdbs, err := pdb.NewLimits(ctx, kubeClient)
	if err != nil {
		return nil, fmt.Errorf("tracking PodDisruptionBudgets, %w", err)
	}
	// We use a clock which has a fixed value for now so all candidates get the same value.
	// now is used in the calculation of DisruptionCost.
	// Using a fixed value is important for repair which tie-breaks candidates based on their DisruptionCost.
	// We don't want the order in which candidates are evaluated to affect their DisruptionCost.
	var fixedNowClk clock.Clock = fixedNowClock{Clock: clk, now: clk.Now()}
	nodes := deepCopyNodes()
	return &CandidateSnapshot{
		kubeClient:                 kubeClient,
		recorder:                   recorder,
		clk:                        fixedNowClk,
		queue:                      queue,
		nodePoolMap:                nodePoolMap,
		nodePoolToInstanceTypesMap: nodePoolToInstanceTypesMap,
		pdbs:                       pdbs,
		nodes:                      nodes,
		pods:                       make([]*validatedPods, len(nodes)),
	}, nil
}

// Candidates returns the snapshot's nodes that are candidates for disruptionClass, in snapshot order, publishing the
// same events NewCandidate does. Every call returns new Candidates, each with its own shallow copy of the StateNode,
// that the caller owns: methods and the queue write to them (Repair sets RepairPolicyResult, RebootEscalated and
// TerminationGracePeriod; the queue replaces NodeClaim). The objects those copies point to (Node, NodeClaim, pod
// requests) and the pods (the informer cache's own) are shared by all callers and must not be modified.
func (s *CandidateSnapshot) Candidates(ctx context.Context, disruptionClass string) []*Candidate {
	return lo.FilterMap(s.nodes, func(_ *state.StateNode, i int) (*Candidate, bool) {
		cn, err := s.candidate(ctx, i, disruptionClass)
		return cn, err == nil
	})
}

// NodePoolTotals is the NodePoolTotals of GetCandidatesWithTotals for allCandidates, which Candidates returned.
func (s *CandidateSnapshot) NodePoolTotals(ctx context.Context, allCandidates []*Candidate, clusterCost *cost.ClusterCost) map[string]NodePoolTotals {
	// Compute totals using ALL nodes for disruption cost denominator (RFC requirement:
	// "Non-candidate nodes still contribute to the denominators").
	return computeNodePoolTotals(ctx, allCandidates, []*state.StateNode(s.nodes), clusterCost)
}

func (s *CandidateSnapshot) candidate(ctx context.Context, i int, disruptionClass string) (*Candidate, error) {
	node := lo.ToPtr(*s.nodes[i])
	return newCandidate(ctx, s.recorder, s.clk, node, s.nodePoolMap, s.nodePoolToInstanceTypesMap, s.queue, disruptionClass, func() ([]*corev1.Pod, error) {
		if s.pods[i] == nil {
			rec := &bufferedRecorder{}
			pods, err := s.nodes[i].ValidatePodsDisruptable(ctx, s.kubeClient, s.pdbs, s.clk, rec)
			s.pods[i] = &validatedPods{pods: pods, err: err, events: rec.events}
		}
		if len(s.pods[i].events) > 0 {
			s.recorder.Publish(s.pods[i].events...)
		}
		return s.pods[i].pods, s.pods[i].err
	})
}

// bufferedRecorder holds published events instead of publishing them.
type bufferedRecorder struct {
	events []events.Event
}

func (r *bufferedRecorder) Publish(evts ...events.Event) { r.events = append(r.events, evts...) }

// fixedNowClock is a clock that has a fixed value for now.
type fixedNowClock struct {
	clock.Clock
	now time.Time
}

func (c fixedNowClock) Now() time.Time                  { return c.now }
func (c fixedNowClock) Since(t time.Time) time.Duration { return c.now.Sub(t) }

// BuildNodePoolMap builds a provName -> nodePool map and a provName -> instanceName -> instance type map
func BuildNodePoolMap(ctx context.Context, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider) (map[string]*v1.NodePool, map[string]map[string]*cloudprovider.InstanceType, error) {
	nodePoolMap := map[string]*v1.NodePool{}
	nodePools, err := nodepoolutils.ListManaged(ctx, kubeClient, cloudProvider)
	if err != nil {
		return nil, nil, fmt.Errorf("listing node pools, %w", err)
	}

	nodePoolToInstanceTypesMap := map[string]map[string]*cloudprovider.InstanceType{}
	for _, np := range nodePools {
		nodePoolMap[np.Name] = np

		nodePoolInstanceTypes, err := cloudProvider.GetInstanceTypes(ctx, np)
		if err != nil {
			if cloudprovider.IsUnevaluatedNodePoolError(err) {
				log.FromContext(ctx).WithValues("NodePool", klog.KObj(np)).Error(err, "skipping, node overlays are not applied")
				continue
			}
			// don't error out on building the node pool, we just won't be able to handle any nodes that
			// were created by it
			log.FromContext(ctx).Error(err, "failed listing instance types", "nodepool", np.Name)
			continue
		}
		if len(nodePoolInstanceTypes) == 0 {
			continue
		}
		nodePoolToInstanceTypesMap[np.Name] = map[string]*cloudprovider.InstanceType{}
		for _, it := range nodePoolInstanceTypes {
			nodePoolToInstanceTypesMap[np.Name][it.Name] = it
		}
	}
	return nodePoolMap, nodePoolToInstanceTypesMap, nil
}

// The disrupting count is reason-dependent, so pass the same reason on to
// MustGetAllowedDisruptions or NodePoolBudgetMap.
func NodePoolStats(cluster *state.Cluster, reason v1.DisruptionReason) (numNodes, disrupting map[string]int) {
	return NodePoolStatsFromNodes(cluster.DeepCopyNodes(), reason)
}

func NodePoolBudgetMap(ctx context.Context, clk clock.Clock, nodePools map[string]*v1.NodePool, numNodes, disrupting map[string]int, reason v1.DisruptionReason) map[string]int {
	out := map[string]int{}
	for name, np := range nodePools {
		allowed := np.MustGetAllowedDisruptions(clk, numNodes[name], reason)
		remaining := allowed - disrupting[name]
		if remaining < 0 {
			log.FromContext(ctx).V(1).WithValues(
				"nodePool", name,
				"reason", string(reason),
				"allowed", allowed,
				"disrupting", disrupting[name],
			).Info("disruption budget already exhausted; clamping to 0")
			remaining = 0
		}
		out[name] = remaining
	}
	return out
}

// NodePoolStats against a snapshot the caller already holds.
func NodePoolStatsFromNodes(nodes []*state.StateNode, reason v1.DisruptionReason) (numNodes, disrupting map[string]int) {
	numNodes = map[string]int{}
	disrupting = map[string]int{}
	for _, node := range nodes {
		// We only consider nodes that we own and are initialized towards the total.
		// If a node is launched/registered, but not initialized, pods aren't scheduled
		// to the node, and these are treated as unhealthy until they're cleaned up.
		// This prevents odd roundup cases with percentages where replacement nodes that
		// aren't initialized could be counted towards the total, resulting in more disruptions
		// to active nodes than desired, where Karpenter should wait for these nodes to be
		// healthy before continuing. Reboot resets Initialized after issuance,
		// but the node still counts against the budget until reboot completes.
		if !node.Managed() || (!node.Initialized() && !node.RebootInProgress()) {
			continue
		}

		// Additionally, don't consider nodeclaims that have the terminating condition. A nodeclaim should have
		// the Terminating condition only when the node is drained and cloudprovider.Delete() was successful
		// on the underlying cloud provider machine.
		if node.NodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() {
			continue
		}

		nodePool := node.Labels()[v1.NodePoolLabelKey]
		numNodes[nodePool]++

		// A node is subtracted from the allowed disruptions when it is:
		//   1. Marked for deletion (a disruption is already in flight),
		//   2. Rebooting (an in-place disruption is already in flight), for every reason, or
		//   3. NotReady — EXCEPT for the Unhealthy (repair) budget.
		// Repair must not count merely-unhealthy nodes: NotReady is precisely repair's trigger, so counting those
		// nodes against the repair budget would let a wave of unhealthy nodes starve the very budget that repairs them
		// (a node.health cohort could zero the budget and freeze repair). An in-flight repair still counts once its
		// candidate is MarkedForDeletion. Other reasons keep the reason-agnostic "unhealthy nodes consume budget"
		// semantics introduced for consolidation/drift (kubernetes-sigs/karpenter#981).
		notReady := nodeutils.GetCondition(node.Node, corev1.NodeReady).Status != corev1.ConditionTrue
		// Count active reboots against every disruption budget otherwise
		// other methods could disrupt concurrently and exceed the NodePool budget.
		if node.MarkedForDeletion() || node.RebootInProgress() || (notReady && reason != v1.DisruptionReasonUnhealthy) {
			disrupting[nodePool]++
		}
	}
	return numNodes, disrupting
}

// BuildDisruptionBudgets prepares our disruption budget mapping. The disruption budget maps each disruption reason to the number of allowed disruptions.
// We calculate allowed disruptions by taking the max disruptions allowed by disruption reason and subtracting the number of nodes that are NotReady and already being deleted by that disruption reason.
func BuildDisruptionBudgetMapping(ctx context.Context, cluster *state.Cluster, clk clock.Clock, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider, recorder events.Recorder, reason v1.DisruptionReason) (map[string]int, error) {
	disruptionBudgetMapping := map[string]int{}
	numNodes, disrupting := NodePoolStats(cluster, reason)
	nodePools, err := nodepoolutils.ListManaged(ctx, kubeClient, cloudProvider)
	if err != nil {
		return disruptionBudgetMapping, fmt.Errorf("listing node pools, %w", err)
	}
	for _, nodePool := range nodePools {
		allowedDisruptions := nodePool.MustGetAllowedDisruptions(clk, numNodes[nodePool.Name], reason)
		disruptionBudgetMapping[nodePool.Name] = lo.Max([]int{allowedDisruptions - disrupting[nodePool.Name], 0})
		NodePoolAllowedDisruptions.Set(float64(allowedDisruptions), map[string]string{
			metrics.NodePoolLabel: nodePool.Name, metrics.ReasonLabel: strings.ToLower(string(reason)),
		})
		NodePoolNodesConsumingBudgets.Set(float64(disrupting[nodePool.Name]), map[string]string{
			metrics.NodePoolLabel: nodePool.Name, metrics.ReasonLabel: strings.ToLower(string(reason)),
		})
		if numNodes[nodePool.Name] != 0 && allowedDisruptions == 0 {
			recorder.Publish(disruptionevents.NodePoolBlockedForDisruptionReason(nodePool, reason))
		}
	}
	return disruptionBudgetMapping, nil
}

// mapCandidates maps the list of proposed candidates with the current state
func mapCandidates(proposed, current []*Candidate) []*Candidate {
	proposedNames := sets.NewString(lo.Map(proposed, func(c *Candidate, i int) string { return c.Name() })...)
	return lo.Filter(current, func(c *Candidate, _ int) bool {
		return proposedNames.Has(c.Name())
	})
}

// staticReservations reports whether a static NodePool's replacement can only land in full capacity reservations, so
// pre-spinning it would fail to launch until a reserved node frees its slot, and which compatible reservations are still
// offered (Available, even if full). A compatible non-reserved offering, even an unavailable one, means the replacement
// doesn't depend on a reservation slot, so the NodePool waits for that capacity rather than terminating first. A static
// replacement is the bare NodePool template, so this is an offering lookup rather than a scheduling simulation.
// GetInstanceTypes is NodeClass-scoped, so offerings are filtered by the template's requirements.
func staticReservations(ctx context.Context, cloudProvider cloudprovider.CloudProvider, np *v1.NodePool, nct *scheduling.NodeClaimTemplate) (bool, sets.Set[string], error) {
	its, err := cloudProvider.GetInstanceTypes(ctx, np)
	if err != nil {
		return false, nil, serrors.Wrap(fmt.Errorf("getting instance types, %w", err), "NodePool", klog.KObj(np))
	}
	offered := sets.New[string]()
	for _, it := range its {
		if !nct.Requirements.IsCompatible(it.Requirements, pkgscheduling.AllowUndefinedWellKnownLabels) {
			continue
		}
		for _, o := range it.Offerings.Compatible(nct.Requirements) {
			if o.CapacityType() != v1.CapacityTypeReserved || o.Launchable() {
				return false, nil, nil
			}
			if o.Available {
				offered.Insert(o.ReservationID())
			}
		}
	}
	return true, offered, nil
}

// holdsOfferedReservation reports whether the candidate holds a slot in a reservation that's still offered, so
// terminating it frees a slot its refill can launch into.
func holdsOfferedReservation(c *Candidate, offered sets.Set[string]) bool {
	return c.capacityType == v1.CapacityTypeReserved && offered.Has(c.Labels()[cloudprovider.ReservationIDLabel])
}
