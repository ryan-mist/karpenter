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
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/karpenter/pkg/utils/pretty"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	nodeclaimdisruption "sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/nodepoolbackoff"
)

// Drift is a subreconciler that replaces drifted candidates, or reboots them in place when the cloud provider resolves
// their drift that way.
type Drift struct {
	kubeClient    client.Client
	cluster       *state.Cluster
	provisioner   *provisioning.Provisioner
	cloudProvider cloudprovider.CloudProvider
	recorder      events.Recorder
	clock         clock.Clock
	backoff       *nodepoolbackoff.State
}

func NewDrift(kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner, cloudProvider cloudprovider.CloudProvider,
	recorder events.Recorder, clk clock.Clock, backoff *nodepoolbackoff.State) *Drift {
	return &Drift{
		kubeClient:    kubeClient,
		cluster:       cluster,
		provisioner:   provisioner,
		cloudProvider: cloudProvider,
		recorder:      recorder,
		clock:         clk,
		backoff:       backoff,
	}
}

// ShouldDisrupt is a predicate used to filter candidates
func (d *Drift) ShouldDisrupt(ctx context.Context, c *Candidate) bool {
	return !c.OwnedByStaticNodePool() && c.NodeClaim.StatusConditions().Get(string(d.Reason())).IsTrue()
}

// ComputeCommand generates a disruption command given candidates
func (d *Drift) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	initDriftBackoffMetrics(ctx, d.backoff, lo.Uniq(lo.Map(candidates, func(c *Candidate, _ int) string { return c.NodePool.Name }))...)

	sort.Slice(candidates, func(i int, j int) bool {
		return candidates[i].NodeClaim.StatusConditions().Get(string(d.Reason())).LastTransitionTime.Time.Before(
			candidates[j].NodeClaim.StatusConditions().Get(string(d.Reason())).LastTransitionTime.Time)
	})

	emptyCandidates, nonEmptyCandidates := lo.FilterReject(candidates, func(c *Candidate, _ int) bool {
		return len(c.reschedulablePods) == 0
	})

	// Prioritize empty candidates since we want them to get priority over non-empty candidates if the budget is constrained.
	// Disrupting empty candidates first also helps reduce the overall churn because if a non-empty candidate is disrupted first,
	// the pods from that node can reschedule on the empty nodes and will need to move again when those nodes get disrupted.
	for _, candidate := range slices.Concat(emptyCandidates, nonEmptyCandidates) {
		// If the disruption budget doesn't allow this candidate to be disrupted,
		// continue to the next candidate. We don't need to decrement any budget
		// counter since drift commands can only have one candidate.
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			continue
		}
		if isDriftBackedOff(ctx, d.backoff, d.recorder, candidate.NodePool) {
			continue
		}
		driftStatus, ok := resolveDriftStatus(ctx, d.cloudProvider, candidate)
		if !ok {
			continue
		}
		// An in-place reboot keeps the node, so it needs no rescheduling simulation or replacement.
		if driftStatus.Action == cloudprovider.RebootNode {
			committed, err := commitDriftReboot(ctx, d.kubeClient, d.cluster, d.clock, candidate, driftStatus)
			if err != nil {
				return []Command{}, err
			}
			if !committed {
				continue
			}
			return []Command{rebootCommand(candidate)}, nil
		}
		// Simulate rescheduling the candidate's pods. When they can't be replaced-first and the candidate holds a full
		// reservation, this reports terminate-first (RFC #3203): delete the candidate and let reactive provisioning
		// refill the freed slot. See SimulateSchedulingWithReservedFallback.
		results, terminateFirst, err := SimulateSchedulingWithReservedFallback(ctx, d.kubeClient, d.cluster, d.provisioner, d.clock, d.recorder, candidate, options.FromContext(ctx).FeatureGates.TerminateFirstDrift, SimulationOptions{})
		if err != nil {
			// if a candidate is now deleting, just retry
			if errors.Is(err, errCandidateDeleting) {
				continue
			}
			return []Command{}, err
		}
		if terminateFirst {
			// Delete-only (no Replacements): carry the Results so existing nodes that can absorb the freed pods get
			// nominated. Reactive provisioning handles the rest.
			return []Command{{
				Candidates:           []*Candidate{candidate},
				Results:              results,
				PoolDisruptionCosts:  computePoolDisruptionCosts([]*Candidate{candidate}),
				TerminateFirstReason: TerminateFirstNoReservedCapacity,
			}}, nil
		}
		// Emit an event that we couldn't reschedule the pods on the node.
		if !results.AllNonPendingPodsScheduled() {
			d.recorder.Publish(disruptionevents.Blocked(candidate.Node, candidate.NodeClaim, pretty.Sentence(results.NonPendingPodSchedulingErrors()))...)
			continue
		}

		cmd := Command{
			Candidates:          []*Candidate{candidate},
			Replacements:        replacementsFromNodeClaims(results.NewNodeClaims...),
			Results:             results,
			PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{candidate}),
		}
		return []Command{cmd}, nil

	}
	return []Command{}, nil
}

// coreDriftReasons are the drift reasons core detects before asking the cloud provider. A provider's in-place action
// can't resolve them, so candidates drifted for one of these reasons are always replaced.
var coreDriftReasons = sets.New(
	string(nodeclaimdisruption.NodePoolDrifted),
	string(nodeclaimdisruption.RequirementsDrifted),
	string(nodeclaimdisruption.InstanceTypeNotFound),
)

// resolveDriftStatus returns how to resolve the candidate's drift, and false when the candidate should be skipped this
// pass. The Drifted condition can lag, so for provider-detected drift the provider is asked again at commit time:
// an error (for example an in-place operation that is still unresolved) or a NoOp skips the candidate, and the drift
// subcontroller clears a stale condition. Drift that core detected itself is replaced without asking the provider.
func resolveDriftStatus(ctx context.Context, cloudProvider cloudprovider.CloudProvider, candidate *Candidate) (cloudprovider.DriftStatus, bool) {
	reason := candidate.NodeClaim.StatusConditions().Get(v1.ConditionTypeDrifted).Reason
	if coreDriftReasons.Has(reason) {
		return cloudprovider.DriftStatus{Action: cloudprovider.ReplaceNode, Reason: cloudprovider.DriftReason(reason)}, true
	}
	driftStatus, err := cloudProvider.GetDriftStatus(ctx, candidate.NodeClaim)
	if err != nil {
		log.FromContext(ctx).WithValues("NodeClaim", klog.KObj(candidate.NodeClaim)).V(1).Info(fmt.Sprintf("skipping drift candidate, failed getting drift status, %s", err))
		return cloudprovider.DriftStatus{}, false
	}
	switch driftStatus.Action {
	case cloudprovider.ReplaceNode, cloudprovider.RebootNode:
		return driftStatus, true
	case cloudprovider.NoOp, "":
		return cloudprovider.DriftStatus{}, false
	default:
		log.FromContext(ctx).WithValues("NodeClaim", klog.KObj(candidate.NodeClaim), "action", driftStatus.Action).Error(
			fmt.Errorf("unsupported drift action %q", driftStatus.Action), "skipping drift candidate")
		return cloudprovider.DriftStatus{}, false
	}
}

// commitDriftReboot commits an in-place reboot that the cloud provider resolves the candidate's drift with.
func commitDriftReboot(ctx context.Context, kubeClient client.Client, cluster *state.Cluster, clk clock.Clock, candidate *Candidate,
	driftStatus cloudprovider.DriftStatus) (bool, error) {
	return commitReboot(ctx, kubeClient, cluster, clk, candidate, v1.DisruptionReasonDrifted,
		fmt.Sprintf("rebooting to resolve drift (%s)", driftStatus.Reason))
}

// rebootCommand records a committed reboot. The reboot controller already owns it, so the queue only records it.
func rebootCommand(candidate *Candidate) Command {
	return Command{
		Candidates:          []*Candidate{candidate},
		PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{candidate}),
		Reboot:              true,
	}
}

func (d *Drift) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonDrifted
}

func (d *Drift) Class() string {
	return EventualDisruptionClass
}

func (d *Drift) ConsolidationType() string {
	return ""
}

func driftBackoffEnabled(ctx context.Context, backoff *nodepoolbackoff.State) bool {
	return options.FromContext(ctx).FeatureGates.NodePoolDriftBackoff && backoff != nil
}

// clearUndriftedNodePoolBackoffs resets back-off state (including escalated level) for any NodePool
// that no longer has any non-deleted drifted NodeClaims in cluster state.
func clearUndriftedNodePoolBackoffs(ctx context.Context, cluster *state.Cluster, backoff *nodepoolbackoff.State) {
	if !driftBackoffEnabled(ctx, backoff) {
		return
	}
	driftedNodePools := sets.New[string]()
	for n := range cluster.Nodes() {
		if n.NodeClaim != nil && !n.Deleted() && n.NodeClaim.StatusConditions().Get(v1.ConditionTypeDrifted).IsTrue() {
			driftedNodePools.Insert(n.Labels()[v1.NodePoolLabelKey])
		}
	}
	backoff.ResetUndrifted(driftedNodePools)
}

// initDriftBackoffMetrics registers a zero-valued back-off counter for each NodePool so the metric is visible (at 0)
// for healthy pools rather than being absent until the first back-off. Add(0) is idempotent: it only ensures the
// series exists and never clobbers an incremented value.
func initDriftBackoffMetrics(ctx context.Context, backoff *nodepoolbackoff.State, nodePoolNames ...string) {
	if !driftBackoffEnabled(ctx, backoff) {
		return
	}
	for _, nodePoolName := range nodePoolNames {
		DriftBackoffsTotal.Add(0, map[string]string{metrics.NodePoolLabel: nodePoolName})
	}
}

// isDriftBackedOff reports whether the NodePool is currently backed off after repeated unrecoverable drift replacement
// failures, publishing an event when it is.
func isDriftBackedOff(ctx context.Context, backoff *nodepoolbackoff.State, recorder events.Recorder, nodePool *v1.NodePool) bool {
	if !driftBackoffEnabled(ctx, backoff) {
		return false
	}
	level, until, backedOff := backoff.GetBackoff(nodePool)
	if backedOff {
		recorder.Publish(disruptionevents.NodePoolDriftBackoff(nodePool, until, level))
	}
	return backedOff
}
