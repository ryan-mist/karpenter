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
	"math"
	"slices"
	"sort"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/utils/pretty"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// syntheticFreedSlotSuffix is appended to the real NodePool name to name the synthetic freed-slot
// NodePool (and its reservation ID) injected during the terminate-first detection simulation.
const syntheticFreedSlotSuffix = "-synthetic-freed-slot"

// Drift is a subreconciler that deletes drifted candidates.
type Drift struct {
	kubeClient  client.Client
	cluster     *state.Cluster
	provisioner *provisioning.Provisioner
	recorder    events.Recorder
	clock       clock.Clock
}

func NewDrift(kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner, recorder events.Recorder, clk clock.Clock) *Drift {
	return &Drift{
		kubeClient:  kubeClient,
		cluster:     cluster,
		provisioner: provisioner,
		recorder:    recorder,
		clock:       clk,
	}
}

// ShouldDisrupt is a predicate used to filter candidates
func (d *Drift) ShouldDisrupt(ctx context.Context, c *Candidate) bool {
	return !c.OwnedByStaticNodePool() && c.NodeClaim.StatusConditions().Get(string(d.Reason())).IsTrue()
}

// ComputeCommand generates a disruption command given candidates
func (d *Drift) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
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

		// Terminate-first detection (RFC #3203): when this candidate sits on a healthy-but-full reserved
		// offering, run the scheduling simulation with a sim-scoped launchable filter and a synthetic
		// lowest-weight freed-slot NodePool injected. If the replacement can only land on that synthetic
		// pool, deleting the candidate is the only way to make room, so we issue a delete-only command.
		tfMode, syntheticPool, syntheticInstanceTypes, syntheticName := d.terminateFirstSimulation(ctx, candidate)
		var schedulerOpts []pscheduling.Options
		if tfMode != terminateFirstOff {
			schedulerOpts = append(schedulerOpts, pscheduling.TerminateFirstSimulation(syntheticPool, syntheticInstanceTypes))
		}

		// Check if we need to create any NodeClaims.
		results, err := SimulateScheduling(ctx, d.kubeClient, d.cluster, d.provisioner, d.clock, d.recorder, schedulerOpts, candidate)
		if err != nil {
			// if a candidate is now deleting, just retry
			if errors.Is(err, errCandidateDeleting) {
				continue
			}
			return []Command{}, err
		}
		// Emit an event that we couldn't reschedule the pods on the node.
		if !results.AllNonPendingPodsScheduled() {
			d.recorder.Publish(disruptionevents.Blocked(candidate.Node, candidate.NodeClaim, pretty.Sentence(results.NonPendingPodSchedulingErrors()))...)
			continue
		}

		// Terminate-first: if every new NodeClaim would land on the synthetic freed-slot pool, then the only
		// place the pods fit is the slot that would be freed by deleting the candidate. Issue a delete-only
		// command (no Replacements, no Results) and let reactive provisioning refill the freed reservation.
		if tfMode == terminateFirstInject && len(results.NewNodeClaims) > 0 &&
			lo.EveryBy(results.NewNodeClaims, func(nc *pscheduling.NodeClaim) bool { return nc.NodePoolName == syntheticName }) {
			return []Command{{
				Candidates:          []*Candidate{candidate},
				PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{candidate}),
			}}, nil
		}

		// Replace-first: a replacement landed on a real NodePool. Strip any synthetic NewNodeClaims from the
		// results before building Replacements so a fake NodePoolName never reaches Create.
		if tfMode == terminateFirstInject {
			results.NewNodeClaims = lo.Filter(results.NewNodeClaims, func(nc *pscheduling.NodeClaim, _ int) bool {
				return nc.NodePoolName != syntheticName
			})
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

// terminateFirstMode describes how the terminate-first detection simulation should be run for a candidate.
type terminateFirstMode int

const (
	// terminateFirstOff runs a normal drift simulation (feature disabled, non-reserved candidate, or a
	// reserved offering that still has spare capacity — the normal replace-first path handles those).
	terminateFirstOff terminateFirstMode = iota
	// terminateFirstInject runs the simulation with the launchable filter AND a synthetic freed-slot pool.
	// Used when the candidate's reserved offering is healthy but full.
	terminateFirstInject
	// terminateFirstFilterOnly runs the simulation with the launchable filter but no synthetic pool. Used
	// when the candidate's reserved offering is unhealthy: no replacement can be staged anywhere, so the sim
	// blocks and no terminate-first (or replace) command is produced.
	terminateFirstFilterOnly
)

// terminateFirstSimulation decides whether/how to run a terminate-first detection simulation for the
// candidate, returning the mode and (for terminateFirstInject) the synthetic freed-slot NodePool, its
// instance types, and its name.
func (d *Drift) terminateFirstSimulation(ctx context.Context, candidate *Candidate) (terminateFirstMode, *v1.NodePool, []*cloudprovider.InstanceType, string) {
	if !options.FromContext(ctx).FeatureGates.TerminateFirst {
		return terminateFirstOff, nil, nil, ""
	}
	if candidate.capacityType != v1.CapacityTypeReserved || candidate.instanceType == nil {
		return terminateFirstOff, nil, nil, ""
	}
	// Find the candidate's reserved offering for its zone.
	reservedOffering, ok := lo.Find(candidate.instanceType.Offerings, func(o *cloudprovider.Offering) bool {
		return o.CapacityType() == v1.CapacityTypeReserved && o.Zone() == candidate.zone
	})
	if !ok {
		return terminateFirstOff, nil, nil, ""
	}
	switch {
	case reservedOffering.Available && reservedOffering.ReservationCapacity == 0:
		// Healthy but full: inject the synthetic freed-slot pool to detect terminate-first.
		pool, instanceTypes, name := buildSyntheticFreedSlotPool(candidate, reservedOffering)
		return terminateFirstInject, pool, instanceTypes, name
	case !reservedOffering.Available:
		// Unhealthy: no replacement can be staged; filter-only so the sim blocks and we don't terminate-first.
		return terminateFirstFilterOnly, nil, nil, ""
	default:
		// Spare reservation capacity remains: the normal replace-first path can stage a reserved replacement.
		return terminateFirstOff, nil, nil, ""
	}
}

// buildSyntheticFreedSlotPool constructs the in-memory, lowest-weight NodePool (and its single instance type)
// representing the reservation slot that deleting the candidate would free. The synthetic instance type is
// cloned from the candidate's instance type and given a single reserved offering with a DISTINCT reservation
// ID and ReservationCapacity=1. The distinct ID is mandatory: the reservation manager tracks the MIN capacity
// across a shared reservation ID, and the candidate's real (full) reservation is 0.
func buildSyntheticFreedSlotPool(candidate *Candidate, reservedOffering *cloudprovider.Offering) (*v1.NodePool, []*cloudprovider.InstanceType, string) {
	name := candidate.NodePool.Name + syntheticFreedSlotSuffix

	pool := candidate.NodePool.DeepCopy()
	pool.Name = name
	pool.Spec.Weight = lo.ToPtr(int32(math.MinInt32 / 2))
	pool.Status = v1.NodePoolStatus{}
	pool.DeletionTimestamp = nil

	// Build the synthetic reserved offering with a distinct reservation ID.
	reservationID := reservedOffering.ReservationID() + syntheticFreedSlotSuffix
	reqs := scheduling.NewRequirements()
	for _, r := range reservedOffering.Requirements.Values() {
		if r.Key == cloudprovider.ReservationIDLabel {
			continue
		}
		reqs.Add(r)
	}
	reqs.Add(scheduling.NewRequirement(cloudprovider.ReservationIDLabel, corev1.NodeSelectorOpIn, reservationID))

	instanceType := candidate.instanceType.DeepCopy()
	instanceType.Offerings = cloudprovider.Offerings{{
		Available:           true,
		ReservationCapacity: 1,
		Price:               reservedOffering.Price,
		Requirements:        reqs,
	}}
	return pool, []*cloudprovider.InstanceType{instanceType}, name
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
