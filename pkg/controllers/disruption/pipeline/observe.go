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
	"strings"
	"sync"
	"time"

	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
)

// metricsObserver is the pipeline's Observer: it emits the pipeline's metrics, the disruption controller's metrics
// whose meaning carries over, and the pipeline's events.
//
// Events: a round's buffered events go to the recorder as the disruption controller publishes them (the recorder's
// dedupe applies as today). A planner's explanations of why it did nothing for a NodePool become NodePool events,
// published when the explanation changes.
type metricsObserver struct {
	clock      clock.Clock
	kubeClient client.Client
	recorder   events.Recorder
	ledger     Ledger
	laneOf     map[string]types.LaneID
	reasons    map[types.LaneID][]v1.DisruptionReason
	planners   map[types.LaneID][]Planner

	mu        sync.Mutex
	lastRound map[types.LaneID]time.Time
	// unresolved counts each lane's submitted, unresolved plans. A plan rejected at Submit resolves before its round
	// counts it, so the count may be below zero inside a round; the gauge never is.
	unresolved map[types.LaneID]int
	// roundGates are the gates the current round of a lane hit, counted once per round.
	roundGates    map[types.LaneID]sets.Set[types.GateReason]
	unsyncedSince time.Time
	// explained is the last explanation published per (planner, NodePool).
	explained map[[2]string]string
}

func NewMetricsObserver(clk clock.Clock, kubeClient client.Client, recorder events.Recorder, ledger Ledger, lanes ...LaneSpec) Observer {
	o := &metricsObserver{
		clock: clk, kubeClient: kubeClient, recorder: recorder, ledger: ledger,
		laneOf: map[string]types.LaneID{}, reasons: map[types.LaneID][]v1.DisruptionReason{}, planners: map[types.LaneID][]Planner{},
		lastRound: map[types.LaneID]time.Time{}, unresolved: map[types.LaneID]int{}, explained: map[[2]string]string{},
		roundGates: map[types.LaneID]sets.Set[types.GateReason]{},
	}
	for _, l := range lanes {
		for _, p := range l.Planners {
			o.laneOf[p.Name()] = l.ID
			o.planners[l.ID] = append(o.planners[l.ID], p)
		}
		o.reasons[l.ID] = lo.Uniq(lo.Map(l.Planners, func(p Planner, _ int) v1.DisruptionReason { return p.Reason() }))
		UnresolvedPlans.Set(0, map[string]string{laneLabel: string(l.ID)})
	}
	return o
}

func (o *metricsObserver) RoundStarted(types.LaneID) {}

func (o *metricsObserver) RoundFinished(ctx context.Context, lane types.LaneID, r StepResult, ev *RoundEvents) {
	labels := map[string]string{laneLabel: string(lane)}
	o.mu.Lock()
	last := o.lastRound[lane]
	o.lastRound[lane] = r.EndedAt
	o.unresolved[lane] += r.Plans
	UnresolvedPlans.Set(float64(max(o.unresolved[lane], 0)), labels)
	gates := o.roundGates[lane]
	delete(o.roundGates, lane)
	o.mu.Unlock()
	// Budgets are explained whether or not the lane planned: a lane gated on budget is the common case.
	explanations := o.budgets(ctx, lane)
	if ev != nil {
		o.recorder.Publish(ev.Events()...)
		explanations = append(explanations, ev.NoActions()...)
	}
	o.explain(ctx, explanations)
	// Each gate the round hit (the lane's, or one or more of its planners') accounts for the round's interval once.
	if r.Gated != types.GateNone {
		gates = gates.Union(sets.New(r.Gated))
	}
	if !last.IsZero() {
		for gate := range gates {
			GatedSecondsTotal.Add(r.EndedAt.Sub(last).Seconds(), lo.Assign(labels, map[string]string{gateLabel: string(gate)}))
		}
	}
	switch {
	case r.Gated != types.GateNone:
		RoundsTotal.Inc(lo.Assign(labels, map[string]string{resultLabel: roundGated}))
		return
	case r.Plans > 0:
		RoundsTotal.Inc(lo.Assign(labels, map[string]string{resultLabel: roundPlanned}))
	default:
		RoundsTotal.Inc(lo.Assign(labels, map[string]string{resultLabel: roundNoop}))
	}
	RoundDurationSeconds.Observe(r.SnapshotTook.Seconds(), lo.Assign(labels, map[string]string{phaseLabel: phaseSnapshot}))
	RoundDurationSeconds.Observe(r.PlanTook.Seconds(), lo.Assign(labels, map[string]string{phaseLabel: phasePlan}))
}

// budgets sets the budget gauges, publishes the disruption controller's event for a NodePool whose budget allows no
// disruption for a reason, and returns explanations for the planners whose NodePool's budget is used up.
func (o *metricsObserver) budgets(ctx context.Context, lane types.LaneID) (out []NoAction) {
	var nodePools map[string]*v1.NodePool
	for _, reason := range o.reasons[lane] {
		for _, u := range o.ledger.Usage(ctx, reason) {
			allowed := u.Allowed[reason]
			l := map[string]string{metrics.NodePoolLabel: u.NodePool, metrics.ReasonLabel: strings.ToLower(string(reason))}
			disruption.NodePoolAllowedDisruptions.Set(float64(allowed), l)
			disruption.NodePoolNodesConsumingBudgets.Set(float64(u.Used), l)
			LedgerReservedNodes.Set(float64(u.Reserved), map[string]string{metrics.NodePoolLabel: u.NodePool})
			if u.NumNodes == 0 {
				continue
			}
			if nodePools == nil {
				nodePools = o.nodePools(ctx)
			}
			np, ok := nodePools[u.NodePool]
			if !ok {
				continue
			}
			if allowed == 0 {
				o.recorder.Publish(disruptionevents.NodePoolBlockedForDisruptionReason(np, reason))
				continue
			}
			if u.Used >= allowed {
				for _, p := range o.planners[lane] {
					if p.Reason() == reason {
						out = append(out, NoAction{Planner: p.Name(), NodePool: u.NodePool, Reason: events.DisruptionBudgetExhausted,
							Message: budgetExhaustedMessage(p.Name(), reason, u.Used, allowed)})
					}
				}
			}
		}
	}
	return out
}

func budgetExhaustedMessage(planner string, reason v1.DisruptionReason, used, allowed int) string {
	return fmt.Sprintf("%s is waiting for budget: %d of %d allowed nodes are disrupting (reason %s; in-flight nodes count against every reason)",
		planner, used, allowed, reason)
}

// explain publishes the explanations that changed since the last one for the same planner and NodePool.
func (o *metricsObserver) explain(ctx context.Context, all []NoAction) {
	if len(all) == 0 {
		return
	}
	nodePools := o.nodePools(ctx)
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, n := range all {
		key, value := [2]string{n.Planner, n.NodePool}, n.Reason+"/"+n.Message
		np, ok := nodePools[n.NodePool]
		if !ok || o.explained[key] == value {
			continue
		}
		o.explained[key] = value
		o.recorder.Publish(disruptionevents.NodePoolNoAction(np, n.Reason, n.Planner, n.Message))
	}
}

func (o *metricsObserver) nodePools(ctx context.Context) map[string]*v1.NodePool {
	list := &v1.NodePoolList{}
	if err := o.kubeClient.List(ctx, list); err != nil {
		return nil
	}
	return lo.SliceToMap(list.Items, func(np v1.NodePool) (string, *v1.NodePool) { return np.Name, &np })
}

func (o *metricsObserver) Gated(lane types.LaneID, planner string, gate types.GateReason) {
	if planner == "" {
		return // the lane's own gate is in its round's result
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.roundGates[lane] = o.roundGates[lane].Union(sets.New(gate))
}

func (o *metricsObserver) Planned(_ types.LaneID, p Planner, candidates int, out PlanOutput, took time.Duration) {
	PlannerDurationSeconds.Observe(took.Seconds(), map[string]string{plannerLabel: p.Name()})
	// The disruption controller's method metrics, which keep their meaning: when the method last ran, and its
	// eligible nodes.
	labels := map[string]string{metrics.ReasonLabel: strings.ToLower(string(p.Reason())), disruption.ConsolidationTypeLabel: consolidationType(p)}
	disruption.LastEvaluationTimestampSeconds.Set(float64(o.clock.Now().Unix()), labels)
	disruption.EligibleNodes.Set(float64(candidates), map[string]string{metrics.ReasonLabel: strings.ToLower(string(p.Reason()))})
	if len(out.Plans) == 0 {
		return
	}
	// A planner that acts on a NodePool gets its explanation published again the next time it does nothing there.
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, plan := range out.Plans {
		for _, c := range plan.Candidates {
			delete(o.explained, [2]string{p.Name(), c.NodePool.Name})
		}
	}
}

func consolidationType(p any) string {
	if ct, ok := p.(interface{ ConsolidationType() string }); ok {
		return ct.ConsolidationType()
	}
	return ""
}

func (o *metricsObserver) Admitted(group []*Pending, r AdmitResult, took time.Duration) {
	planner := group[0].Plan.Planner
	AdmissionDurationSeconds.Observe(took.Seconds(), map[string]string{plannerLabel: planner})
	if lane, ok := o.laneOf[planner]; ok {
		RoundDurationSeconds.Observe(took.Seconds(), map[string]string{laneLabel: string(lane), phaseLabel: phaseAdmit})
	}
	for _, cause := range r.Resims {
		AdmissionResimsTotal.Inc(map[string]string{plannerLabel: planner, causeLabel: string(cause)})
	}
	// The re-validation's events (why a candidate is now blocked), as the disruption controller's validation publishes.
	for _, ev := range r.Events {
		o.recorder.Publish(ev.Events()...)
	}
	// Consolidation's validation failures keep their counter, counted per candidate as the disruption controller's
	// validation counts them.
	for _, p := range group {
		if _, rejected := r.Rejected[p.Plan.ID]; rejected && p.Plan.Method != nil && p.Plan.Method.ConsolidationType() != "" {
			disruption.FailedValidationsTotal.Add(float64(len(p.Plan.Candidates)), map[string]string{disruption.ConsolidationTypeLabel: p.Plan.Method.ConsolidationType()})
		}
	}
}

func (o *metricsObserver) Resolved(p *Plan, r Resolution, waited time.Duration) {
	result := map[ResolutionKind]string{Started: planAdmitted, Rejected: planRejected, Dropped: planDropped}[r.Kind]
	PlansTotal.Inc(map[string]string{plannerLabel: p.Planner, resultLabel: result, metrics.ReasonLabel: string(r.Reason)})
	lane, ok := o.laneOf[p.Planner]
	if r.Kind == Started {
		LastAdmittedTimestampSeconds.Set(float64(o.clock.Now().Unix()), map[string]string{plannerLabel: p.Planner})
		if ok {
			PlanWaitSeconds.Observe(waited.Seconds(), map[string]string{laneLabel: string(lane), plannerLabel: p.Planner})
		}
	}
	// Every submitted plan resolves once, back-pressure rejections too: the lane counted them in its round's Plans.
	if ok {
		o.mu.Lock()
		o.unresolved[lane]--
		UnresolvedPlans.Set(float64(max(o.unresolved[lane], 0)), map[string]string{laneLabel: string(lane)})
		o.mu.Unlock()
	}
}

func (o *metricsObserver) Swept(took time.Duration, _ error) {
	TaintSweepDurationSeconds.Observe(took.Seconds(), nil)
}

// CommitStepFinished observes each period in which cluster state was unsynced when it ends, as the disruption
// controller does at its sync gate. Pod-free plans may still be admitted during it.
func (o *metricsObserver) CommitStepFinished(_ CommitStepResult, synced bool, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	switch {
	case !synced && o.unsyncedSince.IsZero():
		o.unsyncedSince = o.clock.Now()
	case synced && !o.unsyncedSince.IsZero():
		disruption.ClusterStateGateDurationSeconds.Observe(o.clock.Since(o.unsyncedSince).Seconds(), nil)
		o.unsyncedSince = time.Time{}
	}
}
