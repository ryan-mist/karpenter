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
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/serrors"
	"github.com/awslabs/operatorpkg/singleton"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	nodepoolutils "sigs.k8s.io/karpenter/pkg/utils/nodepool"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
)

type Controller struct {
	queue         *Queue
	kubeClient    client.Client
	cluster       *state.Cluster
	provisioner   *provisioning.Provisioner
	recorder      events.Recorder
	clock         clock.Clock
	cloudProvider cloudprovider.CloudProvider
	clusterCost   *cost.ClusterCost
	methods       []Method
	mu            sync.Mutex
	lastRun       map[string]time.Time
	// unsyncedSince is when the loop first found cluster state unsynced; zero while it is synced.
	unsyncedSince time.Time
}

// pollingPeriod that we inspect cluster to look for opportunities to disrupt
const pollingPeriod = 10 * time.Second

type ControllerOptions struct {
	methods []Method
}

func WithMethods(methods ...Method) option.Function[ControllerOptions] {
	return func(o *ControllerOptions) {
		o.methods = methods
	}
}

func NewController(ctx context.Context, clk clock.Clock, kubeClient client.Client, provisioner *provisioning.Provisioner,
	cp cloudprovider.CloudProvider, recorder events.Recorder, cluster *state.Cluster, queue *Queue, clusterCost *cost.ClusterCost, opts ...option.Function[ControllerOptions]) *Controller {

	o := option.Resolve(append([]option.Function[ControllerOptions]{WithMethods(NewMethods(ctx, clk, cluster, kubeClient, provisioner, cp, recorder, queue)...)}, opts...)...)
	// Initialize the skipped series to 0 so a method that is never starved still reports a rate.
	for _, m := range o.methods {
		SkippedEvaluationsTotal.Add(0, methodLabels(m))
	}
	return &Controller{
		queue:         queue,
		clock:         clk,
		kubeClient:    kubeClient,
		cluster:       cluster,
		provisioner:   provisioner,
		recorder:      recorder,
		cloudProvider: cp,
		clusterCost:   clusterCost,
		lastRun:       map[string]time.Time{},
		methods:       o.methods,
	}
}

func NewMethods(ctx context.Context, clk clock.Clock, cluster *state.Cluster, kubeClient client.Client, provisioner *provisioning.Provisioner, cp cloudprovider.CloudProvider, recorder events.Recorder, queue *Queue) []Method {
	c := MakeConsolidation(clk, cluster, kubeClient, provisioner, cp, recorder, queue)
	methods := []Method{}
	// Repair runs first: fixing a fault outranks any discretionary rebalance. Do not register the method while the
	// feature gate is disabled, since candidate construction performs cluster-wide API and scheduling work.
	// --legacy-node-repair runs the legacy node.health controller in place of this method.
	if options.FromContext(ctx).FeatureGates.NodeRepair && !options.FromContext(ctx).LegacyNodeRepair {
		methods = append(methods, NewRepair(c))
	}
	return append(methods, []Method{
		// Delete empty nodes across all consolidation policies (WhenEmpty, WhenEmptyOrUnderutilized, Balanced).
		NewEmptiness(c),
		// Terminate and create replacement for drifted NodeClaims in Static NodePool
		NewStaticDrift(cluster, provisioner, cp, recorder),
		// Terminate any NodeClaims that have drifted from provisioning specifications, allowing the pods to reschedule.
		NewDrift(kubeClient, cluster, provisioner, recorder, clk),
		// Attempt to identify multiple NodeClaims that we can consolidate simultaneously to reduce pod churn
		NewMultiNodeConsolidation(c),
		// And finally fall back our single NodeClaim consolidation to further reduce cluster cost.
		NewSingleNodeConsolidation(c),
	}...)
}

func (c *Controller) Name() string {
	return "disruption"
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}

//nolint:gocyclo
func (c *Controller) Reconcile(ctx context.Context) (reconciler.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())

	// this won't catch if the reconciler loop hangs forever, but it will catch other issues
	c.logAbnormalRuns(ctx)
	defer c.logAbnormalRuns(ctx)
	c.recordRun("disruption-loop")

	// Log if there are any budgets that are misconfigured that weren't caught by validation.
	// Only validate the first reason, since CEL validation will catch invalid disruption reasons
	c.logInvalidBudgets(ctx)

	// We need to ensure that our internal cluster state mechanism is synced before we proceed
	// with making any scheduling decision off of our state nodes. Otherwise, we have the potential to make
	// a scheduling decision based on a smaller subset of nodes in our cluster state than actually exist.
	if !c.cluster.Synced(ctx) {
		c.recordGated()
		return reconciler.Result{RequeueAfter: time.Second}, nil
	}
	c.recordUngated()

	// Karpenter taints nodes with a karpenter.sh/disruption taint as part of the disruption process while it progresses in memory.
	// If Karpenter restarts or fails with an error during a disruption action, some nodes can be left tainted.
	// Idempotently remove this taint from candidates that are not in the orchestration queue before continuing.
	// Reboot bypasses the queue, so preserve its DisruptionReason during drain.
	outdatedNodes := lo.Reject(c.cluster.DeepCopyNodes(), func(s *state.StateNode, _ int) bool {
		return c.queue.HasAny(s.ProviderID()) || s.MarkedForDeletion() || s.RebootInProgress()
	})
	if err := state.RequireNoScheduleTaint(ctx, c.kubeClient, false, outdatedNodes...); err != nil {
		if errors.IsConflict(err) {
			return reconciler.Result{Requeue: true}, nil
		}
		return reconciler.Result{}, serrors.Wrap(fmt.Errorf("removing taint from nodes, %w", err), "taint", pretty.Taint(v1.DisruptedNoScheduleTaint))
	}
	if err := state.ClearNodeClaimsCondition(ctx, c.kubeClient, c.clock, v1.ConditionTypeDisruptionReason, outdatedNodes...); err != nil {
		if errors.IsConflict(err) {
			return reconciler.Result{Requeue: true}, nil
		}
		return reconciler.Result{}, serrors.Wrap(fmt.Errorf("removing condition from nodeclaims, %w", err), "condition", v1.ConditionTypeDisruptionReason)
	}
	if options.FromContext(ctx).FeatureGates.PredictionEnabled && !c.provisioner.PredictionStoreHydrated(ctx) {
		return reconciler.Result{RequeueAfter: time.Second}, nil
	}

	// Attempt different disruption methods in order. A method whose commands only delete nodes lets the later methods
	// run in the same pass; any other command ends the pass.
	// Consecutive methods share one candidate snapshot, until one of them has candidates (see methodCandidates).
	var snapshot *CandidateSnapshot
	disrupted := false
	for i, m := range c.methods {
		c.recordRun(fmt.Sprintf("%T", m))
		cmds, err := c.disrupt(ctx, m, &snapshot)
		if err != nil {
			if errors.IsConflict(err) {
				return reconciler.Result{Requeue: true}, nil
			}
			return reconciler.Result{}, serrors.Wrap(fmt.Errorf("disrupting, %w", err), strings.ToLower(string(m.Reason())), "reason")
		}
		if len(cmds) == 0 {
			continue
		}
		disrupted = true
		// StartCommand marks delete-only candidates for deletion and adds them to the queue before it returns, so the
		// candidates and budgets that later methods build already account for them. Replacements are different: a
		// launching NodeClaim is not yet a StateNode, so later simulations can't see it until cluster state syncs.
		// Terminate-first and reboot also lead to capacity that isn't in cluster state yet, so they end the pass too.
		if !lo.EveryBy(cmds, func(cmd Command) bool { return cmd.Decision() == DeleteDecision }) || !c.cluster.Synced(ctx) {
			// The methods after this one are not evaluated this pass; a steady rate here is a starvation signal.
			for _, skipped := range c.methods[i+1:] {
				SkippedEvaluationsTotal.Inc(methodLabels(skipped))
			}
			return reconciler.Result{RequeueAfter: singleton.RequeueImmediately}, nil
		}
	}
	if disrupted {
		return reconciler.Result{RequeueAfter: singleton.RequeueImmediately}, nil
	}

	// All methods did nothing, so return nothing to do
	return reconciler.Result{RequeueAfter: pollingPeriod}, nil
}

// disrupt runs one method and starts its commands. It returns the commands it started.
func (c *Controller) disrupt(ctx context.Context, disruption Method, snapshot **CandidateSnapshot) ([]Command, error) {
	labels := methodLabels(disruption)
	defer metrics.Measure(EvaluationDurationSeconds, labels)()
	LastEvaluationTimestampSeconds.Set(float64(c.clock.Now().Unix()), labels)
	stats := &evaluationStats{}
	ctx = withEvaluationStats(ctx, stats)
	defer func() { SchedulingSimulationsPerEvaluation.Observe(float64(stats.simulations.Load()), labels) }()

	phaseStart := time.Now()
	candidates, allCandidates, s, err := methodCandidates(ctx, disruption, snapshot, func() (*CandidateSnapshot, error) {
		return NewCandidateSnapshot(ctx, c.cluster, c.kubeClient, c.recorder, c.clock, c.cloudProvider, c.queue)
	})
	observePhase(CandidatesPhase, labels, time.Since(phaseStart))
	if err != nil {
		return nil, fmt.Errorf("determining candidates, %w", err)
	}
	EligibleNodes.Set(float64(len(candidates)), map[string]string{
		metrics.ReasonLabel: strings.ToLower(string(disruption.Reason())),
	})

	// If there are no candidates, move to the next disruption
	if len(candidates) == 0 {
		return nil, nil
	}
	// Pass precomputed NodePool totals to consolidation methods for balanced scoring
	if setter, ok := disruption.(NodePoolTotalsSetter); ok {
		setter.SetNodePoolTotals(s.NodePoolTotals(ctx, allCandidates, c.clusterCost))
	}
	phaseStart = time.Now()
	disruptionBudgetMapping, err := BuildDisruptionBudgetMapping(ctx, c.cluster, c.clock, c.kubeClient, c.cloudProvider, c.recorder, disruption.Reason())
	observePhase(BudgetsPhase, labels, time.Since(phaseStart))
	if err != nil {
		return nil, fmt.Errorf("building disruption budgets, %w", err)
	}
	// Determine the disruption action
	phaseStart = time.Now()
	cmds, err := disruption.ComputeCommands(ctx, disruptionBudgetMapping, candidates...)
	validating := time.Duration(stats.validating.Load())
	observePhase(ComputePhase, labels, max(time.Since(phaseStart)-validating, 0))
	if stats.validations.Load() > 0 {
		observePhase(ValidatePhase, labels, validating)
	}
	// Computing a command with a static replacement reserves a node-count slot that StartCommand releases. Release the
	// slot of any command that never reaches StartCommand (ParallelizeUntil skips the rest once ctx is cancelled), or
	// static scale-up of its NodePool stays capped.
	started := make([]bool, len(cmds))
	defer func() {
		for i := range cmds {
			if !started[i] {
				c.queue.releaseStaticReservations(&cmds[i])
			}
		}
	}()
	if err != nil {
		return nil, fmt.Errorf("computing disruption decision, %w", err)
	}
	toStart := lo.Filter(lo.Range(len(cmds)), func(i int, _ int) bool { return cmds[i].Decision() != NoOpDecision })
	if len(toStart) == 0 {
		return nil, nil
	}

	phaseStart = time.Now()
	defer func() { observePhase(StartPhase, labels, time.Since(phaseStart)) }()
	errs := make([]error, len(toStart))
	workqueue.ParallelizeUntil(ctx, len(toStart), len(toStart), func(i int) {
		started[toStart[i]] = true
		cmd := cmds[toStart[i]]

		// Assign common fields
		cmd.CreationTimestamp = c.clock.Now()
		cmd.ID = uuid.New()
		cmd.Method = disruption

		// Attempt to disrupt
		if err := c.queue.StartCommand(ctx, &cmd); err != nil {
			errs[i] = fmt.Errorf("disrupting candidates, %w", err)
		}
	})
	if err = multierr.Combine(errs...); err != nil {
		return nil, fmt.Errorf("disrupting candidates, %w", err)
	}
	return lo.Map(toStart, func(i int, _ int) Command { return cmds[i] }), nil
}

// recordGated counts a pass that stopped at the cluster state sync gate and starts the gate period if it is new.
func (c *Controller) recordGated() {
	ClusterStateGatedPassesTotal.Inc(nil)
	if c.unsyncedSince.IsZero() {
		c.unsyncedSince = c.clock.Now()
	}
}

// recordUngated ends the current gate period, if any, and observes how long the loop waited.
func (c *Controller) recordUngated() {
	if c.unsyncedSince.IsZero() {
		return
	}
	ClusterStateGateDurationSeconds.Observe(c.clock.Since(c.unsyncedSince).Seconds(), nil)
	c.unsyncedSince = time.Time{}
}

// methodCandidates returns m's candidates, and all candidates of m's class (for NodePoolTotals), from *snapshot. It
// builds *snapshot first if it is nil. A method with no candidates only runs ShouldDisrupt, which writes nothing but its
// own Candidates, so the next method may reuse the snapshot. A method with candidates goes on to ComputeCommands, which
// can take minutes (simulations, validation waits) and modifies candidate pods (scheduling injects topology spread
// constraints and relaxes preferences in place), so *snapshot is cleared and the next method builds a fresh one, as
// every method did before snapshots were shared.
func methodCandidates(ctx context.Context, m Method, snapshot **CandidateSnapshot, build func() (*CandidateSnapshot, error)) (candidates, allCandidates []*Candidate, s *CandidateSnapshot, err error) {
	if *snapshot == nil {
		if *snapshot, err = build(); err != nil {
			return nil, nil, nil, err
		}
	}
	s = *snapshot
	allCandidates = s.Candidates(ctx, m.Class())
	candidates = lo.Filter(allCandidates, func(cn *Candidate, _ int) bool { return m.ShouldDisrupt(ctx, cn) })
	if len(candidates) > 0 {
		*snapshot = nil
	}
	return candidates, allCandidates, s, nil
}

func (c *Controller) recordRun(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastRun[s] = c.clock.Now()
}

func (c *Controller) logAbnormalRuns(ctx context.Context) {
	const AbnormalTimeLimit = 15 * time.Minute
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, runTime := range c.lastRun {
		if timeSince := c.clock.Since(runTime); timeSince > AbnormalTimeLimit {
			log.FromContext(ctx).V(1).Info("abnormal time between runs", "name", name, "time_since", timeSince)
		}
	}
}

// logInvalidBudgets will log if there are any invalid schedules detected
func (c *Controller) logInvalidBudgets(ctx context.Context) {
	nps, err := nodepoolutils.ListManaged(ctx, c.kubeClient, c.cloudProvider)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed listing nodepools")
		return
	}
	var buf bytes.Buffer
	for _, np := range nps {
		// Use a dummy value of 100 since we only care if this errors.
		for _, method := range c.methods {
			if _, err := np.GetAllowedDisruptionsByReason(c.clock, 100, method.Reason()); err != nil {
				fmt.Fprintf(&buf, "invalid disruption budgets in nodepool %s, %s", np.Name, err)
				break // Prevent duplicate error message
			}
		}
	}
	if buf.Len() > 0 {
		log.FromContext(ctx).Error(stderrors.New(buf.String()), "detected disruption budget errors")
	}
}
