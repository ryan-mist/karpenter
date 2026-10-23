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

// Package pipeline is the disruption controller behind the DisruptionPipeline feature gate (alpha, default off). It
// replaces the disruption package's "first method wins" loop with Observe → Plan → Admit & Execute: lanes of
// side-effect-free planners plan on one snapshot per round, and a single commit loop admits their plans against live
// state and starts them through the shared disruption.Queue.
//
// With the gate off this package is not constructed, and the disruption package's controller runs as on mainline.
// The package imports disruption; disruption never imports it.
package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/api/errors"

	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/singleton"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/cost"
)

// pollingPeriod is how long the controller waits after a round that admitted nothing, as the disruption controller's
// own polling period.
const pollingPeriod = 10 * time.Second

// Options are the pipeline's parts. Every part is injectable; NewController fills in the defaults.
type Options struct {
	// Lanes run in this order every Reconcile.
	Lanes     []LaneSpec
	Snapshots SnapshotBuilder
	Ledger    Ledger
	Admitter  Admitter
	Starter   Starter
	Executor  Executor
	Budgets   BudgetView
	Reserved  ReservationView
	Observers []Observer
	// Reference plans as a pass of the disruption controller does (WithReferenceMode).
	Reference bool
}

// WithReferenceMode makes the pipeline plan as a pass of the disruption controller does, for the batch=1
// differential (G9), its only user: without injected lanes, ReferenceLanes over
// DefaultPlanners(..., WithReference()); and the nodes reserved plans move pods onto aren't candidates, as the
// disruption controller nominates a started command's destinations before its next method looks for candidates.
func WithReferenceMode() option.Function[Options] {
	return func(o *Options) { o.Reference = true }
}

func WithLedger(l Ledger) option.Function[Options] {
	return func(o *Options) { o.Ledger = l }
}

func WithStarter(s Starter) option.Function[Options] {
	return func(o *Options) { o.Starter = s }
}

func WithLanes(lanes ...LaneSpec) option.Function[Options] {
	return func(o *Options) { o.Lanes = lanes }
}

func WithExecutor(e Executor) option.Function[Options] {
	return func(o *Options) { o.Executor = e }
}

func WithSnapshots(b SnapshotBuilder) option.Function[Options] {
	return func(o *Options) { o.Snapshots = b }
}

func WithBudgets(b BudgetView) option.Function[Options] {
	return func(o *Options) { o.Budgets = b }
}

func WithReservations(r ReservationView) option.Function[Options] {
	return func(o *Options) { o.Reserved = r }
}

func WithObservers(obs ...Observer) option.Function[Options] {
	return func(o *Options) { o.Observers = append(o.Observers, obs...) }
}

// Controller is the pipeline disruption controller. It shares the disruption.Queue with the disruption package, so
// started commands are executed exactly as the disruption controller's are.
//
// Reconcile is the synchronous runner: every lane runs one round in order, and after each round the commit loop
// runs until no plan is ready, waiting on the clock for a plan's ValidateAt as the disruption controller waits for its
// validation period. That keeps singleton-reconciled tests deterministic.
type Controller struct {
	clock    clock.Clock
	cluster  *state.Cluster
	lanes    []Lane
	executor Executor
	wakes    *WakeHub
}

func NewController(ctx context.Context, clk clock.Clock, kubeClient client.Client, provisioner *provisioning.Provisioner,
	cp cloudprovider.CloudProvider, recorder events.Recorder, cluster *state.Cluster, queue *disruption.Queue, clusterCost *cost.ClusterCost,
	opts ...option.Function[Options],
) *Controller {
	o := option.Resolve(opts...)
	if o.Ledger == nil {
		o.Ledger = NewLedger(kubeClient, cluster, cp, clk, queue, lo.Ternary(o.Reference, []option.Function[LedgerOptions]{withReservedCredit()}, nil)...)
	}
	if o.Budgets == nil {
		o.Budgets = o.Ledger.Budgets()
	}
	// The disruption methods' own events (filters, validators) are buffered and published by the Observer.
	methodEvents := &RoundEvents{}
	switch {
	case o.Lanes != nil:
	case o.Reference:
		o.Lanes = ReferenceLanes(DefaultPlanners(ctx, clk, cluster, kubeClient, provisioner, cp, queue, methodEvents, o.Ledger.View(), WithReference()))
	default:
		o.Lanes = DefaultLanes(DefaultPlanners(ctx, clk, cluster, kubeClient, provisioner, cp, queue, methodEvents, o.Ledger.View()))
	}
	wakes := NewWakeHub()
	mo := NewMetricsObserver(clk, kubeClient, recorder, o.Ledger, methodEvents, o.Lanes...)
	mo.(*metricsObserver).logger = log.FromContext(ctx).WithName("disruption.pipeline")
	o.Observers = append([]Observer{mo, newWakeObserver(wakes, o.Lanes...)}, o.Observers...)
	// A completed command frees its nodes' budget slots.
	queue.WithCompletionHook(func(*disruption.Command) { wakes.Publish("", types.WakeLedgerRelease) })
	if o.Reserved == nil {
		o.Reserved = o.Ledger.View()
		if o.Reference {
			o.Reserved = referenceView{o.Reserved}
		}
	}
	if o.Admitter == nil {
		planners := lo.FlatMap(o.Lanes, func(l LaneSpec, _ int) []Planner { return l.Planners })
		o.Admitter = NewAdmitter(o.Ledger, NewCandidateRebuilder(kubeClient, cluster, clk, cp, queue), planners...)
	}
	if o.Starter == nil {
		o.Starter = NewQueueStarter(queue, clk)
	}
	if o.Executor == nil {
		o.Executor = NewExecutor(ExecutorDeps{
			Planners: lo.FlatMap(o.Lanes, func(l LaneSpec, _ int) []Planner { return l.Planners }),
			Ledger:   o.Ledger,
			Admitter: o.Admitter,
			Starter:  o.Starter,
			Sweeper:  disruption.NewMarkSweeper(kubeClient, cluster, clk),
			Queue:    queue,
			Cluster:  cluster,
			Clock:    clk,
			Scorer:   NewScorer(o.Lanes...),
			Observer: observers(o.Observers),
		})
	}
	if o.Snapshots == nil {
		o.Snapshots = NewSnapshotBuilder(kubeClient, cluster, clk, cp, queue, clusterCost, o.Reserved)
	}
	deps := LaneDeps{
		Clock:     clk,
		Cluster:   cluster,
		Snapshots: o.Snapshots,
		Executor:  o.Executor,
		Budgets:   o.Budgets,
		Reserved:  o.Reserved,
		Hydrated: func(ctx context.Context) bool {
			return !options.FromContext(ctx).FeatureGates.PredictionEnabled || provisioner.PredictionStoreHydrated(ctx)
		},
		Observer: observers(o.Observers),
	}
	return &Controller{
		clock:    clk,
		cluster:  cluster,
		lanes:    lo.Map(o.Lanes, func(spec LaneSpec, _ int) Lane { return NewLane(spec, deps) }),
		executor: o.Executor,
		wakes:    wakes,
	}
}

func (c *Controller) Name() string {
	return "disruption.pipeline"
}

// Register runs the pipeline asynchronously on the leader: one goroutine per lane, paced by the lane's trigger, and
// one for the commit loop. Reconcile (the synchronous runner) is for tests.
func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	for _, l := range c.lanes {
		if err := m.Add(&laneRunner{lane: l, trigger: c.trigger(l.Spec()), clock: c.clock}); err != nil {
			return err
		}
	}
	return m.Add(&commitRunner{executor: c.executor, clock: c.clock})
}

// trigger is the lane's trigger; a PollTrigger (the default) is also woken by the pipeline's wake-ups.
func (c *Controller) trigger(spec LaneSpec) Trigger {
	switch t := spec.Trigger.(type) {
	case nil:
		return wakeTrigger{wake: c.wakes.Subscribe(spec.ID)}
	case PollTrigger:
		return wakeTrigger{PollTrigger: t, wake: c.wakes.Subscribe(spec.ID)}
	default:
		return t
	}
}

// Reconcile runs one round of every lane, each followed by the commit loop (which also runs once with no lanes, for
// its sweep). It requeues immediately if a plan started, after a second if plans wait for a gate (as the disruption
// controller waits for cluster state to sync), and after the polling period otherwise.
func (c *Controller) Reconcile(ctx context.Context) (reconciler.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())
	started, waiting := 0, 0
	if len(c.lanes) == 0 {
		n, w, err := c.drain(ctx)
		if err != nil {
			return reconciler.Result{}, err
		}
		started, waiting = n, w
	}
	for _, l := range c.lanes {
		r := l.Step(ctx)
		if r.Err != nil {
			if errors.IsConflict(r.Err) {
				return reconciler.Result{Requeue: true}, nil
			}
			return reconciler.Result{}, fmt.Errorf("planning in lane %s, %w", r.Lane, r.Err)
		}
		n, w, err := c.drain(ctx)
		started += n
		waiting = w
		if err != nil {
			return reconciler.Result{}, err
		}
	}
	switch {
	case started > 0:
		return reconciler.Result{RequeueAfter: singleton.RequeueImmediately}, nil
	case waiting > 0:
		return reconciler.Result{RequeueAfter: time.Second}, nil
	default:
		return reconciler.Result{RequeueAfter: pollingPeriod}, nil
	}
}

// drain runs commit steps until no plan is ready, waiting for a waiting plan's ValidateAt. It returns how many plans
// started and how many still wait for a gate.
func (c *Controller) drain(ctx context.Context) (started, waiting int, err error) {
	if c.executor == nil {
		return 0, 0, nil
	}
	for {
		r := c.executor.Step(ctx)
		started += r.Admitted
		// Plans that waited for ones admitted in this step to start (rule 9) are ready now.
		if r.Waiting > 0 && r.Admitted > 0 {
			continue
		}
		if r.Waiting == 0 || r.NextReady.IsZero() {
			return started, r.Waiting, nil
		}
		select {
		case <-c.clock.After(r.NextReady.Sub(c.clock.Now())):
		case <-ctx.Done():
			return started, r.Waiting, ctx.Err()
		}
	}
}
