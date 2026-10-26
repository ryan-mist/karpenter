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
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/awslabs/operatorpkg/serrors"
	"github.com/go-logr/logr"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/state/cost"
)

// ShadowEnv is the environment variable that turns the shadow mode on: the fraction of the disruption controller's
// method computations (0 to 1) the pipeline plans beside. It is undocumented and unsupported, for tests, KWOK and soak
// clusters only; there is no flag, option or Helm value for it (RFC D.3, F.1).
const ShadowEnv = "KARPENTER_DISRUPTION_PIPELINE_SHADOW"

// Internal constants of the shadow mode. None of them is a user setting.
const (
	// shadowResamples is how many times a recheck samples the planner, since upstream is nondeterministic on some states
	// (zone ties, equal disruption costs, NodePools in map order).
	shadowResamples = 4
	// shadowLogEvery is how often one (planner, level, kind) mismatch is logged.
	shadowLogEvery = 10 * time.Minute
	// shadowWait is how long a sample waits for the method's commands before it gives up.
	shadowWait = 15 * time.Minute
)

// ShadowRate is the sample rate ShadowEnv sets: 0 if it is unset, not a number, not finite or not positive, and at
// most 1.
func ShadowRate() float64 {
	rate, err := strconv.ParseFloat(os.Getenv(ShadowEnv), 64)
	if err != nil || math.IsNaN(rate) || math.IsInf(rate, 0) || rate <= 0 {
		return 0
	}
	return min(rate, 1)
}

// Shadow is the pipeline's shadow mode (RFC D.3): with the DisruptionPipeline gate off, the disruption controller is
// the reference and the only actor, and for a sampled fraction of its method computations the pipeline plans on the
// same state beside it and never writes.
//   - snapshot level: the planner's candidates (Snapshot.CandidatesFor on the nodes the method's candidates were built
//     from) against the method's, except nodes in-flight commands move pods onto (R72), and the Ledger's remaining budget
//     per NodePool against the method's budgets. Computations without candidates are compared too.
//   - reference level: one plan of the reference planner (WithReference, no consolidation dedup) against the commands
//     the method computed, exactly or up to equivalent nodes.
//
// Off the disruption controller's path: Computing, called before the method computes, only builds the snapshot on the
// method's nodes, compares the snapshot level and copies the nodes; a goroutine then plans on that copy (its
// simulations see the state the method computed on, whatever the disruption controller starts meanwhile), waits for
// the method's commands (Computed only hands them over) and compares. A mismatch is re-checked in the same goroutine:
// the shadow's own copy of the method's core (legacy options without validation, dedup, metrics or side effects) and
// the planner (up to shadowResamples samples, since upstream is nondeterministic on some states) run back to back on
// fresh state; if they agree, the difference came from a state change or upstream nondeterminism (cause race),
// otherwise cause logic. Static drift isn't re-checked, so its mismatches are logic. No computation is sampled while a
// sample's goroutine runs (RFC D.3). The shadow's work runs with the controller name disruption.shadow, outside the
// disruption controller's evaluation statistics, with the methods' own logging discarded; mismatches are logged to
// the root logger, captured at construction, once per (planner, level, kind) per 10 minutes.
//
// Everything the shadow runs is its own: methods and planners built with a discarding recorder, simulations without
// pending-pod side effects, a Ledger that is never reserved, static counts only read, no starter, no queue hook, and
// none of the disruption controller's metrics. A sampled computation costs the disruption controller a snapshot build
// (lists of NodePools, instance types and PDBs, and the planner's candidates) and a copy of the nodes; the planner call,
// and on a mismatch a core call and up to shadowResamples planner calls, run in the goroutine.
type Shadow struct {
	clock         clock.Clock
	kubeClient    client.Client
	cluster       *state.Cluster
	cloudProvider cloudprovider.CloudProvider
	queue         *disruption.Queue
	clusterCost   *cost.ClusterCost
	ledger        Ledger
	planners      map[string]Planner
	// methods are the shadow's own copies of the disruption methods, the ones its planners plan with, by planner name.
	methods map[string]disruption.Method
	sim     disruption.Simulator
	logger  logr.Logger
	rate    float64
	sample  func() float64

	// busy is set while a sample's goroutine runs (plan, compare, recheck); no computation is sampled meanwhile.
	busy atomic.Bool
	jobs sync.WaitGroup

	mu      sync.Mutex
	pending map[string]*shadowJob
	agreed  map[string][2]int
	logged  map[[3]string]time.Time
}

// shadowJob is one sampled computation: the goroutine that plans it waits for the method's result.
type shadowJob struct {
	// legacy receives the method's commands, or nil if it failed (buffered: Computed never waits).
	legacy chan *[]disruption.Command
}

// NewShadow returns the shadow mode at the given sample rate (ShadowRate), for disruption.WithComputeObserver.
func NewShadow(ctx context.Context, clk clock.Clock, kubeClient client.Client, provisioner *provisioning.Provisioner, cp cloudprovider.CloudProvider,
	cluster *state.Cluster, queue *disruption.Queue, clusterCost *cost.ClusterCost, rate float64,
) *Shadow {
	l := NewLedger(kubeClient, cluster, cp, clk, queue)
	planners := DefaultPlanners(ctx, clk, cluster, kubeClient, provisioner, cp, queue, discardRecorder{}, l.View(),
		WithReference(), func(o *PlannerOptions) { o.shadow = true })
	methods := map[string]disruption.Method{}
	for name, p := range planners {
		if mo, ok := p.(interface{ methodOf() disruption.Method }); ok {
			methods[name] = mo.methodOf()
		}
	}
	return &Shadow{
		clock:         clk,
		kubeClient:    kubeClient,
		cluster:       cluster,
		cloudProvider: cp,
		queue:         queue,
		clusterCost:   clusterCost,
		ledger:        l,
		planners:      planners,
		methods:       methods,
		sim:           NewSimulator(disruption.NewLegacySimulator(kubeClient, cluster, provisioner, clk, discardRecorder{}), nil),
		logger:        log.FromContext(ctx).WithName("disruption.pipeline.shadow"),
		rate:          rate,
		sample:        rand.Float64,
		pending:       map[string]*shadowJob{},
		agreed:        map[string][2]int{},
		logged:        map[[3]string]time.Time{},
	}
}

// discardRecorder publishes nothing: the shadow's planners never publish.
type discardRecorder struct{}

func (discardRecorder) Publish(...events.Event) {}

// plannerFor is the planner name of a disruption method.
func plannerFor(m disruption.Method) string {
	switch m.(type) {
	case *disruption.Repair:
		return RepairPlanner
	case *disruption.Emptiness:
		return EmptinessPlanner
	case *disruption.StaticDrift:
		return StaticDriftPlanner
	case *disruption.Drift:
		return DriftPlanner
	case *disruption.MultiNodeConsolidation:
		return MultiNodePlanner
	case *disruption.SingleNodeConsolidation:
		return SingleNodePlanner
	}
	return ""
}

// shadowContext is ctx for the shadow's own work: none of it counts as the disruption controller's evaluation, its
// simulations are attributed to the controller disruption.shadow, and the methods' own logging is discarded (the
// shadow's findings go to its root logger).
func shadowContext(ctx context.Context) context.Context {
	return log.IntoContext(injection.WithControllerName(disruption.WithoutEvaluationStats(ctx), "disruption.shadow"), logr.Discard())
}

// Computing samples the method's computation. Synchronously it only builds the snapshot on the method's nodes,
// compares candidates and budgets, and copies the nodes; the plan, the comparison and any recheck run in a goroutine,
// whose simulations run on that copy, so the disruption controller starting commands meanwhile changes nothing it
// simulates on.
func (s *Shadow) Computing(ctx context.Context, m disruption.Method, snapshot *disruption.CandidateSnapshot, candidates []*disruption.Candidate, budgets map[string]int) {
	name := plannerFor(m)
	s.mu.Lock()
	delete(s.pending, name)
	s.mu.Unlock()
	p, ok := s.planners[name]
	if !ok || s.busy.Load() || s.sample() >= s.rate {
		return
	}
	// A consolidation method skips an unchanged cluster state; the shadow, which plans only sampled passes, can't.
	if d, ok := m.(interface{ IsConsolidated() bool }); ok && d.IsConsolidated() {
		return
	}
	if !s.busy.CompareAndSwap(false, true) {
		return
	}
	started := false
	defer func() {
		if !started {
			s.busy.Store(false)
		}
	}()
	ctx = shadowContext(ctx)
	snap, err := NewSnapshotBuilder(s.kubeClient, s.cluster, s.clock, s.cloudProvider, s.queue, s.clusterCost, s.ledger.View(),
		WithNodeSource(func(*state.Cluster) state.StateNodes { return snapshot.Nodes() })).Build(ctx, ScopeFor(types.LaneFast, p))
	if err != nil {
		return
	}
	pipelineCandidates := s.compareSnapshot(ctx, p, m.Reason(), snap, candidates, budgets)
	// Without candidates the method computes nothing; the snapshot level is all there is to compare.
	if len(candidates) == 0 {
		return
	}
	nodes := lo.Map(snapshot.Nodes(), func(n *state.StateNode, _ int) *state.StateNode { return n.DeepCopy() })
	in := PlanInput{Candidates: pipelineCandidates, Budgets: mapBudgets(lo.Assign(budgets)), Reserved: s.ledger.View(), Snapshot: snap,
		Now: snap.BuiltAt, Synced: s.cluster.Synced(ctx)}
	job := &shadowJob{legacy: make(chan *[]disruption.Command, 1)}
	s.mu.Lock()
	s.pending[name] = job
	s.mu.Unlock()
	started = true
	s.jobs.Add(1)
	go func() {
		defer s.jobs.Done()
		defer s.busy.Store(false)
		s.run(withSimNodes(ctx, nodes), name, p, in, job)
	}()
}

// Computed hands the method's commands to the sample's goroutine and returns at once.
func (s *Shadow) Computed(_ context.Context, m disruption.Method, cmds []disruption.Command, err error) {
	name := plannerFor(m)
	s.mu.Lock()
	job := s.pending[name]
	delete(s.pending, name)
	s.mu.Unlock()
	if job == nil {
		return
	}
	if err != nil {
		job.legacy <- nil
		return
	}
	cmds = lo.Filter(cmds, func(c disruption.Command, _ int) bool { return c.Decision() != disruption.NoOpDecision })
	job.legacy <- &cmds
}

// run plans the sampled computation, waits for the method's commands, compares them, and rechecks a mismatch.
func (s *Shadow) run(ctx context.Context, name string, p Planner, in PlanInput, job *shadowJob) {
	planned, err := s.plan(ctx, p, in)
	var legacy *[]disruption.Command
	select {
	case legacy = <-job.legacy:
	case <-ctx.Done():
		return
	case <-time.After(shadowWait):
		return
	}
	if err != nil || legacy == nil {
		return
	}
	ShadowComparisonsTotal.Inc(map[string]string{ShadowLevelDim.Name: shadowReference, PlannerDim.Name: name})
	if passSignature(planned, cmdSignature) == passSignature(*legacy, cmdSignature) || passSignature(planned, cmdTieSignature) == passSignature(*legacy, cmdTieSignature) {
		s.agree(name, true)
		return
	}
	s.agree(name, false)
	detail := fmt.Sprintf("the disruption controller computed %s; the pipeline planned %s", passSignature(*legacy, cmdSignature), passSignature(planned, cmdSignature))
	// The recheck simulates on fresh state, not on the copy.
	cause := s.recheck(context.WithValue(ctx, simNodesKey{}, nil), s.methods[name], p)
	if ctx.Err() == nil {
		s.mismatch(shadowReference, name, kindCommands, cause, detail)
	}
}

// compareSnapshot compares the planner's candidates and the Ledger's budgets with the method's, and returns the
// planner's candidates. budgets is nil for a method without candidates.
func (s *Shadow) compareSnapshot(ctx context.Context, p Planner, reason v1.DisruptionReason, snap *Snapshot, candidates []*disruption.Candidate,
	budgets map[string]int,
) []*disruption.Candidate {
	ShadowComparisonsTotal.Inc(map[string]string{ShadowLevelDim.Name: shadowSnapshot, PlannerDim.Name: p.Name()})
	pipelineCandidates := snap.CandidatesFor(ctx, p)
	want := sets.New(lo.Map(candidates, func(c *disruption.Candidate, _ int) string { return c.ProviderID() })...)
	got := sets.New(lo.Map(pipelineCandidates, func(c *disruption.Candidate, _ int) string { return c.ProviderID() })...)
	// The pipeline doesn't disrupt a node an in-flight command moves pods onto (R72); the disruption controller may.
	missing := want.Difference(got).Difference(s.queue.InFlightDestinations())
	if extra := got.Difference(want); missing.Len() > 0 || extra.Len() > 0 {
		s.mismatch(shadowSnapshot, p.Name(), kindCandidates, causeLogic, fmt.Sprintf("candidates only the disruption controller has %v, only the pipeline has %v",
			sets.List(missing), sets.List(extra)))
	}
	for np, n := range budgets {
		if got := s.ledger.Budgets().Remaining(ctx, np, reason); got != n {
			s.mismatch(shadowSnapshot, p.Name(), kindBudget, causeLogic, fmt.Sprintf("nodepool %s: %d nodes may be disrupted for %s, the pipeline says %d", np, n, reason, got))
		}
	}
	return pipelineCandidates
}

// plan plans once with in.
func (s *Shadow) plan(ctx context.Context, p Planner, in PlanInput) ([]disruption.Command, error) {
	if len(in.Candidates) == 0 {
		return nil, nil
	}
	if tc, ok := p.(TotalsConsumer); ok && tc.NeedsTotals() {
		in.Evaluator = disruption.NewBalancedEvaluatorWithoutMetrics(in.Snapshot.Totals(ctx, p.Class()), in.Snapshot.Events)
	}
	out, err := p.Plan(ctx, in)
	if err != nil {
		return nil, err
	}
	return lo.Map(out.Plans, func(plan *Plan, _ int) disruption.Command { return plan.ToCommand(time.Time{}) }), nil
}

// legacyCore is a disruption method's core, as the disruption controller computes it.
type legacyCore interface {
	Compute(ctx context.Context, budgets map[string]int, opts disruption.CoreOptions, candidates ...*disruption.Candidate) (disruption.CoreResult, error)
	LegacyOptions() disruption.CoreOptions
}

// recheck runs the shadow's copy of the method's core and the planner back to back on fresh state: race if they agree.
// It uses only the shadow's own objects: the method the planner plans with, a simulator without pending-pod side
// effects, a discarding recorder, an evaluator without metrics and the private Ledger's budgets.
func (s *Shadow) recheck(ctx context.Context, m disruption.Method, p Planner) string {
	core, ok := m.(legacyCore)
	if _, static := m.(*disruption.StaticDrift); !ok || static {
		return causeLogic
	}
	snap, err := NewSnapshotBuilder(s.kubeClient, s.cluster, s.clock, s.cloudProvider, s.queue, s.clusterCost, s.ledger.View()).Build(ctx, ScopeFor(types.LaneFast, p))
	if err != nil {
		return causeLogic
	}
	cands := snap.CandidatesFor(ctx, p)
	budgets := mapBudgets{}
	for _, c := range cands {
		budgets[c.NodePool.Name] = s.ledger.Budgets().Remaining(ctx, c.NodePool.Name, m.Reason())
	}
	opts := core.LegacyOptions()
	opts.Validator, opts.Dedup, opts.WithoutMetrics = nil, nil, true
	opts.Effects, opts.Sim = effects{recorder: discardRecorder{}, cluster: s.cluster}, s.sim
	if opts.Evaluator != nil {
		opts.Evaluator = disruption.NewBalancedEvaluatorWithoutMetrics(snap.Totals(ctx, p.Class()), discardRecorder{})
	}
	var legacy []disruption.Command
	if len(cands) > 0 {
		// The core gets its own copies: it may write to them (repair stamps a drain bound).
		res, err := core.Compute(ctx, budgets, opts, snap.CandidatesFor(ctx, p)...)
		if err != nil {
			return causeLogic
		}
		legacy = lo.Filter(res.Commands, func(c disruption.Command, _ int) bool { return c.Decision() != disruption.NoOpDecision })
	}
	for range shadowResamples {
		cmds, err := s.plan(ctx, p, PlanInput{Candidates: snap.CandidatesFor(ctx, p), Budgets: budgets, Reserved: s.ledger.View(), Snapshot: snap,
			Now: snap.BuiltAt, Synced: s.cluster.Synced(ctx)})
		if err != nil {
			return causeLogic
		}
		if passSignature(cmds, cmdSignature) == passSignature(legacy, cmdSignature) || passSignature(cmds, cmdTieSignature) == passSignature(legacy, cmdTieSignature) {
			return causeRace
		}
	}
	return causeLogic
}

func (s *Shadow) agree(planner string, agreed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.agreed[planner]
	n[1]++
	if agreed {
		n[0]++
	}
	s.agreed[planner] = n
	ShadowAgreementRatio.Set(float64(n[0])/float64(n[1]), map[string]string{PlannerDim.Name: planner})
}

func (s *Shadow) mismatch(level, planner, kind, cause, detail string) {
	ShadowMismatchesTotal.Inc(map[string]string{ShadowLevelDim.Name: level, PlannerDim.Name: planner, ShadowKindDim.Name: kind, ShadowCauseDim.Name: cause})
	key := [3]string{planner, level, kind}
	s.mu.Lock()
	last, ok := s.logged[key]
	now := s.clock.Now()
	log := !ok || now.Sub(last) >= shadowLogEvery
	if log {
		s.logged[key] = now
	}
	s.mu.Unlock()
	if log {
		s.logger.Error(serrors.Wrap(errors.New(detail), "planner", planner, "level", level, "kind", kind, "cause", cause), "disruption pipeline shadow mismatch")
	}
}

// mapBudgets is the method's budget mapping as a BudgetView.
type mapBudgets map[string]int

func (b mapBudgets) Remaining(_ context.Context, nodePool string, _ v1.DisruptionReason) int {
	return b[nodePool]
}
func (b mapBudgets) AnyRemaining(context.Context, v1.DisruptionReason) bool {
	return lo.SomeBy(lo.Values(b), func(n int) bool { return n > 0 })
}

// passSignature is a computation's commands, canonically ordered.
func passSignature(cmds []disruption.Command, sig func(disruption.Command) string) string {
	sigs := lo.Map(cmds, func(c disruption.Command, _ int) string { return sig(c) })
	sort.Strings(sigs)
	return strings.Join(sigs, "; ")
}

// cmdSignature is what a command decides: its decision, candidates, and per replacement its instance types.
func cmdSignature(c disruption.Command) string {
	cands := lo.Map(c.Candidates, func(c *disruption.Candidate, _ int) string { return c.ProviderID() })
	sort.Strings(cands)
	return fmt.Sprintf("%s %v %v", c.Decision(), cands, replacementSignatures(c))
}

// cmdTieSignature is cmdSignature with each candidate as its NodePool, for states with equivalent nodes.
func cmdTieSignature(c disruption.Command) string {
	pools := lo.Map(c.Candidates, func(c *disruption.Candidate, _ int) string { return c.NodePool.Name })
	sort.Strings(pools)
	return fmt.Sprintf("%s %v %v", c.Decision(), pools, replacementSignatures(c))
}

func replacementSignatures(c disruption.Command) []string {
	reps := lo.Map(c.Results.NewNodeClaims, func(nc *pscheduling.NodeClaim, _ int) string {
		its := lo.Map(nc.InstanceTypeOptions, func(it *cloudprovider.InstanceType, _ int) string { return it.Name })
		sort.Strings(its)
		return strings.Join(its, ",")
	})
	sort.Strings(reps)
	return reps
}
