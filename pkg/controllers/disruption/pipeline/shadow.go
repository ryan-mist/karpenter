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
	// The method's candidates were evaluated at its snapshot's fixed now, possibly well before this method ran (one
	// snapshot serves the pass); the pipeline's are evaluated at the same now.
	// The Ledger's budgets first, right after the method's were built: building the snapshot takes a while at scale.
	ledgerBudgets := lo.MapValues(budgets, func(_ int, np string) int { return s.ledger.Budgets().Remaining(ctx, np, m.Reason()) })
	snap, err := s.snapshotAt(ctx, p, snapshot.Nodes(), snapshot.Now())
	if err != nil {
		return
	}
	pipelineCandidates := s.compareSnapshot(ctx, p, m, snap, candidates, budgets, ledgerBudgets)
	// Without candidates the method computes nothing; the snapshot level is all there is to compare.
	if len(candidates) == 0 {
		return
	}
	// The method's simulations read live cluster state when it computes, which is now (its candidates come from the
	// pass's snapshot, possibly built well before: one snapshot serves several methods, and earlier methods may have
	// started commands since). The shadow's simulations run on a copy of the live nodes taken now, so the commands the
	// disruption controller starts meanwhile change nothing they see (KWOK soak: on the snapshot's nodes the shadow
	// planned "delete X" where the method, seeing those starts, planned "replace X").
	nodes := s.cluster.DeepCopyNodes()
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
	cause := s.recheck(ctx, s.methods[name], p)
	if ctx.Err() == nil {
		s.mismatch(shadowReference, name, kindCommands, cause, detail)
	}
}

// compareSnapshot compares the planner's candidates and the Ledger's budgets with the method's, and returns the
// planner's candidates. budgets is nil for a method without candidates; ledgerBudgets are the Ledger's for its NodePools. A difference is rechecked on fresh state (see
// recheckCandidates, recheckBudget): race if the recheck agrees, logic otherwise.
func (s *Shadow) compareSnapshot(ctx context.Context, p Planner, m disruption.Method, snap *Snapshot, candidates []*disruption.Candidate,
	budgets, ledgerBudgets map[string]int,
) []*disruption.Candidate {
	ShadowComparisonsTotal.Inc(map[string]string{ShadowLevelDim.Name: shadowSnapshot, PlannerDim.Name: p.Name()})
	pipelineCandidates := snap.CandidatesFor(ctx, p)
	var rechecks []func()
	if missing, extra := s.candidateDiff(candidates, pipelineCandidates); missing.Len() > 0 || extra.Len() > 0 {
		detail := fmt.Sprintf("candidates only the disruption controller has %v, only the pipeline has %v", sets.List(missing), sets.List(extra))
		rechecks = append(rechecks, func() { s.mismatch(shadowSnapshot, p.Name(), kindCandidates, s.recheckCandidates(ctx, p, m), detail) })
	}
	for np, n := range budgets {
		if got := ledgerBudgets[np]; got != n {
			detail := fmt.Sprintf("nodepool %s: %d nodes may be disrupted for %s, the pipeline says %d", np, n, m.Reason(), got)
			rechecks = append(rechecks, func() {
				s.mismatch(shadowSnapshot, p.Name(), kindBudget, s.recheckBudget(ctx, snap.NodePools, np, m.Reason()), detail)
			})
		}
	}
	if len(rechecks) > 0 {
		// Off the disruption controller's path, like the reference level's recheck.
		s.jobs.Add(1)
		go func() {
			defer s.jobs.Done()
			for _, r := range rechecks {
				if ctx.Err() != nil {
					return
				}
				r()
			}
		}()
	}
	return pipelineCandidates
}

// candidateDiff is what only the method's candidates have, except nodes in-flight commands move pods onto (the
// pipeline doesn't disrupt those, R72; the disruption controller may), and what only the pipeline's have.
func (s *Shadow) candidateDiff(legacy, pipeline []*disruption.Candidate) (missing, extra sets.Set[string]) {
	want := sets.New(lo.Map(legacy, func(c *disruption.Candidate, _ int) string { return c.ProviderID() })...)
	got := sets.New(lo.Map(pipeline, func(c *disruption.Candidate, _ int) string { return c.ProviderID() })...)
	return want.Difference(got).Difference(s.queue.InFlightDestinations()), got.Difference(want)
}

// snapshotAt builds the planner's snapshot on nodes, evaluated at now.
func (s *Shadow) snapshotAt(ctx context.Context, p Planner, nodes state.StateNodes, now time.Time) (*Snapshot, error) {
	return NewSnapshotBuilder(s.kubeClient, s.cluster, fixedClock{Clock: s.clock, now: now}, s.cloudProvider, s.queue, s.clusterCost, s.ledger.View(),
		WithNodeSource(func(*state.Cluster) state.StateNodes { return nodes })).Build(ctx, ScopeFor(types.LaneFast, p))
}

// recheckCandidates computes the method's candidates (the shadow's copy of it) and the planner's back to back, on one
// fresh copy of the nodes at one now: race if they agree (the state changed between the method's snapshot and the
// comparison: pods bound or nominated, PDBs, NodePools), logic otherwise.
func (s *Shadow) recheckCandidates(ctx context.Context, p Planner, m disruption.Method) string {
	method, ok := s.methods[p.Name()]
	if !ok {
		method = m
	}
	cs, err := disruption.NewCandidateSnapshotFromNodes(ctx, s.cluster.DeepCopyNodes, s.kubeClient, discardRecorder{}, s.clock, s.cloudProvider, s.queue)
	if err != nil {
		return causeLogic
	}
	legacy := lo.Filter(cs.Candidates(ctx, method.Class()), func(c *disruption.Candidate, _ int) bool { return method.ShouldDisrupt(ctx, c) })
	snap, err := s.snapshotAt(ctx, p, cs.Nodes(), cs.Now())
	if err != nil {
		return causeLogic
	}
	if missing, extra := s.candidateDiff(legacy, snap.CandidatesFor(ctx, p)); missing.Len() > 0 || extra.Len() > 0 {
		return causeLogic
	}
	return causeRace
}

// recheckBudget computes the disruption controller's budget for nodePool (NodePoolStats and NodePoolBudgetMap, as
// BuildDisruptionBudgetMapping does, without its metrics and events) and the Ledger's back to back on fresh state: race
// if they agree (a disruption started or finished, or a node initialized, in between), logic otherwise.
func (s *Shadow) recheckBudget(ctx context.Context, nodePools map[string]*v1.NodePool, nodePool string, reason v1.DisruptionReason) string {
	np, ok := nodePools[nodePool]
	if !ok {
		return causeLogic
	}
	numNodes, disrupting := disruption.NodePoolStats(s.cluster, reason)
	want := disruption.NodePoolBudgetMap(ctx, s.clock, map[string]*v1.NodePool{nodePool: np}, numNodes, disrupting, reason)[nodePool]
	if s.ledger.Budgets().Remaining(ctx, nodePool, reason) != want {
		return causeLogic
	}
	return causeRace
}

// fixedClock is a clock whose now is fixed.
type fixedClock struct {
	clock.Clock
	now time.Time
}

func (c fixedClock) Now() time.Time                  { return c.now }
func (c fixedClock) Since(t time.Time) time.Duration { return c.now.Sub(t) }

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

// recheck runs the shadow's copy of the method's core and the planner back to back on one fresh copy of the nodes: race
// if they agree.
// It uses only the shadow's own objects: the method the planner plans with, a simulator without pending-pod side
// effects, a discarding recorder, an evaluator without metrics and the private Ledger's budgets.
func (s *Shadow) recheck(ctx context.Context, m disruption.Method, p Planner) string {
	core, ok := m.(legacyCore)
	if _, static := m.(*disruption.StaticDrift); !ok || static {
		return causeLogic
	}
	// One fresh copy of the nodes at one now for both: the core's and the planner's simulations see the same nodes,
	// whatever the disruption controller starts while they run.
	fresh := s.cluster.DeepCopyNodes()
	ctx = withSimNodes(ctx, fresh)
	snap, err := s.snapshotAt(ctx, p, fresh, s.clock.Now())
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
