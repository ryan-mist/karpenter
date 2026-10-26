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

package disruption_test

// The pipeline at batch=1 (RFC D.1, closes G9): on generated clusters, a plan-only pipeline in reference mode
// (WithReferenceMode: ReferenceLanes over DefaultPlanners(..., WithReference()); the sync runner, its own Ledger and a
// Starter that only records) plans the same commands as a pass of the disruption controller on the same state. Each step runs the
// pipeline first (it writes nothing), then the disruption controller, whose commands start and advance the state;
// their replacements launch and their candidates stay in flight, so later steps see in-flight commands.
//
// Reference mode switches off the gate-on behaviour changes that have a switch: R45 (the lane skips while unsynced),
// R71 (pending-only NodeClaims stay on the first command), R73 (no NodePool round-robin or backoff), decision #9 (a
// reserved plan's candidates simulate as deleting, so a later planner of the round re-places their pods, as a later
// method of the pass does for started commands); batching is 1. The intended difference left is classified, and ends
// the seed's comparison (the states diverge from there):
//   - R72: the pipeline doesn't disrupt a node an in-flight command moves pods onto; the disruption controller may.
// R22 (PreFilter) and R74 (SingleNode cursor) aren't implemented, so they make no difference. Decision #9 in the
// default mode is pinned by the pipeline's Simulator spec.

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	ptypes "sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/test"
)

// recordingStarter records the command each admitted plan would start, and marks its candidates for deletion in
// cluster state as Queue.StartCommand does, so the rest of the round sees them as started; planReference unmarks
// them. It writes nothing to the API server.
type recordingStarter struct {
	cluster *state.Cluster
	mu      sync.Mutex
	cmds    []disruption.Command
}

func (s *recordingStarter) Start(_ context.Context, p *pipeline.Plan) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cmds = append(s.cmds, p.ToCommand(time.Time{}))
	s.cluster.MarkForDeletion(lo.Map(p.Candidates, func(c *disruption.Candidate, _ int) string { return c.ProviderID() })...)
	return nil
}

// planReference runs one sync round of a fresh plan-only reference pipeline and returns what it would start.
func (e *coreEnv) planReference() []disruption.Command {
	GinkgoHelper()
	starter := &recordingStarter{cluster: e.cluster}
	c := pipeline.NewController(e.ctx, env.Clock, e.client, e.prov, cloudProvider, test.NewEventRecorder(), e.cluster, e.queue, e.clusterCost,
		pipeline.WithReferenceMode(), pipeline.WithStarter(starter), pipeline.WithObservers(debugObserver{}))
	_, err := c.Reconcile(e.ctx)
	Expect(err).ToNot(HaveOccurred())
	e.cluster.UnmarkForDeletion(lo.FlatMap(starter.cmds, func(cmd disruption.Command, _ int) []string {
		return lo.Map(cmd.Candidates, func(c *disruption.Candidate, _ int) string { return c.ProviderID() })
	})...)
	return starter.cmds
}

// passLegacy runs one pass of the disruption controller and returns the commands it started.
func (e *coreEnv) passLegacy(c *disruption.Controller) []disruption.Command {
	GinkgoHelper()
	before := sets.New(lo.Map(e.queue.GetCommands(), func(cmd *disruption.Command, _ int) string { return cmd.ID.String() })...)
	_, err := c.Reconcile(e.ctx)
	Expect(err).ToNot(HaveOccurred())
	started := lo.FilterMap(e.queue.GetCommands(), func(cmd *disruption.Command, _ int) (disruption.Command, bool) {
		return *cmd, !before.Has(cmd.ID.String())
	})
	// In start order: a method's commands start after the earlier methods' validation delays.
	sort.SliceStable(started, func(i, j int) bool { return started[i].CreationTimestamp.Before(started[j].CreationTimestamp) })
	return started
}

// launchRequirements are the requirements Provisioner.CreateNodeClaims adds to a replacement when the command starts,
// derived from its instance type options (which the signature keeps): the recorded plans never start.
var launchRequirements = regexp.MustCompile(`(` + regexp.QuoteMeta(v1.CapacityTypeLabelKey) + `|` + regexp.QuoteMeta(corev1.LabelInstanceTypeStable) + `) In \[[^]]*\] ?`)

// passSignature is a pass's commands in a canonical order: the pipeline may admit them in another order (a deletion
// waits out its validation delay, a replacement doesn't).
func (gc *genCluster) passSignature(cmds []disruption.Command) string {
	sigs := lo.Map(cmds, func(c disruption.Command, _ int) string {
		return launchRequirements.ReplaceAllString(gc.cmdSignature(c), "")
	})
	sort.Strings(sigs)
	return fmt.Sprint(sigs)
}

// passTieSignature is a pass's commands without node identities (tieSignature): on a state with ties (equal
// disruption costs, NodePools tried in map order) upstream picks one of several equivalent candidate sets.
func passTieSignature(cmds []disruption.Command) string {
	sigs := lo.Map(cmds, func(c disruption.Command, _ int) string {
		return launchRequirements.ReplaceAllString(tieSignature(c), "")
	})
	sort.Strings(sigs)
	return fmt.Sprint(sigs)
}

// zoneRequirement is a replacement's zone requirement: with zone topology spread, upstream picks among domains with equal
// skew in an order that isn't deterministic (nextDomainTopologySpread).
var zoneRequirement = regexp.MustCompile(regexp.QuoteMeta(corev1.LabelTopologyZone) + ` In \[[^]]*\] ?`)

// passZoneTieSignature is passTieSignature without the replacements' zones.
func passZoneTieSignature(cmds []disruption.Command) string {
	return zoneRequirement.ReplaceAllString(passTieSignature(cmds), "")
}

// intendedDifference classifies a mismatch: R72 if the disruption controller disrupts a node an in-flight command moves
// pods onto.
func intendedDifference(legacy []disruption.Command, inFlightDestinations sets.Set[string]) string {
	if lo.SomeBy(legacy, func(c disruption.Command) bool {
		return lo.SomeBy(c.Candidates, func(cand *disruption.Candidate) bool { return inFlightDestinations.Has(cand.ProviderID()) })
	}) {
		return "R72"
	}
	return ""
}

var _ = Describe("Pipeline at batch=1", func() {
	var e *coreEnv
	AfterEach(func() {
		if e != nil {
			e.stop()
			e = nil
		}
	})
	budgets := func(_ int, r *rand.Rand) string { return []string{"1", "10%", "20%", "100%"}[r.Intn(4)] }
	nodes := coreDiffNodes([]int{10, 20, 40})
	for _, seed := range coreSeeds(3001, 12) {
		spec := randomGenSpec(seed, nodes, budgets)
		// A third of the seeds have no drift and consolidate underutilized nodes of mixed sizes; the others drift with
		// consolidation enabled, so the pass reaches consolidation once drift is done or blocked.
		spec.ConsolidateAfter = "0s"
		spec.MixedTypes = seed%3 != 1
		if seed%3 == 0 {
			spec.DriftFrac, spec.Util = 0, []float64{0.3, 0.5}[seed%2]
		}
		It(fmt.Sprintf("seed=%d %+v", seed, spec), func() {
			e = newCoreEnv(ctx, genInstanceTypesOf(genZones(spec.Zones), v1.CapacityTypeOnDemand, v1.CapacityTypeSpot))
			gc := e.buildGen(spec)
			e.stepClock()
			legacy := disruption.NewController(e.ctx, env.Clock, e.client, e.prov, cloudProvider, e.recorder, e.cluster, e.queue, e.clusterCost,
				disruption.WithMethods(e.methods...))
			// Upstream is nondeterministic on some states (zone ties in topology spread, equal transition times, equal
			// disruption costs, NodePools in map order), and the disruption controller's pass can't be repeated without
			// starting it: the pipeline is resampled instead, and a pass matches if a sample equals it exactly or up to
			// ties. Only the disruption controller's commands start, so a match up to ties doesn't fork the state. A
			// state whose samples already disagree is sampled more (multi-node consolidation over many equal nodes has
			// a dozen outcomes).
			resamples := coreDiffEnv("KARPENTER_PIPELINE_REF_RESAMPLES", 8)
			maxResamples := coreDiffEnv("KARPENTER_PIPELINE_REF_MAX_RESAMPLES", 32)
			for step := range coreDiffEnv("KARPENTER_PIPELINE_REF_STEPS", 6) {
				inFlight := e.queue.InFlightDestinations()
				// The pipeline plans first; it writes nothing, so every resample sees the same state.
				samples := lo.Times(resamples, func(int) []disruption.Command { return e.planReference() })
				outcomes := lo.SliceToMap(samples, func(cmds []disruption.Command) (string, bool) { return gc.passSignature(cmds), true })
				for len(outcomes) > 1 && len(samples) < maxResamples {
					cmds := e.planReference()
					samples = append(samples, cmds)
					outcomes[gc.passSignature(cmds)] = true
				}
				ref := e.passLegacy(legacy)
				// The finest level a sample matches at: exact, up to ties of equivalent nodes, up to zone ties.
				level, got := "", samples[0]
				for _, l := range []struct {
					name string
					sig  func([]disruption.Command) string
				}{{"exact", gc.passSignature}, {"ties", passTieSignature}, {"zones", passZoneTieSignature}} {
					if match, ok := lo.Find(samples, func(cmds []disruption.Command) bool { return l.sig(cmds) == l.sig(ref) }); ok {
						level, got = l.name, match
						break
					}
				}
				equal, exact := level != "", level == "exact"
				intended := lo.Ternary(equal, "", intendedDifference(ref, inFlight))
				emitCoreDiff(map[string]any{"test": "pipeline-batch1", "spec": spec, "step": step, "equal": equal, "exact": exact, "level": level, "intended": intended,
					"pipelineOutcomes": len(outcomes), "legacyCommands": len(ref), "pipelineCommands": len(got)})
				if intended != "" {
					AddReportEntry("intended difference", fmt.Sprintf("seed %d step %d: %s\nlegacy:\n%s\npipeline:\n%s", seed, step, intended,
						gc.cmdsSignature(ref), gc.cmdsSignature(got)))
					break // the states diverge from here
				}
				Expect(equal).To(BeTrue(), "step %d: the disruption controller started\n%s\nthe pipeline planned\n%s", step,
					gc.cmdsSignature(ref), gc.cmdsSignature(got))
				if len(ref) == 0 {
					break
				}
				e.genLaunch(gc, lo.ToSlicePtr(ref))
			}
		})
	}
})

var _ = Describe("Pipeline reference mode", func() {
	It("lists the planners in the disruption controller's method order in one first-wins lane that skips while unsynced", func() {
		names := []string{pipeline.SingleNodePlanner, pipeline.DriftPlanner, pipeline.RepairPlanner, pipeline.MultiNodePlanner,
			pipeline.StaticDriftPlanner, pipeline.EmptinessPlanner}
		lanes := pipeline.ReferenceLanes(lo.SliceToMap(names, func(n string) (string, pipeline.Planner) { return n, namedPlanner(n) }))
		Expect(lanes).To(HaveLen(1))
		Expect(lanes[0].FirstWins).To(BeTrue())
		Expect(lanes[0].SkipWhileUnsynced).To(BeTrue())
		Expect(lo.Map(lanes[0].Planners, func(p pipeline.Planner, _ int) string { return p.Name() })).To(Equal([]string{
			pipeline.RepairPlanner, pipeline.EmptinessPlanner, pipeline.StaticDriftPlanner, pipeline.DriftPlanner,
			pipeline.MultiNodePlanner, pipeline.SingleNodePlanner}))
	})
})

// namedPlanner is a Planner with only a name.
type namedPlanner string

func (n namedPlanner) Name() string              { return string(n) }
func (namedPlanner) Reason() v1.DisruptionReason { return "" }
func (namedPlanner) Class() string               { return disruption.GracefulDisruptionClass }
func (namedPlanner) Shape() ptypes.Shape         { return 0 }
func (namedPlanner) Filter(context.Context, *disruption.Candidate, time.Time) (bool, *ptypes.Verdict) {
	return false, nil
}
func (namedPlanner) Plan(context.Context, pipeline.PlanInput) (pipeline.PlanOutput, error) {
	return pipeline.PlanOutput{}, nil
}

// debugObserver logs what the reference pipeline planned and how each plan resolved, with
// KARPENTER_PIPELINE_REF_DEBUG set.
type debugObserver struct{ pipeline.NopObserver }

func (debugObserver) Resolved(p *pipeline.Plan, r pipeline.Resolution, _ time.Duration) {
	if os.Getenv("KARPENTER_PIPELINE_REF_DEBUG") != "" {
		fmt.Fprintf(GinkgoWriter, "DEBUG resolved %s %v %s\n", p.Planner, r.Kind, r.Reason)
	}
}
func (debugObserver) Planned(lane ptypes.LaneID, p pipeline.Planner, candidates int, out pipeline.PlanOutput, _ time.Duration) {
	if os.Getenv("KARPENTER_PIPELINE_REF_DEBUG") != "" {
		fmt.Fprintf(GinkgoWriter, "DEBUG planned %s candidates=%d plans=%d\n", p.Name(), candidates, len(out.Plans))
	}
}
