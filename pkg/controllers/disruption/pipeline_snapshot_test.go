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

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

// Differential test for the pipeline's snapshot (invariant B1): for random cluster states, one pipeline Snapshot per
// round gives every planner the candidates one GetCandidatesWithTotals for its method returned before snapshots were
// shared (the legacy oracle in candidatesnapshot_export_test.go): the same ids, order, Candidate fields, StateNodes,
// events and NodePool totals. A planner here is the method itself (Filter = ShouldDisrupt). Every planner's Candidates
// are overwritten after use, so a planner that writes to its Candidates can't leak into another one (F14).
//
//	KARPENTER_PIPELINE_SNAPSHOT_SEEDS=100 KARPENTER_PIPELINE_SNAPSHOT_STEPS=40 \
//	go test ./pkg/controllers/disruption/ --ginkgo.focus="Pipeline Snapshot" -v
var _ = Describe("Pipeline Snapshot", func() {
	seeds := snapshotDiffEnv("KARPENTER_PIPELINE_SNAPSHOT_SEEDS", 4)
	steps := snapshotDiffEnv("KARPENTER_PIPELINE_SNAPSHOT_STEPS", 15)
	for seed := 1; seed <= seeds; seed++ {
		It(fmt.Sprintf("gives every planner the candidates of one build per method (seed=%d steps=%d)", seed, steps), func() {
			w := newSnapshotWorld(seed)
			checkPipelineSnapshot(w, "initial", nil)
			for range steps {
				checkPipelineSnapshot(w, w.randomOp(), nil)
			}
		})
	}
	It("excludes the nodes of reserved plans and the destinations of started commands, and nothing else", func() {
		w := newSnapshotWorld(1)
		// Exclude nodes that some method takes, so the exclusion is visible.
		ids := lo.Uniq(lo.FlatMap(w.methods, func(m disruption.Method, _ int) []string {
			return lo.Map(w.legacy(m).fields, func(f disruption.CandidateFields, _ int) string { return f.ProviderID })
		}))
		Expect(len(ids)).To(BeNumerically(">=", 3))
		view := &fakeReservations{reserved: sets.New(ids[0], ids[2]), inFlight: sets.New(ids[1])}
		checkPipelineSnapshot(w, "reserved", view)
	})
	It("evaluates every planner at the round's now, however late it reads its candidates", func() {
		w := newSnapshotWorld(2)
		s := buildPipelineSnapshot(w, nil)
		// Every candidate of the repair class, so nodes whose expireAfter makes their disruption cost decay are among them.
		p := anyCandidate{methodPlanner{Method: w.methods[0]}}
		fields := func(cs []*disruption.Candidate) []disruption.CandidateFields {
			return lo.Map(cs, func(c *disruption.Candidate, _ int) disruption.CandidateFields { return c.Fields() })
		}
		before := fields(s.CandidatesFor(ctx, p))
		Expect(before).ToNot(BeEmpty())
		env.Clock.Step(30 * time.Minute)
		Expect(fields(s.CandidatesFor(ctx, p))).To(Equal(before))
		// A snapshot built at the new time scores differently (lifetime decay), so the comparison above is sensitive to now.
		Expect(fields(buildPipelineSnapshot(w, nil).CandidatesFor(ctx, p))).ToNot(Equal(before))
	})
	It("treats a plan as pod-free only if it moves no pods, launches nothing and doesn't reboot", func() {
		w := newSnapshotWorld(3)
		s := buildPipelineSnapshot(w, nil)
		all := s.CandidatesFor(ctx, anyCandidate{methodPlanner{Method: w.methods[0]}}) // repair's class takes the most nodes
		empty, busy := lo.FilterReject(all, func(c *disruption.Candidate, _ int) bool { return len(c.ReschedulablePods()) == 0 })
		Expect(empty).ToNot(BeEmpty())
		Expect(busy).ToNot(BeEmpty())
		Expect((&pipeline.Plan{Candidates: empty}).PodFree()).To(BeTrue())
		Expect((&pipeline.Plan{Candidates: append(empty, busy[0])}).PodFree()).To(BeFalse())
		Expect((&pipeline.Plan{Candidates: empty, Action: types.ActionReboot}).PodFree()).To(BeFalse())
		Expect((&pipeline.Plan{Candidates: empty, Replacements: []*disruption.Replacement{{}}}).PodFree()).To(BeFalse())
		// Static drift is never pod-free, not even a terminate-first delete of an empty node (RFC C.4).
		Expect((&pipeline.Plan{Candidates: empty, TerminateFirst: disruption.TerminateFirstStaticAtLimit}).PodFree()).To(BeFalse())
		Expect((&pipeline.Plan{Candidates: empty, Method: w.methods[2]}).PodFree()).To(BeFalse())
		Expect((&pipeline.Plan{Candidates: empty, Footprint: types.Footprint{StaticNodeCount: map[string]int64{"static": 1}}}).PodFree()).To(BeFalse())
		// Emptiness is.
		Expect((&pipeline.Plan{Candidates: empty, Method: w.methods[1]}).PodFree()).To(BeTrue())
	})
	It("starts a plan as the command it describes", func() {
		w := newSnapshotWorld(4)
		s := buildPipelineSnapshot(w, nil)
		cs := s.CandidatesFor(ctx, anyCandidate{methodPlanner{Method: w.methods[0]}})
		p := &pipeline.Plan{ID: uuid.New(), Method: w.methods[0], Candidates: cs[:1], Action: types.ActionReboot,
			TerminateFirst: disruption.TerminateFirstNoReservedCapacity, PoolDisruptionCosts: map[string]float64{"a": 1},
			ValidateAt: env.Clock.Now(), ComputedAt: env.Clock.Now().Add(-time.Minute)}
		cmd := p.ToCommand(env.Clock.Now().Add(time.Hour))
		Expect(cmd.ID).To(Equal(p.ID))
		Expect(cmd.Method).To(Equal(p.Method))
		Expect(cmd.Candidates).To(Equal(p.Candidates))
		Expect(cmd.Reboot).To(BeTrue())
		Expect(cmd.TerminateFirstReason).To(Equal(p.TerminateFirst))
		Expect(cmd.PoolDisruptionCosts).To(Equal(p.PoolDisruptionCosts))
		// The queue's retry window starts when the command starts, not when it was planned.
		Expect(cmd.CreationTimestamp).To(Equal(env.Clock.Now().Add(time.Hour)))
	})
})

// checkPipelineSnapshot compares every planner's CandidatesFor from one pipeline Snapshot with the legacy build of its
// method, minus the nodes view excludes.
func checkPipelineSnapshot(w *snapshotWorld, op string, view *fakeReservations) {
	GinkgoHelper()
	w.rec.take()
	s := buildPipelineSnapshot(w, view)
	excluded := 0
	// An excluded node never reaches a planner's Filter, so the events a method's ShouldDisrupt publishes about it are
	// missing from the pipeline's round. Compare the events of the other nodes.
	var excludedObjects []string
	if view != nil {
		for _, n := range sortedNodes() {
			if view.reserved.Has(n.ProviderID()) || view.inFlight.Has(n.ProviderID()) {
				excludedObjects = append(excludedObjects, "*v1.Node//"+n.Node.Name+" ", "*v1.NodeClaim//"+n.NodeClaim.Name+" ")
			}
		}
	}
	notExcluded := func(evs []string) []string {
		return lo.Reject(evs, func(e string, _ int) bool {
			return lo.SomeBy(excludedObjects, func(o string) bool { return strings.HasPrefix(e, o) })
		})
	}
	for _, m := range w.methods {
		where := fmt.Sprintf("op=%s method=%s", op, methodName(m))
		want := w.legacy(m)
		if view != nil {
			keep := lo.Map(want.fields, func(f disruption.CandidateFields, _ int) bool {
				return !view.reserved.Has(f.ProviderID) && !view.inFlight.Has(f.ProviderID)
			})
			excluded += lo.Count(keep, false)
			want.fields = lo.Filter(want.fields, func(_ disruption.CandidateFields, i int) bool { return keep[i] })
			want.stateNodes = lo.Filter(want.stateNodes, func(_ *state.StateNode, i int) bool { return keep[i] })
			want.events = notExcluded(want.events)
		}
		p := methodPlanner{Method: m}
		got := s.CandidatesFor(ctx, p)
		// Candidate construction publishes to the round's buffer; ShouldDisrupt still publishes to the method's recorder.
		replay := &orderedRecorder{}
		replay.Publish(s.Events.Events()...)
		*s.Events = pipeline.RoundEvents{}
		evs := append(replay.take(), w.rec.take()...)
		if view != nil {
			evs = notExcluded(evs)
		}
		expectSameResult(where, want, result(got, evs, s.Totals(ctx, m.Class())))
		for _, c := range got {
			c.Scribble()
		}
	}
	if view != nil {
		Expect(excluded).To(BeNumerically(">", 0), "op=%s: the excluded nodes should include a candidate", op)
	}
}

func buildPipelineSnapshot(w *snapshotWorld, view *fakeReservations) *pipeline.Snapshot {
	GinkgoHelper()
	var reserved pipeline.ReservationView
	if view != nil {
		reserved = view
	}
	b := pipeline.NewSnapshotBuilder(env.Client, cluster, env.Clock, cloudProvider, queue, clusterCost, reserved,
		pipeline.WithNodeSource(func(*state.Cluster) state.StateNodes { return sortedNodes() }))
	planners := lo.Map(w.methods, func(m disruption.Method, _ int) pipeline.Planner { return methodPlanner{Method: m} })
	s, err := b.Build(ctx, pipeline.ScopeFor(types.LaneFast, planners...))
	Expect(err).ToNot(HaveOccurred())
	return s
}

// methodPlanner is a disruption method as a pipeline planner that only filters.
type methodPlanner struct {
	disruption.Method
}

func (p methodPlanner) Name() string       { return methodName(p.Method) }
func (p methodPlanner) Shape() types.Shape { return types.ShapeReplacing }
func (p methodPlanner) NeedsTotals() bool  { return true }
func (p methodPlanner) Filter(ctx context.Context, c *disruption.Candidate, _ time.Time) (bool, *types.Verdict) {
	return p.ShouldDisrupt(ctx, c), nil
}
func (p methodPlanner) Plan(context.Context, pipeline.PlanInput) (pipeline.PlanOutput, error) {
	return pipeline.PlanOutput{}, nil
}

// anyCandidate is a planner that takes every candidate of its class.
type anyCandidate struct {
	methodPlanner
}

func (anyCandidate) Filter(context.Context, *disruption.Candidate, time.Time) (bool, *types.Verdict) {
	return true, nil
}

type fakeReservations struct {
	reserved, inFlight sets.Set[string]
}

func (f *fakeReservations) Version() uint64                        { return 0 }
func (f *fakeReservations) IsReserved(id string) bool              { return f.reserved.Has(id) }
func (f *fakeReservations) Unstarted() sets.Set[string]            { return f.reserved.Clone() }
func (f *fakeReservations) Footprints() []types.Footprint          { return nil }
func (f *fakeReservations) InFlightDestinations() sets.Set[string] { return f.inFlight.Clone() }
func (f *fakeReservations) StaticHeld(string) int64                { return 0 }
func (f *fakeReservations) ReservationsHeld(string) int            { return 0 }
