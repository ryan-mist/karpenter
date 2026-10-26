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

package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// acceptingPlanner is a stub planner that accepts plans at admission as revalidate decides (by default: as planned,
// on its fresh candidates), and records what it was asked.
type acceptingPlanner struct {
	*stubPlanner
	always     bool
	revalidate func(pipeline.RevalidateInput) ([]*pipeline.Plan, error)
	replan     func([]*pipeline.Plan) ([]*pipeline.Plan, error)
	inputs     []pipeline.RevalidateInput
}

func (p *acceptingPlanner) AlwaysResimulate() bool { return p.always }
func (p *acceptingPlanner) Revalidate(_ context.Context, in pipeline.RevalidateInput) ([]*pipeline.Plan, error) {
	p.inputs = append(p.inputs, in)
	if p.revalidate != nil {
		return p.revalidate(in)
	}
	if len(in.Rejected) > 0 {
		return nil, &pipeline.RejectError{Reason: lo.Values(in.Rejected)[0]}
	}
	in.Plan.Candidates = in.Fresh
	return []*pipeline.Plan{in.Plan}, nil
}
func (p *acceptingPlanner) Replan(_ context.Context, survivors []*pipeline.Plan, _ pipeline.RevalidateInput) ([]*pipeline.Plan, error) {
	if p.replan == nil {
		return nil, &pipeline.RejectError{Reason: types.ReasonSiblingRejected}
	}
	return p.replan(survivors)
}

// jointPlanner is an acceptingPlanner that re-simulates a batch's plans jointly: one plan over all their fresh
// candidates.
type jointPlanner struct {
	*acceptingPlanner
	joint [][]pipeline.RevalidateInput
	fail  bool
	// destinations, if set, are the joint plan's destinations.
	destinations sets.Set[string]
}

func (p *jointPlanner) RevalidateJointly(_ context.Context, ins []pipeline.RevalidateInput) ([]*pipeline.Plan, error) {
	p.joint = append(p.joint, ins)
	if p.fail {
		return nil, &pipeline.RejectError{Reason: types.ReasonResimUnschedulable}
	}
	q := deletePlan(lo.FlatMap(ins, func(in pipeline.RevalidateInput, _ int) []*disruption.Candidate { return in.Fresh })...)
	q.ID, q.BatchID, q.Siblings, q.Planner, q.Reason = uuid.New(), ins[0].Plan.BatchID, 1, ins[0].Plan.Planner, ins[0].Plan.Reason
	q.Footprint.BudgetNodes = ins[0].Plan.Footprint.BudgetNodes
	q.Footprint.Destinations = p.destinations
	for _, in := range ins[1:] {
		for np, ids := range in.Plan.Footprint.BudgetNodes {
			q.Footprint.BudgetNodes[np] = q.Footprint.BudgetNodes[np].Union(ids)
		}
	}
	return []*pipeline.Plan{q}, nil
}

var _ = Describe("Admitter", func() {
	var w *world
	var l pipeline.Ledger
	var planner *acceptingPlanner
	var admitter pipeline.Admitter
	var candidates map[string]*disruption.Candidate
	var ids []string

	// plan reserves a delete plan for ids, computed now.
	plan := func(ids ...string) *pipeline.Pending {
		GinkgoHelper()
		p := deletePlan(lo.Map(ids, func(id string, _ int) *disruption.Candidate { return candidates[id] })...)
		p.ID = uuid.New()
		p.BatchID, p.Siblings = p.ID, 1
		p.Planner, p.Reason = planner.Name(), planner.Reason()
		p.ComputedAt = env.Clock.Now()
		p.Footprint.ComputedAt = l.Version()
		p.Footprint.BudgetNodes = map[string]sets.Set[string]{w.nodePool.Name: sets.New(ids...)}
		Expect(l.Reserve(ctx, p)).To(Equal([]error{nil}))
		return &pipeline.Pending{Plan: p, SubmittedAt: env.Clock.Now()}
	}
	step := func() *pipeline.CommitStep {
		return &pipeline.CommitStep{Now: env.Clock.Now(), Synced: cluster.Synced(ctx)}
	}
	started := func(r pipeline.AdmitResult) []string {
		return lo.FlatMap(r.Start, func(p *pipeline.Plan, _ int) []string { return providerIDsOf(p.Candidates) })
	}
	BeforeEach(func() {
		w = newWorld(6, 4)
		ids = w.providerIDs()
		l = pipeline.NewLedger(env.Client, cluster, cloudProvider, env.Clock, queue)
		planner = &acceptingPlanner{stubPlanner: newStubPlanner("drift")}
		admitter = pipeline.NewAdmitter(l, pipeline.NewCandidateRebuilder(env.Client, cluster, env.Clock, cloudProvider, queue), planner)
		s, err := pipeline.NewSnapshotBuilder(env.Client, cluster, env.Clock, cloudProvider, queue, clusterCost, nil).Build(ctx, pipeline.Scope{})
		Expect(err).ToNot(HaveOccurred())
		candidates = lo.SliceToMap(s.CandidatesFor(ctx, planner), func(c *disruption.Candidate) (string, *disruption.Candidate) { return c.ProviderID(), c })
		Expect(candidates).To(HaveLen(6))
	})

	It("starts a valid plan on its rebuilt candidates", func() {
		p := plan(ids[0], ids[1])
		r := admitter.Admit(ctx, []*pipeline.Pending{p}, step())
		Expect(r.Rejected).To(BeEmpty())
		Expect(started(r)).To(Equal([]string{ids[0], ids[1]}))
		// The candidates were rebuilt, not the planned ones.
		Expect(r.Start[0].Candidates[0]).ToNot(BeIdenticalTo(candidates[ids[0]]))
		Expect(planner.inputs[0].Causes).To(BeEmpty())
	})
	Context("while cluster state is unsynced", func() {
		BeforeEach(func() {
			unlaunched := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: w.nodePool.Name}}})
			unlaunched.Status.ProviderID = ""
			cluster.UpdateNodeClaim(unlaunched)
			Expect(cluster.Synced(ctx)).To(BeFalse())
		})
		It("admits only pod-free plans", func() {
			podFree, moving := plan(ids[0]), plan(ids[4])
			Expect(started(admitter.Admit(ctx, []*pipeline.Pending{podFree}, step()))).To(Equal([]string{ids[0]}))
			r := admitter.Admit(ctx, []*pipeline.Pending{moving}, step())
			Expect(r.Start).To(BeEmpty())
			Expect(r.Wait).To(HaveLen(1))
			replacing := plan(ids[1])
			replacing.Plan.Replacements = []*disruption.Replacement{{}}
			Expect(admitter.Admit(ctx, []*pipeline.Pending{replacing}, step()).Wait).To(HaveLen(1))
			reboot := plan(ids[2])
			reboot.Plan.Action = types.ActionReboot
			Expect(admitter.Admit(ctx, []*pipeline.Pending{reboot}, step()).Wait).To(HaveLen(1))
		})
		It("waits with a pod-free plan whose candidate got a pod since planning", func() {
			p := plan(ids[0])
			pod := test.Pod(test.PodOptions{NodeName: w.nodes[0].Name, Phase: corev1.PodRunning,
				ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs", UID: "rs", Controller: lo.ToPtr(true)}}}})
			ExpectApplied(ctx, env.Client, pod)
			ExpectReconcileSucceeded(ctx, podStateController, client.ObjectKeyFromObject(pod))
			Expect(admitter.Admit(ctx, []*pipeline.Pending{p}, step()).Wait).To(HaveLen(1))
		})
	})
	It("rejects a plan whose candidate is nominated, deleting, or no longer passes the planner's filter", func() {
		nominated, deleting, ineligible := plan(ids[0]), plan(ids[1]), plan(ids[2])
		cluster.NominateNodeForPod(ctx, ids[0])
		cluster.MarkForDeletion(ids[1])
		planner.filter = func(c *disruption.Candidate) bool { return c.ProviderID() != ids[2] }
		for p, reason := range map[*pipeline.Pending]types.RejectReason{nominated: types.ReasonCandidateNominated, deleting: types.ReasonCandidateInvalid, ineligible: types.ReasonCandidateIneligible} {
			Expect(admitter.Admit(ctx, []*pipeline.Pending{p}, step()).Rejected).To(Equal(map[uuid.UUID]types.RejectReason{p.Plan.ID: reason}))
		}
	})
	It("rechecks the Ledger live, before the planner re-validates", func() {
		p := plan(ids[0], ids[1])
		w.nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "1"}}
		ExpectApplied(ctx, env.Client, w.nodePool)
		Expect(admitter.Admit(ctx, []*pipeline.Pending{p}, step()).Rejected).To(HaveKeyWithValue(p.Plan.ID, types.ReasonBudget))
		// Rejected by the recheck, not by the planner's acceptance or the commit.
		Expect(planner.inputs).To(BeEmpty())
	})
	It("rejects a plan whose budget window closed while it waited", func() {
		// A blocking budget window opens a minute from now (budget schedules are UTC).
		at := env.Clock.Now().UTC().Add(time.Minute)
		w.nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "100%"},
			{Nodes: "0", Schedule: lo.ToPtr(fmt.Sprintf("%d %d * * *", at.Minute(), at.Hour())), Duration: &metav1.Duration{Duration: time.Hour}}}
		ExpectApplied(ctx, env.Client, w.nodePool)
		p := plan(ids[0])
		// The plan waits (for example a long validation) past the window's start.
		p.Plan.ValidateAt = env.Clock.Now().Add(90 * time.Second)
		env.Clock.Step(2 * time.Minute)
		Expect(admitter.Admit(ctx, []*pipeline.Pending{p}, step()).Rejected).To(HaveKeyWithValue(p.Plan.ID, types.ReasonBudget))
	})
	It("rejects as canceled when the lease is lost during admission", func() {
		p := plan(ids[0])
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		a := pipeline.NewAdmitter(l, failingRebuilder{}, planner)
		Expect(a.Admit(cancelled, []*pipeline.Pending{p}, step()).Rejected).To(HaveKeyWithValue(p.Plan.ID, types.ReasonCanceled))
		Expect(a.Admit(ctx, []*pipeline.Pending{p}, step()).Rejected).To(HaveKeyWithValue(p.Plan.ID, types.ReasonError))
	})
	It("rejects a plan that deletes a node a started command is moving pods onto", func() {
		p := plan(ids[0])
		queue.ProviderIDToCommand[ids[5]] = &disruption.Command{Results: pscheduling.Results{ExistingNodes: []*pscheduling.ExistingNode{existingNodeWithPod(ids[0])}}}
		Expect(admitter.Admit(ctx, []*pipeline.Pending{p}, step()).Rejected).To(HaveKeyWithValue(p.Plan.ID, types.ReasonConflict))
	})
	Context("re-simulation causes", func() {
		It("re-simulates a plan coupled to one that started after its simulation, and only then", func() {
			p := plan(ids[0])
			p.Plan.Footprint.Destinations = sets.New(ids[3])
			q := plan(ids[1])
			q.Plan.Footprint.Destinations = sets.New(ids[3])
			independent := plan(ids[2])
			independent.Plan.Footprint.Destinations = sets.New(ids[5])
			Expect(l.Amend(ctx, p.Plan.ID, p.Plan.Footprint)).To(Succeed())
			Expect(l.Amend(ctx, q.Plan.ID, q.Plan.Footprint)).To(Succeed())
			Expect(l.Amend(ctx, independent.Plan.ID, independent.Plan.Footprint)).To(Succeed())
			l.MarkStarted(q.Plan.ID)
			admitter.Admit(ctx, []*pipeline.Pending{p}, step())
			admitter.Admit(ctx, []*pipeline.Pending{independent}, step())
			Expect(planner.inputs[0].Causes).To(ConsistOf(types.ResimCoupled))
			Expect(planner.inputs[1].Causes).To(BeEmpty())
			// A plan computed after q started already saw it.
			later := plan(ids[4])
			later.Plan.Footprint.Destinations = sets.New(ids[3])
			admitter.Admit(ctx, []*pipeline.Pending{later}, step())
			Expect(planner.inputs[2].Causes).To(BeEmpty())
		})
		It("rejects a plan that deletes the destination of a plan started after its simulation", func() {
			p := plan(ids[0])
			q := plan(ids[1])
			q.Plan.Footprint.Destinations = sets.New(ids[2])
			Expect(l.Amend(ctx, q.Plan.ID, q.Plan.Footprint)).To(Succeed())
			l.MarkStarted(q.Plan.ID)
			p.Plan.Footprint.Removed.Insert(ids[2])
			Expect(admitter.Admit(ctx, []*pipeline.Pending{p}, step()).Rejected).To(HaveKeyWithValue(p.Plan.ID, types.ReasonConflict))
		})
		It("re-simulates a stale plan, a plan whose catalog or pods changed, a plan with a rejected sibling, and consolidation always", func() {
			stale := plan(ids[0])
			stale.SubmittedAt = env.Clock.Now().Add(-pipeline.StaleAfter - time.Second)
			catalog := plan(ids[1])
			catalog.Plan.CatalogVersions = map[string]string{w.nodePool.Name: "old"}
			pods := plan(ids[4])
			pods.Plan.PodSetHash = map[string]uint64{ids[4]: 1}
			sibling := plan(ids[2])
			sibling.SiblingRejected = true
			for _, p := range []*pipeline.Pending{stale, catalog, pods, sibling} {
				admitter.Admit(ctx, []*pipeline.Pending{p}, step())
			}
			Expect(lo.Map(planner.inputs, func(in pipeline.RevalidateInput, _ int) []types.ResimCause { return in.Causes })).To(Equal([][]types.ResimCause{
				{types.ResimStale}, {types.ResimCatalog}, {types.ResimPodsChanged}, {types.ResimSibling},
			}))
			planner.always = true
			consolidation := plan(ids[3])
			admitter.Admit(ctx, []*pipeline.Pending{consolidation}, step())
			Expect(planner.inputs[4].Causes).To(Equal([]types.ResimCause{types.ResimAlways}))
		})
		It("counts staleness from the plan's submission, not from when its round started", func() {
			// A long joint simulation: computed long ago, submitted just now.
			p := plan(ids[0])
			p.Plan.ComputedAt = env.Clock.Now().Add(-pipeline.StaleAfter - 15*time.Second)
			admitter.Admit(ctx, []*pipeline.Pending{p}, step())
			Expect(planner.inputs[0].Causes).To(BeEmpty())
			// A round older than StaleAfter+MaxPlanTime is stale anyway: the Ledger no longer keeps older starts.
			old := plan(ids[1])
			old.Plan.ComputedAt = env.Clock.Now().Add(-pipeline.StaleAfter - pipeline.MaxPlanTime - time.Second)
			admitter.Admit(ctx, []*pipeline.Pending{old}, step())
			Expect(planner.inputs[1].Causes).To(Equal([]types.ResimCause{types.ResimStale}))
			// A plan that waited longer than StaleAfter since its submission is stale.
			waited := plan(ids[2])
			waited.Plan.ComputedAt = env.Clock.Now().Add(-pipeline.StaleAfter - time.Second)
			waited.SubmittedAt = waited.Plan.ComputedAt
			admitter.Admit(ctx, []*pipeline.Pending{waited}, step())
			Expect(planner.inputs[2].Causes).To(Equal([]types.ResimCause{types.ResimStale}))
		})
		It("rejects a consolidation candidate whose pods changed instead of re-placing them", func() {
			planner.always = true
			p := plan(ids[4])
			p.Plan.PodSetHash = map[string]uint64{ids[4]: 1}
			Expect(admitter.Admit(ctx, []*pipeline.Pending{p}, step()).Rejected).To(HaveKeyWithValue(p.Plan.ID, types.ReasonPodsChanged))
		})
		It("rejects a plan that needs a re-simulation its planner can't run", func() {
			plain := newStubPlanner("plain")
			a := pipeline.NewAdmitter(l, pipeline.NewCandidateRebuilder(env.Client, cluster, env.Clock, cloudProvider, queue), plain)
			p := plan(ids[0])
			p.Plan.Planner = "plain"
			p.Plan.ComputedAt = env.Clock.Now().Add(-time.Hour)
			Expect(a.Admit(ctx, []*pipeline.Pending{p}, step()).Rejected).To(HaveKeyWithValue(p.Plan.ID, types.ReasonResimChanged))
		})
	})
	It("amends the Ledger with the footprint the planner accepted", func() {
		w.nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "3"}}
		ExpectApplied(ctx, env.Client, w.nodePool)
		p := plan(ids[0], ids[1])
		planner.revalidate = func(in pipeline.RevalidateInput) ([]*pipeline.Plan, error) {
			// Emptiness shrinks a plan to the candidates still empty.
			in.Plan.Candidates = in.Fresh[:1]
			in.Plan.Footprint.Removed = sets.New(ids[0])
			in.Plan.Footprint.BudgetNodes = map[string]sets.Set[string]{w.nodePool.Name: sets.New(ids[0])}
			return []*pipeline.Plan{in.Plan}, nil
		}
		Expect(started(admitter.Admit(ctx, []*pipeline.Pending{p}, step()))).To(Equal([]string{ids[0]}))
		Expect(l.View().Unstarted().UnsortedList()).To(ConsistOf(ids[0]))
		Expect(l.Budgets().Remaining(ctx, w.nodePool.Name, v1.DisruptionReasonDrifted)).To(Equal(2))
	})
	It("makes a plan related to one admitted earlier in the step wait for the next step", func() {
		p := plan(ids[0])
		p.Plan.Footprint.Destinations = sets.New(ids[3])
		s := step()
		s.Admitted = []*pipeline.Plan{{ID: uuid.New(), BatchID: uuid.New(), Footprint: types.Footprint{Removed: sets.New(ids[1]), Destinations: sets.New(ids[3])}}}
		r := admitter.Admit(ctx, []*pipeline.Pending{p}, s)
		Expect(r.Start).To(BeEmpty())
		Expect(r.Wait).To(HaveLen(1))
	})
	It("makes a plan that always re-simulates wait while another plan admitted earlier in the step hasn't started", func() {
		planner.always = true
		p := plan(ids[0])
		s := step()
		// Unrelated to p: only its validator's re-simulation, which sees live state, would see it as not started.
		s.Admitted = []*pipeline.Plan{{ID: uuid.New(), BatchID: uuid.New(), Footprint: types.Footprint{Removed: sets.New(ids[1])}}}
		r := admitter.Admit(ctx, []*pipeline.Pending{p}, s)
		Expect(r.Start).To(BeEmpty())
		Expect(r.Wait).To(HaveLen(1))
		Expect(planner.inputs).To(BeEmpty())
		// With nothing admitted before it, it re-validates.
		Expect(started(admitter.Admit(ctx, []*pipeline.Pending{p}, step()))).To(Equal([]string{ids[0]}))
	})
	Context("batches", func() {
		batch := func(ps ...*pipeline.Pending) []*pipeline.Pending {
			for _, p := range ps {
				p.Plan.BatchID, p.Plan.Siblings = ps[0].Plan.ID, len(ps)
			}
			return ps
		}
		It("re-simulates a batch's stale plans with one joint simulation, and starts its plans in their place", func() {
			jp := &jointPlanner{acceptingPlanner: planner}
			admitter = pipeline.NewAdmitter(l, pipeline.NewCandidateRebuilder(env.Client, cluster, env.Clock, cloudProvider, queue), jp)
			g := batch(plan(ids[0]), plan(ids[1]), plan(ids[2]))
			for _, p := range g {
				p.SubmittedAt = env.Clock.Now().Add(-pipeline.StaleAfter - time.Second)
			}
			r := admitter.Admit(ctx, g, step())
			Expect(planner.inputs).To(BeEmpty()) // no plan was re-simulated alone
			Expect(jp.joint).To(HaveLen(1))
			Expect(jp.joint[0]).To(HaveLen(3))
			Expect(r.Rejected).To(BeEmpty())
			Expect(started(r)).To(ConsistOf(ids[0], ids[1], ids[2]))
			Expect(r.Start).To(HaveLen(1))
			for _, p := range g {
				Expect(r.Replaced[p.Plan.ID]).To(Equal([]uuid.UUID{r.Start[0].ID}))
			}
			Expect(l.View().Unstarted().UnsortedList()).To(ConsistOf(ids[0], ids[1], ids[2]))
		})
		It("rejects the whole batch when its joint re-simulation fails", func() {
			jp := &jointPlanner{acceptingPlanner: planner, fail: true}
			admitter = pipeline.NewAdmitter(l, pipeline.NewCandidateRebuilder(env.Client, cluster, env.Clock, cloudProvider, queue), jp)
			g := batch(plan(ids[0]), plan(ids[1]))
			for _, p := range g {
				p.SubmittedAt = env.Clock.Now().Add(-pipeline.StaleAfter - time.Second)
			}
			r := admitter.Admit(ctx, g, step())
			Expect(r.Start).To(BeEmpty())
			Expect(r.Rejected).To(Equal(map[uuid.UUID]types.RejectReason{g[0].Plan.ID: types.ReasonResimUnschedulable, g[1].Plan.ID: types.ReasonSiblingRejected}))
		})
		It("starts a batch as a unit", func() {
			Expect(started(admitter.Admit(ctx, batch(plan(ids[0]), plan(ids[1])), step()))).To(Equal([]string{ids[0], ids[1]}))
		})
		It("commits a batch all or nothing: a member that can't be committed leaves no other member's change (B2-1)", func() {
			w.nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "3"}}
			ExpectApplied(ctx, env.Client, w.nodePool)
			g := batch(plan(ids[0]), plan(ids[1]))
			a, b := g[0].Plan, g[1].Plan
			planner.revalidate = func(in pipeline.RevalidateInput) ([]*pipeline.Plan, error) {
				if in.Plan.ID == a.ID {
					// The re-simulation returns the plan under a new ID (it split into new commands).
					replacement := deletePlan(in.Fresh...)
					replacement.ID, replacement.BatchID, replacement.Siblings, replacement.Reason = uuid.New(), a.BatchID, 1, a.Reason
					replacement.Footprint.BudgetNodes = map[string]sets.Set[string]{w.nodePool.Name: sets.New(ids[0])}
					return []*pipeline.Plan{replacement}, nil
				}
				// b's accepted footprint no longer fits the budget.
				in.Plan.Footprint.BudgetNodes = map[string]sets.Set[string]{w.nodePool.Name: sets.New(ids[1], ids[2], ids[3], ids[5])}
				return []*pipeline.Plan{in.Plan}, nil
			}
			r := admitter.Admit(ctx, g, step())
			Expect(r.Start).To(BeEmpty())
			// One plan carries why (budget), the other is its sibling, so plans_total counts the reason once.
			Expect(r.Rejected).To(Equal(map[uuid.UUID]types.RejectReason{a.ID: types.ReasonBudget, b.ID: types.ReasonSiblingRejected}))
			// The executor releases the group's plans by their own IDs; nothing else stays reserved.
			l.Release(a.ID, r.Rejected[a.ID])
			l.Release(b.ID, r.Rejected[b.ID])
			Expect(l.View().Unstarted().UnsortedList()).To(BeEmpty())
		})
		It("makes a re-planned batch coupled to a plan admitted earlier in the step wait (B2-2)", func() {
			g := batch(plan(ids[0]), plan(ids[1]), plan(ids[2]))
			cluster.MarkForDeletion(ids[1])
			planner.replan = func(survivors []*pipeline.Plan) ([]*pipeline.Plan, error) {
				joint := deletePlan(lo.FlatMap(survivors, func(p *pipeline.Plan, _ int) []*disruption.Candidate { return p.Candidates })...)
				joint.ID = uuid.New()
				joint.BatchID, joint.Siblings, joint.Reason = joint.ID, 1, v1.DisruptionReasonDrifted
				joint.Footprint.Destinations = sets.New(ids[3])
				return []*pipeline.Plan{joint}, nil
			}
			s := step()
			s.Admitted = []*pipeline.Plan{{ID: uuid.New(), BatchID: uuid.New(), Footprint: types.Footprint{Removed: sets.New(ids[5]), Destinations: sets.New(ids[3])}}}
			r := admitter.Admit(ctx, g, s)
			Expect(r.Start).To(BeEmpty())
			Expect(r.Rejected).To(BeEmpty())
			Expect(r.Wait).To(HaveLen(3))
			// Nothing changed in the Ledger.
			Expect(l.View().Unstarted().UnsortedList()).To(ConsistOf(ids[0], ids[1], ids[2]))
		})
		It("makes a batch whose joint re-simulation is coupled to a plan admitted earlier in the step wait", func() {
			// Rule 9 for the joint path: the members alone relate to nothing admitted, but the joint plans do; they
			// must wait for the next step, whose re-simulation sees that plan started.
			jp := &jointPlanner{acceptingPlanner: planner, destinations: sets.New(ids[3])}
			admitter = pipeline.NewAdmitter(l, pipeline.NewCandidateRebuilder(env.Client, cluster, env.Clock, cloudProvider, queue), jp)
			g := batch(plan(ids[0]), plan(ids[1]))
			for _, p := range g {
				p.SubmittedAt = env.Clock.Now().Add(-pipeline.StaleAfter - time.Second)
			}
			s := step()
			s.Admitted = []*pipeline.Plan{{ID: uuid.New(), BatchID: uuid.New(), Footprint: types.Footprint{Removed: sets.New(ids[5]), Destinations: sets.New(ids[3])}}}
			r := admitter.Admit(ctx, g, s)
			Expect(jp.joint).To(HaveLen(1))
			Expect(r.Start).To(BeEmpty())
			Expect(r.Rejected).To(BeEmpty())
			Expect(r.Replaced).To(BeEmpty())
			Expect(r.Wait).To(HaveLen(2))
			// Nothing changed in the Ledger.
			Expect(l.View().Unstarted().UnsortedList()).To(ConsistOf(ids[0], ids[1]))
			// With nothing admitted before it, the same batch starts.
			Expect(started(admitter.Admit(ctx, g, step()))).To(ConsistOf(ids[0], ids[1]))
		})
		It("makes the whole batch wait while unsynced if one member moves pods", func() {
			unlaunched := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: w.nodePool.Name}}})
			unlaunched.Status.ProviderID = ""
			cluster.UpdateNodeClaim(unlaunched)
			g := batch(plan(ids[0]), plan(ids[4]))
			r := admitter.Admit(ctx, g, step())
			Expect(r.Start).To(BeEmpty())
			Expect(r.Wait).To(HaveLen(2))
			Expect(planner.inputs).To(BeEmpty())
		})
		It("makes the whole batch wait if one member is coupled to a plan admitted earlier in the step", func() {
			g := batch(plan(ids[0]), plan(ids[1]))
			g[1].Plan.Footprint.Destinations = sets.New(ids[3])
			Expect(l.Amend(ctx, g[1].Plan.ID, g[1].Plan.Footprint)).To(Succeed())
			s := step()
			s.Admitted = []*pipeline.Plan{{ID: uuid.New(), BatchID: uuid.New(), Footprint: types.Footprint{Removed: sets.New(ids[5]), Destinations: sets.New(ids[3])}}}
			r := admitter.Admit(ctx, g, s)
			Expect(r.Start).To(BeEmpty())
			Expect(r.Wait).To(HaveLen(2))
			Expect(r.Resims).To(BeEmpty())
		})
		It("re-plans the survivors once if one plan is rejected, or starts none", func() {
			g := batch(plan(ids[0]), plan(ids[1]), plan(ids[2]))
			cluster.MarkForDeletion(ids[1])
			var replanned []*pipeline.Plan
			planner.replan = func(survivors []*pipeline.Plan) ([]*pipeline.Plan, error) {
				replanned = survivors
				joint := deletePlan(lo.FlatMap(survivors, func(p *pipeline.Plan, _ int) []*disruption.Candidate { return p.Candidates })...)
				joint.ID = uuid.New()
				joint.BatchID, joint.Siblings, joint.Reason = joint.ID, 1, v1.DisruptionReasonDrifted
				return []*pipeline.Plan{joint}, nil
			}
			r := admitter.Admit(ctx, g, step())
			Expect(r.Rejected).To(Equal(map[uuid.UUID]types.RejectReason{g[1].Plan.ID: types.ReasonCandidateInvalid}))
			Expect(providerIDsOf(lo.FlatMap(replanned, func(p *pipeline.Plan, _ int) []*disruption.Candidate { return p.Candidates }))).To(Equal([]string{ids[0], ids[2]}))
			Expect(started(r)).To(Equal([]string{ids[0], ids[2]}))
			Expect(r.Replaced).To(HaveKeyWithValue(g[0].Plan.ID, []uuid.UUID{r.Start[0].ID}))
			// The re-plan holds the survivors' reservation now; the executor releases the rejected plan.
			Expect(l.Recheck(ctx, r.Start[0].ID)).To(Succeed())
			Expect(pipeline.RejectReasonOf(l.Recheck(ctx, g[0].Plan.ID))).To(Equal(types.ReasonCanceled))
			Expect(pipeline.RejectReasonOf(l.Recheck(ctx, g[2].Plan.ID))).To(Equal(types.ReasonCanceled))

			planner.replan = nil
			g = batch(plan(ids[3]), plan(ids[4]))
			cluster.MarkForDeletion(ids[4])
			r = admitter.Admit(ctx, g, step())
			Expect(r.Start).To(BeEmpty())
			Expect(r.Rejected).To(Equal(map[uuid.UUID]types.RejectReason{g[0].Plan.ID: types.ReasonSiblingRejected, g[1].Plan.ID: types.ReasonCandidateInvalid}))
		})
	})
	It("never admits static drift while unsynced, not even an empty terminate-first node (B2-3)", func() {
		unlaunched := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: w.nodePool.Name}}})
		unlaunched.Status.ProviderID = ""
		cluster.UpdateNodeClaim(unlaunched)
		p := plan(ids[0])
		p.Plan.TerminateFirst = disruption.TerminateFirstStaticAtLimit
		r := admitter.Admit(ctx, []*pipeline.Pending{p}, step())
		Expect(r.Start).To(BeEmpty())
		Expect(r.Wait).To(HaveLen(1))
	})
	It("drops a plan that waited for a gate longer than MaxAdmissionWait", func() {
		p := plan(ids[0])
		p.Plan.ValidateAt = env.Clock.Now().Add(15 * time.Second)
		env.Clock.Step(15*time.Second + pipeline.MaxAdmissionWait + time.Second)
		Expect(admitter.Admit(ctx, []*pipeline.Pending{p}, step()).Dropped).To(HaveKeyWithValue(p.Plan.ID, types.ReasonDeadline))
	})
})

var _ = Describe("Relate", func() {
	fp := func(removed, dests []string, groups ...uint64) types.Footprint {
		return types.Footprint{Removed: sets.New(removed...), Destinations: sets.New(dests...), TopologyGroups: sets.New(groups...)}
	}
	DescribeTable("classifies two plans",
		func(a, b types.Footprint, hard []pipeline.Conflict, coupled []pipeline.Coupling) {
			r := pipeline.Relate(a, b)
			Expect(r.Hard).To(ConsistOf(lo.Map(hard, func(c pipeline.Conflict, _ int) any { return c })...))
			Expect(r.Coupled).To(ConsistOf(lo.Map(coupled, func(c pipeline.Coupling, _ int) any { return c })...))
		},
		Entry("disjoint", fp([]string{"a"}, []string{"x"}), fp([]string{"b"}, []string{"y"}), nil, nil),
		Entry("same candidate", fp([]string{"a"}, nil), fp([]string{"a"}, nil), []pipeline.Conflict{pipeline.SameCandidate}, nil),
		Entry("a deletes b's destination", fp([]string{"a"}, nil), fp([]string{"b"}, []string{"a"}), []pipeline.Conflict{pipeline.RemovesDestination}, nil),
		Entry("b deletes a's destination", fp([]string{"a"}, []string{"b"}), fp([]string{"b"}, nil), nil, []pipeline.Coupling{pipeline.DestinationRemoved}),
		Entry("shared destination", fp([]string{"a"}, []string{"x"}), fp([]string{"b"}, []string{"x"}), nil, []pipeline.Coupling{pipeline.SharedDestination}),
		Entry("shared zone topology group", fp([]string{"a"}, nil, 7), fp([]string{"b"}, nil, 7), nil, []pipeline.Coupling{pipeline.SharedTopology}),
		Entry("NodePool membership alone is not a relation", fp([]string{"a"}, nil), fp([]string{"b"}, nil), nil, nil),
	)
})

// failingRebuilder fails every rebuild, as the API server does when the context is cancelled.
type failingRebuilder struct{}

func (failingRebuilder) Rebuild(ctx context.Context, _ string, _ ...string) (*pipeline.Rebuilt, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return nil, errors.New("the server is currently unable to handle the request")
}
