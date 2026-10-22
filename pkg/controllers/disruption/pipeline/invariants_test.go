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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// The pipeline's invariants with both lanes running concurrently (the production runners) on drift and consolidation
// demand: the budget caps the nodes being disrupted across reasons (I1), and no node is in two plans (I2).
//
// I2 is asserted on the pipeline's own state, a Ledger wrapper that sees every reservation: no reserved plan removes a
// node another reserved plan removes, or one that is already MarkedForDeletion (started). The Queue refusing a node
// that is already queued is a second line that the pipeline must not rely on, so it isn't what the spec checks. An
// intruder planner in the Background lane plans the nodes of started commands (as a planner with a stale snapshot
// would) without budget nodes, so only the Ledger's ownership check stands between it and a second plan for a node.
var _ = Describe("Invariants with concurrent lanes", func() {
	It("never disrupts more nodes than the budget allows, nor reserves a node twice", func() {
		w := newWorld(20)
		w.nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "5"}}
		ExpectApplied(ctx, env.Client, w.nodePool)
		w.withPods("1", lo.Range(15)...)
		l := &checkingLedger{Ledger: pipeline.NewLedger(env.Client, cluster, cloudProvider, env.Clock, queue), owner: map[string]uuid.UUID{}, removes: map[uuid.UUID][]string{}}
		planners := pipeline.DefaultPlanners(ctx, env.Clock, cluster, env.Client, prov, cloudProvider, queue, &pipeline.RoundEvents{}, l.View())
		lanes := pipeline.DefaultLanes(planners)
		intruder := newStubPlanner("intruder")
		intruder.plan = func(in pipeline.PlanInput) (pipeline.PlanOutput, error) {
			var out pipeline.PlanOutput
			for _, n := range cluster.DeepCopyNodes() {
				if n.MarkedForDeletion() {
					p := deletePlan()
					p.Footprint.Removed = sets.New(n.ProviderID())
					p.ValidateAt = in.Now.Add(time.Hour)
					out.Plans = append(out.Plans, p)
				}
			}
			return out, nil
		}
		// Budget-free (like repair of an uninitialized node), so it plans although drift uses the whole budget.
		lanes[1].Planners = append(lanes[1].Planners, &budgetFreePlanner{intruder})
		c := pipeline.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			pipeline.WithLedger(l), pipeline.WithLanes(lanes...))
		m := &addRecorder{}
		Expect(c.Register(ctx, m)).To(Succeed())
		runCtx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		for _, r := range m.added {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				Expect(r.Start(runCtx)).To(Succeed())
			}()
		}
		check := func() {
			GinkgoHelper()
			Expect(l.violations()).To(BeEmpty())
			disrupting := lo.SumBy(queue.GetCommands(), func(cmd *disruption.Command) int { return len(cmd.Candidates) })
			Expect(disrupting).To(BeNumerically("<=", 5), "disrupting %d nodes with a budget of 5", disrupting)
		}
		for range 40 {
			time.Sleep(100 * time.Millisecond)
			check()
			launchReplacements()
			env.Clock.Step(time.Second)
		}
		cancel()
		wg.Wait()
		check()
		// The budget was used, and the intruder tried: work happened on both sides of the check.
		Expect(lo.SumBy(queue.GetCommands(), func(cmd *disruption.Command) int { return len(cmd.Candidates) })).To(Equal(5))
		Expect(intruder.planned()).ToNot(BeEmpty())
		Expect(l.attempts.Load()).To(BeNumerically(">", 0))
	})
})

// checkingLedger records every reserved plan's Removed nodes and reports a node reserved by two plans, or reserved
// while MarkedForDeletion. Its mutex is held across the inner call, so its record follows the Ledger's order.
type checkingLedger struct {
	pipeline.Ledger
	mu       sync.Mutex
	owner    map[string]uuid.UUID
	removes  map[uuid.UUID][]string
	found    []string
	attempts atomic.Int64
}

func (l *checkingLedger) add(p *pipeline.Plan) {
	marked := lo.SliceToMap(lo.Filter(cluster.DeepCopyNodes(), func(n *state.StateNode, _ int) bool { return n.MarkedForDeletion() }),
		func(n *state.StateNode) (string, bool) { return n.ProviderID(), true })
	for id := range p.Footprint.Removed {
		if other, ok := l.owner[id]; ok && other != p.ID {
			l.found = append(l.found, fmt.Sprintf("node %s reserved by plans %s and %s", id, other, p.ID))
		}
		if marked[id] {
			l.found = append(l.found, fmt.Sprintf("node %s reserved by plan %s while MarkedForDeletion", id, p.ID))
		}
		l.owner[id] = p.ID
	}
	l.removes[p.ID] = p.Footprint.Removed.UnsortedList()
}

func (l *checkingLedger) drop(id uuid.UUID) {
	for _, n := range l.removes[id] {
		if l.owner[n] == id {
			delete(l.owner, n)
		}
	}
	delete(l.removes, id)
}

func (l *checkingLedger) Reserve(ctx context.Context, plans ...*pipeline.Plan) []error {
	l.mu.Lock()
	defer l.mu.Unlock()
	errs := l.Ledger.Reserve(ctx, plans...)
	for i, p := range plans {
		if p.Planner == "intruder" {
			l.attempts.Add(1)
		}
		if errs[i] == nil {
			l.add(p)
		}
	}
	return errs
}

func (l *checkingLedger) Swap(ctx context.Context, old []uuid.UUID, replans []*pipeline.Plan) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	err := l.Ledger.Swap(ctx, old, replans)
	if err == nil {
		for _, id := range old {
			l.drop(id)
		}
		for _, p := range replans {
			l.add(p)
		}
	}
	return err
}

func (l *checkingLedger) Amend(ctx context.Context, id uuid.UUID, fp types.Footprint) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	err := l.Ledger.Amend(ctx, id, fp)
	if err == nil {
		l.drop(id)
		l.add(&pipeline.Plan{ID: id, Footprint: fp})
	}
	return err
}

func (l *checkingLedger) MarkStarted(id uuid.UUID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Ledger.MarkStarted(id)
	l.drop(id)
}

func (l *checkingLedger) Release(id uuid.UUID, reason types.RejectReason) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Ledger.Release(id, reason)
	l.drop(id)
}

func (l *checkingLedger) violations() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.found...)
}

// Decision #9 (F26): another lane's simulation must not move pods onto the candidates of a reserved, unstarted plan, or
// it would plan capacity on a node that is about to go away (and, once that plan starts and its pods are re-placed,
// provision twice).
var _ = Describe("Simulator", func() {
	It("records nothing about pending pods while it plans", func() {
		w := newWorld(1)
		w.withPods("1", 0)
		// A pending pod the provisioner ignores: its volume claim doesn't exist.
		ignored := test.UnschedulablePod(test.PodOptions{PersistentVolumeClaims: []string{"missing"}})
		ExpectApplied(ctx, env.Client, ignored)
		s, err := pipeline.NewSnapshotBuilder(env.Client, cluster, env.Clock, cloudProvider, queue, clusterCost, nil).Build(ctx, pipeline.Scope{})
		Expect(err).ToNot(HaveOccurred())
		cand := s.CandidatesFor(ctx, newStubPlanner("drift"))[0]
		inner := disruption.NewLegacySimulator(env.Client, cluster, prov, env.Clock, recorder)
		_, err = pipeline.NewSimulator(inner, nil).Simulate(ctx, disruption.SimOptions{}, cand)
		Expect(err).ToNot(HaveOccurred())
		Expect(recorder.Calls(events.FailedScheduling)).To(BeZero())
		Expect(cluster.PodSchedulingDecisionTime(client.ObjectKeyFromObject(ignored)).IsZero()).To(BeTrue())
		// The disruption controller's simulation records both, as before.
		_, err = inner.Simulate(ctx, disruption.SimOptions{}, cand)
		Expect(err).ToNot(HaveOccurred())
		Expect(recorder.Calls(events.FailedScheduling)).To(Equal(1))
		Expect(cluster.PodSchedulingDecisionTime(client.ObjectKeyFromObject(ignored)).IsZero()).To(BeFalse())
	})
	It("doesn't use the candidates of reserved plans as destinations", func() {
		w := newWorld(2)
		w.withPods("1", 1)
		s, err := pipeline.NewSnapshotBuilder(env.Client, cluster, env.Clock, cloudProvider, queue, clusterCost, nil).Build(ctx, pipeline.Scope{})
		Expect(err).ToNot(HaveOccurred())
		cands := lo.SliceToMap(s.CandidatesFor(ctx, newStubPlanner("drift")), func(c *disruption.Candidate) (string, *disruption.Candidate) { return c.ProviderID(), c })
		ids := w.providerIDs()
		inner := disruption.NewLegacySimulator(env.Client, cluster, prov, env.Clock, recorder)
		// Without a reservation, node 1's pod moves to node 0, which has room.
		out, err := pipeline.NewSimulator(inner, nil).Simulate(ctx, disruption.SimOptions{}, cands[ids[1]])
		Expect(err).ToNot(HaveOccurred())
		Expect(lo.Map(lo.Filter(out.Results.ExistingNodes, func(n *pscheduling.ExistingNode, _ int) bool { return len(n.Pods) > 0 }),
			func(n *pscheduling.ExistingNode, _ int) string { return n.ProviderID() })).To(ConsistOf(ids[0]))
		// With node 0 reserved by another plan, it goes to a new NodeClaim instead.
		reserved := &fakeView{reserved: sets.New(ids[0]), channels: map[*pipeline.Plan]chan pipeline.Resolution{}}
		out, err = pipeline.NewSimulator(inner, reserved).Simulate(ctx, disruption.SimOptions{}, cands[ids[1]])
		Expect(err).ToNot(HaveOccurred())
		Expect(lo.Filter(out.Results.ExistingNodes, func(n *pscheduling.ExistingNode, _ int) bool { return len(n.Pods) > 0 })).To(BeEmpty())
		Expect(out.Results.NewNodeClaims).To(HaveLen(1))
		Expect(out.Results.AllNonPendingPodsScheduled()).To(BeTrue())
	})
})
