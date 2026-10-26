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
	"sync"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// budgetPlan disrupts ids of nodePool for reason, charging the budget for each.
func budgetPlan(nodePool string, reason v1.DisruptionReason, ids ...string) *pipeline.Plan {
	id := uuid.New()
	return &pipeline.Plan{ID: id, BatchID: id, Siblings: 1, Reason: reason, Footprint: types.Footprint{
		Removed:     sets.New(ids...),
		BudgetNodes: map[string]sets.Set[string]{nodePool: sets.New(ids...)},
	}}
}

func reasonsOf(errs []error) []types.RejectReason {
	return lo.Map(errs, func(err error, _ int) types.RejectReason { return pipeline.RejectReasonOf(err) })
}

var _ = Describe("Ledger", func() {
	var w *world
	var l pipeline.Ledger
	var ids []string
	setBudget := func(nodes string) {
		GinkgoHelper()
		w.nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: nodes}}
		ExpectApplied(ctx, env.Client, w.nodePool)
	}
	BeforeEach(func() {
		w = newWorld(10)
		ids = w.providerIDs()
		l = pipeline.NewLedger(env.Client, cluster, cloudProvider, env.Clock, queue)
	})

	Context("Budgets", func() {
		It("caps the nodes disrupting or reserved across reasons, first come first served", func() {
			setBudget("3")
			Expect(reasonsOf(l.Reserve(ctx, budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[0], ids[1])))).To(Equal([]types.RejectReason{""}))
			// The drift reservation counts against every reason, as in-flight disruption does today.
			Expect(reasonsOf(l.Reserve(ctx,
				budgetPlan(w.nodePool.Name, v1.DisruptionReasonEmpty, ids[2], ids[3]),
				budgetPlan(w.nodePool.Name, v1.DisruptionReasonUnderutilized, ids[4]),
				budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[5]),
			))).To(Equal([]types.RejectReason{types.ReasonBudget, "", types.ReasonBudget}))
			Expect(l.Budgets().Remaining(ctx, w.nodePool.Name, v1.DisruptionReasonDrifted)).To(Equal(0))
			Expect(l.Budgets().AnyRemaining(ctx, v1.DisruptionReasonEmpty)).To(BeFalse())
		})
		It("honors per-reason budgets with the shared count", func() {
			// The documented pattern: drift may use 2 of the 10% (here 4) that all reasons share.
			w.nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "2", Reasons: []v1.DisruptionReason{v1.DisruptionReasonDrifted}}, {Nodes: "4"}}
			ExpectApplied(ctx, env.Client, w.nodePool)
			Expect(reasonsOf(l.Reserve(ctx,
				budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[0], ids[1]),
				budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[2]),
				budgetPlan(w.nodePool.Name, v1.DisruptionReasonUnderutilized, ids[3], ids[4]),
				budgetPlan(w.nodePool.Name, v1.DisruptionReasonEmpty, ids[5]),
			))).To(Equal([]types.RejectReason{"", types.ReasonBudget, "", types.ReasonBudget}))
		})
		It("counts NotReady nodes against every reason but Unhealthy, and still charges a NotReady candidate", func() {
			setBudget("3")
			node := ExpectExists(ctx, env.Client, w.nodes[0])
			node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
			ExpectApplied(ctx, env.Client, node)
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))
			Expect(l.Budgets().Remaining(ctx, w.nodePool.Name, v1.DisruptionReasonDrifted)).To(Equal(2))
			Expect(l.Budgets().Remaining(ctx, w.nodePool.Name, v1.DisruptionReasonUnhealthy)).To(Equal(3))
			// Drift has 2 left: the NotReady node itself is charged again, as the disruption controller decrements its
			// budget once per candidate, so the NotReady node and one more fill it.
			Expect(reasonsOf(l.Reserve(ctx, budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[0], ids[1], ids[2])))).To(Equal([]types.RejectReason{types.ReasonBudget}))
			Expect(reasonsOf(l.Reserve(ctx, budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[0], ids[1])))).To(Equal([]types.RejectReason{""}))
		})
		It("counts a node once while its plan hands over from reservation to MarkedForDeletion", func() {
			setBudget("2")
			p := budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[0])
			Expect(l.Reserve(ctx, p)).To(Equal([]error{nil}))
			Expect(l.Budgets().Remaining(ctx, w.nodePool.Name, v1.DisruptionReasonDrifted)).To(Equal(1))
			cluster.MarkForDeletion(ids[0])
			Expect(l.Budgets().Remaining(ctx, w.nodePool.Name, v1.DisruptionReasonDrifted)).To(Equal(1))
			l.MarkStarted(p.ID)
			Expect(l.Budgets().Remaining(ctx, w.nodePool.Name, v1.DisruptionReasonDrifted)).To(Equal(1))
			Expect(l.View().IsReserved(ids[0])).To(BeFalse())
			// A marked node isn't charged again.
			Expect(reasonsOf(l.Reserve(ctx, budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[1])))).To(Equal([]types.RejectReason{""}))
			Expect(reasonsOf(l.Reserve(ctx, budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[2])))).To(Equal([]types.RejectReason{types.ReasonBudget}))
		})
		It("rechecks a reserved plan without its own reservation, at the live budget", func() {
			setBudget("2")
			p := budgetPlan(w.nodePool.Name, v1.DisruptionReasonUnderutilized, ids[0], ids[1])
			Expect(l.Reserve(ctx, p)).To(Equal([]error{nil}))
			Expect(l.Recheck(ctx, p.ID)).To(Succeed())
			// The budget shrinks while the plan waits (e.g. a cron window closes).
			setBudget("1")
			Expect(pipeline.RejectReasonOf(l.Recheck(ctx, p.ID))).To(Equal(types.ReasonBudget))
		})
		It("rechecks several plans against one reading of live state, each without its own reservation", func() {
			setBudget("2")
			a := budgetPlan(w.nodePool.Name, v1.DisruptionReasonUnderutilized, ids[0])
			b := budgetPlan(w.nodePool.Name, v1.DisruptionReasonUnderutilized, ids[1])
			Expect(l.Reserve(ctx, a, b)).To(Equal([]error{nil, nil}))
			Expect(l.RecheckAll(ctx, a.ID, b.ID)).To(Equal([]error{nil, nil}))
			setBudget("1")
			errs := l.RecheckAll(ctx, a.ID, b.ID, uuid.New())
			Expect(lo.Map(errs, func(err error, _ int) types.RejectReason { return pipeline.RejectReasonOf(err) })).To(Equal(
				[]types.RejectReason{types.ReasonBudget, types.ReasonBudget, types.ReasonCanceled}))
		})
		It("rejects the rest of a batch once one of its plans doesn't fit", func() {
			setBudget("2")
			batch := lo.Map(ids[:3], func(id string, _ int) *pipeline.Plan {
				return budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, id)
			})
			for _, p := range batch {
				p.BatchID = batch[0].ID
				p.Siblings = 3
			}
			batch[0], batch[1] = batch[1], budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[3], ids[4])
			batch[1].BatchID = batch[2].BatchID
			Expect(reasonsOf(l.Reserve(ctx, append(batch, budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[5]))...))).To(Equal(
				[]types.RejectReason{"", types.ReasonBudget, types.ReasonBudget, ""}))
		})
		It("is consistent under concurrent reservations for the last slots", func() {
			setBudget("5")
			var wg sync.WaitGroup
			results := make([][]error, 10)
			for i := range ids {
				wg.Add(1)
				go func() {
					defer GinkgoRecover()
					defer wg.Done()
					results[i] = l.Reserve(ctx, budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[i]))
				}()
			}
			wg.Wait()
			Expect(lo.CountBy(results, func(errs []error) bool { return errs[0] == nil })).To(Equal(5))
			Expect(l.View().Unstarted().Len()).To(Equal(5))
		})
		It("has the disruption controller's remaining budget when nothing is reserved", func() {
			w.nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "40%"}, {Nodes: "3", Reasons: []v1.DisruptionReason{v1.DisruptionReasonDrifted}}}
			ExpectApplied(ctx, env.Client, w.nodePool)
			check := func() {
				GinkgoHelper()
				for _, reason := range []v1.DisruptionReason{v1.DisruptionReasonDrifted, v1.DisruptionReasonEmpty, v1.DisruptionReasonUnderutilized, v1.DisruptionReasonUnhealthy} {
					want, err := disruption.BuildDisruptionBudgetMapping(ctx, cluster, env.Clock, env.Client, cloudProvider, recorder, reason)
					Expect(err).ToNot(HaveOccurred())
					Expect(l.Budgets().Remaining(ctx, w.nodePool.Name, reason)).To(Equal(want[w.nodePool.Name]), "reason %s", reason)
					Expect(l.Budgets().AnyRemaining(ctx, reason)).To(Equal(want[w.nodePool.Name] > 0), "reason %s", reason)
				}
			}
			check()
			cluster.MarkForDeletion(ids[0], ids[1])
			check()
			for _, n := range w.nodes[2:4] {
				node := ExpectExists(ctx, env.Client, n)
				node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
				ExpectApplied(ctx, env.Client, node)
				ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))
			}
			check()
			nc := ExpectExists(ctx, env.Client, w.nodeClaims[5])
			nc.StatusConditions().SetTrue(v1.ConditionTypeInstanceTerminating)
			ExpectApplied(ctx, env.Client, nc)
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nc))
			check()
			cluster.MarkForDeletion(ids[6], ids[7], ids[8])
			check()
		})
	})

	Context("Ownership", func() {
		It("gives a node to at most one plan, and never one a started command is moving pods onto", func() {
			p := budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[0])
			p.Footprint.Destinations = sets.New(ids[1])
			Expect(l.Reserve(ctx, p)).To(Equal([]error{nil}))
			Expect(reasonsOf(l.Reserve(ctx,
				budgetPlan(w.nodePool.Name, v1.DisruptionReasonEmpty, ids[0]),
				budgetPlan(w.nodePool.Name, v1.DisruptionReasonEmpty, ids[1]),
			))).To(Equal([]types.RejectReason{types.ReasonConflict, types.ReasonConflict}))
			cluster.MarkForDeletion(ids[2])
			Expect(reasonsOf(l.Reserve(ctx, budgetPlan(w.nodePool.Name, v1.DisruptionReasonEmpty, ids[2])))).To(Equal([]types.RejectReason{types.ReasonConflict}))
			// A started command moving pods onto ids[3].
			cmd := &disruption.Command{Results: pscheduling.Results{ExistingNodes: []*pscheduling.ExistingNode{existingNodeWithPod(ids[3])}}}
			queue.ProviderIDToCommand[ids[4]] = cmd
			Expect(reasonsOf(l.Reserve(ctx,
				budgetPlan(w.nodePool.Name, v1.DisruptionReasonEmpty, ids[3]),
				budgetPlan(w.nodePool.Name, v1.DisruptionReasonEmpty, ids[4]),
			))).To(Equal([]types.RejectReason{types.ReasonConflict, types.ReasonConflict}))
			Expect(l.View().InFlightDestinations().UnsortedList()).To(ConsistOf(ids[3]))
		})
		It("never gives another plan a node that a reserved plan or a started command depends on (decision #16)", func() {
			// A reserved plan whose pods were simulated onto ids[1], which wasn't initialized.
			p := budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[0])
			p.Footprint = pipeline.FootprintOf(disruption.Command{Dependencies: []string{ids[1]}}, l.Version())
			p.Footprint.Removed, p.Footprint.BudgetNodes = sets.New(ids[0]), map[string]sets.Set[string]{w.nodePool.Name: sets.New(ids[0])}
			Expect(l.Reserve(ctx, p)).To(Equal([]error{nil}))
			// A started command that depends on ids[3] (its results don't list it).
			queue.ProviderIDToCommand[ids[2]] = &disruption.Command{Dependencies: []string{ids[3]}}
			Expect(l.View().InFlightDestinations().UnsortedList()).To(ConsistOf(ids[3]))
			// Repair is the method that disrupts uninitialized nodes; any planner is rejected the same way.
			Expect(reasonsOf(l.Reserve(ctx,
				budgetPlan(w.nodePool.Name, v1.DisruptionReasonUnhealthy, ids[1]),
				budgetPlan(w.nodePool.Name, v1.DisruptionReasonEmpty, ids[3]),
				budgetPlan(w.nodePool.Name, v1.DisruptionReasonEmpty, ids[5]),
			))).To(Equal([]types.RejectReason{types.ReasonConflict, types.ReasonConflict, ""}))
		})
		It("releases a rejected plan, and keeps a started plan's footprint for coupling", func() {
			a, b := budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[0]), budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[1])
			v0 := l.Version()
			Expect(l.Reserve(ctx, a, b)).To(Equal([]error{nil, nil}))
			Expect(l.Version()).To(BeNumerically(">", v0))
			Expect(l.View().Unstarted().UnsortedList()).To(ConsistOf(ids[0], ids[1]))
			version := l.Version()
			l.Release(a.ID, types.ReasonCandidateInvalid)
			l.MarkStarted(b.ID)
			Expect(l.View().Unstarted().Len()).To(BeZero())
			Expect(l.Released()).To(Receive())
			started := l.StartedSince(version)
			Expect(started).To(HaveLen(1))
			Expect(started[0].PlanID).To(Equal(b.ID))
			Expect(l.StartedSince(l.Version())).To(BeEmpty())
			// Once no reserved plan predates it and it is older than StaleAfter+MaxPlanTime, it is dropped.
			env.Clock.Step(pipeline.StaleAfter + pipeline.MaxPlanTime + time.Second)
			c := budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[2])
			c.Footprint.ComputedAt = l.Version()
			Expect(l.Reserve(ctx, c)).To(Equal([]error{nil}))
			l.Release(c.ID, types.ReasonCanceled)
			Expect(l.StartedSince(0)).To(BeEmpty())
		})
		It("swaps re-planned siblings all or nothing", func() {
			setBudget("3")
			a, b := budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[0]), budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[1])
			Expect(l.Reserve(ctx, a, b)).To(Equal([]error{nil, nil}))
			// Too big: nothing changes.
			Expect(pipeline.RejectReasonOf(l.Swap(ctx, []uuid.UUID{a.ID}, []*pipeline.Plan{budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[0], ids[2], ids[3])}))).To(Equal(types.ReasonBudget))
			Expect(l.View().Unstarted().UnsortedList()).To(ConsistOf(ids[0], ids[1]))
			replan := budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[0], ids[2])
			Expect(l.Swap(ctx, []uuid.UUID{a.ID}, []*pipeline.Plan{replan})).To(Succeed())
			Expect(l.View().Unstarted().UnsortedList()).To(ConsistOf(ids[0], ids[1], ids[2]))
			Expect(pipeline.RejectReasonOf(l.Recheck(ctx, a.ID))).To(Equal(types.ReasonCanceled))
			Expect(l.Recheck(ctx, replan.ID)).To(Succeed())
		})
	})

	Context("NodePool limits", func() {
		It("credits candidates' capacity and adds every reserved plan's new NodeClaims", func() {
			w.nodePool.Spec.Limits = v1.Limits{resources.Node: resource.MustParse("10")}
			ExpectApplied(ctx, env.Client, w.nodePool)
			replace := func(id string, newNodes int) *pipeline.Plan {
				p := budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, id)
				p.Footprint.LimitCost = map[string]corev1.ResourceList{w.nodePool.Name: {resources.Node: *resource.NewQuantity(int64(newNodes), resource.DecimalSI)}}
				return p
			}
			// At the limit, a same-size replacement fits: its candidate's node is credited.
			Expect(reasonsOf(l.Reserve(ctx, replace(ids[0], 1)))).To(Equal([]types.RejectReason{""}))
			// Two replacements for one node exceed it.
			Expect(reasonsOf(l.Reserve(ctx, replace(ids[1], 2)))).To(Equal([]types.RejectReason{types.ReasonNodePoolLimit}))
			w.nodePool.Spec.Limits = v1.Limits{resources.Node: resource.MustParse("11")}
			ExpectApplied(ctx, env.Client, w.nodePool)
			Expect(reasonsOf(l.Reserve(ctx, replace(ids[1], 2)))).To(Equal([]types.RejectReason{""}))
			// The second reserved plan's extra node is counted at the next one's Reserve.
			Expect(reasonsOf(l.Reserve(ctx, replace(ids[2], 2)))).To(Equal([]types.RejectReason{types.ReasonNodePoolLimit}))
		})
	})

	Context("Static node counts", func() {
		It("takes the node-count slots at Reserve and returns them on release, not once the queue owns them", func() {
			static := test.StaticNodePool(v1.NodePool{Spec: v1.NodePoolSpec{Replicas: lo.ToPtr(int64(0)), Limits: v1.Limits{resources.Node: resource.MustParse("2")}}})
			ExpectApplied(ctx, env.Client, static)
			want := func(n int64) *pipeline.Plan {
				p := budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted)
				p.Footprint.StaticNodeCount = map[string]int64{static.Name: n}
				return p
			}
			a := want(2)
			Expect(l.Reserve(ctx, a)).To(Equal([]error{nil}))
			Expect(l.View().StaticHeld(static.Name)).To(Equal(int64(2)))
			Expect(reasonsOf(l.Reserve(ctx, want(1)))).To(Equal([]types.RejectReason{types.ReasonStaticNodeLimit}))
			// A short grant is returned, so the counter is unchanged.
			l.Release(a.ID, types.ReasonCandidateInvalid)
			Expect(cluster.NodePoolState.ReserveNodeCount(static.Name, 2, 2)).To(Equal(int64(2)))
			cluster.NodePoolState.ReleaseNodeCount(static.Name, 2)
			b := want(1)
			Expect(l.Reserve(ctx, b)).To(Equal([]error{nil}))
			l.MarkStarting(b.ID)
			l.Release(b.ID, types.ReasonStartFailed)
			// The queue owns the slot now and releases it itself.
			Expect(cluster.NodePoolState.ReserveNodeCount(static.Name, 2, 2)).To(Equal(int64(1)))
		})
	})

	Context("Capacity reservations", func() {
		It("doesn't let two plans count on the last slot", func() {
			cloudProvider.InstanceTypes = append(cloudProvider.InstanceTypes, fake.NewInstanceType("reserved-type",
				fake.WithOfferings(cloudprovider.Offering{Available: true, ReservationCapacity: 1, Price: 1,
					Requirements: scheduling.NewLabelRequirements(map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeReserved,
						corev1.LabelTopologyZone: "test-zone-1", cloudprovider.ReservationIDLabel: "r-1"})})))
			into := func(id string) *pipeline.Plan {
				p := budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, id)
				p.Footprint.Reservations = map[string]int{"r-1": 1}
				return p
			}
			Expect(reasonsOf(l.Reserve(ctx, into(ids[0]), into(ids[1])))).To(Equal([]types.RejectReason{"", types.ReasonCapacityReservation}))
			Expect(l.View().ReservationsHeld("r-1")).To(Equal(1))
		})
	})

	Context("Review regressions", func() {
		limitPlan := func(id string, newNodes int) *pipeline.Plan {
			p := budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, id)
			p.Footprint.LimitCost = map[string]corev1.ResourceList{w.nodePool.Name: {resources.Node: *resource.NewQuantity(int64(newNodes), resource.DecimalSI)}}
			return p
		}
		It("doesn't credit another reserved plan's candidates against the limits (R1)", func() {
			w.nodePool.Spec.Limits = v1.Limits{resources.Node: resource.MustParse("10")}
			ExpectApplied(ctx, env.Client, w.nodePool)
			// An emptiness plan that may still be rejected (a pod lands during its validation wait).
			Expect(l.Reserve(ctx, budgetPlan(w.nodePool.Name, v1.DisruptionReasonEmpty, ids[0]))).To(Equal([]error{nil}))
			// A drift plan that grows the pool by one node doesn't fit: 10 nodes + 2 - 1 = 11.
			Expect(reasonsOf(l.Reserve(ctx, limitPlan(ids[1], 2)))).To(Equal([]types.RejectReason{types.ReasonNodePoolLimit}))
			// Another reserved plan's growth counts; its shrink doesn't.
			grow := limitPlan(ids[2], 1)
			Expect(l.Reserve(ctx, grow)).To(Equal([]error{nil}))
			shrink := limitPlan(ids[3], 0)
			Expect(l.Reserve(ctx, shrink)).To(Equal([]error{nil}))
			Expect(reasonsOf(l.Reserve(ctx, limitPlan(ids[4], 2)))).To(Equal([]types.RejectReason{types.ReasonNodePoolLimit}))
		})
		It("credits another reserved plan's candidates in reference mode, as if it had started", func() {
			l = pipeline.NewLedger(env.Client, cluster, cloudProvider, env.Clock, queue, pipeline.WithReservedCredit())
			w.nodePool.Spec.Limits = v1.Limits{resources.Node: resource.MustParse("10")}
			ExpectApplied(ctx, env.Client, w.nodePool)
			Expect(l.Reserve(ctx, budgetPlan(w.nodePool.Name, v1.DisruptionReasonEmpty, ids[0]))).To(Equal([]error{nil}))
			// 10 nodes - 1 (the emptiness plan's) + 2 - 1 = 10.
			Expect(l.Reserve(ctx, limitPlan(ids[1], 2))).To(Equal([]error{nil}))
			Expect(reasonsOf(l.Reserve(ctx, limitPlan(ids[2], 2)))).To(Equal([]types.RejectReason{types.ReasonNodePoolLimit}))
		})
		It("rechecks a plan against limits that shrank", func() {
			w.nodePool.Spec.Limits = v1.Limits{resources.Node: resource.MustParse("10")}
			ExpectApplied(ctx, env.Client, w.nodePool)
			p := limitPlan(ids[0], 1)
			Expect(l.Reserve(ctx, p)).To(Equal([]error{nil}))
			Expect(l.Recheck(ctx, p.ID)).To(Succeed())
			w.nodePool.Spec.Limits = v1.Limits{resources.Node: resource.MustParse("9")}
			ExpectApplied(ctx, env.Client, w.nodePool)
			Expect(pipeline.RejectReasonOf(l.Recheck(ctx, p.ID))).To(Equal(types.ReasonNodePoolLimit))
		})
		It("counts a NotReady candidate the same before and after its plan is reserved (R2)", func() {
			setBudget("2")
			node := ExpectExists(ctx, env.Client, w.nodes[0])
			node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
			ExpectApplied(ctx, env.Client, node)
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))
			p1 := budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[0])
			Expect(l.Reserve(ctx, p1)).To(Equal([]error{nil}))
			// The NotReady node holds both units: no later plan takes one, and p1 still fits.
			Expect(reasonsOf(l.Reserve(ctx, budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[1])))).To(Equal([]types.RejectReason{types.ReasonBudget}))
			Expect(l.Recheck(ctx, p1.ID)).To(Succeed())
			Expect(l.Budgets().Remaining(ctx, w.nodePool.Name, v1.DisruptionReasonDrifted)).To(Equal(0))
		})
		It("checks ownership both ways, at Reserve and at Amend (R5)", func() {
			a := budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[0])
			Expect(l.Reserve(ctx, a)).To(Equal([]error{nil}))
			onto := budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[1])
			onto.Footprint.Destinations = sets.New(ids[0])
			Expect(reasonsOf(l.Reserve(ctx, onto))).To(Equal([]types.RejectReason{types.ReasonConflict}))
			cluster.MarkForDeletion(ids[2])
			onto.Footprint.Destinations = sets.New(ids[2])
			Expect(reasonsOf(l.Reserve(ctx, onto))).To(Equal([]types.RejectReason{types.ReasonConflict}))
			b := budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, ids[3])
			Expect(l.Reserve(ctx, b)).To(Equal([]error{nil}))
			grown := b.Footprint
			grown.Removed = sets.New(ids[3], ids[0])
			Expect(pipeline.RejectReasonOf(l.Amend(ctx, b.ID, grown))).To(Equal(types.ReasonConflict))
			grown.Removed, grown.Destinations = sets.New(ids[3]), sets.New(ids[0])
			Expect(pipeline.RejectReasonOf(l.Amend(ctx, b.ID, grown))).To(Equal(types.ReasonConflict))
		})
		It("rechecks budgets and limits at Amend", func() {
			setBudget("2")
			w.nodePool.Spec.Limits = v1.Limits{resources.Node: resource.MustParse("10")}
			ExpectApplied(ctx, env.Client, w.nodePool)
			p := limitPlan(ids[0], 1)
			Expect(l.Reserve(ctx, p)).To(Equal([]error{nil}))
			bigger := p.Footprint
			bigger.BudgetNodes = map[string]sets.Set[string]{w.nodePool.Name: sets.New(ids[0], ids[1], ids[2])}
			Expect(pipeline.RejectReasonOf(l.Amend(ctx, p.ID, bigger))).To(Equal(types.ReasonBudget))
			bigger = p.Footprint
			bigger.LimitCost = map[string]corev1.ResourceList{w.nodePool.Name: {resources.Node: resource.MustParse("2")}}
			Expect(pipeline.RejectReasonOf(l.Amend(ctx, p.ID, bigger))).To(Equal(types.ReasonNodePoolLimit))
			Expect(l.Amend(ctx, p.ID, p.Footprint)).To(Succeed())
		})
		Context("static node counts", func() {
			var static *v1.NodePool
			// free is how many of the static NodePool's 3 slots nobody holds.
			free := func() int64 {
				g := cluster.NodePoolState.ReserveNodeCount(static.Name, 3, 3)
				cluster.NodePoolState.ReleaseNodeCount(static.Name, g)
				return g
			}
			want := func(n int64) *pipeline.Plan {
				p := budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted)
				p.Footprint.StaticNodeCount = map[string]int64{static.Name: n}
				return p
			}
			BeforeEach(func() {
				static = test.StaticNodePool(v1.NodePool{Spec: v1.NodePoolSpec{Replicas: lo.ToPtr(int64(0)), Limits: v1.Limits{resources.Node: resource.MustParse("3")}}})
				ExpectApplied(ctx, env.Client, static)
			})
			It("returns a partial grant", func() {
				Expect(l.Reserve(ctx, want(2))).To(Equal([]error{nil}))
				Expect(reasonsOf(l.Reserve(ctx, want(2)))).To(Equal([]types.RejectReason{types.ReasonStaticNodeLimit}))
				// The one slot granted to the rejected plan went back.
				Expect(free()).To(Equal(int64(1)))
			})
			It("swaps a same-size static re-plan at the limit (R4)", func() {
				a := want(3)
				Expect(l.Reserve(ctx, a)).To(Equal([]error{nil}))
				replan := want(3)
				Expect(l.Swap(ctx, []uuid.UUID{a.ID}, []*pipeline.Plan{replan})).To(Succeed())
				Expect(l.View().StaticHeld(static.Name)).To(Equal(int64(3)))
				Expect(free()).To(BeZero())
				// A smaller re-plan returns the rest; a failed swap changes nothing.
				smaller := want(1)
				Expect(l.Swap(ctx, []uuid.UUID{replan.ID}, []*pipeline.Plan{smaller})).To(Succeed())
				Expect(free()).To(Equal(int64(2)))
				Expect(pipeline.RejectReasonOf(l.Swap(ctx, []uuid.UUID{smaller.ID}, []*pipeline.Plan{want(4)}))).To(Equal(types.ReasonStaticNodeLimit))
				Expect(l.View().StaticHeld(static.Name)).To(Equal(int64(1)))
				Expect(free()).To(Equal(int64(2)))
			})
			It("amends a static plan by the difference", func() {
				a := want(1)
				Expect(l.Reserve(ctx, a)).To(Equal([]error{nil}))
				fp := a.Footprint
				fp.StaticNodeCount = map[string]int64{static.Name: 3}
				Expect(l.Amend(ctx, a.ID, fp)).To(Succeed())
				Expect(free()).To(BeZero())
				fp.StaticNodeCount = map[string]int64{static.Name: 2}
				Expect(l.Amend(ctx, a.ID, fp)).To(Succeed())
				Expect(free()).To(Equal(int64(1)))
			})
		})
		It("uses the smallest capacity any NodePool reports for a reservation", func() {
			other := test.NodePool()
			ExpectApplied(ctx, env.Client, other)
			offering := func(capacity int) *cloudprovider.InstanceType {
				return fake.NewInstanceType("reserved-type", fake.WithOfferings(cloudprovider.Offering{Available: true, ReservationCapacity: capacity, Price: 1,
					Requirements: scheduling.NewLabelRequirements(map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeReserved,
						corev1.LabelTopologyZone: "test-zone-1", cloudprovider.ReservationIDLabel: "r-1"})}))
			}
			cloudProvider.InstanceTypesForNodePool[w.nodePool.Name] = []*cloudprovider.InstanceType{offering(3)}
			cloudProvider.InstanceTypesForNodePool[other.Name] = []*cloudprovider.InstanceType{offering(1)}
			into := func(id string) *pipeline.Plan {
				p := budgetPlan(w.nodePool.Name, v1.DisruptionReasonDrifted, id)
				p.Footprint.Reservations = map[string]int{"r-1": 1}
				return p
			}
			Expect(reasonsOf(l.Reserve(ctx, into(ids[0]), into(ids[1])))).To(Equal([]types.RejectReason{"", types.ReasonCapacityReservation}))
		})
		It("reports every reason's allowed disruptions and the disrupting nodes by reason", func() {
			setBudget("4")
			Expect(l.Reserve(ctx, budgetPlan(w.nodePool.Name, v1.DisruptionReasonEmpty, ids[0]))).To(Equal([]error{nil}))
			u := lo.Must(lo.Find(l.Usage(ctx, v1.DisruptionReasonDrifted), func(u pipeline.PoolUsage) bool { return u.NodePool == w.nodePool.Name }))
			Expect(u.Allowed).To(HaveLen(4))
			Expect(u.ByReason).To(HaveKeyWithValue(v1.DisruptionReasonEmpty, 1))
			Expect(u.Used).To(Equal(1))
			Expect(u.Reserved).To(Equal(1))
		})
	})
})

func existingNodeWithPod(providerID string) *pscheduling.ExistingNode {
	n := &pscheduling.ExistingNode{StateNode: lo.Must(lo.Find(cluster.DeepCopyNodes(), func(s *state.StateNode) bool { return s.ProviderID() == providerID }))}
	n.Pods = []*corev1.Pod{test.Pod()}
	return n
}
