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
	"time"

	"github.com/awslabs/operatorpkg/singleton"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// reconcileThrough runs one synchronous pipeline Reconcile, stepping the fake clock through each validation wait.
func reconcileThrough(c *pipeline.Controller) time.Duration {
	GinkgoHelper()
	done := make(chan time.Duration, 1)
	go func() {
		defer GinkgoRecover()
		r, err := c.Reconcile(ctx)
		Expect(err).ToNot(HaveOccurred())
		done <- r.RequeueAfter
	}()
	for {
		select {
		case d := <-done:
			return d
		case <-time.After(50 * time.Millisecond):
			if env.Clock.HasWaiters() {
				env.Clock.Step(disruption.CommandValidationDelay)
			}
		}
	}
}

// only keeps the given condition on every NodeClaim of the world.
func (w *world) only(condition string) {
	GinkgoHelper()
	for i := range w.nodeClaims {
		nc := ExpectExists(ctx, env.Client, w.nodeClaims[i])
		for _, t := range []string{v1.ConditionTypeDrifted, v1.ConditionTypeConsolidatable} {
			if t != condition {
				Expect(nc.StatusConditions().Clear(t)).To(Succeed())
			}
		}
		ExpectApplied(ctx, env.Client, nc)
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nc))
		w.nodeClaims[i] = nc
	}
}

// withPods gives each listed node a reschedulable pod requesting cpu.
func (w *world) withPods(cpu string, nodes ...int) {
	GinkgoHelper()
	for _, i := range nodes {
		pod := test.Pod(test.PodOptions{
			ObjectMeta:           metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs", UID: "rs", Controller: lo.ToPtr(true)}}},
			NodeName:             w.nodes[i].Name,
			Phase:                corev1.PodRunning,
			ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}},
		})
		ExpectApplied(ctx, env.Client, pod)
		ExpectReconcileSucceeded(ctx, podStateController, client.ObjectKeyFromObject(pod))
	}
}

// capturingExecutor records the plans submitted through it.
type capturingExecutor struct {
	pipeline.Executor
	plans *[]*pipeline.Plan
}

func (e *capturingExecutor) Submit(ctx context.Context, plans ...*pipeline.Plan) <-chan pipeline.Resolution {
	*e.plans = append(*e.plans, plans...)
	return e.Executor.Submit(ctx, plans...)
}

// launchReplacements gives every NodeClaim without a providerID one, and tells cluster state, as a launch would, so
// cluster state is synced again.
func launchReplacements() {
	GinkgoHelper()
	list := &v1.NodeClaimList{}
	Expect(env.Client.List(ctx, list)).To(Succeed())
	for i := range list.Items {
		nc := &list.Items[i]
		if nc.Status.ProviderID == "" {
			nc.Status.ProviderID = test.RandomProviderID()
			Expect(env.Client.Status().Update(ctx, nc)).To(Succeed())
		}
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nc))
	}
	Expect(cluster.Synced(ctx)).To(BeTrue())
}

// expectSubmitted waits until a round of the lane submitted a plan (its unresolved_plans gauge counts it).
func expectSubmitted(lane types.LaneID) {
	GinkgoHelper()
	Eventually(func() float64 {
		m, ok := FindMetricWithLabelValues("karpenter_voluntary_disruption_pipeline_unresolved_plans", map[string]string{"lane": string(lane)})
		return lo.Ternary(ok, m.GetGauge().GetValue(), 0)
	}).Should(BeNumerically(">=", 1))
}

func queuedCandidates() []string {
	return lo.FlatMap(queue.GetCommands(), func(cmd *disruption.Command, _ int) []string {
		return lo.Map(cmd.Candidates, func(c *disruption.Candidate, _ int) string { return c.ProviderID() })
	})
}

var _ = Describe("Planners", func() {
	var c *pipeline.Controller
	newController := func() {
		c = pipeline.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost)
	}
	It("deletes every empty node in one plan, after the validation delay", func() {
		w := newWorld(4)
		w.only(v1.ConditionTypeConsolidatable)
		w.withPods("1", 3)
		newController()
		Expect(reconcileThrough(c)).To(Equal(singleton.RequeueImmediately))
		Expect(queue.GetCommands()).To(HaveLen(1))
		Expect(queuedCandidates()).To(ConsistOf(w.providerIDs()[:3]))
		Expect(queue.GetCommands()[0].Decision()).To(Equal(disruption.DeleteDecision))
	})
	It("counts emptiness' failed validations per candidate, as the disruption controller does", func() {
		disruption.FailedValidationsTotal.Reset()
		pipeline.UnresolvedPlans.Reset()
		w := newWorld(4)
		w.only(v1.ConditionTypeConsolidatable)
		w.withPods("1", 3)
		newController()
		done := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			_, err := c.Reconcile(ctx)
			Expect(err).ToNot(HaveOccurred())
			close(done)
		}()
		// The plan waits for its validation delay; meanwhile a pending pod is nominated to one of its nodes (the
		// nomination may last only 10 s, so step in 5 s increments).
		expectSubmitted(types.LaneFast)
		env.Clock.Step(10 * time.Second)
		cluster.NominateNodeForPod(ctx, w.providerIDs()[0])
		Eventually(func() bool {
			if env.Clock.HasWaiters() {
				env.Clock.Step(5 * time.Second)
			}
			select {
			case <-done:
				return true
			default:
				return false
			}
		}).Should(BeTrue())
		empty, ok := lo.Find(queue.GetCommands(), func(cmd *disruption.Command) bool { return cmd.Method.Reason() == v1.DisruptionReasonEmpty })
		Expect(ok).To(BeTrue())
		Expect(lo.Map(empty.Candidates, func(c *disruption.Candidate, _ int) string { return c.ProviderID() })).To(ConsistOf(w.providerIDs()[1:3]))
		ExpectMetricCounterValue(disruption.FailedValidationsTotal, 1, map[string]string{disruption.ConsolidationTypeLabel: "empty"})
	})
	It("replaces many drifted nodes in one round, in commands of at most ComponentCap candidates", func() {
		w := newWorld(12)
		w.only(v1.ConditionTypeDrifted)
		w.withPods("1", lo.Range(12)...)
		newController()
		Expect(reconcileThrough(c)).To(Equal(singleton.RequeueImmediately))
		// The disruption controller replaces one drifted node per pass. A chunk whose simulation runs out of time (a
		// loaded machine) is deferred to the next round, so allow a few.
		for i := 0; i < 3 && len(queuedCandidates()) < 12; i++ {
			launchReplacements()
			reconcileThrough(c)
		}
		Expect(queuedCandidates()).To(ConsistOf(w.providerIDs()))
		for _, cmd := range queue.GetCommands() {
			Expect(len(cmd.Candidates)).To(BeNumerically("<=", disruption.ComponentCap))
			Expect(cmd.Decision()).To(Equal(disruption.ReplaceDecision))
		}
		// Batched: fewer commands than nodes.
		Expect(len(queue.GetCommands())).To(BeNumerically("<", 12))
	})
	It("disrupts no more drifted nodes than the budget allows", func() {
		w := newWorld(6)
		w.nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "2"}}
		ExpectApplied(ctx, env.Client, w.nodePool)
		w.only(v1.ConditionTypeDrifted)
		w.withPods("1", lo.Range(6)...)
		newController()
		reconcileThrough(c)
		Expect(queuedCandidates()).To(HaveLen(2))
		// The next round finds the budget used up.
		reconcileThrough(c)
		Expect(queuedCandidates()).To(HaveLen(2))
	})
	It("deletes empty nodes while cluster state is unsynced, but doesn't replace drifted ones", func() {
		w := newWorld(3)
		w.withPods("1", 2)
		unlaunched := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: w.nodePool.Name}}})
		unlaunched.Status.ProviderID = ""
		cluster.UpdateNodeClaim(unlaunched)
		newController()
		Expect(reconcileThrough(c)).To(Equal(singleton.RequeueImmediately))
		Expect(queuedCandidates()).To(ConsistOf(w.providerIDs()[:2]))
	})
	It("replaces a drifted node of a static NodePool, taking its node-count slot at Reserve", func() {
		static := test.StaticNodePool(v1.NodePool{Spec: v1.NodePoolSpec{Replicas: lo.ToPtr(int64(2)), Limits: v1.Limits{resources.Node: resource.MustParse("3")},
			Disruption: v1.Disruption{Budgets: []v1.Budget{{Nodes: "100%"}}}}})
		w := newWorldIn(static, 2)
		w.only(v1.ConditionTypeDrifted)
		newController()
		reconcileThrough(c)
		// One slot above the two nodes: one replacement at a time.
		Expect(queuedCandidates()).To(HaveLen(1))
		Expect(queue.GetCommands()[0].Replacements).To(HaveLen(1))
		Expect(queue.GetCommands()[0].Replacements[0].IsStaticNodeClaim).To(BeTrue())
	})
	It("repairs an unhealthy node", func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodeRepair: lo.ToPtr(true)}}))
		// One of six nodes: below repair's 20% breaker.
		w := newWorld(6)
		w.only("")
		w.withPods("1", lo.Range(6)...)
		node := ExpectExists(ctx, env.Client, w.nodes[0])
		// The fake repair policy tolerates BadNode for 30 minutes, counted from no earlier than the node's creation.
		if env.Clock.Now().Before(node.CreationTimestamp.Time) {
			env.Clock.SetTime(node.CreationTimestamp.Time)
		}
		node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{Type: "BadNode", Status: corev1.ConditionFalse,
			LastTransitionTime: metav1.Time{Time: env.Clock.Now()}})
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))
		env.Clock.Step(31 * time.Minute)
		newController()
		reconcileThrough(c)
		Expect(queuedCandidates()).To(ConsistOf(w.providerIDs()[0]))
		Expect(queue.GetCommands()[0].Reason()).To(Equal(v1.DisruptionReasonUnhealthy))
	})
	It("repairs an unhealthy node that never initialized although no budget is left", func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodeRepair: lo.ToPtr(true)}}))
		w := newWorld(5)
		w.nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "0"}}
		ExpectApplied(ctx, env.Client, w.nodePool)
		it := cloudProvider.InstanceTypes[len(cloudProvider.InstanceTypes)-1]
		nc, node := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			v1.NodePoolLabelKey: w.nodePool.Name, corev1.LabelInstanceTypeStable: it.Name, v1.NodeRegisteredLabelKey: "true",
		}}})
		nc.StatusConditions().SetTrue(v1.ConditionTypeLaunched)
		nc.StatusConditions().SetTrue(v1.ConditionTypeRegistered)
		node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{Type: "BadNode", Status: corev1.ConditionFalse,
			LastTransitionTime: metav1.Time{Time: env.Clock.Now()}})
		ExpectApplied(ctx, env.Client, nc, node)
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nc))
		env.Clock.Step(31 * time.Minute)
		newController()
		reconcileThrough(c)
		Expect(queuedCandidates()).To(ConsistOf(nc.Status.ProviderID))
	})
	It("re-plans the surviving plans of a batch jointly when one is rejected at admission", func() {
		w := newWorld(12)
		w.only(v1.ConditionTypeDrifted)
		w.withPods("1", lo.Range(12)...)
		l := pipeline.NewLedger(env.Client, cluster, cloudProvider, env.Clock, queue)
		events := &pipeline.RoundEvents{}
		planners := pipeline.DefaultPlanners(ctx, env.Clock, cluster, env.Client, prov, cloudProvider, queue, events, l.View())
		exec := pipeline.NewExecutor(pipeline.ExecutorDeps{Ledger: l, Admitter: pipeline.NewAdmitter(l, pipeline.NewCandidateRebuilder(env.Client, cluster, env.Clock, cloudProvider, queue), planners["drift"]),
			Starter: pipeline.NewQueueStarter(queue, env.Clock), Sweeper: disruption.NewMarkSweeper(env.Client, cluster, env.Clock), Queue: queue, Cluster: cluster, Clock: env.Clock})
		var submitted []*pipeline.Plan
		capture := &capturingExecutor{Executor: exec, plans: &submitted}
		lane := pipeline.NewLane(pipeline.LaneSpec{ID: "fast", Planners: []pipeline.Planner{planners["drift"]}}, pipeline.LaneDeps{
			Clock: env.Clock, Cluster: cluster, Executor: capture, Budgets: l.Budgets(), Reserved: l.View(),
			Snapshots: pipeline.NewSnapshotBuilder(env.Client, cluster, env.Clock, cloudProvider, queue, clusterCost, l.View()),
		})
		Expect(lane.Step(ctx).Err).ToNot(HaveOccurred())
		Expect(len(submitted)).To(BeNumerically(">=", 2))
		Expect(lo.Uniq(lo.Map(submitted, func(p *pipeline.Plan, _ int) uuid.UUID { return p.BatchID }))).To(HaveLen(1))
		// A pending pod is nominated to a node of the last plan before admission, so that plan is rejected and the other
		// plans of the batch are planned again together (their topology may have relied on it), then started.
		last := submitted[len(submitted)-1]
		cluster.NominateNodeForPod(ctx, last.Candidates[0].ProviderID())
		r := exec.Step(ctx)
		Expect(r.Rejected).To(Equal(1))
		Expect(r.Admitted).To(BeNumerically(">=", 1))
		survivors := lo.FlatMap(submitted[:len(submitted)-1], func(p *pipeline.Plan, _ int) []string { return providerIDsOf(p.Candidates) })
		Expect(queuedCandidates()).To(ConsistOf(survivors))
	})
	It("counts consolidation's failed validation when admission rejects a candidate", func() {
		disruption.FailedValidationsTotal.Reset()
		pipeline.UnresolvedPlans.Reset()
		w := newWorld(3)
		w.only(v1.ConditionTypeConsolidatable)
		w.withPods("100m", 0, 1, 2)
		newController()
		done := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			_, err := c.Reconcile(ctx)
			Expect(err).ToNot(HaveOccurred())
			close(done)
		}()
		expectSubmitted(types.LaneBackground)
		env.Clock.Step(10 * time.Second)
		for _, id := range w.providerIDs() {
			cluster.NominateNodeForPod(ctx, id)
		}
		Eventually(func() bool {
			if env.Clock.HasWaiters() {
				env.Clock.Step(5 * time.Second)
			}
			select {
			case <-done:
				return true
			default:
				return false
			}
		}).Should(BeTrue())
		Expect(queue.GetCommands()).To(BeEmpty())
		failed := 0.0
		for _, ct := range []string{"multi", "single"} {
			if m, ok := FindMetricWithLabelValues("karpenter_voluntary_disruption_failed_validations_total", map[string]string{disruption.ConsolidationTypeLabel: ct}); ok {
				failed += m.GetCounter().GetValue()
			}
		}
		Expect(failed).To(BeNumerically(">=", 1))
	})
	It("consolidates underutilized nodes after the validation delay", func() {
		w := newWorld(3)
		w.only(v1.ConditionTypeConsolidatable)
		w.withPods("100m", 0, 1, 2)
		newController()
		reconcileThrough(c)
		Expect(queue.GetCommands()).ToNot(BeEmpty())
		Expect(lo.Map(queue.GetCommands(), func(cmd *disruption.Command, _ int) string { return cmd.ConsolidationType() })).To(ContainElement(BeElementOf("multi", "single")))
	})
})
