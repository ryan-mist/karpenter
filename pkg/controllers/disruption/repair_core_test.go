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

// Tests of the repair core (Repair.Compute).
//
//  1. Differential: on generated clusters with unhealthy nodes, Repair.ComputeCommands (the core with the
//     controller's options) and a verbatim copy of ComputeCommands before the core existed
//     (disruption.LegacyRepairComputeCommands) produce the same commands (including each candidate's drain bound),
//     publish the same events, and leave the same writes behind once the reference command starts. Unhealthy
//     conditions match a replace policy with a zero drain bound, a replace policy without one, and a reboot policy, some
//     still within their toleration; some nodes carry do-not-repair, and enough nodes can be unhealthy to trip a
//     NodePool's breaker. Results that differ are resampled as in drift_core_test.go.
//  2. Batched repair: the joint path replaces several unhealthy nodes at once, leaves reboots to the single path, and
//     stamps every candidate's drain bound; reboots can be planned from the cached NodeClaim.

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var genRepairPolicies = []cloudprovider.RepairPolicy{
	{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute,
		TerminationGracePeriod: lo.ToPtr(time.Duration(0)), Action: cloudprovider.ReplaceNode},
	{ConditionType: "SlowNode", ConditionStatus: corev1.ConditionTrue, ReasonRegex: ".+", TolerationDuration: 10 * time.Minute, Action: cloudprovider.ReplaceNode},
	{ConditionType: "StuckKernel", ConditionStatus: corev1.ConditionTrue, ReasonRegex: ".+", TolerationDuration: 5 * time.Minute,
		TerminationGracePeriod: lo.ToPtr(2 * time.Minute), Action: cloudprovider.RebootNode},
}

type repairSpec struct {
	genSpec
	UnhealthyFrac  float64 `json:"unhealthyFrac"`
	TerminateFirst bool    `json:"terminateFirst"`
}

// makeUnhealthy gives a seeded fraction of the nodes a condition that matches a repair policy, and do-not-repair to a
// few of them. Odd seeds wait out every toleration, even seeds all but the longest.
func (e *coreEnv) makeUnhealthy(spec repairSpec, policies []cloudprovider.RepairPolicy) {
	GinkgoHelper()
	r := rand.New(rand.NewSource(spec.Seed*31 + 7))
	nodes := &corev1.NodeList{}
	Expect(env.Client.List(ctx, nodes, client.HasLabels{v1.NodePoolLabelKey})).To(Succeed())
	sort.Slice(nodes.Items, func(i, j int) bool { return nodes.Items[i].Name < nodes.Items[j].Name })
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if r.Float64() >= spec.UnhealthyFrac {
			continue
		}
		p := policies[r.Intn(len(policies))]
		stored := n.DeepCopy()
		n.Status.Conditions = append(n.Status.Conditions, corev1.NodeCondition{Type: p.ConditionType, Status: p.ConditionStatus, Reason: "Generated",
			LastHeartbeatTime: metav1.NewTime(env.Clock.Now()), LastTransitionTime: metav1.NewTime(env.Clock.Now())})
		Expect(env.Client.Status().Patch(ctx, n, client.MergeFrom(stored))).To(Succeed())
		if r.Intn(10) == 0 {
			stored = n.DeepCopy()
			n.Annotations = lo.Assign(n.Annotations, map[string]string{v1.DoNotRepairAnnotationKey: "true"})
			Expect(env.Client.Patch(ctx, n, client.MergeFrom(stored))).To(Succeed())
		}
		Eventually(func(g Gomega) {
			got := &corev1.Node{}
			g.Expect(e.client.Get(ctx, client.ObjectKeyFromObject(n), got)).To(Succeed())
			g.Expect(got.Status.Conditions).To(HaveLen(len(n.Status.Conditions)))
			g.Expect(got.Annotations).To(Equal(n.Annotations))
		}).WithTimeout(time.Minute).Should(Succeed())
		ExpectReconcileSucceeded(e.ctx, e.nodeCtrl, client.ObjectKeyFromObject(n))
	}
	// Tolerations count from no earlier than the Node's creation, so move the clock past them: 20 minutes leaves the
	// 30 minute policy within its toleration.
	env.Clock.Step(time.Duration(spec.Seed%2*100+20) * time.Minute)
}

func repairSignature(gc *genCluster, cmd disruption.Command) string {
	tgps := lo.Map(cmd.Candidates, func(c *disruption.Candidate, _ int) string {
		return fmt.Sprintf("%s:%v", gc.nodeIdx[c.ProviderID()], lo.FromPtrOr(c.TerminationGracePeriod, -1))
	})
	sort.Strings(tgps)
	return fmt.Sprintf("%s reboot=%v tgp=%v", gc.cmdSignature(cmd), cmd.Reboot, tgps)
}

var _ = Describe("Repair Core", func() {
	var e *coreEnv
	AfterEach(func() {
		if e != nil {
			e.stop()
			e = nil
		}
	})
	fresh := func(spec repairSpec, policies ...cloudprovider.RepairPolicy) *genCluster {
		e = newCoreEnv(ctx, genInstanceTypes(genZones(spec.Zones)), lo.Ternary(len(policies) > 0, policies, genRepairPolicies)...)
		gc := e.buildGen(spec.genSpec)
		e.makeUnhealthy(spec, lo.Ternary(len(policies) > 0, policies, genRepairPolicies))
		return gc
	}

	Context("differential against ComputeCommands before the core", func() {
		budgets := func(_ int, r *rand.Rand) string { return []string{"1", "10%", "50%", "100%"}[r.Intn(4)] }
		nodes := coreDiffNodes([]int{10, 25, 50})
		for _, seed := range coreSeeds(4001, 10) {
			spec := repairSpec{genSpec: randomGenSpec(seed, nodes, budgets)}
			spec.DriftFrac = 0
			spec.UnhealthyFrac = []float64{0.1, 0.2, 0.4}[seed%3]
			spec.TerminateFirst = seed%2 == 0
			It(fmt.Sprintf("seed=%d %+v", seed, spec), func() {
				gc := fresh(spec)
				rp := coreMethod[*disruption.Repair](e)
				gctx := options.ToContext(e.ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{
					NodeRepair: lo.ToPtr(true), TerminateFirstRepair: lo.ToPtr(spec.TerminateFirst)}}))
				type result struct {
					cmds   []string
					events []string
				}
				key := func(r result) string { return fmt.Sprint(r.cmds, r.events) }
				run := func(f func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error)) ([]disruption.Command, result) {
					var res result
					cmds, _ := e.compute(rp, func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
						e.recorder.Reset()
						defer func() { res.events = eventStrings(e.recorder.Events()); sort.Strings(res.events) }()
						return f(budgets, cands)
					})
					res.cmds = lo.Map(cmds, func(c disruption.Command, _ int) string { return repairSignature(gc, c) })
					return cmds, res
				}
				legacy := func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
					return disruption.LegacyRepairComputeCommands(gctx, rp, budgets, cands...)
				}
				current := func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
					return rp.ComputeCommands(gctx, budgets, cands...)
				}
				// The batched path forced onto single candidates: it plans reboots ranked ahead of a batch first and ends a
				// batch at one ranked inside it (batchWindow), so its commands must be upstream's too. It doesn't publish the
				// events of candidates it drops from a batch, so only commands are compared.
				forced := func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
					o := rp.LegacyOptions()
					disruption.SetBatchMinSize(&o, 1)
					res, err := rp.Compute(gctx, budgets, o, cands...)
					return res.Commands, err
				}
				for step := range 5 {
					ref, want := run(legacy)
					_, got := run(current)
					_, gotForced := run(forced)
					upstream := map[string]bool{key(want): true}
					upstreamCmds := map[string]bool{fmt.Sprint(want.cmds): true}
					for i := 0; i < 16 && (!upstream[key(got)] || !upstreamCmds[fmt.Sprint(gotForced.cmds)]); i++ {
						_, again := run(legacy)
						upstream[key(again)] = true
						upstreamCmds[fmt.Sprint(again.cmds)] = true
					}
					emitCoreDiff(map[string]any{"test": "repair", "spec": spec, "step": step, "upstreamOutcomes": len(upstream), "equal": upstream[key(got)],
						"forcedEqual": upstreamCmds[fmt.Sprint(gotForced.cmds)],
						"refDecision": lo.Map(ref, func(c disruption.Command, _ int) string { return string(c.Decision()) })})
					Expect(upstream).To(HaveKey(key(got)), "repair core differs from every upstream result: %v", got.cmds)
					Expect(upstreamCmds).To(HaveKey(fmt.Sprint(gotForced.cmds)), "the batched path on single candidates differs from every upstream result: %v", gotForced.cmds)
					if len(upstream) > 1 || len(ref) == 0 {
						break
					}
					started, failed := e.startCommands(rp, ref)
					Expect(failed).To(BeZero())
					e.genLaunch(gc, started)
				}
			})
		}
	})

	It("replaces several unhealthy nodes from one joint simulation and stamps their drain bounds", func() {
		// About 10% unhealthy: below the 20% breaker.
		spec := repairSpec{genSpec: genSpec{Seed: 11, Nodes: 40, MaxPodsPerNode: 4, Pools: 1, Zones: 1, Budget: "100%", Util: 0.5}, UnhealthyFrac: 0.1}
		gc := fresh(spec, genRepairPolicies[:2]...)
		rp := coreMethod[*disruption.Repair](e)
		var res disruption.CoreResult
		cmds, numCands := e.compute(rp, func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
			o := rp.LegacyOptions()
			o.Batch = 100
			var err error
			res, err = rp.Compute(e.ctx, budgets, o, cands...)
			return res.Commands, err
		})
		Expect(numCands).To(BeNumerically(">=", 2))
		Expect(res.Batch).ToNot(BeNil())
		Expect(len(res.Batch.Accepted)).To(BeNumerically(">=", 2))
		Expect(lo.SumBy(cmds, func(c disruption.Command) int { return len(c.Candidates) })).To(Equal(len(res.Batch.Accepted)))
		for _, c := range lo.FlatMap(cmds, func(c disruption.Command, _ int) []*disruption.Candidate { return c.Candidates }) {
			// The NodeClaims set no drain bound of their own, so the policy's bound applies.
			Expect(c.TerminationGracePeriod).To(Equal(c.RepairPolicyResult.TerminationGracePeriod), gc.nodeIdx[c.ProviderID()])
		}
	})

	It("packs a batch onto an uninitialized node and records it as a dependency", func() {
		e = newCoreEnv(ctx, genInstanceTypes(genZones(1)), genRepairPolicies[:1]...)
		// As in "Uninitialized dependencies": d is being replaced by x, which hasn't registered, and unhealthy c1 and c2
		// fit nowhere but on x. Ten full healthy nodes keep the NodePool below the 20% unhealthy breaker.
		nodes := []depNode{
			{name: "d", instanceType: "gen-4cpu", deleting: true, podCPU: []string{"2"}},
			{name: "x", instanceType: "gen-8cpu", uninitialized: true},
			{name: "c1", instanceType: "gen-2cpu", podCPU: []string{"1"}},
			{name: "c2", instanceType: "gen-2cpu", podCPU: []string{"1"}},
		}
		for i := range 10 {
			nodes = append(nodes, depNode{name: fmt.Sprintf("h%d", i), instanceType: "gen-2cpu", podCPU: []string{"1900m"}})
		}
		dc := e.buildDeps(nodes...)
		for _, name := range []string{"c1", "c2"} {
			nl := &corev1.NodeList{}
			Expect(env.Client.List(ctx, nl)).To(Succeed())
			n, ok := lo.Find(nl.Items, func(n corev1.Node) bool { return n.Spec.ProviderID == dc.providerID[name] })
			Expect(ok).To(BeTrue())
			stored := n.DeepCopy()
			n.Status.Conditions = append(n.Status.Conditions, corev1.NodeCondition{Type: "BadNode", Status: corev1.ConditionFalse, Reason: "Generated",
				LastHeartbeatTime: metav1.NewTime(env.Clock.Now()), LastTransitionTime: metav1.NewTime(env.Clock.Now())})
			Expect(env.Client.Status().Patch(ctx, &n, client.MergeFrom(stored))).To(Succeed())
			Eventually(func(g Gomega) {
				got := &corev1.Node{}
				g.Expect(e.client.Get(ctx, client.ObjectKeyFromObject(&n), got)).To(Succeed())
				g.Expect(got.Status.Conditions).To(HaveLen(len(n.Status.Conditions)))
			}).WithTimeout(time.Minute).Should(Succeed())
			ExpectReconcileSucceeded(e.ctx, e.nodeCtrl, client.ObjectKeyFromObject(&n))
		}
		env.Clock.Step(40 * time.Minute)
		rp := coreMethod[*disruption.Repair](e)
		plan := func(batch int) []disruption.Command {
			cmds, _ := e.compute(rp, func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
				o := rp.LegacyOptions()
				o.Batch = batch
				res, err := rp.Compute(e.ctx, budgets, o, cands...)
				return res.Commands, err
			})
			return cmds
		}
		cmds := plan(10)
		Expect(cmds).To(HaveLen(1))
		Expect(lo.Map(cmds[0].Candidates, func(c *disruption.Candidate, _ int) string { return c.ProviderID() })).To(ConsistOf(dc.providerID["c1"], dc.providerID["c2"]))
		Expect(cmds[0].Results.NewNodeClaims).To(BeEmpty())
		Expect(cmds[0].Dependencies).To(Equal([]string{dc.providerID["x"]}))
		// The disruption controller's options keep today's behaviour: c1's pods would land on x, so c1 gets a
		// replacement of its own or nothing, and no command has dependencies.
		for _, c := range plan(1) {
			Expect(c.Dependencies).To(BeEmpty())
			for _, en := range c.Results.ExistingNodes {
				Expect(en.ProviderID()).ToNot(Equal(dc.providerID["x"]))
			}
		}
	})

	It("plans a reboot from the cached NodeClaim", func() {
		spec := repairSpec{genSpec: genSpec{Seed: 12, Nodes: 3, MaxPodsPerNode: 2, Pools: 1, Zones: 1, Budget: "100%", Util: 0.3}}
		e = newCoreEnv(ctx, genInstanceTypes(genZones(1)), genRepairPolicies...)
		e.buildGen(spec.genSpec)
		spec.UnhealthyFrac = 0.4
		e.makeUnhealthy(spec, genRepairPolicies[2:])
		rp := coreMethod[*disruption.Repair](e)
		plan := func() []disruption.Command {
			var cmds []disruption.Command
			e.compute(rp, func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
				o := rp.LegacyOptions()
				o.CachedReboot = true
				res, err := rp.Compute(e.ctx, budgets, o, cands...)
				cmds = res.Commands
				return res.Commands, err
			})
			return cmds
		}
		cmds := plan()
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Decision()).To(Equal(disruption.RebootDecision))
		// The handoff patches the cached NodeClaim when the command starts.
		started, failed := e.startCommands(rp, cmds)
		Expect(failed).To(BeZero())
		nc := &v1.NodeClaim{}
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(started[0].Candidates[0].NodeClaim), nc)).To(Succeed())
		Expect(nc.StatusConditions().Get(v1.ConditionTypeRebooting).IsTrue()).To(BeTrue())
		Expect(nc.Annotations).To(HaveKeyWithValue(v1.RebootTerminationGracePeriodAnnotationKey, (2 * time.Minute).String()))
		// A rebooting node isn't planned again.
		Expect(plan()).To(BeEmpty())
	})
})
