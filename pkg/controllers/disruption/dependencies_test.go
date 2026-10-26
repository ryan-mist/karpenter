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
	"errors"
	"fmt"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// depNode is a node of a dependency scenario. An uninitialized node is a launched NodeClaim without a Node (an
// in-flight replacement whose instance hasn't registered); a deleting node is marked for deletion in cluster state (a
// candidate of a started command).
type depNode struct {
	name, instanceType string
	drifted, deleting  bool
	uninitialized      bool
	podCPU             []string
}

// depCluster is a built dependency scenario: providerIDs and pods by name.
type depCluster struct {
	providerID map[string]string
	pods       map[string]*corev1.Pod
}

// buildDeps builds nodes in one zone of one NodePool (budget 100%, no consolidation). Pod "<node>-<i>" runs on node
// <node> with podCPU[i] and a ReplicaSet owner, so it is reschedulable.
func (e *coreEnv) buildDeps(nodes ...depNode) *depCluster {
	GinkgoHelper()
	zone := genZones(1)[0]
	np := test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
		ConsolidateAfter: v1.MustParseNillableDuration("Never"), Budgets: []v1.Budget{{Nodes: "100%"}},
	}}})
	np.Spec.Template.Spec.Requirements = append(np.Spec.Template.Spec.Requirements, v1.NodeSelectorRequirementWithMinValues{
		Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{zone},
	})
	ExpectApplied(ctx, env.Client, np)
	ExpectReconcileSucceeded(e.ctx, e.npCtrl, client.ObjectKeyFromObject(np))
	rs := test.ReplicaSet()
	ExpectApplied(ctx, env.Client, rs)
	owner := []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true)}}
	dc := &depCluster{providerID: map[string]string{}, pods: map[string]*corev1.Pod{}}
	var nodeClaims, linked []*v1.NodeClaim
	var ns []*corev1.Node
	var pods []*corev1.Pod
	for _, d := range nodes {
		it, ok := lo.Find(cloudProvider.InstanceTypes, func(i *cloudprovider.InstanceType) bool { return i.Name == d.instanceType })
		Expect(ok).To(BeTrue(), d.instanceType)
		nc, n := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey: np.Name, corev1.LabelInstanceTypeStable: it.Name, v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand,
				corev1.LabelTopologyZone: zone, corev1.LabelArchStable: "amd64", corev1.LabelOSStable: "linux",
			}},
			Status: v1.NodeClaimStatus{ProviderID: test.RandomProviderID(), Allocatable: it.Allocatable(), Capacity: it.Capacity},
		})
		dc.providerID[d.name] = nc.Status.ProviderID
		nc.StatusConditions().SetTrue(v1.ConditionTypeLaunched)
		if d.uninitialized {
			nc.Status.NodeName = ""
			coreApply(ctx, env.Client, nc)
			nodeClaims = append(nodeClaims, nc)
			continue
		}
		for _, c := range []string{v1.ConditionTypeRegistered, v1.ConditionTypeInitialized, v1.ConditionTypeConsolidatable} {
			nc.StatusConditions().SetTrue(c)
		}
		if d.drifted {
			nc.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
		}
		n.Spec.Taints = nil
		n.Labels[v1.NodeRegisteredLabelKey] = "true"
		n.Labels[v1.NodeInitializedLabelKey] = "true"
		n.Labels[corev1.LabelHostname] = n.Name
		n.Status.Phase = corev1.NodeRunning
		n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			LastHeartbeatTime: metav1.NewTime(env.Clock.Now()), LastTransitionTime: metav1.NewTime(env.Clock.Now())}}
		nc.Status.NodeName = n.Name
		coreApply(ctx, env.Client, nc)
		coreApply(ctx, env.Client, n)
		nodeClaims, linked, ns = append(nodeClaims, nc), append(linked, nc), append(ns, n)
		for i, cpu := range d.podCPU {
			p := test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-%d-%s", d.name, i, uuid.NewString()[:5]), OwnerReferences: owner},
				NodeName:   n.Name, Phase: corev1.PodRunning,
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}},
			})
			ExpectApplied(ctx, env.Client, p)
			pods = append(pods, p)
			dc.pods[fmt.Sprintf("%s-%d", d.name, i)] = p
		}
	}
	// Cluster state must know the NodeClaims without a Node before hydrate checks that it is synced.
	for _, nc := range lo.Without(nodeClaims, linked...) {
		Eventually(func() error { return e.client.Get(ctx, client.ObjectKeyFromObject(nc), &v1.NodeClaim{}) }).Should(Succeed())
		ExpectReconcileSucceeded(e.ctx, e.ncCtrl, client.ObjectKeyFromObject(nc))
	}
	e.hydrate(len(pods), len(ns), 0, linked, ns, pods)
	for _, d := range nodes {
		if d.deleting {
			e.cluster.MarkForDeletion(dc.providerID[d.name])
		}
	}
	return dc
}

// batchedDrift plans drift with every candidate in one joint simulation, as the pipeline does.
func (e *coreEnv) batchedDrift() []disruption.Command {
	GinkgoHelper()
	drift := e.drift()
	cmds, _ := e.compute(drift, func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
		o := drift.LegacyOptions()
		o.Batch, o.Pending = len(cands), disruption.DropPendingOnly
		disruption.SetBatchMinSize(&o, 1)
		r, err := drift.Compute(e.ctx, budgets, o, cands...)
		return r.Commands, err
	})
	return cmds
}

func podNames(pods []*corev1.Pod) []string {
	return lo.Map(pods, func(p *corev1.Pod, _ int) string { return p.Name })
}

var _ = Describe("Uninitialized dependencies", func() {
	var e *coreEnv
	BeforeEach(func() { e = newCoreEnv(ctx, genInstanceTypes(genZones(1))) })
	AfterEach(func() { e.stop() })

	// d is being replaced by x, an 8 CPU replacement that hasn't registered; d's 2 CPU pod will go there. Drifted c1 and
	// c2 each run a 1 CPU pod on a full 2 CPU node, so their pods fit nowhere but on x.
	scenario := func() *depCluster {
		return e.buildDeps(
			depNode{name: "d", instanceType: "gen-4cpu", drifted: true, deleting: true, podCPU: []string{"2"}},
			depNode{name: "x", instanceType: "gen-8cpu", uninitialized: true},
			depNode{name: "c1", instanceType: "gen-2cpu", drifted: true, podCPU: []string{"1"}},
			depNode{name: "c2", instanceType: "gen-2cpu", drifted: true, podCPU: []string{"1"}},
		)
	}

	It("packs a batch onto an uninitialized node: no extra NodeClaim, the node is a dependency", func() {
		dc := scenario()
		cmds := e.batchedDrift()
		Expect(cmds).To(HaveLen(1))
		cmd := cmds[0]
		// c1 and c2 share x, so they are one command.
		Expect(lo.Map(cmd.Candidates, func(c *disruption.Candidate, _ int) string { return c.ProviderID() })).To(ConsistOf(dc.providerID["c1"], dc.providerID["c2"]))
		Expect(cmd.Replacements).To(BeEmpty())
		Expect(cmd.Results.NewNodeClaims).To(BeEmpty())
		Expect(cmd.Dependencies).To(Equal([]string{dc.providerID["x"]}))
		x, ok := lo.Find(cmd.Results.ExistingNodes, func(en *pscheduling.ExistingNode) bool { return en.ProviderID() == dc.providerID["x"] })
		Expect(ok).To(BeTrue())
		Expect(podNames(x.Pods)).To(ConsistOf(dc.pods["d-0"].Name, dc.pods["c1-0"].Name, dc.pods["c2-0"].Name))
	})
	It("keeps today's behaviour without the option: pods on an uninitialized node fail, the legacy path plans nothing", func() {
		dc := scenario()
		cands, _, err := disruption.GetCandidatesWithTotals(e.ctx, e.cluster, e.client, e.recorder, env.Clock, cloudProvider, e.drift().ShouldDisrupt,
			e.drift().Class(), e.queue, e.clusterCost)
		Expect(err).ToNot(HaveOccurred())
		Expect(cands).To(HaveLen(2))
		results, err := disruption.SimulateScheduling(e.ctx, e.client, e.cluster, e.prov, env.Clock, e.recorder, nil, disruption.SimulationOptions{}, cands...)
		Expect(err).ToNot(HaveOccurred())
		uninitialized := lo.Filter(lo.Values(results.PodErrors), func(err error, _ int) bool {
			return errors.As(err, lo.ToPtr((*disruption.UninitializedNodeError)(nil)))
		})
		Expect(uninitialized).To(HaveLen(2))
		Expect(disruption.UninitializedDependencies(cands, results)).To(Equal([]string{dc.providerID["x"]}))

		tracked, err := disruption.SimulateScheduling(e.ctx, e.client, e.cluster, e.prov, env.Clock, e.recorder, nil,
			disruption.SimulationOptions{TrackUninitialized: true}, cands...)
		Expect(err).ToNot(HaveOccurred())
		Expect(tracked.AllNonPendingPodsScheduled()).To(BeTrue())

		cmds, _ := e.compute(e.drift(), func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
			return e.drift().ComputeCommands(e.ctx, budgets, cands...)
		})
		Expect(cmds).To(BeEmpty())
	})
	It("leaves an uninitialized replacement to the pods of the node it replaces", func() {
		// x (4 CPU) is d's replacement and holds d's 3 CPU pod, nothing else. c1's 3.5 CPU pod is bigger, so without
		// ordering it would be scheduled first, take x, and push d's pod onto a new NodeClaim: capacity provisioned
		// twice for d's pod.
		dc := e.buildDeps(
			depNode{name: "d", instanceType: "gen-4cpu", drifted: true, deleting: true, podCPU: []string{"3"}},
			depNode{name: "x", instanceType: "gen-4cpu", uninitialized: true},
			depNode{name: "c1", instanceType: "gen-4cpu", drifted: true, podCPU: []string{"3500m"}},
			depNode{name: "c2", instanceType: "gen-2cpu", podCPU: []string{"1900m"}},
		)
		cmds := e.batchedDrift()
		Expect(cmds).To(HaveLen(1))
		cmd := cmds[0]
		Expect(cmd.Dependencies).To(BeEmpty())
		Expect(cmd.Results.NewNodeClaims).To(HaveLen(1))
		Expect(podNames(cmd.Results.NewNodeClaims[0].Pods)).To(ConsistOf(dc.pods["c1-0"].Name))
		x, ok := lo.Find(cmd.Results.ExistingNodes, func(en *pscheduling.ExistingNode) bool { return en.ProviderID() == dc.providerID["x"] })
		Expect(ok).To(BeTrue())
		Expect(podNames(x.Pods)).To(ConsistOf(dc.pods["d-0"].Name))
	})
})

var _ = Describe("Split with uninitialized nodes", func() {
	uninitialized := func(pods ...*corev1.Pod) *pscheduling.ExistingNode {
		nc := test.NodeClaim(v1.NodeClaim{Status: v1.NodeClaimStatus{ProviderID: test.RandomProviderID()}})
		return &pscheduling.ExistingNode{StateNode: &state.StateNode{NodeClaim: nc}, Pods: pods}
	}
	initialized := func(pods ...*corev1.Pod) *pscheduling.ExistingNode {
		en := uninitialized(pods...)
		en.Node = test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodeInitializedLabelKey: "true"}},
			ProviderID: en.NodeClaim.Status.ProviderID})
		return en
	}
	It("connects candidates that share an uninitialized node and records it as their dependency", func() {
		cs := bareCandidates(4)
		x, y := uninitialized(podOf(cs[0]), podOf(cs[2])), initialized(podOf(cs[1]), podOf(cs[3]))
		pieces := disruption.Split(cs, pscheduling.Results{ExistingNodes: []*pscheduling.ExistingNode{x, y}}, disruption.KeepOnFirst)
		Expect(lo.Map(pieces, func(p disruption.Piece, _ int) []string { return ids(p.Candidates) })).To(Equal([][]string{{"c00", "c02"}, {"c01"}, {"c03"}}))
		Expect(pieces[0].Dependencies).To(Equal([]string{x.ProviderID()}))
		Expect(pieces[1].Dependencies).To(BeEmpty())
		Expect(pieces[2].Dependencies).To(BeEmpty())
	})
	It("doesn't record an uninitialized node that holds none of the piece's pods", func() {
		cs := bareCandidates(1)
		x := uninitialized(test.Pod())
		pieces := disruption.Split(cs, pscheduling.Results{ExistingNodes: []*pscheduling.ExistingNode{x}}, disruption.KeepOnFirst)
		Expect(pieces).To(HaveLen(1))
		Expect(pieces[0].Results.ExistingNodes).To(ConsistOf(x))
		Expect(pieces[0].Dependencies).To(BeEmpty())
	})
})
