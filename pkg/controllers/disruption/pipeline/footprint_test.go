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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

var _ = Describe("Footprint", func() {
	var w *world
	var cands map[string]*disruption.Candidate
	BeforeEach(func() {
		w = newWorld(4, 0, 1)
		s, err := pipeline.NewSnapshotBuilder(env.Client, cluster, env.Clock, cloudProvider, queue, clusterCost, nil).Build(ctx, pipeline.Scope{})
		Expect(err).ToNot(HaveOccurred())
		cands = lo.SliceToMap(s.CandidatesFor(ctx, newStubPlanner("drift")), func(c *disruption.Candidate) (string, *disruption.Candidate) { return c.ProviderID(), c })
	})
	It("counts only the candidates' own pods, and the pending pods that share their new NodeClaims", func() {
		ids := w.providerIDs()
		a, b := cands[ids[0]], cands[ids[1]]
		ownA, ownB := a.ReschedulablePods()[0], b.ReschedulablePods()[0]
		pending := test.Pod()
		deleting := test.Pod(test.PodOptions{NodeName: "elsewhere"})
		small := fake.NewInstanceType("small", fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}))
		big := fake.NewInstanceType("big", fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")}))
		dest := existingNodeWithPod(ids[2])
		dest.Pods = []*corev1.Pod{ownA}
		other := existingNodeWithPod(ids[3]) // hosts only a deleting node's pod
		other.Pods = []*corev1.Pod{deleting}
		mixed := &pscheduling.NodeClaim{NodeClaimTemplate: pscheduling.NodeClaimTemplate{NodePoolName: w.nodePool.Name, InstanceTypeOptions: cloudprovider.InstanceTypes{small, big}},
			Pods: []*corev1.Pod{ownB, pending}}
		cmd := disruption.Command{
			Candidates:   []*disruption.Candidate{a, b},
			Replacements: []*disruption.Replacement{{NodeClaim: mixed}},
			Results: pscheduling.Results{
				ExistingNodes: []*pscheduling.ExistingNode{dest, other},
				// A NodeClaim for pending pods only, which the core dropped from the replacements, isn't the plan's.
				NewNodeClaims: []*pscheduling.NodeClaim{mixed, {NodeClaimTemplate: pscheduling.NodeClaimTemplate{NodePoolName: w.nodePool.Name,
					InstanceTypeOptions: cloudprovider.InstanceTypes{big}}, Pods: []*corev1.Pod{test.Pod()}}},
			},
		}
		fp := pipeline.FootprintOf(cmd, 7)
		Expect(fp.Removed.UnsortedList()).To(ConsistOf(ids[0], ids[1]))
		Expect(fp.BudgetNodes).To(Equal(map[string]sets.Set[string]{w.nodePool.Name: sets.New(ids[0], ids[1])}))
		Expect(fp.Destinations.UnsortedList()).To(ConsistOf(ids[2]))
		Expect(fp.Dependencies).To(BeEmpty())
		// Dependencies (decision #16) are destinations too.
		cmd.Dependencies = []string{ids[2], ids[3]}
		withDeps := pipeline.FootprintOf(cmd, 7)
		Expect(withDeps.Dependencies.UnsortedList()).To(ConsistOf(ids[2], ids[3]))
		Expect(withDeps.Destinations.UnsortedList()).To(ConsistOf(ids[2], ids[3]))
		cost := fp.LimitCost[w.nodePool.Name]
		Expect(lo.ToPtr(cost[corev1.ResourceCPU]).String()).To(Equal("8"))
		Expect(lo.ToPtr(cost[resources.Node]).String()).To(Equal("1"))
		Expect(fp.PendingPods.UnsortedList()).To(ConsistOf(pending.UID))
		Expect(fp.StaticNodeCount).To(BeEmpty())
		// A static replacement holds no pods but still takes a node-count slot and limits.
		static := pipeline.FootprintOf(disruption.Command{Candidates: []*disruption.Candidate{a}, Replacements: []*disruption.Replacement{{NodeClaim: &pscheduling.NodeClaim{
			NodeClaimTemplate: pscheduling.NodeClaimTemplate{NodePoolName: "static", IsStaticNodeClaim: true, InstanceTypeOptions: cloudprovider.InstanceTypes{small}}}}}}, 0)
		Expect(static.StaticNodeCount).To(Equal(map[string]int64{"static": 1}))
		Expect(lo.ToPtr(static.LimitCost["static"][resources.Node]).String()).To(Equal("1"))
		Expect(fp.ComputedAt).To(Equal(uint64(7)))
		Expect(fp.TopologyGroups.UnsortedList()).To(ConsistOf(lo.Uniq(append(pipeline.CouplingKeys(ownA), pipeline.CouplingKeys(ownB)...))))
	})
	It("plans the command as a delete, a replace or a reboot, with its pod-set hashes and catalog versions", func() {
		s, err := pipeline.NewSnapshotBuilder(env.Client, cluster, env.Clock, cloudProvider, queue, clusterCost, nil).Build(ctx, pipeline.Scope{})
		Expect(err).ToNot(HaveOccurred())
		c := cands[w.providerIDs()[1]]
		p := pipeline.PlanFrom(disruption.Command{Candidates: []*disruption.Candidate{c}}, 3, env.Clock.Now(), s.CatalogVersion)
		Expect(p.Action).To(Equal(types.ActionDelete))
		Expect(p.BatchID).To(Equal(p.ID))
		Expect(p.PodSetHash).To(Equal(map[string]uint64{c.ProviderID(): pipeline.PodSetHash(c)}))
		Expect(p.CatalogVersions).To(Equal(map[string]string{w.nodePool.Name: s.CatalogVersion(w.nodePool.Name)}))
		Expect(p.Footprint.ComputedAt).To(Equal(uint64(3)))
		Expect(pipeline.PlanFrom(disruption.Command{Candidates: []*disruption.Candidate{c}, Replacements: []*disruption.Replacement{{NodeClaim: &pscheduling.NodeClaim{}}}}, 3, env.Clock.Now(), s.CatalogVersion).Action).To(Equal(types.ActionReplace))
		Expect(pipeline.PlanFrom(disruption.Command{Candidates: []*disruption.Candidate{c}, Reboot: true}, 3, env.Clock.Now(), s.CatalogVersion).Action).To(Equal(types.ActionReboot))
	})
	DescribeTable("couples pods of the same namespace, and pods of the namespaces their non-hostname terms select",
		func(a, b *corev1.Pod, coupled bool) {
			fa := types.Footprint{TopologyGroups: sets.New(pipeline.CouplingKeys(a)...)}
			fb := types.Footprint{TopologyGroups: sets.New(pipeline.CouplingKeys(b)...)}
			Expect(lo.Contains(pipeline.Relate(fa, fb).Coupled, pipeline.SharedTopology)).To(Equal(coupled))
			Expect(lo.Contains(pipeline.Relate(fb, fa).Coupled, pipeline.SharedTopology)).To(Equal(coupled))
		},
		Entry("same namespace", podIn("a", nil), podIn("a", nil), true),
		Entry("different namespaces", podIn("a", nil), podIn("b", nil), false),
		Entry("hostname spread doesn't couple across namespaces", podIn("a", spread(corev1.LabelHostname)), podIn("b", nil), false),
		Entry("a custom topology key couples only pods that use it", podIn("a", spread("rack")), podIn("b", spread("rack")), false),
		Entry("cross-namespace affinity", podWithAffinity("a", "b", corev1.LabelTopologyZone), podIn("b", nil), true),
		Entry("a custom topology key couples the same namespace", podIn("a", spread("rack")), podIn("a", nil), true),
		Entry("cross-namespace affinity on a custom key", podWithAffinity("a", "b", "rack"), podIn("b", nil), true),
		Entry("cross-namespace hostname affinity doesn't couple", podWithAffinity("a", "b", corev1.LabelHostname), podIn("b", nil), false),
		// Review C-5: a namespaceSelector (here {}, every namespace) may select a pod of any namespace.
		Entry("a namespaceSelector couples every namespace", podWithNamespaceSelector("a", corev1.LabelTopologyZone), podIn("b", nil), true),
		Entry("a namespaceSelector on a custom key couples every namespace", podWithNamespaceSelector("a", "rack"), podIn("c", nil), true),
	)
})

func podIn(ns string, tsc []corev1.TopologySpreadConstraint) *corev1.Pod {
	p := test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Namespace: ns}, TopologySpreadConstraints: tsc})
	p.UID = k8stypes.UID(ns + "-" + p.Name)
	return p
}

func spread(key string) []corev1.TopologySpreadConstraint {
	return []corev1.TopologySpreadConstraint{{TopologyKey: key, MaxSkew: 1, WhenUnsatisfiable: corev1.DoNotSchedule}}
}

func podWithNamespaceSelector(ns, key string) *corev1.Pod {
	p := podIn(ns, nil)
	p.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
		TopologyKey: key, NamespaceSelector: &metav1.LabelSelector{}, LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "x"}},
	}}}}
	return p
}

func podWithAffinity(ns, other, key string) *corev1.Pod {
	p := podIn(ns, nil)
	p.Spec.Affinity = &corev1.Affinity{PodAffinity: &corev1.PodAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
		TopologyKey: key, Namespaces: []string{other}, LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "x"}},
	}}}}
	return p
}
