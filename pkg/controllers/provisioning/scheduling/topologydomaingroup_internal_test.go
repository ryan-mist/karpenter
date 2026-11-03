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

package scheduling

import (
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

var (
	domainGroupTaintA       = corev1.Taint{Key: "example.com/a", Value: "true", Effect: corev1.TaintEffectNoSchedule}
	domainGroupTaintAOther  = corev1.Taint{Key: "example.com/a", Value: "false", Effect: corev1.TaintEffectNoSchedule}
	domainGroupTaintATimed  = corev1.Taint{Key: "example.com/a", Value: "true", Effect: corev1.TaintEffectNoSchedule, TimeAdded: &metav1.Time{Time: time.Unix(1700000000, 0)}}
	domainGroupTaintB       = corev1.Taint{Key: "example.com/b", Effect: corev1.TaintEffectNoExecute}
	domainGroupTaintC       = corev1.Taint{Key: "example.com/c", Value: "x", Effect: corev1.TaintEffectPreferNoSchedule}
	domainGroupTaintChoices = [][]corev1.Taint{
		nil,
		{},
		{domainGroupTaintA},
		{domainGroupTaintAOther},
		{domainGroupTaintATimed},
		{domainGroupTaintB},
		{domainGroupTaintC},
		{domainGroupTaintA, domainGroupTaintB},
		{domainGroupTaintB, domainGroupTaintA},
		{domainGroupTaintA, domainGroupTaintB, domainGroupTaintC},
	}
	domainGroupTolerations = [][]corev1.Toleration{
		nil,
		{{Key: domainGroupTaintA.Key, Operator: corev1.TolerationOpEqual, Value: "true", Effect: corev1.TaintEffectNoSchedule}},
		{{Key: domainGroupTaintA.Key, Operator: corev1.TolerationOpExists}},
		{{Key: domainGroupTaintA.Key, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute}},
		{{Key: domainGroupTaintB.Key, Operator: corev1.TolerationOpExists}},
		{{Key: domainGroupTaintC.Key, Operator: corev1.TolerationOpEqual, Value: "x"}},
		{{Key: domainGroupTaintA.Key, Operator: corev1.TolerationOpExists}, {Key: domainGroupTaintB.Key, Operator: corev1.TolerationOpExists}},
		{{Operator: corev1.TolerationOpExists}},
	}
)

// buildDomainGroupsReference is the implementation of buildDomainGroups (and TopologyDomainGroup.Insert) prior to
// building the merged requirements once per NodePool and dropping identical taint sets. It's kept to assert that the
// current implementation is equivalent.
// nolint:gocyclo
func buildDomainGroupsReference(nodePools []*v1.NodePool, instanceTypes map[string][]*cloudprovider.InstanceType) map[string]TopologyDomainGroup {
	insert := func(t TopologyDomainGroup, domain string, taints ...corev1.Taint) {
		if _, ok := t[domain]; !ok || len(taints) == 0 {
			t[domain] = [][]corev1.Taint{taints}
			return
		}
		if len(t[domain][0]) == 0 {
			return
		}
		t[domain] = append(t[domain], taints)
	}
	nodePoolIndex := lo.SliceToMap(nodePools, func(np *v1.NodePool) (string, *v1.NodePool) { return np.Name, np })
	domainGroups := map[string]TopologyDomainGroup{}
	for npName, its := range instanceTypes {
		np := nodePoolIndex[npName]
		for _, it := range its {
			requirements := scheduling.NewNodeSelectorRequirementsWithMinValues(np.Spec.Template.Spec.Requirements...)
			requirements.Add(scheduling.NewLabelRequirements(np.Spec.Template.Labels).Values()...)
			requirements.Add(it.Requirements.Values()...)
			for topologyKey, requirement := range requirements {
				if _, ok := domainGroups[topologyKey]; !ok {
					domainGroups[topologyKey] = NewTopologyDomainGroup()
				}
				for _, domain := range requirement.Values() {
					insert(domainGroups[topologyKey], domain, np.Spec.Template.Spec.Taints...)
				}
			}
		}
		requirements := scheduling.NewNodeSelectorRequirementsWithMinValues(np.Spec.Template.Spec.Requirements...)
		requirements.Add(scheduling.NewLabelRequirements(np.Spec.Template.Labels).Values()...)
		for key, requirement := range requirements {
			if requirement.Operator() == corev1.NodeSelectorOpIn {
				if _, ok := domainGroups[key]; !ok {
					domainGroups[key] = NewTopologyDomainGroup()
				}
				for _, value := range requirement.Values() {
					insert(domainGroups[key], value, np.Spec.Template.Spec.Taints...)
				}
			}
		}
	}
	return domainGroups
}

// taintSetID identifies a taint set by every field of every taint, in order.
func taintSetID(taints []corev1.Taint) string {
	return strings.Join(lo.Map(taints, func(t corev1.Taint, _ int) string {
		return fmt.Sprintf("%s=%s:%s@%v", t.Key, t.Value, t.Effect, t.TimeAdded)
	}), ",")
}

// distinctTaintSets maps topology key -> domain -> the distinct taint sets tracked for the domain.
func distinctTaintSets(domainGroups map[string]TopologyDomainGroup) map[string]map[string]sets.Set[string] {
	return lo.MapValues(domainGroups, func(group TopologyDomainGroup, _ string) map[string]sets.Set[string] {
		return lo.MapValues(group, func(taintSets [][]corev1.Taint, _ string) sets.Set[string] {
			return sets.New(lo.Map(taintSets, func(taints []corev1.Taint, _ int) string { return taintSetID(taints) })...)
		})
	})
}

func eligibleDomains(group TopologyDomainGroup, pod *corev1.Pod, policy corev1.NodeInclusionPolicy) sets.Set[string] {
	domains := sets.New[string]()
	group.ForEachDomain(pod, policy, func(domain string) { domains.Insert(domain) })
	return domains
}

// domainGroupsFixture builds NodePools with random requirements, labels, and taints, each resolving a random subset of
// the instance types. Taint sets are drawn from a small pool so that NodePools share identical, reordered, and
// overlapping taint sets; half the NodePools get their own copy of the taint slice and half share it.
func domainGroupsFixture(r *rand.Rand, nodePoolCount int, instanceTypes []*cloudprovider.InstanceType) ([]*v1.NodePool, map[string][]*cloudprovider.InstanceType) {
	zones := []string{"test-zone-1", "test-zone-2", "test-zone-3", "test-zone-4"}
	pick := func(vals []string) []string {
		return lo.Map(r.Perm(len(vals))[:1+r.Intn(len(vals))], func(i int, _ int) string { return vals[i] })
	}
	ops := []corev1.NodeSelectorOperator{corev1.NodeSelectorOpIn, corev1.NodeSelectorOpNotIn, corev1.NodeSelectorOpExists, corev1.NodeSelectorOpDoesNotExist}
	nodePools := make([]*v1.NodePool, 0, nodePoolCount)
	its := map[string][]*cloudprovider.InstanceType{}
	for i := range nodePoolCount {
		np := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("np-%d", i)}}
		if r.Intn(2) == 0 {
			np.Spec.Template.Labels = map[string]string{"example.com/nodepool-index": fmt.Sprint(i % 7)}
		}
		reqs := []v1.NodeSelectorRequirementWithMinValues{
			{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: pick([]string{v1.CapacityTypeSpot, v1.CapacityTypeOnDemand})},
		}
		switch op := ops[r.Intn(len(ops))]; op {
		case corev1.NodeSelectorOpIn, corev1.NodeSelectorOpNotIn:
			reqs = append(reqs, v1.NodeSelectorRequirementWithMinValues{Key: corev1.LabelTopologyZone, Operator: op, Values: pick(zones)})
		default:
			reqs = append(reqs, v1.NodeSelectorRequirementWithMinValues{Key: corev1.LabelTopologyZone, Operator: op})
		}
		if r.Intn(2) == 0 {
			reqs = append(reqs, v1.NodeSelectorRequirementWithMinValues{Key: fake.IntegerInstanceLabelKey, Operator: corev1.NodeSelectorOpLt, Values: []string{fmt.Sprint(2 + r.Intn(30))}})
		}
		if r.Intn(3) == 0 {
			reqs = append(reqs, v1.NodeSelectorRequirementWithMinValues{Key: "example.com/nodepool-only", Operator: ops[r.Intn(len(ops))], Values: pick([]string{"a", "b", "c"})})
		}
		// Drop values for operators that don't take them
		for j := range reqs {
			if reqs[j].Operator == corev1.NodeSelectorOpExists || reqs[j].Operator == corev1.NodeSelectorOpDoesNotExist {
				reqs[j].Values = nil
			}
		}
		np.Spec.Template.Spec.Requirements = reqs
		taints := domainGroupTaintChoices[r.Intn(len(domainGroupTaintChoices))]
		if r.Intn(2) == 0 && taints != nil {
			taints = slices.Clone(taints)
		}
		np.Spec.Template.Spec.Taints = taints
		nodePools = append(nodePools, np)
		its[np.Name] = lo.Filter(instanceTypes, func(_ *cloudprovider.InstanceType, _ int) bool { return r.Intn(4) != 0 })
	}
	return nodePools, its
}

var _ = Describe("TopologyDomainGroup", func() {
	Context("Insert", func() {
		It("should not track a taint set identical to one already tracked", func() {
			group := NewTopologyDomainGroup()
			group.Insert("domain", domainGroupTaintA, domainGroupTaintB)
			group.Insert("domain", slices.Clone([]corev1.Taint{domainGroupTaintA, domainGroupTaintB})...)
			group.Insert("domain", domainGroupTaintA, domainGroupTaintB)
			Expect(group["domain"]).To(Equal([][]corev1.Taint{{domainGroupTaintA, domainGroupTaintB}}))
		})
		It("should track taint sets that differ in any field, order, or length", func() {
			group := NewTopologyDomainGroup()
			candidates := [][]corev1.Taint{
				{domainGroupTaintA},
				{domainGroupTaintAOther},
				{domainGroupTaintATimed},
				{{Key: domainGroupTaintA.Key, Value: domainGroupTaintA.Value, Effect: corev1.TaintEffectNoExecute}},
				{domainGroupTaintA, domainGroupTaintB},
				{domainGroupTaintB, domainGroupTaintA},
				{domainGroupTaintA, domainGroupTaintB, domainGroupTaintC},
			}
			for _, taints := range candidates {
				group.Insert("domain", taints...)
			}
			Expect(group["domain"]).To(Equal(candidates))
		})
		It("should only track the empty taint set once it's inserted", func() {
			group := NewTopologyDomainGroup()
			group.Insert("domain", domainGroupTaintA)
			group.Insert("domain", domainGroupTaintB)
			group.Insert("domain")
			group.Insert("domain", domainGroupTaintC)
			Expect(group["domain"]).To(HaveLen(1))
			Expect(group["domain"][0]).To(BeEmpty())
		})
	})
	Context("buildDomainGroups", func() {
		It("should keep a domain eligible for a pod that tolerates any NodePool offering it", func() {
			nodePools := []*v1.NodePool{
				{ObjectMeta: metav1.ObjectMeta{Name: "a"}, Spec: v1.NodePoolSpec{Template: v1.NodeClaimTemplate{Spec: v1.NodeClaimTemplateSpec{Taints: []corev1.Taint{domainGroupTaintA}}}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "a-copy"}, Spec: v1.NodePoolSpec{Template: v1.NodeClaimTemplate{Spec: v1.NodeClaimTemplateSpec{Taints: []corev1.Taint{domainGroupTaintA}}}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "b"}, Spec: v1.NodePoolSpec{Template: v1.NodeClaimTemplate{Spec: v1.NodeClaimTemplateSpec{Taints: []corev1.Taint{domainGroupTaintB}}}}},
			}
			instanceTypes := fake.InstanceTypes(5)
			domainGroups := buildDomainGroups(nodePools, map[string][]*cloudprovider.InstanceType{"a": instanceTypes, "a-copy": instanceTypes, "b": instanceTypes})
			zones := domainGroups[corev1.LabelTopologyZone]
			Expect(zones).ToNot(BeEmpty())
			for _, taintSets := range zones {
				Expect(taintSets).To(ConsistOf([]corev1.Taint{domainGroupTaintA}, []corev1.Taint{domainGroupTaintB}))
			}
			podFor := func(tolerations ...corev1.Toleration) *corev1.Pod {
				return &corev1.Pod{Spec: corev1.PodSpec{Tolerations: tolerations}}
			}
			allZones := sets.KeySet(zones)
			Expect(eligibleDomains(zones, podFor(), corev1.NodeInclusionPolicyHonor)).To(BeEmpty())
			Expect(eligibleDomains(zones, podFor(), corev1.NodeInclusionPolicyIgnore)).To(Equal(allZones))
			Expect(eligibleDomains(zones, podFor(corev1.Toleration{Key: domainGroupTaintA.Key, Operator: corev1.TolerationOpExists}), corev1.NodeInclusionPolicyHonor)).To(Equal(allZones))
			Expect(eligibleDomains(zones, podFor(corev1.Toleration{Key: domainGroupTaintB.Key, Operator: corev1.TolerationOpExists}), corev1.NodeInclusionPolicyHonor)).To(Equal(allZones))
		})
		It("should be equivalent to merging NodePool and instance type requirements per instance type", func() {
			instanceTypes := append(fake.InstanceTypesAssorted(), fake.InstanceTypes(50)...)
			pods := lo.Map(domainGroupTolerations, func(tolerations []corev1.Toleration, _ int) *corev1.Pod {
				return &corev1.Pod{Spec: corev1.PodSpec{Tolerations: tolerations}}
			})
			for seed := range int64(25) {
				r := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test fixtures
				nodePools, its := domainGroupsFixture(r, 1+r.Intn(40), instanceTypes)
				want := buildDomainGroupsReference(nodePools, its)
				got := buildDomainGroups(nodePools, its)

				// The same topology keys and domains, tracking the same distinct taint sets, with no duplicates
				Expect(distinctTaintSets(got)).To(Equal(distinctTaintSets(want)), "seed %d", seed)
				for key, group := range got {
					for domain, taintSets := range group {
						Expect(sets.New(lo.Map(taintSets, func(t []corev1.Taint, _ int) string { return taintSetID(t) })...).Len()).To(Equal(len(taintSets)), "seed %d, %s=%s", seed, key, domain)
					}
				}
				// The same domains are eligible for every pod, which is the only way the taint sets are consumed
				for key := range want {
					for _, pod := range pods {
						for _, policy := range []corev1.NodeInclusionPolicy{corev1.NodeInclusionPolicyHonor, corev1.NodeInclusionPolicyIgnore} {
							Expect(eligibleDomains(got[key], pod, policy)).To(Equal(eligibleDomains(want[key], pod, policy)), "seed %d, key %s, tolerations %v, policy %s", seed, key, pod.Spec.Tolerations, policy)
						}
					}
				}
			}
		})
	})
})

// BenchmarkBuildDomainGroups models many NodePools that share a NodeClass and so resolve the same instance types.
func BenchmarkBuildDomainGroups(b *testing.B) {
	instanceTypes := fake.InstanceTypesAssorted()
	for _, nodePoolCount := range []int{1, 50, 500} {
		b.Run(fmt.Sprintf("nodepools=%d/instancetypes=%d", nodePoolCount, len(instanceTypes)), func(b *testing.B) {
			nodePools := make([]*v1.NodePool, 0, nodePoolCount)
			its := map[string][]*cloudprovider.InstanceType{}
			for i := range nodePoolCount {
				np := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("np-%d", i)}}
				np.Spec.Template.Labels = map[string]string{"example.com/nodepool": np.Name}
				np.Spec.Template.Spec.Requirements = []v1.NodeSelectorRequirementWithMinValues{
					{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeSpot, v1.CapacityTypeOnDemand}},
					{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"test-zone-1", "test-zone-2", "test-zone-3"}},
				}
				if i%2 == 0 {
					np.Spec.Template.Spec.Taints = []corev1.Taint{domainGroupTaintA}
				}
				nodePools = append(nodePools, np)
				its[np.Name] = instanceTypes
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				buildDomainGroups(nodePools, its)
			}
		})
	}
}
