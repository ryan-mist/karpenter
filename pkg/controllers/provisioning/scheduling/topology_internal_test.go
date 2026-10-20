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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
)

var _ = Describe("Topology domain groups", func() {
	taint := func(key string) corev1.Taint {
		return corev1.Taint{Key: key, Value: "true", Effect: corev1.TaintEffectNoSchedule}
	}
	nodePool := func(name string, taints []corev1.Taint, requirements ...corev1.NodeSelectorRequirement) *v1.NodePool {
		return &v1.NodePool{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: v1.NodePoolSpec{Template: v1.NodeClaimTemplate{Spec: v1.NodeClaimTemplateSpec{
				Taints: taints,
				Requirements: lo.Map(requirements, func(r corev1.NodeSelectorRequirement, _ int) v1.NodeSelectorRequirementWithMinValues {
					return v1.NodeSelectorRequirementWithMinValues{Key: r.Key, Operator: r.Operator, Values: r.Values}
				}),
			}}},
		}
	}
	// instanceTypes returns n instance types spread across zones z1..z3.
	instanceTypes := func(n int) []*cloudprovider.InstanceType {
		return lo.Times(n, func(i int) *cloudprovider.InstanceType {
			return &cloudprovider.InstanceType{
				Name: fmt.Sprintf("it-%d", i),
				Requirements: scheduling.NewRequirements(
					scheduling.NewRequirement(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, fmt.Sprintf("it-%d", i)),
					scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, lo.Ternary(i%2 == 0, []string{"z1", "z2"}, []string{"z2", "z3"})...),
				),
			}
		})
	}

	// domainGroups reads each topology key's domains through a legacyTopology's lazily built domain groups.
	domainGroups := func(nodePools []*v1.NodePool, its map[string][]*cloudprovider.InstanceType, keys ...string) map[string]TopologyDomainGroup {
		t := &legacyTopology{domainGroups: map[string]TopologyDomainGroup{}, domainSources: newDomainSources(nodePools, its)}
		return lo.SliceToMap(keys, func(key string) (string, TopologyDomainGroup) { return key, t.domainGroup(key) })
	}

	It("should only build the domains of the topology keys that are read", func() {
		np := nodePool("tainted", []corev1.Taint{taint("a")})
		t := &legacyTopology{domainGroups: map[string]TopologyDomainGroup{}, domainSources: newDomainSources([]*v1.NodePool{np}, map[string][]*cloudprovider.InstanceType{np.Name: instanceTypes(10)})}
		zones := t.domainGroup(corev1.LabelTopologyZone)
		Expect(sets.KeySet(zones)).To(Equal(sets.New("z1", "z2", "z3")))
		Expect(sets.KeySet(t.domainGroups)).To(Equal(sets.New(corev1.LabelTopologyZone)))
		zones["z4"] = nil
		Expect(t.domainGroup(corev1.LabelTopologyZone)).To(HaveKey("z4"), "domain groups should be built once per key")
		Expect(t.domainGroup(corev1.LabelHostname)).To(BeEmpty())
	})
	It("should record each NodePool's taints once per domain, not once per instance type", func() {
		np := nodePool("tainted", []corev1.Taint{taint("a")})
		groups := domainGroups([]*v1.NodePool{np}, map[string][]*cloudprovider.InstanceType{np.Name: instanceTypes(100)}, corev1.LabelTopologyZone)
		for _, zone := range []string{"z1", "z2", "z3"} {
			Expect(groups[corev1.LabelTopologyZone][zone]).To(Equal([][]corev1.Taint{{taint("a")}}))
		}
	})
	It("should give every pod the same domains as recording taints once per instance type", func() {
		nodePools := []*v1.NodePool{
			nodePool("a", []corev1.Taint{taint("a")}, corev1.NodeSelectorRequirement{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"z1", "z2", "z4"}}),
			nodePool("b", []corev1.Taint{taint("b")}),
			nodePool("ab", []corev1.Taint{taint("a"), taint("b")}, corev1.NodeSelectorRequirement{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpNotIn, Values: []string{"z1"}}),
			nodePool("untainted", nil, corev1.NodeSelectorRequirement{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"z3"}}),
		}
		its := map[string][]*cloudprovider.InstanceType{}
		for _, np := range nodePools {
			its[np.Name] = instanceTypes(20)
		}
		// Reference: insert the NodePool's taints for every instance type, as the domain groups did before.
		reference := map[string]map[string][][]corev1.Taint{}
		for _, np := range nodePools {
			for _, it := range its[np.Name] {
				requirements := scheduling.NewNodeSelectorRequirementsWithMinValues(np.Spec.Template.Spec.Requirements...)
				requirements.Add(it.Requirements.Values()...)
				for key, requirement := range requirements {
					for _, domain := range requirement.Values() {
						reference[key] = lo.Assign(reference[key], map[string][][]corev1.Taint{domain: append(reference[key][domain], np.Spec.Template.Spec.Taints)})
					}
				}
			}
			requirements := scheduling.NewNodeSelectorRequirementsWithMinValues(np.Spec.Template.Spec.Requirements...)
			for key, requirement := range requirements {
				if requirement.Operator() == corev1.NodeSelectorOpIn {
					for _, domain := range requirement.Values() {
						reference[key] = lo.Assign(reference[key], map[string][][]corev1.Taint{domain: append(reference[key][domain], np.Spec.Template.Spec.Taints)})
					}
				}
			}
		}

		groups := domainGroups(nodePools, its, lo.Keys(reference)...)
		for _, tolerated := range [][]string{nil, {"a"}, {"b"}, {"a", "b"}} {
			pod := &corev1.Pod{Spec: corev1.PodSpec{Tolerations: lo.Map(tolerated, func(k string, _ int) corev1.Toleration {
				return corev1.Toleration{Key: k, Operator: corev1.TolerationOpExists}
			})}}
			for key, domains := range reference {
				expected := sets.New[string]()
				for domain, taintSets := range domains {
					if lo.SomeBy(taintSets, func(ts []corev1.Taint) bool { return len(ts) == 0 || scheduling.Taints(ts).ToleratesPod(pod) == nil }) {
						expected.Insert(domain)
					}
				}
				got := sets.New[string]()
				groups[key].ForEachDomain(pod, corev1.NodeInclusionPolicyHonor, func(domain string) { got.Insert(domain) })
				Expect(got).To(Equal(expected), "key %s, tolerations %v", key, tolerated)
			}
		}
	})
	It("should build the same domain groups as intersecting requirements per instance type", func() {
		keys := []string{corev1.LabelTopologyZone, v1.CapacityTypeLabelKey, corev1.LabelArchStable, fake.IntegerInstanceLabelKey, fake.LabelInstanceSize, "custom/team", "custom/int"}
		values := []string{"test-zone-1", "test-zone-2", "test-zone-3", "test-zone-4", "spot", "on-demand", "reserved", "amd64", "arm64", "a", "b", "1", "2", "3", "5", "8", "13", "small", "large"}
		taintSets := [][]corev1.Taint{nil, {taint("a")}, {taint("a"), taint("b")}, {taint("b"), taint("a")}, {{Key: "c", Effect: corev1.TaintEffectPreferNoSchedule}}}
		randomRequirement := func(r *rand.Rand) v1.NodeSelectorRequirementWithMinValues {
			requirement := v1.NodeSelectorRequirementWithMinValues{Key: keys[r.Intn(len(keys))]}
			for range 1 + r.Intn(3) {
				requirement.Values = append(requirement.Values, values[r.Intn(len(values))])
			}
			if r.Intn(5) == 0 {
				requirement.MinValues = lo.ToPtr(1 + r.Intn(2))
			}
			switch r.Intn(8) {
			case 0, 1, 2:
				requirement.Operator = corev1.NodeSelectorOpIn
			case 3:
				requirement.Operator = corev1.NodeSelectorOpNotIn
			case 4:
				requirement.Operator, requirement.Values = corev1.NodeSelectorOpExists, nil
			case 5:
				requirement.Operator, requirement.Values = corev1.NodeSelectorOpDoesNotExist, nil
			case 6:
				requirement.Operator, requirement.Values = corev1.NodeSelectorOpGt, []string{fmt.Sprint(r.Intn(6))}
			default:
				requirement.Operator, requirement.Values = corev1.NodeSelectorOpLt, []string{fmt.Sprint(r.Intn(10))}
			}
			return requirement
		}
		randomInstanceTypes := func(r *rand.Rand, n int) []*cloudprovider.InstanceType {
			return lo.Times(n, func(i int) *cloudprovider.InstanceType {
				opts := []fake.InstanceTypeOptions{fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(fmt.Sprint(1 + r.Intn(8)))})}
				for range r.Intn(3) {
					requirement := randomRequirement(r)
					opts = append(opts, fake.WithRequirements(scheduling.NewRequirementWithFlexibility(requirement.Key, requirement.Operator, nil, requirement.Values...)))
				}
				var offerings []cloudprovider.Offering
				for _, zone := range []string{"test-zone-1", "test-zone-2", "test-zone-3", "test-zone-4"} {
					for _, capacityType := range []string{v1.CapacityTypeSpot, v1.CapacityTypeOnDemand, v1.CapacityTypeReserved} {
						if r.Intn(3) == 0 {
							offerings = append(offerings, cloudprovider.Offering{Available: true, Requirements: scheduling.NewLabelRequirements(map[string]string{v1.CapacityTypeLabelKey: capacityType, corev1.LabelTopologyZone: zone})})
						}
					}
				}
				if len(offerings) > 0 {
					opts = append(opts, fake.WithOfferings(offerings...))
				}
				return fake.NewInstanceType(fmt.Sprintf("it-%d", i), opts...)
			})
		}
		// canonical renders each domain's taint groups in a stable order so that results can be compared regardless of
		// map iteration order. Keys without domains are dropped since they are indistinguishable from missing keys.
		canonical := func(groups map[string]TopologyDomainGroup) map[string]map[string][]string {
			result := map[string]map[string][]string{}
			for key, group := range groups {
				if len(group) == 0 {
					continue
				}
				result[key] = lo.MapValues(group, func(taintGroups [][]corev1.Taint, _ string) []string {
					return lo.Map(taintGroups, func(taints []corev1.Taint, _ int) string { return fmt.Sprint(taints) })
				})
				for _, taintGroups := range result[key] {
					slices.Sort(taintGroups)
				}
			}
			return result
		}
		pods := lo.Map([][]string{nil, {"a"}, {"b"}, {"c"}, {"a", "b"}}, func(tolerated []string, _ int) *corev1.Pod {
			return test.Pod(test.PodOptions{Tolerations: lo.Map(tolerated, func(k string, _ int) corev1.Toleration {
				return corev1.Toleration{Key: k, Operator: corev1.TolerationOpExists}
			})})
		})
		for seed := range int64(2000) {
			r := rand.New(rand.NewSource(seed))
			nodePools := lo.Times(1+r.Intn(5), func(i int) *v1.NodePool {
				np := nodePool(fmt.Sprintf("np-%d", i), taintSets[r.Intn(len(taintSets))])
				np.Spec.Template.Spec.Requirements = lo.Times(r.Intn(4), func(int) v1.NodeSelectorRequirementWithMinValues { return randomRequirement(r) })
				np.Spec.Template.Labels = map[string]string{}
				for range r.Intn(3) {
					np.Spec.Template.Labels[keys[r.Intn(len(keys))]] = values[r.Intn(len(values))]
				}
				return np
			})
			shared := randomInstanceTypes(r, r.Intn(12))
			its := map[string][]*cloudprovider.InstanceType{}
			for _, np := range nodePools {
				its[np.Name] = lo.Ternary(r.Intn(2) == 0, shared, randomInstanceTypes(r, r.Intn(8)))
			}

			expected := buildDomainGroupsReference(nodePools, its)
			// Also read keys that no NodePool or instance type defines, which have no domains
			actual := domainGroups(nodePools, its, lo.Uniq(append(append(lo.Keys(expected), keys...), corev1.LabelHostname))...)
			Expect(canonical(actual)).To(Equal(canonical(expected)), "seed %d", seed)
			for key := range expected {
				for _, pod := range pods {
					want, got := sets.New[string](), sets.New[string]()
					expected[key].ForEachDomain(pod, corev1.NodeInclusionPolicyHonor, func(domain string) { want.Insert(domain) })
					actual[key].ForEachDomain(pod, corev1.NodeInclusionPolicyHonor, func(domain string) { got.Insert(domain) })
					Expect(got).To(Equal(want), "seed %d, key %s, tolerations %v", seed, key, pod.Spec.Tolerations)
				}
			}
		}
	})
})

// buildDomainGroupsReference is the previous implementation of the domain groups, which builds every topology key
// and intersects the NodePool's requirements with each instance type's requirements.
func buildDomainGroupsReference(nodePools []*v1.NodePool, instanceTypes map[string][]*cloudprovider.InstanceType) map[string]TopologyDomainGroup {
	nodePoolIndex := lo.SliceToMap(nodePools, func(np *v1.NodePool) (string, *v1.NodePool) {
		return np.Name, np
	})
	domainGroups := map[string]TopologyDomainGroup{}
	for npName, its := range instanceTypes {
		np := nodePoolIndex[npName]
		domains := map[string]sets.Set[string]{}
		addDomains := func(topologyKey string, values ...string) {
			if _, ok := domains[topologyKey]; !ok {
				domains[topologyKey] = sets.New[string]()
			}
			domains[topologyKey].Insert(values...)
		}
		for _, it := range its {
			requirements := scheduling.NewNodeSelectorRequirementsWithMinValues(np.Spec.Template.Spec.Requirements...)
			requirements.Add(scheduling.NewLabelRequirements(np.Spec.Template.Labels).Values()...)
			requirements.Add(it.Requirements.Values()...)

			for topologyKey, requirement := range requirements {
				addDomains(topologyKey, requirement.Values()...)
			}
		}

		requirements := scheduling.NewNodeSelectorRequirementsWithMinValues(np.Spec.Template.Spec.Requirements...)
		requirements.Add(scheduling.NewLabelRequirements(np.Spec.Template.Labels).Values()...)
		for key, requirement := range requirements {
			if requirement.Operator() == corev1.NodeSelectorOpIn {
				addDomains(key, requirement.Values()...)
			}
		}

		for topologyKey, values := range domains {
			if _, ok := domainGroups[topologyKey]; !ok {
				domainGroups[topologyKey] = NewTopologyDomainGroup()
			}
			for domain := range values {
				domainGroups[topologyKey].Insert(domain, np.Spec.Template.Spec.Taints...)
			}
		}
	}
	return domainGroups
}
