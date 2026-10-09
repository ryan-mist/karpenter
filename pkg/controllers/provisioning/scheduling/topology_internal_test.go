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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

var _ = Describe("buildDomainGroups", func() {
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

	It("should record each NodePool's taints once per domain, not once per instance type", func() {
		np := nodePool("tainted", []corev1.Taint{taint("a")})
		groups := buildDomainGroups([]*v1.NodePool{np}, map[string][]*cloudprovider.InstanceType{np.Name: instanceTypes(100)})
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
		// Reference: insert the NodePool's taints for every instance type, as buildDomainGroups did before.
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

		groups := buildDomainGroups(nodePools, its)
		Expect(sets.KeySet(groups)).To(Equal(sets.KeySet(reference)))
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
})
