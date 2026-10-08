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

package scheduling_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	pscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
)

var _ = Describe("TopologyDomainGroup", func() {
	taint := func(key string) corev1.Taint {
		return corev1.Taint{Key: key, Value: "true", Effect: corev1.TaintEffectNoSchedule}
	}
	podTolerating := func(keys ...string) *corev1.Pod {
		return test.Pod(test.PodOptions{Tolerations: lo.Map(keys, func(k string, _ int) corev1.Toleration {
			return corev1.Toleration{Key: k, Operator: corev1.TolerationOpExists}
		})})
	}
	domains := func(dg scheduling.TopologyDomainGroup, pod *corev1.Pod, policy corev1.NodeInclusionPolicy) sets.Set[string] {
		got := sets.New[string]()
		dg.ForEachDomain(pod, policy, func(domain string) { got.Insert(domain) })
		return got
	}

	It("should track a NodePool's taint slice once when it is inserted for many instance types", func() {
		dg := scheduling.NewTopologyDomainGroup()
		taints := []corev1.Taint{taint("a"), taint("b")}
		for range 100 {
			dg.Insert("zone-1", taints...)
		}
		Expect(dg["zone-1"]).To(HaveLen(1))
		Expect(dg["zone-1"][0]).To(Equal(taints))
	})
	It("should keep distinct taint slices, including value-equal copies and sub-slices of the same array", func() {
		dg := scheduling.NewTopologyDomainGroup()
		taints := []corev1.Taint{taint("a"), taint("b")}
		dg.Insert("zone-1", taints...)
		dg.Insert("zone-1", taints[:1]...)
		dg.Insert("zone-1", taints[1:]...)
		dg.Insert("zone-1", append([]corev1.Taint{}, taints...)...)
		Expect(dg["zone-1"]).To(HaveLen(4))
	})
	It("should give the same ForEachDomain results as tracking every inserted taint set", func() {
		// Interleave several NodePools' taint slices across domains, each inserted once per "instance type", the way
		// buildDomainGroups does, and compare against a reference that keeps every inserted taint set.
		npA := []corev1.Taint{taint("a")}
		npB := []corev1.Taint{taint("b")}
		npAB := []corev1.Taint{taint("a"), taint("b")}
		npACopy := []corev1.Taint{taint("a")}
		inserts := []struct {
			domain string
			taints []corev1.Taint
		}{
			{"zone-1", npA}, {"zone-1", npA}, {"zone-1", npA},
			{"zone-2", npA}, {"zone-2", npA},
			{"zone-1", npB}, {"zone-1", npB},
			{"zone-2", npAB}, {"zone-2", npAB}, {"zone-2", npAB[:1]},
			{"zone-3", npB}, {"zone-3", npB}, {"zone-3", npACopy}, {"zone-3", npACopy},
			{"zone-4", npAB}, {"zone-4", nil}, {"zone-4", npAB}, {"zone-4", npB},
			{"zone-5", nil}, {"zone-5", npA}, {"zone-5", npA},
		}
		dg := scheduling.NewTopologyDomainGroup()
		reference := map[string][][]corev1.Taint{}
		for _, in := range inserts {
			dg.Insert(in.domain, in.taints...)
			reference[in.domain] = append(reference[in.domain], in.taints)
		}
		Expect(dg["zone-1"]).To(HaveLen(2))
		Expect(dg["zone-2"]).To(HaveLen(3))
		Expect(dg["zone-3"]).To(HaveLen(2))

		for _, pod := range []*corev1.Pod{podTolerating(), podTolerating("a"), podTolerating("b"), podTolerating("a", "b")} {
			expected := sets.New[string]()
			for domain, taintSets := range reference {
				if lo.SomeBy(taintSets, func(ts []corev1.Taint) bool { return pscheduling.Taints(ts).ToleratesPod(pod) == nil }) {
					expected.Insert(domain)
				}
			}
			Expect(domains(dg, pod, corev1.NodeInclusionPolicyHonor)).To(Equal(expected))
			Expect(domains(dg, pod, corev1.NodeInclusionPolicyIgnore)).To(Equal(sets.New("zone-1", "zone-2", "zone-3", "zone-4", "zone-5")))
		}
		// Spot-check the expectations so the reference itself is not vacuous.
		Expect(domains(dg, podTolerating(), corev1.NodeInclusionPolicyHonor)).To(Equal(sets.New("zone-4", "zone-5")))
		Expect(domains(dg, podTolerating("a"), corev1.NodeInclusionPolicyHonor)).To(Equal(sets.New("zone-1", "zone-2", "zone-3", "zone-4", "zone-5")))
		Expect(domains(dg, podTolerating("b"), corev1.NodeInclusionPolicyHonor)).To(Equal(sets.New("zone-1", "zone-3", "zone-4", "zone-5")))
	})
})
