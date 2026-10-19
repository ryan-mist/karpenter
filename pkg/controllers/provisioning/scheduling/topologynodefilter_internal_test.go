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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/scheduling"
)

var _ = Describe("TopologyNodeFilterMemo", func() {
	It("should return the same result as TopologyNodeFilter.Matches", func() {
		r := rand.New(rand.NewSource(42))
		keys := []string{corev1.LabelTopologyZone, corev1.LabelHostname, "class", "tier"}
		values := []string{"", "a", "b", "c"}
		taints := []corev1.Taint{
			{Key: "dedicated", Value: "a", Effect: corev1.TaintEffectNoSchedule},
			{Key: "dedicated", Value: "b", Effect: corev1.TaintEffectNoSchedule},
			{Key: "dedicated", Value: "a", Effect: corev1.TaintEffectNoExecute},
			{Key: "spot", Effect: corev1.TaintEffectPreferNoSchedule},
		}
		operators := []corev1.NodeSelectorOperator{corev1.NodeSelectorOpIn, corev1.NodeSelectorOpNotIn, corev1.NodeSelectorOpExists, corev1.NodeSelectorOpDoesNotExist}
		policies := []corev1.NodeInclusionPolicy{"", corev1.NodeInclusionPolicyHonor, corev1.NodeInclusionPolicyIgnore}

		var nodes []*corev1.Node
		for i := 0; i < 200; i++ {
			node := &corev1.Node{}
			node.Name = fmt.Sprintf("node-%d", i)
			node.Labels = map[string]string{"unrelated": fmt.Sprint(i)}
			for _, key := range keys {
				if r.Intn(4) != 0 {
					node.Labels[key] = values[r.Intn(len(values))]
				}
			}
			if r.Intn(4) == 0 {
				node.Labels[corev1.LabelHostname] = node.Name
			}
			for _, taint := range taints {
				if r.Intn(3) == 0 {
					node.Spec.Taints = append(node.Spec.Taints, taint)
				}
			}
			nodes = append(nodes, node)
		}

		for i := 0; i < 300; i++ {
			pod := &corev1.Pod{}
			pod.Spec.NodeSelector = map[string]string{}
			for _, key := range lo.Samples(keys, r.Intn(3)) {
				pod.Spec.NodeSelector[key] = values[r.Intn(len(values))]
			}
			if r.Intn(2) == 0 {
				var terms []corev1.NodeSelectorTerm
				for j := r.Intn(3); j >= 0; j-- {
					var exprs []corev1.NodeSelectorRequirement
					for _, key := range lo.Samples(keys, r.Intn(3)) {
						op := operators[r.Intn(len(operators))]
						expr := corev1.NodeSelectorRequirement{Key: key, Operator: op}
						if op == corev1.NodeSelectorOpIn || op == corev1.NodeSelectorOpNotIn {
							expr.Values = lo.Samples(values, r.Intn(2)+1)
						}
						exprs = append(exprs, expr)
					}
					terms = append(terms, corev1.NodeSelectorTerm{MatchExpressions: exprs})
				}
				pod.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: terms},
				}}
			}
			for _, taint := range lo.Samples(taints, r.Intn(3)) {
				toleration := corev1.Toleration{Key: taint.Key, Effect: taint.Effect, Value: taint.Value}
				if r.Intn(2) == 0 {
					toleration = corev1.Toleration{Key: taint.Key, Operator: corev1.TolerationOpExists}
				}
				pod.Spec.Tolerations = append(pod.Spec.Tolerations, toleration)
			}
			filter := MakeTopologyNodeFilter(pod, policies[r.Intn(len(policies))], policies[r.Intn(len(policies))])
			if r.Intn(10) == 0 {
				filter = TopologyNodeFilter{}
			}

			memo := newTopologyNodeFilterMemo(filter)
			for _, node := range lo.Shuffle(append(nodes, nodes...)) {
				Expect(memo.Matches(node.Labels, node.Spec.Taints)).To(Equal(filter.Matches(node.Spec.Taints, scheduling.NewLabelRequirements(node.Labels))), "filter %+v, node %+v", filter, node)
			}
			Expect(len(memo.results)).To(BeNumerically("<=", len(nodes)))
		}
	})
})
