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

package topology_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"sigs.k8s.io/karpenter/pkg/scheduling/topology"
)

func TestTopology(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Topology")
}

var _ = Describe("ListSelector", func() {
	It("should select every pod for a nil selector", func() {
		Expect(topology.ListSelector(nil).Matches(labels.Set{"a": "b"})).To(BeTrue())
		Expect(topology.ListSelector(nil).Empty()).To(BeTrue())
	})
	It("should select nothing for an invalid selector", func() {
		for _, selector := range []*metav1.LabelSelector{
			{MatchLabels: map[string]string{"a": "b", "-invalid": "x"}},
			{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "a", Operator: metav1.LabelSelectorOpIn}}},
			{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "a", Operator: "Unknown", Values: []string{"b"}}}},
		} {
			s := topology.ListSelector(selector)
			_, selectable := s.Requirements()
			Expect(selectable).To(BeFalse(), "%v", selector)
		}
	})
	It("should apply match labels and expressions", func() {
		s := topology.ListSelector(&metav1.LabelSelector{
			MatchLabels:      map[string]string{"app": "a"},
			MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "tier", Operator: metav1.LabelSelectorOpNotIn, Values: []string{"db"}}},
		})
		Expect(s.Matches(labels.Set{"app": "a"})).To(BeTrue())
		Expect(s.Matches(labels.Set{"app": "a", "tier": "db"})).To(BeFalse())
		Expect(s.Matches(labels.Set{"app": "b"})).To(BeFalse())
	})
})

var _ = Describe("SelectorKey", func() {
	It("should tell nothing from everything", func() {
		Expect(topology.SelectorKey(labels.Nothing())).ToNot(Equal(topology.SelectorKey(labels.Everything())))
	})
	It("should not depend on the order of requirements or values", func() {
		a := topology.ListSelector(&metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "a"},
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "app", Operator: metav1.LabelSelectorOpIn, Values: []string{"a", "b"}},
				{Key: "tier", Operator: metav1.LabelSelectorOpExists},
			},
		})
		b := topology.ListSelector(&metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "tier", Operator: metav1.LabelSelectorOpExists},
				{Key: "app", Operator: metav1.LabelSelectorOpIn, Values: []string{"b", "a"}},
				{Key: "app", Operator: metav1.LabelSelectorOpIn, Values: []string{"a"}},
			},
		})
		Expect(topology.SelectorKey(a)).To(Equal(topology.SelectorKey(b)))
	})
})

var _ = Describe("CountKey", func() {
	selector := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "a"}}
	It("should be equal for the same counts", func() {
		a := topology.NewCountKey("zone", []string{"a", "b"}, selector, nil)
		b := topology.NewCountKey("zone", []string{"a", "b"}, &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "app", Operator: metav1.LabelSelectorOpIn, Values: []string{"a"}}}}, nil)
		Expect(a.Equal(b)).To(BeTrue())
		Expect(a.Hash()).To(Equal(b.Hash()))
	})
	It("should differ by key, namespaces, selector and node filter", func() {
		base := topology.NewCountKey("zone", []string{"a"}, selector, nil)
		for _, other := range []*topology.CountKey{
			topology.NewCountKey("hostname", []string{"a"}, selector, nil),
			topology.NewCountKey("zone", []string{"a", "b"}, selector, nil),
			topology.NewCountKey("zone", []string{"a"}, nil, nil),
			topology.NewCountKey("zone", []string{"a"}, &metav1.LabelSelector{MatchLabels: map[string]string{"-": "x"}}, nil),
			topology.NewCountKey("zone", []string{"a"}, selector, fakePredicate("x")),
		} {
			Expect(base.Equal(other)).To(BeFalse())
			Expect(other.Equal(base)).To(BeFalse())
		}
		Expect(topology.NewCountKey("zone", []string{"a"}, selector, fakePredicate("x")).Equal(topology.NewCountKey("zone", []string{"a"}, selector, fakePredicate("x")))).To(BeTrue())
		Expect(topology.NewCountKey("zone", []string{"a"}, selector, fakePredicate("x")).Equal(topology.NewCountKey("zone", []string{"a"}, selector, fakePredicate("y")))).To(BeFalse())
	})
})
