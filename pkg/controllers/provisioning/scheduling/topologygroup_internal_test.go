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
	"context"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
)

var _ = Describe("TopologyGroup Hostname Fast Path", func() {
	// newHostnameGroup builds a hostname topology group with the given per-host counts. Pods in the "default" namespace
	// are selected by the group.
	newHostnameGroup := func(topologyType TopologyType, counts map[string]int32) *legacyTopologyGroup {
		tg := &legacyTopologyGroup{
			Type:         topologyType,
			Key:          corev1.LabelHostname,
			namespaces:   sets.New("default"),
			selector:     labels.Everything(),
			maxSkew:      1,
			domains:      map[string]int32{},
			emptyDomains: sets.New[string](),
			owners:       map[types.UID]struct{}{},
		}
		for domain, count := range counts {
			tg.domains[domain] = count
			if count == 0 {
				tg.emptyDomains.Insert(domain)
			}
		}
		return tg
	}
	exists := scheduling.NewRequirement(corev1.LabelHostname, corev1.NodeSelectorOpExists)
	selectedPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}}
	unselectedPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "other"}}

	Context("NotIn with a single value", func() {
		It("should not select the excluded host for topology spread", func() {
			tg := newHostnameGroup(TopologyTypeSpread, map[string]int32{"a": 5, "b": 0})
			req, valid := tg.Get(selectedPod, exists, scheduling.NewRequirement(corev1.LabelHostname, corev1.NodeSelectorOpNotIn, "a"))
			Expect(req.Operator()).To(Equal(corev1.NodeSelectorOpIn))
			Expect(req.Values()).To(ConsistOf("b"))
			Expect(sets.List(valid)).To(ConsistOf("b"))
		})
		It("should not select the excluded host for pod affinity", func() {
			tg := newHostnameGroup(TopologyTypePodAffinity, map[string]int32{"a": 1, "b": 0})
			req, _ := tg.Get(unselectedPod, exists, scheduling.NewRequirement(corev1.LabelHostname, corev1.NodeSelectorOpNotIn, "a"))
			Expect(req.Len()).To(Equal(0))
		})
		It("should not select the excluded host for pod anti-affinity", func() {
			tg := newHostnameGroup(TopologyTypePodAntiAffinity, map[string]int32{"a": 1, "b": 0})
			req, _ := tg.Get(selectedPod, exists, scheduling.NewRequirement(corev1.LabelHostname, corev1.NodeSelectorOpNotIn, "b"))
			Expect(req.Len()).To(Equal(0))
		})
		It("should select an empty registered host for pod anti-affinity", func() {
			tg := newHostnameGroup(TopologyTypePodAntiAffinity, map[string]int32{"a": 1, "b": 0})
			req, _ := tg.Get(selectedPod, exists, scheduling.NewRequirement(corev1.LabelHostname, corev1.NodeSelectorOpNotIn, "c"))
			Expect(req.Values()).To(ConsistOf("b"))
		})
	})
	Context("In with a single value", func() {
		It("should use the fast path for an unregistered host", func() {
			nodeDomains := scheduling.NewRequirement(corev1.LabelHostname, corev1.NodeSelectorOpIn, "new")
			for _, topologyType := range []TopologyType{TopologyTypeSpread, TopologyTypePodAffinity, TopologyTypePodAntiAffinity} {
				tg := newHostnameGroup(topologyType, map[string]int32{"a": 1})
				if topologyType == TopologyTypePodAffinity {
					// affinity can only bootstrap onto a new host when no compatible domain has a matching pod
					tg = newHostnameGroup(topologyType, map[string]int32{"a": 0})
				}
				req, _ := tg.Get(selectedPod, exists, nodeDomains)
				Expect(req.Values()).To(ConsistOf("new"), topologyType.String())
			}
		})
	})
})

var _ = Describe("Legacy TopologyGroup Order Hook", func() {
	ascending := func(domains []string) { slices.Sort(domains) }
	descending := func(domains []string) { slices.Sort(domains); slices.Reverse(domains) }
	newZoneGroup := func(topologyType TopologyType, order func([]string), counts map[string]int32) *legacyTopologyGroup {
		tg := &legacyTopologyGroup{
			Type:         topologyType,
			Key:          corev1.LabelTopologyZone,
			namespaces:   sets.New("default"),
			selector:     labels.Everything(),
			maxSkew:      1,
			domains:      map[string]int32{},
			emptyDomains: sets.New[string](),
			owners:       map[types.UID]struct{}{},
			order:        order,
		}
		for domain, count := range counts {
			tg.domains[domain] = count
			if count == 0 {
				tg.emptyDomains.Insert(domain)
			}
		}
		return tg
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}}
	exists := scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpExists)
	in := scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "c", "a", "b", "d")
	counts := map[string]int32{"a": 0, "b": 0, "c": 0, "d": 1}

	It("should break topology spread ties in hook order", func() {
		for _, nodeDomains := range []*scheduling.Requirement{exists, in} {
			for range 20 {
				req, _ := newZoneGroup(TopologyTypeSpread, ascending, counts).Get(pod, exists, nodeDomains)
				Expect(req.Values()).To(ConsistOf("a"))
				req, _ = newZoneGroup(TopologyTypeSpread, descending, counts).Get(pod, exists, nodeDomains)
				Expect(req.Values()).To(ConsistOf("c"))
			}
		}
	})
	It("should bootstrap pod affinity in hook order", func() {
		empty := map[string]int32{"a": 0, "b": 0, "c": 0}
		podDomains := scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "b", "c")
		for range 20 {
			// the first pick is from podDomains ∩ nodeDomains, the second from podDomains
			req, _ := newZoneGroup(TopologyTypePodAffinity, ascending, empty).Get(pod, podDomains, scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "a", "c"))
			Expect(req.Values()).To(ConsistOf("b", "c"))
			req, _ = newZoneGroup(TopologyTypePodAffinity, descending, empty).Get(pod, podDomains, exists)
			Expect(req.Values()).To(ConsistOf("c"))
		}
	})
	It("should give the order hook every new group's key and hash", func() {
		ctx := context.Background()
		p := test.Pod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "a"}},
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
				MaxSkew: 1, TopologyKey: corev1.LabelTopologyZone, WhenUnsatisfiable: corev1.DoNotSchedule,
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "a"}},
			}},
			PodRequirements:     []corev1.PodAffinityTerm{{TopologyKey: corev1.LabelHostname, LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "a"}}}},
			PodAntiRequirements: []corev1.PodAffinityTerm{{TopologyKey: corev1.LabelHostname, LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "a"}}}},
		})
		called := map[uint64]string{}
		calledInverse := map[uint64]string{}
		t := newTopologyForIndexTest()
		t.domainOrder = func(key string, hash uint64, inverse bool) func([]string) {
			lo.Ternary(inverse, calledInverse, called)[hash] = key
			return ascending
		}
		Expect(t.Update(ctx, p)).To(Succeed())
		// the pod's anti-affinity term creates a forward and an inverse group with the same hash
		Expect(t.topologyGroups).To(HaveLen(3))
		Expect(t.inverseTopologyGroups).To(HaveLen(1))
		for groups, called := range map[*map[uint64]*legacyTopologyGroup]map[uint64]string{&t.topologyGroups: called, &t.inverseTopologyGroups: calledInverse} {
			expected := map[uint64]string{}
			for hash, tg := range *groups {
				Expect(tg.Hash()).To(Equal(hash))
				Expect(tg.order).ToNot(BeNil())
				expected[hash] = tg.Key
			}
			Expect(called).To(Equal(expected))
		}
	})
})
