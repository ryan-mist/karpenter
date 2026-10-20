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
	"math/rand"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
)

var _ = Describe("Topology Seal", func() {
	var ctx context.Context
	BeforeEach(func() {
		ctx = karpopts.ToContext(context.Background(), test.Options())
	})
	It("should release the simulation's state and keep rendering errors", func() {
		g := &identityGen{topologyIndexGen{r: rand.New(rand.NewSource(1))}}
		f := g.fixture(ctx)
		labels := map[string]string{"app": "sealed"}
		pod := test.Pod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: labels},
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
				MaxSkew: 1, TopologyKey: corev1.LabelTopologyZone, WhenUnsatisfiable: corev1.DoNotSchedule,
				LabelSelector: &metav1.LabelSelector{MatchLabels: labels},
			}},
			PodAntiRequirements: []corev1.PodAffinityTerm{{TopologyKey: corev1.LabelHostname, LabelSelector: &metav1.LabelSelector{MatchLabels: labels}}},
		})
		t, err := newOverlayTopology(ctx, f.client, f.cluster, f.stateNodes, f.nodePools, f.instanceTypes, append(f.pods, pod), &options{})
		Expect(err).ToNot(HaveOccurred())
		node := scheduling.NewRequirements(
			scheduling.NewRequirement(corev1.LabelHostname, corev1.NodeSelectorOpIn, "sealed-host"),
			scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "zone-0"),
		)
		requirements, err := t.AddRequirements(pod, nil, scheduling.NewStrictPodRequirements(pod), node)
		Expect(err).ToNot(HaveOccurred())
		t.Register(corev1.LabelHostname, "sealed-host")
		t.Record(pod, nil, requirements)
		// zone-0 now has the pod, so the spread blocks a second pod there
		_, topologyErr := t.AddRequirements(pod, nil, scheduling.NewStrictPodRequirements(pod), node)
		Expect(topologyErr).To(HaveOccurred())

		t.seal()
		Expect(t.src).To(BeNil())
		Expect(t.excluded).To(BeNil())
		Expect(t.plans).To(BeNil())
		Expect(t.universes).To(BeNil())
		Expect(t.presence).To(BeNil())
		Expect(topologyErr.Error()).To(ContainSubstring("zone-0: 1"))
		for _, call := range []func(){
			func() { _ = t.Update(ctx, pod) },
			func() { t.Record(pod, nil, requirements) },
			func() { _, _ = t.AddRequirements(pod, nil, scheduling.NewStrictPodRequirements(pod), node) },
			func() { t.GetTopologyZoneConstraints(pod, scheduling.NewPodRequirements(pod)) },
			func() { t.Register(corev1.LabelHostname, "other") },
			func() { t.Unregister(corev1.LabelHostname, "other") },
			func() { t.BeginPod(pod, scheduling.NewStrictPodRequirements(pod)) },
		} {
			Expect(call).To(PanicWith(ContainSubstring("after the simulation ended")))
		}
	})
})
