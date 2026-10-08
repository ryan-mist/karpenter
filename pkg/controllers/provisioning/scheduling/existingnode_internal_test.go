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
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

var _ = Describe("Existing Node Daemon Compatibility", func() {
	const (
		labelZone = "test.karpenter.sh/zone"
		labelTeam = "test.karpenter.sh/team"
	)
	dedicated := corev1.Taint{Key: "dedicated", Value: "true", Effect: corev1.TaintEffectNoSchedule}
	startup := corev1.Taint{Key: "startup", Value: "true", Effect: corev1.TaintEffectNoSchedule}
	notReady := corev1.Taint{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoSchedule}
	allocatable := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100")}

	// stateNodes returns a fresh set of state nodes covering every source StateNode.Labels() and StateNode.Taints()
	// can read from. Each call returns new objects so the reference and actual paths don't share state.
	stateNodes := func() []*state.StateNode {
		newStateNode := func(node *corev1.Node, nodeClaim *v1.NodeClaim) *state.StateNode {
			n := state.NewNode()
			n.Node = node
			n.NodeClaim = nodeClaim
			return n
		}
		return []*state.StateNode{
			// registered and initialized: labels and taints come from the Node
			newStateNode(
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: "registered", Labels: map[string]string{
						v1.NodeRegisteredLabelKey: "true", v1.NodeInitializedLabelKey: "true", labelZone: "a", labelTeam: "node",
					}},
					Spec:   corev1.NodeSpec{Taints: []corev1.Taint{dedicated}},
					Status: corev1.NodeStatus{Allocatable: allocatable},
				},
				&v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "registered-nc", Labels: map[string]string{labelZone: "b", labelTeam: "claim"}}},
			),
			// unregistered: labels and taints come from the NodeClaim even though a Node exists
			newStateNode(
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "unregistered-node", Labels: map[string]string{labelZone: "a", labelTeam: "node"}}},
				&v1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: "unregistered", Labels: map[string]string{labelZone: "b", labelTeam: "claim"}},
					Spec:       v1.NodeClaimSpec{Taints: []corev1.Taint{dedicated}},
					Status:     v1.NodeClaimStatus{Allocatable: allocatable},
				},
			),
			// NodeClaim only: startup and known ephemeral taints are dropped since the node is uninitialized
			newStateNode(nil, &v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "nodeclaim-only", Labels: map[string]string{labelZone: "b", labelTeam: "node"}},
				Spec:       v1.NodeClaimSpec{Taints: []corev1.Taint{startup, notReady}, StartupTaints: []corev1.Taint{startup}},
				Status:     v1.NodeClaimStatus{Allocatable: allocatable},
			}),
			// unmanaged: labels and taints come from the Node, which also carries a hostname label
			newStateNode(&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "unmanaged", Labels: map[string]string{labelZone: "c", corev1.LabelHostname: "host-unmanaged"}},
				Status:     corev1.NodeStatus{Allocatable: allocatable},
			}, nil),
			// registered but uninitialized: labels come from the Node, startup taint is dropped
			newStateNode(
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: "uninitialized", Labels: map[string]string{
						v1.NodeRegisteredLabelKey: "true", labelZone: "a", labelTeam: "node",
					}},
					Spec:   corev1.NodeSpec{Taints: []corev1.Taint{startup}},
					Status: corev1.NodeStatus{Allocatable: allocatable},
				},
				&v1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: "uninitialized-nc", Labels: map[string]string{labelZone: "b"}},
					Spec:       v1.NodeClaimSpec{StartupTaints: []corev1.Taint{startup}},
				},
			),
		}
	}

	// Each daemon requests a distinct power-of-two CPU amount so the remaining resources identify the compatible set.
	tolerateAll := []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
	daemon := func(name string, milliCPU int64, mutate func(*corev1.Pod)) *corev1.Pod {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: corev1.PodSpec{
				Tolerations: tolerateAll,
				Containers: []corev1.Container{{Name: "c", Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: *resource.NewMilliQuantity(milliCPU, resource.DecimalSI)},
				}}},
			},
		}
		if mutate != nil {
			mutate(p)
		}
		return p
	}
	requiredAffinity := func(reqs ...corev1.NodeSelectorRequirement) *corev1.Affinity {
		return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: reqs}},
		}}}
	}
	daemonSetPods := func() []*corev1.Pod {
		return []*corev1.Pod{
			daemon("tolerate-all", 1, nil),
			daemon("no-tolerations", 2, func(p *corev1.Pod) { p.Spec.Tolerations = nil }),
			daemon("dedicated-team-node", 4, func(p *corev1.Pod) {
				p.Spec.Tolerations = []corev1.Toleration{{Key: dedicated.Key, Operator: corev1.TolerationOpExists}}
				p.Spec.NodeSelector = map[string]string{labelTeam: "node"}
			}),
			daemon("team-claim", 8, func(p *corev1.Pod) { p.Spec.NodeSelector = map[string]string{labelTeam: "claim"} }),
			daemon("zone-not-a", 16, func(p *corev1.Pod) {
				p.Spec.Affinity = requiredAffinity(corev1.NodeSelectorRequirement{Key: labelZone, Operator: corev1.NodeSelectorOpNotIn, Values: []string{"a"}})
			}),
			// preferences are not requirements for daemon compatibility
			daemon("preferred-only", 32, func(p *corev1.Pod) {
				p.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{
					Weight:     1,
					Preference: corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: labelZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"z"}}}},
				}}}}
			}),
			daemon("undefined-label", 64, func(p *corev1.Pod) { p.Spec.NodeSelector = map[string]string{"test.karpenter.sh/undefined": "true"} }),
			// The ExistingNode adds a hostname requirement to the node requirements. The compatibility check must only
			// see the node's labels, so this is only compatible with the node that carries a hostname label.
			daemon("hostname", 128, func(p *corev1.Pod) {
				p.Spec.Affinity = requiredAffinity(corev1.NodeSelectorRequirement{
					Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpIn, Values: []string{"host-unmanaged", "registered", "unregistered", "nodeclaim-only", "uninitialized"},
				})
			}),
			daemon("dra", 256, func(p *corev1.Pod) { p.Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: "claim"}} }),
		}
	}
	expectedCompatible := map[string][]string{
		"registered":     {"tolerate-all", "dedicated-team-node", "preferred-only", "dra"},
		"unregistered":   {"tolerate-all", "team-claim", "zone-not-a", "preferred-only", "dra"},
		"nodeclaim-only": {"tolerate-all", "no-tolerations", "dedicated-team-node", "zone-not-a", "preferred-only", "dra"},
		"unmanaged":      {"tolerate-all", "no-tolerations", "zone-not-a", "preferred-only", "hostname", "dra"},
		"uninitialized":  {"tolerate-all", "no-tolerations", "dedicated-team-node", "preferred-only", "dra"},
	}

	// referenceCompatibleDaemonPods is the per (node, daemon) pair computation the scheduler performed before node and
	// daemon requirements were built once and reused.
	referenceCompatibleDaemonPods := func(ctx context.Context, node *state.StateNode, taints []corev1.Taint, daemonSetPods []*corev1.Pod) []*corev1.Pod {
		var daemons []*corev1.Pod
		for _, p := range daemonSetPods {
			if (&Scheduler{}).shouldSkipDaemonPod(ctx, p) {
				continue
			}
			if err := scheduling.Taints(taints).ToleratesPod(p); err != nil {
				continue
			}
			if err := scheduling.NewLabelRequirements(node.Labels()).Compatible(scheduling.NewStrictPodRequirements(p)); err != nil {
				continue
			}
			daemons = append(daemons, p)
		}
		return daemons
	}

	DescribeTable("should match per-pair daemon compatibility and node requirements",
		func(ignoreDRARequests bool) {
			ctx := karpopts.ToContext(context.Background(), &karpopts.Options{IgnoreDRARequests: ignoreDRARequests})
			daemons := daemonSetPods()
			daemonsByName := lo.SliceToMap(daemons, func(p *corev1.Pod) (string, *corev1.Pod) { return p.Name, p })

			s := &Scheduler{topology: &Topology{}, remainingResources: map[string]corev1.ResourceList{}}
			s.calculateExistingNodeClaims(ctx, stateNodes(), daemons, nil, false)
			actual := lo.SliceToMap(s.existingNodes, func(n *ExistingNode) (string, *ExistingNode) { return n.Name(), n })
			Expect(actual).To(HaveLen(len(expectedCompatible)))

			for _, node := range stateNodes() {
				By(fmt.Sprintf("checking node %s", node.Name()))
				Expect(actual).To(HaveKey(node.Name()))
				taints := node.Taints()
				reference := NewExistingNode(node, &Topology{}, taints, resources.RequestsForPods(referenceCompatibleDaemonPods(ctx, node, taints, daemons)...), nil, false)

				expectedNames := lo.Filter(expectedCompatible[node.Name()], func(name string, _ int) bool { return !ignoreDRARequests || name != "dra" })
				expected := resources.Subtract(allocatable, resources.RequestsForPods(lo.Map(expectedNames, func(name string, _ int) *corev1.Pod { return daemonsByName[name] })...))
				// the reference itself must produce the hand-computed result, so the comparison below is meaningful
				Expect(resources.String(reference.remainingResources)).To(Equal(resources.String(expected)))
				Expect(resources.String(actual[node.Name()].remainingResources)).To(Equal(resources.String(reference.remainingResources)))

				expectedRequirements := scheduling.NewLabelRequirements(node.Labels())
				expectedRequirements.Add(scheduling.NewRequirement(corev1.LabelHostname, corev1.NodeSelectorOpIn, node.HostName()))
				Expect(reference.requirements).To(Equal(expectedRequirements))
				Expect(actual[node.Name()].requirements).To(Equal(expectedRequirements))
				Expect(actual[node.Name()].cachedTaints).To(Equal(taints))
			}
		},
		Entry("with DRA daemons considered", false),
		Entry("with DRA daemons ignored", true),
	)
})
