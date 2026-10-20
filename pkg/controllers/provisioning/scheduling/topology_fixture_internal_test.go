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

	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// topologyFixture is a random cluster for differential topology tests: NodePools and instance types, nodes in the kube
// client and in cluster state (not always both), namespaces, existing pods in every lifecycle phase, and the pods a
// simulation schedules, some of which are bound (as for disruption).
type topologyFixture struct {
	client        client.Client
	cluster       *state.Cluster
	stateNodes    []*state.StateNode
	nodePools     []*v1.NodePool
	instanceTypes map[string][]*cloudprovider.InstanceType
	// pods are the pods to schedule, which the Topology excludes from its counts
	pods []*corev1.Pod
}

var fixtureTaints = []corev1.Taint{
	{Key: "taint-0", Value: "true", Effect: corev1.TaintEffectNoSchedule},
	{Key: "taint-1", Value: "true", Effect: corev1.TaintEffectNoSchedule},
	{Key: "taint-2", Effect: corev1.TaintEffectPreferNoSchedule},
}

// fixture returns a random cluster. The generator's pods are the workload: their topology spread constraints and pod
// (anti-)affinities, and their labels, decide what the groups select.
func (g *identityGen) fixture(ctx context.Context) *topologyFixture {
	kube := fakecr.NewClientBuilder().WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
		return []string{o.(*corev1.Pod).Spec.NodeName}
	}).Build()
	cp := fake.NewCloudProvider()
	cluster := state.NewCluster(clock.NewFakeClock(fakeNow), kube, cp)
	f := &topologyFixture{client: kube, cluster: cluster, instanceTypes: map[string][]*cloudprovider.InstanceType{}}

	for i := range 2 {
		Expect(kube.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("ns-%d", i), Labels: lo.Ternary(g.r.Intn(2) == 0, map[string]string{"team": g.value()}, nil)}})).To(Succeed())
	}

	// NodePools across the zones, some tainted, some limited to a zone
	for i := range 1 + g.r.Intn(3) {
		np := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("np-%d", i)}}
		if g.r.Intn(2) == 0 {
			np.Spec.Template.Spec.Taints = []corev1.Taint{fixtureTaints[g.r.Intn(len(fixtureTaints))]}
		}
		if g.r.Intn(3) == 0 {
			np.Spec.Template.Spec.Requirements = []v1.NodeSelectorRequirementWithMinValues{{Key: corev1.LabelTopologyZone, Operator: lo.Ternary(g.r.Intn(2) == 0, corev1.NodeSelectorOpIn, corev1.NodeSelectorOpNotIn), Values: []string{g.zone()}}}
		}
		f.nodePools = append(f.nodePools, np)
		f.instanceTypes[np.Name] = lo.Times(1+g.r.Intn(3), func(j int) *cloudprovider.InstanceType {
			zones := lo.Uniq([]string{g.zone(), g.zone()})
			return fake.NewInstanceType(fmt.Sprintf("it-%d-%d", i, j), fake.WithOfferings(lo.Map(zones, func(zone string, _ int) cloudprovider.Offering {
				return cloudprovider.Offering{Available: true, Requirements: scheduling.NewLabelRequirements(map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: zone})}
			})...))
		})
	}

	// nodes, each in the kube client, in cluster state, or both
	var nodeNames []string
	for i := range 1 + g.r.Intn(8) {
		nodeLabels, taints := g.node()
		if g.r.Intn(5) != 0 {
			nodeLabels[corev1.LabelHostname] = g.hostname()
		}
		if g.r.Intn(4) == 0 {
			delete(nodeLabels, corev1.LabelTopologyZone)
		} else {
			nodeLabels[corev1.LabelTopologyZone] = g.zone()
		}
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node-%d", i), Labels: nodeLabels}, Spec: corev1.NodeSpec{Taints: taints}}
		nodeNames = append(nodeNames, node.Name)
		switch g.r.Intn(6) {
		case 0: // only in the kube client
			Expect(kube.Create(ctx, node)).To(Succeed())
		case 1: // only in cluster state, as if its deletion hasn't been reconciled yet
			Expect(cluster.UpdateNode(ctx, node.DeepCopy())).To(Succeed())
		default:
			Expect(kube.Create(ctx, node)).To(Succeed())
			Expect(cluster.UpdateNode(ctx, node.DeepCopy())).To(Succeed())
		}
	}
	// an in-flight NodeClaim, which has no Node
	if g.r.Intn(3) == 0 {
		cluster.UpdateNodeClaim(&v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "in-flight", Labels: map[string]string{corev1.LabelTopologyZone: g.zone()}},
			Status:     v1.NodeClaimStatus{ProviderID: "in-flight"},
		})
	}

	// existing pods, bound to the nodes or to a node that doesn't exist
	var bound []*corev1.Pod
	for i := range g.r.Intn(25) {
		p := g.workloadPod(fmt.Sprintf("existing-%d", i))
		p.Spec.NodeName = lo.Ternary(g.r.Intn(10) == 0, "missing", nodeNames[g.r.Intn(len(nodeNames))])
		p.Status.Phase = corev1.PodRunning
		switch g.r.Intn(8) {
		case 0:
			p.Status.Phase = corev1.PodSucceeded
		case 1:
			p.DeletionTimestamp = lo.ToPtr(metav1.Now())
			p.Finalizers = []string{"test/finalizer"}
		case 2:
			p.Spec.NodeName = ""
			p.Status.Phase = corev1.PodPending
		}
		Expect(kube.Create(ctx, p)).To(Succeed())
		// the scheduler only learns of required anti-affinity terms through cluster state
		_ = cluster.UpdatePod(ctx, p)
		if p.Spec.NodeName != "" {
			bound = append(bound, p)
		}
	}

	// the pods to schedule: pending pods, and some bound pods as for a disruption simulation
	for i := range 1 + g.r.Intn(8) {
		p := g.workloadPod(fmt.Sprintf("pending-%d", i))
		if g.r.Intn(3) == 0 {
			Expect(kube.Create(ctx, p)).To(Succeed())
		}
		f.pods = append(f.pods, p)
	}
	for _, p := range bound {
		if g.r.Intn(4) == 0 {
			f.pods = append(f.pods, p)
		}
	}

	// disruption simulations leave some nodes out
	for n := range cluster.Nodes() {
		if g.r.Intn(5) != 0 {
			f.stateNodes = append(f.stateNodes, n.DeepCopy())
		}
	}
	return f
}

// workloadPod returns a random pod whose topology spread constraints carry node filters, min domains and
// matchLabelKeys, and whose pod (anti-)affinity terms sometimes select namespaces by label.
func (g *identityGen) workloadPod(name string) *corev1.Pod {
	p := g.pod(name)
	args := g.args()
	p.Spec.NodeSelector, p.Spec.Tolerations = args.pod.Spec.NodeSelector, args.pod.Spec.Tolerations
	if args.pod.Spec.Affinity != nil && args.pod.Spec.Affinity.NodeAffinity != nil {
		if p.Spec.Affinity == nil {
			p.Spec.Affinity = &corev1.Affinity{}
		}
		p.Spec.Affinity.NodeAffinity = args.pod.Spec.Affinity.NodeAffinity
	}
	for i := range p.Spec.TopologySpreadConstraints {
		tsc := &p.Spec.TopologySpreadConstraints[i]
		tsc.NodeTaintsPolicy, tsc.NodeAffinityPolicy = g.policy(), g.policy()
		if g.r.Intn(3) == 0 {
			tsc.MinDomains = lo.ToPtr(int32(1 + g.r.Intn(4)))
		}
		// the API server requires a label selector for matchLabelKeys
		if tsc.LabelSelector != nil && g.r.Intn(3) == 0 {
			tsc.MatchLabelKeys = []string{g.key()}
		}
	}
	if p.Spec.Affinity != nil {
		var terms []*corev1.PodAffinityTerm
		if a := p.Spec.Affinity.PodAffinity; a != nil {
			terms = append(terms, lo.ToSlicePtr(a.RequiredDuringSchedulingIgnoredDuringExecution)...)
			for i := range a.PreferredDuringSchedulingIgnoredDuringExecution {
				terms = append(terms, &a.PreferredDuringSchedulingIgnoredDuringExecution[i].PodAffinityTerm)
			}
		}
		if a := p.Spec.Affinity.PodAntiAffinity; a != nil {
			for i := range a.RequiredDuringSchedulingIgnoredDuringExecution {
				terms = append(terms, &a.RequiredDuringSchedulingIgnoredDuringExecution[i])
			}
			for i := range a.PreferredDuringSchedulingIgnoredDuringExecution {
				terms = append(terms, &a.PreferredDuringSchedulingIgnoredDuringExecution[i].PodAffinityTerm)
			}
		}
		for _, term := range terms {
			switch g.r.Intn(6) {
			case 0:
				term.NamespaceSelector = &metav1.LabelSelector{}
			case 1:
				term.NamespaceSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"team": g.value()}}
			}
		}
	}
	return p
}

var fakeNow = metav1.Now().Time

func podUIDs(pods []*corev1.Pod) []types.UID {
	return lo.Map(pods, func(p *corev1.Pod, _ int) types.UID { return p.UID })
}
