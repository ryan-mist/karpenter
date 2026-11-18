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
package scenarios

import (
	"fmt"
	"time"

	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/kwok/apis/v1alpha1"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/test/suites/performancev2/perf"
)

const (
	diverseTaint = "perf.karpenter.sh/dedicated"
	diversePool  = "perf.example.com/diverse-pool" // template labels can't use the restricted karpenter.sh domain
)

var diverseToleration = corev1.Toleration{Key: diverseTaint, Operator: corev1.TolerationOpExists}

// diverseNodePools builds n NodePools that no two collapse to the same shape: a rotating capacity type, weight and
// template label everywhere, and on KWOK also a rotating zone, architecture and instance size ceiling, since other
// providers' zones and catalogs vary by account. Every third pool carries a taint the scenario's pods tolerate.
func diverseNodePools(r *perf.Run, n int) []*v1.NodePool {
	_, kwok := r.NodeClass.(*v1alpha1.KWOKNodeClass)
	capacityTypes := [][]string{{v1.CapacityTypeSpot}, {v1.CapacityTypeOnDemand}, {v1.CapacityTypeSpot, v1.CapacityTypeOnDemand}}
	zones := [][]string{{"test-zone-a"}, {"test-zone-b"}, {"test-zone-c"}, {"test-zone-d"}, {"test-zone-a", "test-zone-b"}}
	arches := [][]string{{v1.ArchitectureAmd64}, {v1.ArchitectureArm64}}
	sizes := []string{"8", "16", "32"}
	pools := make([]*v1.NodePool, 0, n)
	for i := range n {
		np := r.NodePool.DeepCopy()
		np.Name = fmt.Sprintf("%s-%d", r.NodePool.Name, i)
		np.Spec.Weight = lo.ToPtr(int32(1 + i%50)) //nolint:gosec // i%50 fits
		np.Spec.Template.Labels = lo.Assign(np.Spec.Template.Labels, map[string]string{diversePool: r.Scenario.Name, "perf.example.com/pool-index": fmt.Sprint(i)})
		reqs := []v1.NodeSelectorRequirementWithMinValues{{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: capacityTypes[i%len(capacityTypes)]}}
		if kwok {
			reqs = append(reqs,
				v1.NodeSelectorRequirementWithMinValues{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: zones[i%len(zones)]},
				v1.NodeSelectorRequirementWithMinValues{Key: corev1.LabelArchStable, Operator: corev1.NodeSelectorOpIn, Values: arches[i%len(arches)]},
				v1.NodeSelectorRequirementWithMinValues{Key: v1alpha1.InstanceSizeLabelKey, Operator: corev1.NodeSelectorOpLt, Values: []string{sizes[i%len(sizes)]}},
			)
		}
		test.ReplaceRequirements(np, reqs...)
		if i%3 == 0 {
			np.Spec.Template.Spec.Taints = append(np.Spec.Template.Spec.Taints, corev1.Taint{Key: diverseTaint, Value: "true", Effect: corev1.TaintEffectNoSchedule})
		}
		pools = append(pools, np)
	}
	return pools
}

// DiverseNodePools is perfv1's "Many Diverse NodePools": NodePool cardinality, not pod count, is the load.
var DiverseNodePools = perf.Scenario{
	Name:    "diverseNodePools",
	Summary: "N diverse NodePools (capacity type, weight, a third tainted, and on KWOK zone, arch and size) and a few zonally spread 50m/128Mi pods that can use any of them: per-NodePool cost in cluster state, cost tracking and scheduling, where nodepools has one usable pool.",
	Vectors: map[perf.Vector]int{perf.VectorNodePools: 500, perf.VectorPods: 100},
	Phases: []perf.PhaseSpec{
		{Name: "scaleOut", Summary: "Pods across the diverse NodePools: NodePool selection and topology/taint bookkeeping over every pool."},
		{Name: "consolidation", Summary: "Scale to 40% until nodes stop being disrupted: disruption and cost recompute across every NodePool."},
	},
	Run: func(r *perf.Run) {
		pods := r.Size(perf.VectorPods)
		r.Create(r.NodeClass)
		r.CreateParallel(objects(diverseNodePools(r, r.Size(perf.VectorNodePools))))
		r.AwaitNodePoolsReady(15 * time.Minute)
		app := r.Deployment("app", pods, "50m", "128Mi", test.WithNodeSelector(map[string]string{diversePool: r.Scenario.Name}),
			test.WithTolerations([]corev1.Toleration{diverseToleration}), zoneSpread())

		scaleOut := r.StartPhase("scaleOut")
		r.Create(app)
		scaleOut.AwaitScaleOut(pods, 10*time.Minute)

		consolidation := r.StartPhase("consolidation")
		r.Scale(pods*2/5, app)
		consolidation.AwaitConsolidation(pods, pods*2/5, 15*time.Minute)
	},
}

// TargetedNodePools is perfv1's "should stay within resource bounds when pods target many NodePools": one pinned
// single-pod deployment per targeted pool, so many pools get real nodes and cost tracking entries at once.
var TargetedNodePools = perf.Scenario{
	Name:    "targetedNodePools",
	Summary: "N diverse NodePools, with one single-pod 50m/128Mi deployment pinned (karpenter.sh/nodepool selector) to each of the first P: many pools provision at once, one node each.",
	Vectors: map[perf.Vector]int{perf.VectorNodePools: 500, perf.VectorPods: 150},
	Phases: []perf.PhaseSpec{
		{Name: "scaleOut", Summary: "P pinned pods, about one node per targeted NodePool: per-NodePool provisioning and cost tracking population."},
		{Name: "consolidation", Summary: "Zero half the deployments until their nodes are gone: emptiness across many NodePools."},
	},
	Run: func(r *perf.Run) {
		targets := r.Size(perf.VectorPods)
		pools := diverseNodePools(r, r.Size(perf.VectorNodePools))
		r.Create(r.NodeClass)
		r.CreateParallel(objects(pools))
		r.AwaitNodePoolsReady(15 * time.Minute)
		deps := make([]*appsv1.Deployment, 0, targets)
		for i := range targets {
			deps = append(deps, r.Deployment(fmt.Sprintf("t%03d", i), 1, "50m", "128Mi",
				test.WithNodeSelector(map[string]string{v1.NodePoolLabelKey: pools[i%len(pools)].Name}),
				test.WithTolerations([]corev1.Toleration{diverseToleration})))
		}

		scaleOut := r.StartPhase("scaleOut")
		r.CreateParallel(objects(deps))
		scaleOut.AwaitScaleOut(targets, 15*time.Minute)

		consolidation := r.StartPhase("consolidation")
		for _, d := range deps[targets/2:] {
			r.Scale(0, d)
		}
		consolidation.AwaitConsolidation(targets, targets/2, 20*time.Minute)
	},
}
