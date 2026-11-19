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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/kwok/apis/v1alpha1"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/test/suites/performancev2/perf"
)

// scoreBucketBelowThreshold is the karpenter_consolidation_score bucket bound just below 1/BalancedK: an approved move's
// lowest bucket must start at or above it.
const scoreBucketBelowThreshold = 0.33

// expectBalancedDecisions checks every Balanced consolidation score the phase recorded against the policy's
// threshold, and returns the NodePools that scored moves. A skipped phase has no Result and isn't checked.
func expectBalancedDecisions(rep *perf.Result) sets.Set[string] {
	GinkgoHelper()
	pools := sets.New[string]()
	if rep == nil {
		return pools
	}
	threshold := 1.0 / float64(v1.BalancedK)
	scored := uint64(0)
	for _, s := range rep.Histograms("karpenter_consolidation_score", map[string]string{"policy": string(v1.ConsolidationPolicyBalanced)}) {
		switch decision := s.Labels["decision"]; decision {
		case "approved":
			Expect(s.Min).To(BeNumerically(">=", scoreBucketBelowThreshold), "%v: approved a move scoring below the %.2f threshold", s.Labels, threshold)
		case "rejected":
			Expect(s.Max).To(BeNumerically("<=", threshold), "%v: rejected a move scoring above the %.2f threshold", s.Labels, threshold)
		default:
			Fail(fmt.Sprintf("%v: unexpected decision %q on a Balanced consolidation score; this check no longer covers it", s.Labels, decision))
		}
		scored += s.Count
		pools.Insert(s.Labels["nodepool"])
	}
	Expect(scored).To(BeNumerically(">", 0), "Balanced recorded no scored consolidation moves")
	return pools
}

func balancedPool(r *perf.Run) {
	r.NodePool.Spec.Disruption.ConsolidationPolicy = v1.ConsolidationPolicyBalanced
}

// BalancedBasic is perfv1's "Balanced Basic Fixture": the pods scenario under the Balanced consolidation policy.
var BalancedBasic = perf.Scenario{
	Name:    "balancedBasic",
	Summary: "The pods workload (half 900m/3100Mi, half 3500m/28Gi, scaled to 70%) under consolidationPolicy Balanced, whose every scored move must respect the 1/BalancedK threshold.",
	Vectors: map[perf.Vector]int{perf.VectorPods: 1000},
	Phases: []perf.PhaseSpec{
		{Name: "scaleOut", Summary: "N pending pods until all are healthy."},
		{Name: "consolidation", Summary: "Scale to 70% until nodes stop being disrupted: Balanced move scoring on top of consolidation."},
	},
	Run: func(r *perf.Run) {
		balancedPool(r)
		half := r.Size(perf.VectorPods) / 2
		small := r.Deployment("small", half, "900m", "3100Mi", ownPool(r))
		large := r.Deployment("large", half, "3500m", "28Gi", ownPool(r))

		r.Create(r.NodeClass, r.NodePool)
		r.AwaitNodePoolsReady(5 * time.Minute)
		scaleOut := r.StartPhase("scaleOut")
		r.Create(small, large)
		scaleOut.AwaitScaleOut(2*half, 15*time.Minute)

		consolidation := r.StartPhase("consolidation")
		r.Scale(half*7/10, small, large)
		expectBalancedDecisions(consolidation.AwaitConsolidation(2*half, 2*(half*7/10), 20*time.Minute))
	},
}

// BalancedChurn is perfv1's "Balanced Churn Chain": repeated scale-in and scale-out under Balanced.
var BalancedChurn = perf.Scenario{
	Name:    "balancedChurn",
	Summary: "N 900m/3100Mi pods under Balanced, then halved, restored and halved again inside one phase: Balanced scoring while the workload keeps moving.",
	Vectors: map[perf.Vector]int{perf.VectorPods: 400},
	Phases: []perf.PhaseSpec{
		{Name: "scaleOut", Summary: "N pending pods until all are healthy."},
		{Name: "consolidation", Summary: "Three rounds (N/2, N, N/2), each settled for twice consolidateAfter, then until nodes stop being disrupted."},
	},
	Run: func(r *perf.Run) {
		balancedPool(r)
		n := r.Size(perf.VectorPods)
		app := r.Deployment("app", n, "900m", "3100Mi", ownPool(r))

		r.Create(r.NodeClass, r.NodePool)
		r.AwaitNodePoolsReady(5 * time.Minute)
		scaleOut := r.StartPhase("scaleOut")
		r.Create(app)
		scaleOut.AwaitScaleOut(n, 15*time.Minute)

		consolidation := r.StartPhase("consolidation")
		settle := 2 * time.Minute
		if d := r.NodePool.Spec.Disruption.ConsolidateAfter.Duration; d != nil {
			settle = 2 * *d
		}
		for _, replicas := range []int{n / 2, n, n / 2} {
			r.Scale(replicas, app)
			r.AwaitHealthyPods(replicas, 10*time.Minute)
			time.Sleep(settle)
		}
		expectBalancedDecisions(consolidation.AwaitConsolidation(n, n/2, 20*time.Minute))
	},
}

// BalancedHeterogeneous is perfv1's "Balanced Heterogeneous NodePools": Balanced across two NodePools with different
// instance families and workload densities. KWOK only, since it selects KWOK's instance-family label.
var BalancedHeterogeneous = perf.Scenario{
	Name:    "balancedHeterogeneous",
	Summary: "Two Balanced NodePools restricted to KWOK instance families c and m, a dense workload (300 per 400, 500m/1Gi) on c and a sparse one (100 per 400, 2500m/8Gi) on m, scaled to 60%: Balanced scoring per NodePool.",
	Vectors: map[perf.Vector]int{perf.VectorPods: 400},
	Phases: []perf.PhaseSpec{
		{Name: "scaleOut", Summary: "Both workloads until all pods are healthy."},
		{Name: "consolidation", Summary: "Scale both to 60% until nodes stop being disrupted; every scored move must belong to one of the two NodePools."},
	},
	Run: func(r *perf.Run) {
		if !isKWOK(r) {
			Skip("balancedHeterogeneous selects KWOK instance families")
		}
		balancedPool(r)
		pool := func(family string) *v1.NodePool {
			np := r.NodePool.DeepCopy()
			np.Name = fmt.Sprintf("%s-%s", r.NodePool.Name, family)
			test.ReplaceRequirements(np, v1.NodeSelectorRequirementWithMinValues{Key: v1alpha1.InstanceFamilyLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{family}})
			return np
		}
		c, m := pool("c"), pool("m")
		n := r.Size(perf.VectorPods)
		denseN, sparseN := n*3/4, n-n*3/4
		dense := r.Deployment("dense", denseN, "500m", "1Gi", test.WithNodeSelector(map[string]string{v1.NodePoolLabelKey: c.Name}))
		sparse := r.Deployment("sparse", sparseN, "2500m", "8Gi", test.WithNodeSelector(map[string]string{v1.NodePoolLabelKey: m.Name}))

		r.Create(r.NodeClass, c, m)
		r.AwaitNodePoolsReady(5 * time.Minute)
		scaleOut := r.StartPhase("scaleOut")
		r.Create(dense, sparse)
		scaleOut.AwaitScaleOut(n, 15*time.Minute)

		consolidation := r.StartPhase("consolidation")
		r.Scale(denseN*3/5, dense)
		r.Scale(sparseN*3/5, sparse)
		pools := expectBalancedDecisions(consolidation.AwaitConsolidation(n, denseN*3/5+sparseN*3/5, 25*time.Minute))
		Expect(pools.Difference(sets.New(c.Name, m.Name)).UnsortedList()).To(BeEmpty(), "Balanced scored a move against a NodePool this scenario didn't create")
	},
}
