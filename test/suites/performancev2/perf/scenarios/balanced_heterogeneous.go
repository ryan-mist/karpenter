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
