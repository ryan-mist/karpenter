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
	"time"

	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/test/suites/performancev2/perf"
)

// DoNotDisrupt is perfv1's "Do Not Disrupt Performance Test": consolidation around nodes it must not touch.
var DoNotDisrupt = perf.Scenario{
	Name:    "doNotDisrupt",
	Summary: "N pods: 5/11 hostname-spread (900m/3100Mi), 5/11 large (3500m/28Gi) and 1/11 with karpenter.sh/do-not-disrupt (900m/450Mi). Halving the first two consolidates around nodes that must not be disrupted.",
	Vectors: map[perf.Vector]int{perf.VectorPods: 1100},
	Phases: []perf.PhaseSpec{
		{Name: "scaleOut", Summary: "N pending pods until all are healthy."},
		{Name: "consolidation", Summary: "Halve the unprotected deployments until nodes stop being disrupted: candidate filtering on do-not-disrupt pods. Every node hosting a protected pod must survive."},
	},
	Run: func(r *perf.Run) {
		n := r.Size(perf.VectorPods)
		protectedN := frac(n, 1, 11)
		each := (n - protectedN) / 2
		spread := r.Deployment("spread", each, "900m", "3100Mi", ownPool(r), test.WithHostnameSpread())
		large := r.Deployment("large", each, "3500m", "28Gi", ownPool(r))
		protected := r.Deployment("protected", protectedN, "900m", "450Mi", ownPool(r), test.WithDoNotDisrupt())
		total := 2*each + protectedN

		r.Create(r.NodeClass, r.NodePool)
		r.AwaitNodePoolsReady(5 * time.Minute)
		scaleOut := r.StartPhase("scaleOut")
		r.Create(spread, large, protected)
		scaleOut.AwaitScaleOut(total, 15*time.Minute)

		protectedNodes := sets.New[string]()
		for _, p := range r.Pods() {
			if p.Labels["app"] == protected.Name && p.Spec.NodeName != "" {
				protectedNodes.Insert(p.Spec.NodeName)
			}
		}
		consolidation := r.StartPhase("consolidation")
		r.Scale(each/2, spread, large)
		if consolidation.AwaitConsolidation(total, 2*(each/2)+protectedN, 25*time.Minute) == nil {
			return
		}
		remaining := sets.New[string]()
		for _, node := range r.Nodes() {
			remaining.Insert(node.Name)
		}
		Expect(protectedNodes.Difference(remaining).UnsortedList()).To(BeEmpty(), "nodes hosting do-not-disrupt pods were disrupted")
	},
}
