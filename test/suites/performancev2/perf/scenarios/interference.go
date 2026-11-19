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

	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/test/suites/performancev2/perf"
)

// Interference is perfv1's "Self Anti-Affinity Deployment Interference": two self anti-affine deployments that scale
// in opposite directions at once.
var Interference = perf.Scenario{
	Name:    "interference",
	Summary: "Two deployments with hostname self anti-affinity (900m/3100Mi and 3500m/28Gi), so every pod needs its own node, scaled in opposite directions at once.",
	Vectors: map[perf.Vector]int{perf.VectorPods: 1000},
	Phases: []perf.PhaseSpec{
		{Name: "scaleOut", Summary: "N/2 small pods, one node each: anti-affinity search and per-pod NodeClaims."},
		{Name: "interference", Summary: "Halve the small deployment while creating N/2 large pods: provisioning while consolidation and deletion churn the same nodes."},
		{Name: "consolidation", Summary: "Grow small to 2N/5 while shrinking large to N/5: consolidation that must reuse freed nodes for the other deployment."},
	},
	Run: func(r *perf.Run) {
		n := r.Size(perf.VectorPods)
		small := r.Deployment("small", n/2, "900m", "3100Mi", ownPool(r), test.WithPodAntiAffinityHostname())
		large := r.Deployment("large", n/2, "3500m", "28Gi", ownPool(r), test.WithPodAntiAffinityHostname())

		r.Create(r.NodeClass, r.NodePool)
		r.AwaitNodePoolsReady(5 * time.Minute)
		scaleOut := r.StartPhase("scaleOut")
		r.Create(small)
		scaleOut.AwaitScaleOut(n/2, 15*time.Minute)

		interference := r.StartPhase("interference")
		r.Scale(n/4, small)
		r.Create(large)
		interference.AwaitScaleOut(n/4+n/2, 10*time.Minute)

		consolidation := r.StartPhase("consolidation")
		r.Scale(n*2/5, small)
		r.Scale(n/5, large)
		consolidation.AwaitConsolidation(n/4+n/2, n*2/5+n/5, 15*time.Minute)
	},
}
