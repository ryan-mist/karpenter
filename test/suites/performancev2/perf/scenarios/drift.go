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

	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/test/suites/performancev2/perf"
)

// Drift is perfv1's "Drift Performance": every node is replaced after a NodePool template change.
var Drift = perf.Scenario{
	Name:    "drift",
	Summary: "N pods, half hostname-spread (900m/3100Mi) and half large (3500m/28Gi), then a NodePool template change drifts every node: drift detection, replacement launch and drain throughput.",
	Vectors: map[perf.Vector]int{perf.VectorPods: 600},
	Phases: []perf.PhaseSpec{
		{Name: "initial", Summary: "N pending pods until all are healthy, the starting state drift replaces."},
		{Name: "drift", Summary: "Annotate the NodePool template until every original node is replaced and the pods are healthy again: drift detection, replacement NodeClaims, and eviction under hostname spread."},
	},
	Run: func(r *perf.Run) {
		n := r.Size(perf.VectorPods)
		spread := r.Deployment("spread", n/2, "900m", "3100Mi", ownPool(r), test.WithHostnameSpread())
		large := r.Deployment("large", n-n/2, "3500m", "28Gi", ownPool(r))

		r.Create(r.NodeClass, r.NodePool)
		r.AwaitNodePoolsReady(5 * time.Minute)
		initial := r.StartPhase("initial")
		r.Create(spread, large)
		initial.AwaitScaleOut(r.Replicas(), 20*time.Minute)

		drift := r.StartPhase("drift")
		r.Patch(r.NodePool, func() {
			if r.NodePool.Spec.Template.Annotations == nil {
				r.NodePool.Spec.Template.Annotations = map[string]string{}
			}
			r.NodePool.Spec.Template.Annotations["perf.karpenter.sh/drift"] = fmt.Sprint(time.Now().Unix())
		})
		drift.AwaitDrift(r.Replicas(), 45*time.Minute)
	},
}
