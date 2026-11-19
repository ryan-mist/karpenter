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

package performancev2

import "sigs.k8s.io/karpenter/test/suites/performancev2/perf"

// targetedNodePoolsLevels: see levels.go for the rule. kwok-ci measures only: perfv1 never ran this on GitHub
// runners.
var targetedNodePoolsLevels = scenarioLevels{
	KWOK: perf.Levels{
		"targetedNodePools/scaleOut": {
			perf.PodReady.P95.Stable(14), perf.PodReady.P99.Stable(14), perf.PodReady.Max.Experimental(14), perf.CPUUtil.Experimental(0.193), perf.MemoryUtil.Experimental(0.055), perf.Nodes.Experimental(180), perf.HeapMB.Experimental(190), perf.CPUCores.Experimental(1.548),
		},
		"targetedNodePools/consolidation": {
			perf.TotalTimeSeconds.Experimental(354), perf.CPUUtil.Experimental(0.193), perf.MemoryUtil.Experimental(0.055), perf.Nodes.Experimental(90), perf.HeapMB.Experimental(178), perf.CPUCores.Experimental(1.435),
		},
	},
	KWOKCI: perf.Levels{
		"targetedNodePools/scaleOut":      {},
		"targetedNodePools/consolidation": {},
	},
}
