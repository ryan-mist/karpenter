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

// driftLevels: see levels.go for the rule.
var driftLevels = scenarioLevels{
	KWOK: perf.Levels{
		"drift/initial": {
			perf.PodReady.P95.Stable(20), perf.PodReady.P99.Stable(21), perf.PodReady.Max.Experimental(21), perf.CPUUtil.Experimental(0.733), perf.MemoryUtil.Experimental(0.771), perf.Nodes.Experimental(360), perf.HeapMB.Experimental(213), perf.CPUCores.Experimental(1.154),
		},
		"drift/drift": {
			perf.TotalTimeSeconds.Experimental(512), perf.CPUUtil.Experimental(0.733), perf.MemoryUtil.Experimental(0.771), perf.Nodes.Experimental(360), perf.PodsDisrupted.Experimental(720), perf.HeapMB.Experimental(249), perf.CPUCores.Experimental(1.019),
		},
	},
	KWOKCI: perf.Levels{
		"drift/initial": {
			perf.CPUCores.Experimental(0.722), perf.CPUUtil.Experimental(0.532), perf.HeapMB.Experimental(176), perf.MemoryUtil.Experimental(0.691), perf.Nodes.Experimental(409),
		},
		"drift/drift": {
			perf.CPUCores.Experimental(0.908), perf.CPUUtil.Experimental(0.545), perf.HeapMB.Experimental(221), perf.MemoryUtil.Experimental(0.698), perf.Nodes.Experimental(362),
		},
	},
}
