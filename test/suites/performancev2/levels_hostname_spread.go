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

// hostnameSpreadLevels: see levels.go for the rule. No Stable gates: mconc1 makes scaleOut time out, which fails the
// spec but leaves no report to count. kwok runs perfv1's XL size (kwok_provider.go Sizes).
var hostnameSpreadLevels = scenarioLevels{
	KWOK: perf.Levels{
		"hostnameSpread/scaleOut": {
			perf.PodReady.P95.Experimental(71), perf.PodReady.P99.Experimental(72), perf.PodReady.Max.Experimental(72), perf.CPUUtil.Experimental(0.723), perf.MemoryUtil.Experimental(0.767), perf.Nodes.Experimental(1250), perf.HeapMB.Experimental(507), perf.CPUCores.Experimental(1.769),
		},
		"hostnameSpread/consolidation": {
			perf.TotalTimeSeconds.Experimental(1572), perf.CPUUtil.Experimental(0.733), perf.MemoryUtil.Experimental(0.771), perf.Nodes.Experimental(840), perf.PodsDisrupted.Experimental(249), perf.HeapMB.Experimental(430), perf.CPUCores.Experimental(1.81),
		},
	},
	KWOKCI: perf.Levels{
		"hostnameSpread/scaleOut": {
			perf.CPUCores.Experimental(1.168), perf.CPUUtil.Experimental(0.533), perf.HeapMB.Experimental(296), perf.MemoryUtil.Experimental(0.691), perf.Nodes.Experimental(672),
		},
		"hostnameSpread/consolidation": {
			perf.CPUCores.Experimental(0.746), perf.CPUUtil.Experimental(0.535), perf.HeapMB.Experimental(273), perf.MemoryUtil.Experimental(0.681), perf.Nodes.Experimental(467),
		},
	},
}
