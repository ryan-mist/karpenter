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

// doNotDisruptLevels: see levels.go for the rule. No Stable gates: mconc1 makes scaleOut time out, which fails the
// spec but leaves no report to count, and the one other catch (rv1973, pod_ready p95 22 s to 31 s) is a single run of
// a patch whose catches don't repeat.
var doNotDisruptLevels = scenarioLevels{
	KWOK: perf.Levels{
		"doNotDisrupt/scaleOut": {
			perf.PodReady.P95.Experimental(33), perf.PodReady.P99.Experimental(45), perf.PodReady.Max.Experimental(45), perf.CPUUtil.Experimental(0.684), perf.MemoryUtil.Experimental(0.741), perf.Nodes.Experimental(663), perf.HeapMB.Experimental(283), perf.CPUCores.Experimental(1.41),
		},
		"doNotDisrupt/consolidation": {
			perf.TotalTimeSeconds.Experimental(1549), perf.CPUUtil.Experimental(0.694), perf.MemoryUtil.Experimental(0.733), perf.Nodes.Experimental(363), perf.PodsDisrupted.Experimental(167), perf.HeapMB.Experimental(202), perf.CPUCores.Experimental(0.638),
		},
	},
	KWOKCI: perf.Levels{
		"doNotDisrupt/scaleOut": {
			perf.CPUCores.Experimental(1.194), perf.CPUUtil.Experimental(0.551), perf.HeapMB.Experimental(299), perf.MemoryUtil.Experimental(0.67), perf.Nodes.Experimental(695),
		},
		"doNotDisrupt/consolidation": {
			perf.CPUCores.Experimental(0.561), perf.CPUUtil.Experimental(0.564), perf.HeapMB.Experimental(251), perf.MemoryUtil.Experimental(0.642), perf.Nodes.Experimental(328),
		},
	},
}
