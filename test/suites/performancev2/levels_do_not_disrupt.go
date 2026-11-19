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

// doNotDisruptLevels follow kwok_provider.go's rule.
//   - kwok: calibration/kwok.json, 5 EKS baselines with the rv2013 control counted as a baseline, and planted regressions
//     mconc1, rv1973, m959, m1966 and m1822. Stable only where a regression breaches the bound by 10% or more. Stable
//     pod-ready bounds are whole seconds and at least 1.5x the slowest baseline; heap, max and wall time stay Experimental.
//   - kwok-ci: Experimental only, from perfv1's 35-37 GitHub-runner runs of the same workload (the same heap and CPU
//     series), until perf-periodic has the 20 runs a Stable gate needs.
var doNotDisruptLevels = scenarioLevels{
	KWOK: perf.Levels{
		"doNotDisrupt/scaleOut": {
			perf.PodReady.P95.Experimental(33), perf.PodReady.P99.Experimental(45), perf.PodReady.Max.Experimental(45), perf.CPUUtil.Experimental(0.684), perf.MemoryUtil.Experimental(0.741), perf.Nodes.Experimental(662), perf.HeapMB.Experimental(283), perf.CPUCores.Experimental(1.41),
		},
		"doNotDisrupt/consolidation": {
			perf.TotalTimeSeconds.Experimental(1549), perf.CPUUtil.Experimental(0.694), perf.MemoryUtil.Experimental(0.733), perf.Nodes.Experimental(362), perf.PodsDisrupted.Experimental(167), perf.HeapMB.Experimental(202), perf.CPUCores.Experimental(0.638),
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
