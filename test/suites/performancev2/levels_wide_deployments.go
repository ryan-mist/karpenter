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

// wideDeploymentsLevels follow kwok_provider.go's rule.
//   - kwok: calibration/kwok.json, 5 EKS baselines with the rv2013 control counted as a baseline, and planted regressions
//     mconc1, rv1973, m959, m1966 and m1822. Stable only where a regression breaches the bound by 10% or more. Stable
//     pod-ready bounds are whole seconds and at least 1.5x the slowest baseline; heap, max and wall time stay Experimental.
//   - kwok-ci: Experimental only, from perfv1's 35-37 GitHub-runner runs of the same workload (the same heap and CPU
//     series), until perf-periodic has the 20 runs a Stable gate needs.
var wideDeploymentsLevels = scenarioLevels{
	KWOK: perf.Levels{
		"wideDeployments/scaleOut": {
			perf.PodReady.P95.Stable(23), perf.PodReady.P99.Stable(26), perf.PodReady.Max.Experimental(26), perf.CPUUtil.Experimental(0.692), perf.MemoryUtil.Experimental(0.683), perf.Nodes.Experimental(198), perf.HeapMB.Experimental(192), perf.CPUCores.Experimental(0.992),
		},
		"wideDeployments/consolidation": {
			perf.TotalTimeSeconds.Experimental(1108), perf.CPUUtil.Experimental(0.771), perf.MemoryUtil.Experimental(0.777), perf.Nodes.Experimental(128), perf.PodsDisrupted.Experimental(252), perf.HeapMB.Experimental(170), perf.CPUCores.Experimental(0.373),
		},
	},
	KWOKCI: perf.Levels{
		"wideDeployments/scaleOut": {
			perf.CPUCores.Experimental(0.71), perf.CPUUtil.Experimental(0.668), perf.HeapMB.Experimental(155), perf.MemoryUtil.Experimental(0.671), perf.Nodes.Experimental(278),
		},
		"wideDeployments/consolidation": {
			perf.CPUCores.Experimental(0.48), perf.CPUUtil.Experimental(0.603), perf.HeapMB.Experimental(146), perf.MemoryUtil.Experimental(0.604), perf.Nodes.Experimental(228),
		},
	},
}
