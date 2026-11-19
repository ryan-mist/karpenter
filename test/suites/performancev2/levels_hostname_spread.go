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

// hostnameSpreadLevels follow kwok_provider.go's rule.
//   - kwok: calibration/kwok.json, 5 EKS baselines with the rv2013 control counted as a baseline, and planted regressions
//     mconc1, rv1973, m959, m1966 and m1822. Stable only where a regression breaches the bound by 10% or more. Stable
//     pod-ready bounds are whole seconds and at least 1.5x the slowest baseline; heap, max and wall time stay Experimental.
//   - kwok-ci: Experimental only, from perfv1's 35-37 GitHub-runner runs of the same workload (the same heap and CPU
//     series), until perf-periodic has the 20 runs a Stable gate needs.
var hostnameSpreadLevels = scenarioLevels{
	KWOK: perf.Levels{
		"hostnameSpread/scaleOut": {
			perf.PodReady.P95.Experimental(71), perf.PodReady.P99.Experimental(72), perf.PodReady.Max.Experimental(72), perf.CPUUtil.Experimental(0.723), perf.MemoryUtil.Experimental(0.767), perf.Nodes.Experimental(1249), perf.HeapMB.Experimental(507), perf.CPUCores.Experimental(1.769),
		},
		"hostnameSpread/consolidation": {
			perf.TotalTimeSeconds.Experimental(1572), perf.CPUUtil.Experimental(0.733), perf.MemoryUtil.Experimental(0.771), perf.Nodes.Experimental(840), perf.PodsDisrupted.Experimental(248), perf.HeapMB.Experimental(430), perf.CPUCores.Experimental(1.81),
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
