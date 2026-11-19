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

// interferenceLevels follow kwok_provider.go's rule.
//   - kwok: calibration/kwok.json, 5 EKS baselines with the rv2013 control counted as a baseline, and planted regressions
//     mconc1, rv1973, m959, m1966 and m1822. Stable only where a regression breaches the bound by 10% or more. Stable
//     pod-ready bounds are whole seconds and at least 1.5x the slowest baseline; heap, max and wall time stay Experimental.
//   - kwok-ci: Experimental only, from perfv1's 35-37 GitHub-runner runs of the same workload (the same heap and CPU
//     series), until perf-periodic has the 20 runs a Stable gate needs.
var interferenceLevels = scenarioLevels{
	KWOK: perf.Levels{
		"interference/scaleOut": {
			perf.PodReady.P95.Experimental(24), perf.PodReady.P99.Experimental(26), perf.PodReady.Max.Experimental(26), perf.CPUUtil.Experimental(0.481), perf.MemoryUtil.Experimental(0.66), perf.Nodes.Experimental(600), perf.HeapMB.Experimental(266), perf.CPUCores.Experimental(1.493),
		},
		"interference/interference": {
			perf.PodReady.P95.Experimental(27), perf.PodReady.P99.Experimental(30), perf.PodReady.Max.Experimental(30), perf.CPUUtil.Experimental(0.637), perf.MemoryUtil.Experimental(0.713), perf.Nodes.Experimental(1200), perf.HeapMB.Experimental(491), perf.CPUCores.Experimental(1.906),
		},
		"interference/consolidation": {
			perf.TotalTimeSeconds.Experimental(300), perf.CPUUtil.Experimental(0.65), perf.MemoryUtil.Experimental(0.741), perf.Nodes.Experimental(720), perf.HeapMB.Experimental(370), perf.CPUCores.Experimental(2.08),
		},
	},
	KWOKCI: perf.Levels{
		"interference/scaleOut": {
			perf.CPUCores.Experimental(0.937), perf.CPUUtil.Experimental(0.458), perf.HeapMB.Experimental(271), perf.MemoryUtil.Experimental(0.67), perf.Nodes.Experimental(600),
		},
		"interference/interference": {
			perf.CPUCores.Experimental(1.511), perf.CPUUtil.Experimental(0.539), perf.HeapMB.Experimental(518), perf.MemoryUtil.Experimental(0.564), perf.Nodes.Experimental(1172),
		},
		"interference/consolidation": {
			perf.CPUCores.Experimental(0.667), perf.CPUUtil.Experimental(0.53), perf.HeapMB.Experimental(379), perf.MemoryUtil.Experimental(0.596), perf.Nodes.Experimental(720),
		},
	},
}
