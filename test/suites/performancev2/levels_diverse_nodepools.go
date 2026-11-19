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

// diverseNodePoolsLevels follow kwok_provider.go's rule.
//   - kwok: calibration/kwok.json, 5 EKS baselines with the rv2013 control counted as a baseline, and planted regressions
//     mconc1, rv1973, m959, m1966 and m1822. Stable only where a regression breaches the bound by 10% or more. Stable
//     pod-ready bounds are whole seconds and at least 1.5x the slowest baseline; heap, max and wall time stay Experimental.
//   - kwok-ci: Experimental only, from perfv1's 35-37 GitHub-runner runs of the same workload (the same heap and CPU
//     series), until perf-periodic has the 20 runs a Stable gate needs.
var diverseNodePoolsLevels = scenarioLevels{
	KWOK: perf.Levels{
		"diverseNodePools/scaleOut": {
			perf.PodReady.P95.Stable(6), perf.PodReady.P99.Stable(6), perf.PodReady.Max.Experimental(6), perf.CPUUtil.Experimental(0.641), perf.MemoryUtil.Experimental(0.681), perf.Nodes.Experimental(5), perf.HeapMB.Experimental(89.3), perf.CPUCores.Experimental(0.814),
		},
		"diverseNodePools/consolidation": {
			perf.TotalTimeSeconds.Experimental(297), perf.CPUUtil.Experimental(0.343), perf.MemoryUtil.Experimental(0.311), perf.Nodes.Experimental(5), perf.PodsDisrupted.Experimental(13.2), perf.HeapMB.Experimental(82.1), perf.CPUCores.Experimental(0.287),
		},
	},
	KWOKCI: perf.Levels{
		"diverseNodePools/scaleOut":      {}, // measure only: perfv1 never ran it on GitHub runners
		"diverseNodePools/consolidation": {}, // measure only: perfv1 never ran it on GitHub runners
	},
}
