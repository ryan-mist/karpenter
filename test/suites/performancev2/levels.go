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

// scenarioLevels are one scenario's levels for both core providers. Scenarios migrated from perfv1 keep theirs in
// levels_<scenario>.go, so each one is reviewed and calibrated on its own.
//
// KWOK levels are what `go run ./test/suites/performancev2/cmd/calibrate -dir runs/ -levels` prints for the scenario's
// runs in calibration/kwok/<scenario>.json: the rule in perf/suggest.go, with the rv2013 control counted as a
// baseline and Stable only where a planted regression breaches the bound by 10% or more.
//
// KWOKCI levels are Experimental, with bounds from the same rule over perfv1's 35-37 GitHub-runner runs of the same
// workload (the same heap, CPU, utilization and node series; perfv1 had no pod latency). They stand in until
// perf-periodic has the 20 runs a Stable gate needs; recalibrate them from those reports with cmd/calibrate.
type scenarioLevels struct{ KWOK, KWOKCI perf.Levels }

var migrated = []scenarioLevels{}

// withMigrated adds every migrated scenario's levels, picked by pick, to base.
func withMigrated(base perf.Levels, pick func(scenarioLevels) perf.Levels) perf.Levels {
	for _, l := range migrated {
		base = base.With(pick(l))
	}
	return base
}
