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

package experiment

import (
	"fmt"
	"strings"
	"testing"
)

// TestCoverage2434RealBaseline runs the REAL shipping MultiNodeConsolidation
// (ComputeCommands) — not the reimplemented baselineStrategy — on genCluster2434
// to confirm the SHIPPING algorithm exhibits #2434 (misses the 2->1 pair).
func TestCoverage2434RealBaseline(t *testing.T) {
	var b strings.Builder
	fmt.Fprintf(&b, "\nReal ComputeCommands on genCluster2434 (OPT = dedicated 2->1 = $0.10):\n")
	for _, fp := range []int{39, 59} { // N ~= 80, 120
		h := newHarness(t)
		target, _ := h.genCluster2434(t, params2434{fillerPools: fp, fillerNodesPerPool: 2, dedicatedZone: "test-zone-1"})
		cands := h.allCandidates(t)
		targetKey := targetKeyOf(cands, target)
		cmds := h.baselineRun(t) // REAL MultiNodeConsolidation.ComputeCommands

		if len(cmds) == 0 {
			fmt.Fprintf(&b, "  N=%-3d : NO command returned (no-op) -> MISSES the pair\n", len(cands))
			continue
		}
		var total float64
		foundPair := false
		for _, cmd := range cmds {
			total += commandSavings(cmd)
			if subsetKey(cmd.Candidates) == targetKey {
				foundPair = true
			}
			fmt.Fprintf(&b, "  N=%-3d : decision=%-7s candidates=%d replacements=%d savings=$%.4f\n",
				len(cands), cmd.Decision(), len(cmd.Candidates), len(cmd.Replacements), commandSavings(cmd))
		}
		fmt.Fprintf(&b, "  N=%-3d : foundDedicatedPair=%v totalSavings=$%.4f\n", len(cands), foundPair, total)
	}
	t.Log(b.String())
}

// TestScale2434 records oracle-call counts and pair-coverage as N grows, for the
// shipping-shape globalBinary vs the two front-ends that find the pair.
func TestScale2434(t *testing.T) {
	type rec struct {
		n                                    int
		gbCalls, psgCalls, simCalls          int
		gbCov, psgCov, simCov                bool
		gbSave, psgSave, simSave             float64
	}
	var recs []rec
	for _, fp := range []int{19, 39, 59} { // N ~= 40, 80, 120
		h := newHarness(t)
		target, _ := h.genCluster2434(t, params2434{fillerPools: fp, fillerNodesPerPool: 2, dedicatedZone: "test-zone-1"})
		cands := h.allCandidates(t)
		tk := targetKeyOf(cands, target)
		budget := 4 * len(cands)
		gb := h.sGlobalBinary(t, cands, tk)
		psg := h.sPerSchedGroupExhaustive(t, cands, tk)
		sim := h.sSimilarity(t, cands, tk, budget)
		recs = append(recs, rec{
			n: len(cands),
			gbCalls: gb.calls, psgCalls: psg.calls, simCalls: sim.calls,
			gbCov: gb.covered, psgCov: psg.covered, simCov: sim.covered,
			gbSave: gb.savings, psgSave: psg.savings, simSave: sim.savings,
		})
	}
	var b strings.Builder
	fmt.Fprintf(&b, "\nScale note on genCluster2434 (calls; covered=finds the $0.10 pair):\n")
	fmt.Fprintf(&b, "%-5s | %-24s | %-24s | %-24s\n", "N", "globalBinary", "perSchedGroup-exhaustive", "similarity")
	fmt.Fprintln(&b, strings.Repeat("-", 90))
	for _, r := range recs {
		fmt.Fprintf(&b, "%-5d | calls=%-3d cov=%-5v save=%.2f | calls=%-3d cov=%-5v save=%.2f | calls=%-3d cov=%-5v save=%.2f\n",
			r.n,
			r.gbCalls, r.gbCov, r.gbSave,
			r.psgCalls, r.psgCov, r.psgSave,
			r.simCalls, r.simCov, r.simSave)
	}
	t.Log(b.String())
}
