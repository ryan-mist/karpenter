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

package disruption_test

// Differential test of the consolidation cores (MultiNodeConsolidation.Compute, SingleNodeConsolidation.Compute): on
// generated underutilized clusters, each method's ComputeCommands (the core with the controller's options) and a
// verbatim copy of it before the core existed (consolidation_export_test.go) produce the same command, publish the same
// events, and leave the same consolidation state behind. NodePools are WhenEmptyOrUnderutilized or Balanced (so the
// balanced evaluator, set per pass as the controller does, approves or rejects). Between steps the reference command is
// started and its replacement launched. Candidate order has ties (sort.Slice on the savings ratio, NodePool order from a
// map), so results are resampled as in drift_core_test.go, and a result that differs only in which of equal nodes it
// picked passes too. Seeds and node counts as in drift_core_test.go.

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"

	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
)

var _ = Describe("Consolidation Core", func() {
	var e *coreEnv
	AfterEach(func() {
		if e != nil {
			e.stop()
			e = nil
		}
	})

	type variant struct {
		name    string
		method  func(e *coreEnv) disruption.Method
		legacy  func(ctx context.Context, m disruption.Method, budgets map[string]int, cands ...*disruption.Candidate) ([]disruption.Command, error)
		seedOff int64
	}
	variants := []variant{
		{"multi", func(e *coreEnv) disruption.Method { return coreMethod[*disruption.MultiNodeConsolidation](e) },
			func(ctx context.Context, m disruption.Method, b map[string]int, c ...*disruption.Candidate) ([]disruption.Command, error) {
				return disruption.LegacyMultiNodeComputeCommands(ctx, m.(*disruption.MultiNodeConsolidation), b, c...)
			}, 5001},
		{"single", func(e *coreEnv) disruption.Method { return coreMethod[*disruption.SingleNodeConsolidation](e) },
			func(ctx context.Context, m disruption.Method, b map[string]int, c ...*disruption.Candidate) ([]disruption.Command, error) {
				return disruption.LegacySingleNodeComputeCommands(ctx, m.(*disruption.SingleNodeConsolidation), b, c...)
			}, 6001},
	}
	budgets := func(_ int, r *rand.Rand) string { return []string{"1", "10%", "50%", "100%"}[r.Intn(4)] }
	nodes := coreDiffNodes([]int{10, 25, 50})
	for _, v := range variants {
		for _, seed := range coreSeeds(v.seedOff, 8) {
			spec := randomGenSpec(seed, nodes, budgets)
			spec.DriftFrac, spec.ConsolidateAfter = 0, "0s"
			spec.Util = []float64{0.2, 0.4, 0.6}[seed%3]
			spec.ConsolidationPolicy = lo.Ternary(seed%2 == 0, v1.ConsolidationPolicyBalanced, v1.ConsolidationPolicyWhenEmptyOrUnderutilized)
			It(fmt.Sprintf("%s matches ComputeCommands before the core (seed=%d %+v)", v.name, seed, spec), func() {
				e = newCoreEnv(ctx, genInstanceTypesOf(genZones(spec.Zones), v1.CapacityTypeOnDemand, v1.CapacityTypeSpot))
				gc := e.buildGen(spec)
				e.stepClock()
				m := v.method(e)
				type result struct {
					sig, ties string
					events    []string
					marked    bool
				}
				key := func(r result) string { return fmt.Sprint(r.sig, r.marked, r.events) }
				// Single publishes an Unconsolidatable event for every candidate it tries before the one it acts on, so
				// with ties the events differ as well; ties compares the commands without node identities.
				tieKey := func(r result) string { return fmt.Sprint(r.ties, r.marked) }
				run := func(f func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error)) ([]disruption.Command, result) {
					var res result
					cmds, _ := e.compute(m, func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
						e.recorder.Reset()
						defer func() { res.events = eventStrings(e.recorder.Events()); sort.Strings(res.events) }()
						return f(budgets, cands)
					})
					res.sig, res.marked = gc.cmdsSignature(cmds), !disruption.MarkedConsolidated(m).IsZero()
					res.ties = fmt.Sprint(lo.Map(cmds, func(c disruption.Command, _ int) string { return tieSignature(c) }))
					disruption.ResetConsolidated(m)
					return cmds, res
				}
				legacy := func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
					return v.legacy(e.ctx, m, budgets, cands...)
				}
				current := func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
					return m.ComputeCommands(e.ctx, budgets, cands...)
				}
				for step := range 4 {
					ref, want := run(legacy)
					_, got := run(current)
					upstream := map[string]bool{key(want): true}
					upstreamTies := map[string]bool{tieKey(want): true}
					// Which of several NodePools Single tries first comes from a map, so upstream has up to (#pools)! orders:
					// sample enough to see them.
					for i := 0; i < 48 && !upstream[key(got)] && !upstreamTies[tieKey(got)]; i++ {
						_, again := run(legacy)
						upstream[key(again)] = true
						upstreamTies[tieKey(again)] = true
					}
					exact := upstream[key(got)]
					emitCoreDiff(map[string]any{"test": v.name, "spec": spec, "step": step, "upstreamOutcomes": len(upstream), "equal": exact,
						"equalUpToTies": upstreamTies[tieKey(got)], "marked": want.marked,
						"refDecision": lo.Map(ref, func(c disruption.Command, _ int) string { return string(c.Decision()) })})
					if !exact {
						Expect(upstreamTies).To(HaveKey(tieKey(got)), "%s core differs from upstream beyond ties: %s (%s)", v.name, got.sig, tieKey(got))
						break // the state paths diverge from here
					}
					if len(ref) == 0 {
						break
					}
					started, failed := e.startCommands(m, ref)
					Expect(failed).To(BeZero())
					e.genLaunch(gc, started)
					// Starting a command nominates its destinations for 20 s, and every validation steps the clock by 15 s,
					// so later samples of the next state would see the nominations expire. Let them expire first.
					env.Clock.Step(time.Minute)
				}
			})
		}
	}

	It("returns multi-node commands unvalidated without a validator and uses the evaluator it is given", func() {
		spec := genSpec{Seed: 21, Nodes: 12, MaxPodsPerNode: 4, Pools: 1, Zones: 1, Budget: "100%", Util: 0.2, ConsolidateAfter: "0s"}
		e = newCoreEnv(ctx, genInstanceTypes(genZones(1)))
		e.buildGen(spec)
		m := coreMethod[*disruption.MultiNodeConsolidation](e)
		run := func(evaluator disruption.Evaluator) disruption.CoreResult {
			var res disruption.CoreResult
			e.compute(m, func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
				o := m.LegacyOptions()
				o.Validator, o.Dedup, o.Evaluator = nil, nil, evaluator
				var err error
				res, err = m.Compute(e.ctx, budgets, o, cands...)
				return res.Commands, err
			})
			return res
		}
		// No clock stepping: an inline validation would block.
		res := run(nil)
		Expect(res.Commands).To(HaveLen(1))
		Expect(len(res.Commands[0].Candidates)).To(BeNumerically(">", 1))
		Expect(run(rejectAll{}).Commands).To(BeEmpty())
		Expect(m.Validator()).ToNot(BeNil())
	})
})

// rejectAll is an Evaluator that rejects every command.
type rejectAll struct{}

func (rejectAll) ApproveCommand(context.Context, disruption.Command) (bool, map[string]disruption.ScoreResult) {
	return false, nil
}
func (rejectAll) CanPassThreshold(*disruption.Candidate) bool { return false }
func (rejectAll) EmitMultiNodeEvents(context.Context, disruption.Command, map[string]disruption.ScoreResult, bool) {
}

// tieSignature is a command without node identities: its decision, its candidates as (NodePool, savings ratio), and
// its replacements' requirements (without the hostname placeholder) and instance types.
func tieSignature(c disruption.Command) string {
	cands := lo.Map(c.Candidates, func(c *disruption.Candidate, _ int) string {
		return fmt.Sprintf("%s/%g", c.NodePool.Name, c.SavingsRatio())
	})
	sort.Strings(cands)
	reps := lo.Map(c.Results.NewNodeClaims, func(nc *pscheduling.NodeClaim, _ int) string {
		var reqs []string
		for _, k := range nc.Requirements.Keys().UnsortedList() {
			if k == corev1.LabelHostname {
				continue
			}
			vals := nc.Requirements.Get(k).Values()
			sort.Strings(vals)
			reqs = append(reqs, fmt.Sprintf("%s %s %v", k, nc.Requirements.Get(k).Operator(), vals))
		}
		sort.Strings(reqs)
		its := lo.Map(nc.InstanceTypeOptions, func(it *cloudprovider.InstanceType, _ int) string { return it.Name })
		sort.Strings(its)
		return fmt.Sprintf("{%v %v}", reqs, its)
	})
	sort.Strings(reps)
	return fmt.Sprintf("%s %v %v", c.Decision(), cands, reps)
}
