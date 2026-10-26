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

// Differential test of the emptiness core (Emptiness.Compute): on generated clusters with empty nodes,
// Emptiness.ComputeCommands (the core with the controller's options) and a verbatim copy of ComputeCommands before
// the core existed (disruption.LegacyEmptinessComputeCommands) produce the same command, publish the same events and
// leave the same consolidation state behind. Between steps, the command is started and one more node is emptied, so
// in-flight deletions, budgets that run out, and the consolidated-state shortcut are covered. Seeds and node counts as
// in drift_core_test.go.

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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// emptyNode deletes the pods of a random node that has some and isn't being disrupted, and updates cluster state.
func (e *coreEnv) emptyNode(r *rand.Rand) {
	GinkgoHelper()
	var hosts []string
	for _, sn := range e.cluster.DeepCopyNodes() {
		if sn.Node != nil && !sn.MarkedForDeletion() {
			hosts = append(hosts, sn.Node.Name)
		}
	}
	pods := &corev1.PodList{}
	Expect(e.client.List(ctx, pods, client.HasLabels{genPodLabel})).To(Succeed())
	hosts = lo.Filter(hosts, func(h string, _ int) bool {
		return lo.SomeBy(pods.Items, func(p corev1.Pod) bool { return p.Spec.NodeName == h })
	})
	if len(hosts) == 0 {
		return
	}
	host := hosts[r.Intn(len(hosts))]
	for i := range pods.Items {
		if p := &pods.Items[i]; p.Spec.NodeName == host {
			Expect(env.Client.Delete(ctx, p, client.GracePeriodSeconds(0))).To(Succeed())
			Eventually(func() bool {
				return e.client.Get(ctx, client.ObjectKeyFromObject(p), &corev1.Pod{}) != nil
			}).WithTimeout(time.Minute).Should(BeTrue())
			ExpectReconcileSucceeded(e.ctx, e.podCtrl, client.ObjectKeyFromObject(p))
		}
	}
}

var _ = Describe("Emptiness Core", func() {
	var e *coreEnv
	AfterEach(func() {
		if e != nil {
			e.stop()
			e = nil
		}
	})

	Context("differential against ComputeCommands before the core", func() {
		budgets := func(_ int, r *rand.Rand) string { return []string{"1", "10%", "50%", "100%"}[r.Intn(4)] }
		nodes := coreDiffNodes([]int{10, 25, 50})
		for seed := int64(2001); seed <= int64(2000+coreDiffEnv("KARPENTER_CORE_DIFF_SEEDS", 8)); seed++ {
			spec := randomGenSpec(seed, nodes, budgets)
			spec.ConsolidateAfter = "0s"
			spec.EmptyFrac = []float64{0, 0.1, 0.3, 0.6}[seed%4]
			// Nodes of different prices, so the savings-ratio order decides which empty nodes a budget keeps.
			spec.MixedTypes = true
			It(fmt.Sprintf("seed=%d %+v", seed, spec), func() {
				e = newCoreEnv(ctx, genInstanceTypes(genZones(spec.Zones)))
				gc := e.buildGen(spec)
				e.stepClock()
				em := coreMethod[*disruption.Emptiness](e)
				r := rand.New(rand.NewSource(spec.Seed))
				// A variant's outcome: its commands, the events it published (as a multiset: candidate builds publish in
				// cluster-state order), and whether it recorded that there was nothing to do. (Not the recorded time, which
				// moves with the fake clock's validation steps, see Cluster.ConsolidationState.)
				type result struct {
					sig    string
					ties   string // the candidates as (NodePool, savings ratio): equal for results that differ only in ties
					events []string
					marked bool
				}
				key := func(r result) string { return fmt.Sprintf("%s|%v|%v", r.sig, r.marked, r.events) }
				// The events a validation's candidate builds publish vary between samples of the same state, so the tie
				// check leaves them out.
				tieKey := func(r result) string { return fmt.Sprintf("%s|%v", r.ties, r.marked) }
				// run runs one variant from an unmarked consolidation state, and resets it afterwards.
				run := func(f func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error)) ([]disruption.Command, result) {
					var res result
					cmds, _ := e.compute(em, func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
						e.recorder.Reset()
						defer func() { res.events = eventStrings(e.recorder.Events()); sort.Strings(res.events) }()
						return f(budgets, cands)
					})
					res.sig, res.marked = gc.cmdsSignature(cmds), !disruption.MarkedConsolidated(em).IsZero()
					ties := lo.FlatMap(cmds, func(c disruption.Command, _ int) []string {
						return lo.Map(c.Candidates, func(c *disruption.Candidate, _ int) string {
							return fmt.Sprintf("%s/%g", c.NodePool.Name, c.SavingsRatio())
						})
					})
					sort.Strings(ties)
					res.ties = fmt.Sprint(len(cmds), ties)
					disruption.ResetConsolidated(em)
					return cmds, res
				}
				legacy := func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
					return disruption.LegacyEmptinessComputeCommands(e.ctx, em, budgets, cands...)
				}
				current := func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
					return em.ComputeCommands(e.ctx, budgets, cands...)
				}
				for step := range 6 {
					ref, want := run(legacy)
					_, got := run(current)
					// sortCandidates uses sort.Slice on the savings ratio, so ties between equal nodes are broken at random
					// and a budget picks a random subset of them. A result passes if upstream produces it on the state
					// (upstream is resampled up to 16 times), or if it differs from upstream's only in which of the tied
					// nodes it picked.
					upstream := map[string]bool{key(want): true}
					upstreamTies := map[string]bool{tieKey(want): true}
					for i := 0; i < 16 && !upstream[key(got)]; i++ {
						_, again := run(legacy)
						upstream[key(again)] = true
						upstreamTies[tieKey(again)] = true
					}
					exact := upstream[key(got)]
					emitCoreDiff(map[string]any{"test": "emptiness", "spec": spec, "step": step,
						"refCandidates": lo.SumBy(ref, func(c disruption.Command) int { return len(c.Candidates) }),
						"marked":        want.marked, "upstreamOutcomes": len(upstream), "equal": exact, "equalUpToTies": upstreamTies[tieKey(got)]})
					if !exact {
						Expect(upstreamTies).To(HaveKey(tieKey(got)), "emptiness core differs from upstream beyond ties: %s", got.sig)
					}
					if len(upstream) > 1 {
						break // the state paths diverge from here
					}
					// The consolidated-state shortcut: once marked, both return nothing until the state changes.
					if want.marked {
						_, again := run(func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
							_, _ = legacy(budgets, cands)
							return em.ComputeCommands(e.ctx, budgets, cands...)
						})
						Expect(again.sig).To(BeEmpty())
					}
					if len(ref) > 0 {
						_, failed := e.startCommands(em, ref)
						Expect(failed).To(BeZero())
					}
					e.emptyNode(r)
				}
			})
		}
	})

	It("consults and records Dedup", func() {
		spec := genSpec{Seed: 4, Nodes: 6, MaxPodsPerNode: 4, Pools: 1, Zones: 1, Budget: "100%", Util: 0.5, ConsolidateAfter: "0s"}
		e = newCoreEnv(ctx, genInstanceTypes(genZones(1)))
		e.buildGen(spec)
		em := coreMethod[*disruption.Emptiness](e)
		all := func(context.Context, *disruption.Candidate) bool { return true }
		cands, _, err := disruption.GetCandidatesWithTotals(e.ctx, e.cluster, e.client, e.recorder, env.Clock, cloudProvider, all, em.Class(), e.queue, e.clusterCost)
		Expect(err).ToNot(HaveOccurred())
		Expect(cands).To(HaveLen(6)) // none of them empty
		d := &fakeDedup{}
		o := em.LegacyOptions()
		o.Dedup = d
		res, err := em.Compute(e.ctx, map[string]int{cands[0].NodePool.Name: 10}, o, cands...)
		Expect(err).ToNot(HaveOccurred())
		Expect(res.Commands).To(BeEmpty())
		Expect(d.marked).To(Equal(1))
		d.consolidated = true
		res, err = em.Compute(e.ctx, map[string]int{cands[0].NodePool.Name: 10}, o, cands...)
		Expect(err).ToNot(HaveOccurred())
		Expect(res.Commands).To(BeEmpty())
		Expect(d.marked).To(Equal(1))
	})
	It("returns the command unvalidated without a validator", func() {
		spec := genSpec{Seed: 3, Nodes: 10, MaxPodsPerNode: 4, Pools: 1, Zones: 1, Budget: "100%", EmptyFrac: 1, Util: 0.5, ConsolidateAfter: "0s"}
		e = newCoreEnv(ctx, genInstanceTypes(genZones(1)))
		e.buildGen(spec)
		em := coreMethod[*disruption.Emptiness](e)
		var res disruption.CoreResult
		e.compute(em, func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
			o := em.LegacyOptions()
			o.Validator, o.Dedup = nil, nil
			var err error
			res, err = em.Compute(e.ctx, budgets, o, cands...)
			return res.Commands, err
		})
		// No clock stepping: an inline validation would block.
		Expect(res.Commands).To(HaveLen(1))
		Expect(res.Commands[0].Candidates).To(HaveLen(10))
		Expect(res.Commands[0].Decision()).To(Equal(disruption.DeleteDecision))
		Expect(disruption.MarkedConsolidated(em)).To(BeZero())
		Expect(em.Validator()).ToNot(BeNil())
	})
})

type fakeDedup struct {
	consolidated bool
	marked       int
}

func (d *fakeDedup) IsConsolidated() bool { return d.consolidated }
func (d *fakeDedup) MarkConsolidated()    { d.marked++ }
