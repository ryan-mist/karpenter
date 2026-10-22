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

// Tests of the drift core (Drift.Compute) and the batching primitives it uses.
//
//  1. Differential, batch=1: on generated clusters, Drift.ComputeCommands (the core with the controller's options),
//     a verbatim copy of ComputeCommands before the core existed (disruption.LegacyDriftComputeCommands), and the
//     batched path forced onto single candidates produce the same commands. ComputeCommands also publishes the same
//     events. Upstream is not always deterministic (zone topology spread picks the first of tied domains while
//     iterating a set), so a result passes if upstream produces it on the state; upstream is resampled up to 16 times.
//     The comparison repeats after each of up to 3 started commands, so in-flight work is covered.
//  2. Invariants, batch=K: batched drift runs to quiescence and is compared with the serial reference on an
//     identically generated cluster: no hard-constraint violation in the plan or the provisioner's end state, no
//     command above ComponentCap candidates, and no more capacity than serial.
//  3. Split hazard (conflicts-topology.md #3 as drift), chunking, SimulateBatch, BatchSizer and the coupling check.
//
// KARPENTER_CORE_DIFF_SEEDS=N runs N seeds per generated table (KARPENTER_CORE_DIFF_SEED_LIST=a,b runs those seeds of
// every table that has them), KARPENTER_CORE_DIFF_NODES=a,b,c picks the node counts and KARPENTER_CORE_DIFF_OUT=<file>
// appends one JSON record per state.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

func coreDiffNodes(def []int) []int {
	raw := os.Getenv("KARPENTER_CORE_DIFF_NODES")
	if raw == "" {
		return def
	}
	return lo.Map(strings.Split(raw, ","), func(s string, _ int) int { return lo.Must(strconv.Atoi(strings.TrimSpace(s))) })
}

// eventStrings identifies events by reason, message and the objects they are about. A "Not all pods would schedule"
// message lists up to 5 of the failing pods, picked from a map, so only the number of failing pods is kept.
func eventStrings(evts []events.Event) []string {
	others := regexp.MustCompile(`and (\d+) other\(s\)`)
	return lo.Map(evts, func(e events.Event, _ int) string {
		msg := e.Message
		if rest, ok := strings.CutPrefix(msg, "Not all pods would schedule, "); ok {
			n := strings.Count(rest, " => ")
			if m := others.FindStringSubmatch(rest); m != nil {
				n += lo.Must(strconv.Atoi(m[1]))
			}
			msg = fmt.Sprintf("Not all pods would schedule (%d pods)", n)
		}
		return fmt.Sprintf("%s %s %v", e.Reason, msg, e.DedupeValues)
	})
}

type driftRun struct {
	Rounds        int      `json:"rounds"`
	Commands      int      `json:"commands"`
	Disrupted     []string `json:"-"`
	Candidates    int      `json:"disrupted"`
	NodeClaims    int      `json:"nodeClaims"`
	MaxCommand    int      `json:"maxCommandCandidates"`
	Deferred      int      `json:"deferred"`
	Sims          uint64   `json:"sims"`
	Planned       endState `json:"planned"`
	Final         endState `json:"final"`
	R1Planned     endState `json:"round1Planned"`
	R1Final       endState `json:"round1Final"`
	StartFailures int      `json:"startFailures"`
	// FinalAllowed is the sum over pools of the drift budget at quiescence.
	FinalAllowed int `json:"finalAllowed"`
}

// runDrift repeats drift rounds (compute with opts, start, launch) until a round starts nothing. opts nil is the
// disruption controller's ComputeCommands.
func (e *coreEnv) runDrift(gc *genCluster, opts func(*disruption.CoreOptions)) driftRun {
	GinkgoHelper()
	drift := e.drift()
	var res driftRun
	var all []*disruption.Command
	sims0 := coreSimCount()
	stepped := false
	for round := 0; round < 10*gc.spec.Nodes+10; round++ {
		cmds, _ := e.compute(drift, func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
			if opts == nil {
				return drift.ComputeCommands(e.ctx, budgets, cands...)
			}
			o := drift.LegacyOptions()
			opts(&o)
			r, err := drift.Compute(e.ctx, budgets, o, cands...)
			res.Deferred += len(r.Deferred)
			return r.Commands, err
		})
		if len(cmds) == 0 {
			// Nominated destinations aren't candidates for the nomination window, so step past it once before calling
			// the loop quiescent.
			if !stepped {
				env.Clock.Step(time.Minute)
				stepped = true
				continue
			}
			break
		}
		stepped = false
		started, failed := e.startCommands(drift, cmds)
		res.StartFailures += failed
		if len(started) == 0 {
			break
		}
		e.genLaunch(gc, started)
		all = append(all, started...)
		res.Rounds++
		if round == 0 {
			res.R1Planned, res.R1Final = e.evaluate(gc, all)
		}
		if os.Getenv("KARPENTER_CORE_DIFF_DEBUG") != "" {
			for _, c := range started {
				fmt.Fprintf(GinkgoWriter, "DEBUG round=%d batch=%s cands=%v reps=%v\n", round, c.BatchID.String()[:4],
					lo.Map(c.Candidates, func(x *disruption.Candidate, _ int) string {
						return gc.nodeIdx[x.ProviderID()] + "/" + x.NodePool.Name[:6]
					}),
					lo.Map(c.Replacements, func(r *disruption.Replacement, _ int) string {
						nc := &v1.NodeClaim{}
						_ = env.Client.Get(ctx, client.ObjectKey{Name: r.Name}, nc)
						return nc.Labels[v1.NodePoolLabelKey][:6] + ":" + nc.Labels[corev1.LabelInstanceTypeStable] + ":" + fmt.Sprint(len(r.InstanceTypeOptions))
					}))
			}
		}
	}
	res.Sims = coreSimCount() - sims0
	res.Commands = len(all)
	res.Disrupted = lo.FlatMap(all, func(c *disruption.Command, _ int) []string {
		return lo.Map(c.Candidates, func(cand *disruption.Candidate, _ int) string { return gc.nodeIdx[cand.ProviderID()] })
	})
	res.Candidates = len(res.Disrupted)
	res.MaxCommand = lo.Max(lo.Map(all, func(c *disruption.Command, _ int) int { return len(c.Candidates) }))
	res.NodeClaims = lo.SumBy(all, func(c *disruption.Command) int { return len(c.Replacements) })
	res.Planned, res.Final = e.evaluate(gc, all)
	numNodes, _ := disruption.NodePoolStats(e.cluster, v1.DisruptionReasonDrifted)
	for _, np := range gc.pools {
		current := &v1.NodePool{}
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(np), current)).To(Succeed())
		res.FinalAllowed += lo.Must(current.GetAllowedDisruptionsByReason(env.Clock, numNodes[np.Name], v1.DisruptionReasonDrifted))
	}
	return res
}

func batchAll(o *disruption.CoreOptions) { o.Batch = 1 << 20 }

func batchSized(o *disruption.CoreOptions) {
	o.Batch = 1 << 20
	o.Sizer = disruption.NewBatchSizer(disruption.BatchSimDeadline)
	o.Deadline = disruption.BatchSimDeadline
	o.Pending = disruption.DropPendingOnly
}

var _ = Describe("Drift Core", func() {
	var e *coreEnv
	AfterEach(func() {
		if e != nil {
			e.stop()
			e = nil
		}
	})
	fresh := func(g genSpec) *genCluster {
		if e != nil {
			e.stop()
			ExpectCleanedUp(ctx, env.Client)
		}
		its := genInstanceTypes(genZones(g.Zones))
		if g.Reserved {
			its = append(its, genReservedInstanceTypes(genZones(g.Zones), g.ReservationCapacity)...)
		}
		e = newCoreEnv(ctx, its)
		return e.buildGen(g)
	}

	Context("batch=1 differential against ComputeCommands before the core", func() {
		budgets := func(_ int, r *rand.Rand) string { return []string{"1", "10%", "100%"}[r.Intn(3)] }
		nodes := coreDiffNodes([]int{10, 25, 50})
		for _, seed := range coreSeeds(1, 8) {
			spec := randomGenSpec(seed, nodes, budgets)
			// Reserved capacity with full or open reservations and TerminateFirstDrift on or off reach the
			// terminate-first path; pods pinned to their node reach the Blocked path.
			spec.Reserved, spec.ReservationCapacity = seed%3 != 1, int(seed/3)%2
			spec.Pinned = []float64{0, 0.02, 0.08}[seed%3]
			terminateFirst := seed%2 == 0
			It(fmt.Sprintf("seed=%d terminateFirst=%v %+v", seed, terminateFirst, spec), func() {
				gc := fresh(spec)
				drift := e.drift()
				ctx := options.ToContext(e.ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{TerminateFirstDrift: lo.ToPtr(terminateFirst)}}))
				// The events each variant publishes itself, after candidates are built.
				var published []string
				recorded := func(f func() ([]disruption.Command, error)) ([]disruption.Command, error) {
					e.recorder.Reset()
					defer func() { published = eventStrings(e.recorder.Events()) }()
					return f()
				}
				legacy := func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
					return recorded(func() ([]disruption.Command, error) {
						return disruption.LegacyDriftComputeCommands(ctx, drift, budgets, cands...)
					})
				}
				current := func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
					return recorded(func() ([]disruption.Command, error) { return drift.ComputeCommands(ctx, budgets, cands...) })
				}
				forced := func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
					o := drift.LegacyOptions()
					disruption.SetBatchMinSize(&o, 1)
					res, err := drift.Compute(ctx, budgets, o, cands...)
					return res.Commands, err
				}
				for step := range 4 {
					ref, numCands := e.compute(drift, legacy)
					refEvents := published
					got, _ := e.compute(drift, current)
					gotEvents := published
					ref2, _ := e.compute(drift, legacy)
					gotForced, _ := e.compute(drift, forced)
					upstream := map[string]bool{gc.cmdsSignature(ref): true, gc.cmdsSignature(ref2): true}
					matches := func(cmds []disruption.Command) bool {
						for i := 0; i < 16 && !upstream[gc.cmdsSignature(cmds)]; i++ {
							again, _ := e.compute(drift, legacy)
							upstream[gc.cmdsSignature(again)] = true
						}
						return upstream[gc.cmdsSignature(cmds)]
					}
					gotOK, forcedOK := matches(got), matches(gotForced)
					emitCoreDiff(map[string]any{"test": "drift-batch1", "spec": spec, "step": step, "candidates": numCands,
						"refDecision":      lo.Map(ref, func(c disruption.Command, _ int) string { return string(c.Decision()) }),
						"refReplacements":  lo.SumBy(ref, func(c disruption.Command) int { return len(c.Replacements) }),
						"upstreamOutcomes": len(upstream), "equal": gotOK, "forcedEqual": forcedOK})
					Expect(gotOK).To(BeTrue(), "ComputeCommands differs from every upstream result: %s", gc.cmdsSignature(got))
					Expect(forcedOK).To(BeTrue(), "the batched path on single candidates differs from every upstream result: %s", gc.cmdsSignature(gotForced))
					if len(upstream) > 1 {
						break // the state paths diverge from here
					}
					Expect(gotEvents).To(Equal(refEvents))
					if len(ref) == 0 {
						break
					}
					started, _ := e.startCommands(drift, ref)
					if len(started) == 0 {
						break
					}
					e.genLaunch(gc, started)
				}
			})
		}
	})

	Context("batch=K invariants against the serial reference", func() {
		// 100% budgets only on small clusters so the serial reference stays affordable.
		budgets := func(n int, r *rand.Rand) string {
			if n <= 50 {
				return []string{"1", "10%", "100%", "100%"}[r.Intn(4)]
			}
			return []string{"10%", "20%"}[r.Intn(2)]
		}
		nodes := coreDiffNodes([]int{10, 25, 50})
		for _, seed := range coreSeeds(1001, 4) {
			spec := randomGenSpec(seed, nodes, budgets)
			It(fmt.Sprintf("seed=%d %+v", seed, spec), func() {
				serial := e.runDrift(fresh(spec), nil)
				batched := e.runDrift(fresh(spec), batchAll)
				sized := e.runDrift(fresh(spec), batchSized)
				emitCoreDiff(map[string]any{"test": "drift-batchK", "spec": spec, "serial": serial, "batched": batched, "sized": sized})
				for name, r := range map[string]driftRun{"batched": batched, "sized": sized} {
					By(name)
					// Batched planning falls back to one candidate at a time whenever a batch plans nothing, so it is
					// quiescent only where serial drift is. It can disrupt fewer nodes in total, because percentage
					// budgets count initialized replacements and packing launches fewer of them.
					if r.Candidates < serial.Candidates {
						Expect(r.FinalAllowed).To(BeNumerically("<", serial.FinalAllowed), name)
					}
					Expect(r.MaxCommand).To(BeNumerically("<=", disruption.ComponentCap), name)
					Expect(r.R1Planned.hardViolations()).To(BeZero(), "%s round-1 plan %+v", name, r.R1Planned)
					Expect(r.Planned.hardViolations()).To(BeZero(), "%s plan %+v", name, r.Planned)
					Expect(r.Final.hardViolations()).To(BeZero(), "%s end state %+v", name, r.Final)
					Expect(r.R1Final.BudgetOver).To(BeZero(), name)
					// Planned replacements plus what the provisioner still launches for moving pods never exceed serial's
					// (tolerance: 1 or 10%). The provisioner's re-solve doesn't always recover a tight packing.
					Expect(r.NodeClaims+r.Final.ExtraNodeClaims).To(BeNumerically("<=",
						serial.NodeClaims+serial.Final.ExtraNodeClaims+max(1, serial.NodeClaims/10)), name)
				}
			})
		}
	})

	Context("chunking", func() {
		// 40 nodes of small pods pack onto few replacements, so the joint simulation is one component of 40.
		spec := genSpec{Seed: 7, Nodes: 40, MaxPodsPerNode: 4, Pools: 1, Zones: 3, Budget: "100%", DriftFrac: 1, Util: 0.3}
		It("caps commands at ComponentCap candidates", func() {
			gc := fresh(spec)
			drift := e.drift()
			var res disruption.CoreResult
			cmds, _ := e.compute(drift, func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
				o := drift.LegacyOptions()
				batchAll(&o)
				var err error
				res, err = drift.Compute(e.ctx, budgets, o, cands...)
				return res.Commands, err
			})
			Expect(res.Batch).ToNot(BeNil())
			Expect(res.Batch.Accepted).To(HaveLen(40))
			Expect(res.Chunked).To(BeTrue())
			Expect(lo.SumBy(cmds, func(c disruption.Command) int { return len(c.Candidates) }) + len(res.Deferred)).To(Equal(40))
			Expect(lo.EveryBy(cmds, func(c disruption.Command) bool { return len(c.Candidates) <= disruption.ComponentCap })).To(BeTrue())
			Expect(len(cmds)).To(BeNumerically(">=", 4))
			Expect(res.Deferred).To(BeEmpty())
			Expect(lo.Uniq(lo.Map(cmds, func(c disruption.Command, _ int) uuid.UUID { return c.BatchID }))).To(HaveLen(1))
			// No two commands share a destination, and no command moves pods onto another command's candidate.
			dests := map[string]int{}
			removed := map[string]int{}
			for i, c := range cmds {
				for _, cand := range c.Candidates {
					removed[cand.ProviderID()] = i
				}
				for _, en := range c.Results.ExistingNodes {
					if j, ok := dests[en.ProviderID()]; ok {
						Expect(j).To(Equal(i), "existing node %s is a destination of two commands", gc.nodeIdx[en.ProviderID()])
					}
					dests[en.ProviderID()] = i
				}
			}
			for id := range dests {
				Expect(removed).ToNot(HaveKey(id))
			}
			started, failed := e.startCommands(drift, cmds)
			Expect(failed).To(BeZero())
			e.genLaunch(gc, started)
			planned, final := e.evaluate(gc, started)
			Expect(planned.hardViolations()).To(BeZero(), "%+v", planned)
			Expect(final.hardViolations()).To(BeZero(), "%+v", final)
		})
		It("counts earlier chunks' replacements against NodePool limits", func() {
			// Limits equal to the current capacity: every chunk may only use what its own candidates free.
			s := spec
			s.Limits, s.Util, s.MaxPodsPerNode = true, 1, 8
			gc := fresh(s)
			drift := e.drift()
			var res disruption.CoreResult
			cmds, _ := e.compute(drift, func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
				o := drift.LegacyOptions()
				batchAll(&o)
				var err error
				res, err = drift.Compute(e.ctx, budgets, o, cands...)
				return res.Commands, err
			})
			Expect(res.Chunked).To(BeTrue())
			Expect(len(cmds)).To(BeNumerically(">", 1))
			started, failed := e.startCommands(drift, cmds)
			Expect(failed).To(BeZero())
			e.genLaunch(gc, started)
			planned, final := e.evaluate(gc, started)
			Expect(planned.hardViolations()).To(BeZero(), "%+v", planned)
			Expect(final.hardViolations()).To(BeZero(), "%+v", final)
		})
		It("defers chunks that share a zone topology spread group with an earlier chunk", func() {
			s := spec
			s.ZoneTSC = true
			s.MaxPodsPerNode = 8
			fresh(s)
			drift := e.drift()
			var res disruption.CoreResult
			cmds, _ := e.compute(drift, func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
				o := drift.LegacyOptions()
				batchAll(&o)
				var err error
				res, err = drift.Compute(e.ctx, budgets, o, cands...)
				return res.Commands, err
			})
			Expect(res.Batch).ToNot(BeNil())
			Expect(len(res.Batch.Accepted)).To(BeNumerically(">", disruption.ComponentCap))
			Expect(lo.SumBy(cmds, func(c disruption.Command) int { return len(c.Candidates) })).To(BeNumerically("<=", disruption.ComponentCap))
			Expect(res.Deferred).ToNot(BeEmpty())
		})
	})
})

var _ = Describe("Drift Core Split Hazard", func() {
	// conflicts-topology.md counter-example #3 ("batch Split swap") as drift. Zones 1/2/3 each hold two pods of a zone
	// TSC group (maxSkew=1). Drifted a1 (zone 1) holds pa (2 cpu) and drifted b1 (zone 2) holds pb (1 cpu). Existing
	// e-a (zone 1) has 1 cpu free and e-b (zone 2) has 2 cpu free; zone 3 is full. The joint simulation swaps them
	// (pa -> e-b, pb -> e-a, no new NodeClaim) and Split yields {a1}, {b1}. Alone, a1 needs a zone-1 NodeClaim.
	var e *coreEnv
	var gc *genCluster
	BeforeEach(func() {
		e = newCoreEnv(ctx, genInstanceTypes(genZones(3)))
		gc = e.buildSplitHazard()
	})
	AfterEach(func() { e.stop() })
	computeBatched := func(only ...string) []disruption.Command {
		drift := e.drift()
		cmds, _ := e.compute(drift, func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
			if len(only) > 0 {
				cands = lo.Filter(cands, func(c *disruption.Candidate, _ int) bool { return lo.Contains(only, gc.nodeIdx[c.ProviderID()]) })
			}
			o := drift.LegacyOptions()
			o.Batch = len(cands)
			r, err := drift.Compute(e.ctx, budgets, o, cands...)
			return r.Commands, err
		})
		return cmds
	}

	It("plans a swap that Split turns into two commands of one batch", func() {
		cmds := computeBatched()
		Expect(lo.Map(cmds, func(c disruption.Command, _ int) string { return gc.cmdSignature(c) })).To(ConsistOf(
			And(ContainSubstring("candidates=[a1] replacements=0 []"), ContainSubstring("e-b:[ptsc-a1]")),
			And(ContainSubstring("candidates=[b1] replacements=0 []"), ContainSubstring("e-a:[ptsc-b1]")),
		))
		Expect(cmds[0].BatchID).ToNot(Equal(uuid.Nil))
		Expect(cmds[0].BatchID).To(Equal(cmds[1].BatchID))
	})
	It("violates zone TSC when one command starts without its sibling", func() {
		cmds := lo.Filter(computeBatched(), func(c disruption.Command, _ int) bool {
			return gc.nodeIdx[c.Candidates[0].ProviderID()] == "a1"
		})
		started, failed := e.startCommands(e.drift(), cmds)
		Expect(failed).To(BeZero())
		e.genLaunch(gc, started)
		planned, final := e.evaluate(gc, started)
		Expect(planned.TSCMaxSkew).To(Equal(2))
		Expect(final.ExtraNodeClaims).To(Equal(1))
	})
	It("keeps zone TSC when the survivor is planned again", func() {
		// Re-planning the survivor of a rejected sibling is a fresh Compute over the survivors.
		cmds := computeBatched("a1")
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Replacements).To(HaveLen(1))
		Expect(cmds[0].Replacements[0].Requirements.Get(corev1.LabelTopologyZone).Values()).To(ConsistOf(genZones(3)[0]))
		started, failed := e.startCommands(e.drift(), cmds)
		Expect(failed).To(BeZero())
		e.genLaunch(gc, started)
		planned, final := e.evaluate(gc, started)
		Expect(planned.hardViolations()).To(BeZero())
		Expect(final.hardViolations()).To(BeZero())
		Expect(final.ExtraNodeClaims).To(BeZero())
	})
})

// buildSplitHazard builds the cluster of the Split hazard specs.
func (e *coreEnv) buildSplitHazard() *genCluster {
	GinkgoHelper()
	zones := genZones(3)
	it, _ := lo.Find(cloudProvider.InstanceTypes, func(i *cloudprovider.InstanceType) bool { return i.Name == "gen-4cpu" })
	run := uuid.NewString()[:6]
	gc := &genCluster{spec: genSpec{Nodes: 6, Pools: 1, Zones: 3, Budget: "100%", ZoneTSC: true}, prefix: "h" + run[:5],
		nodeIdx: map[string]string{}, nodeZone: map[string]string{}}
	gc.groupSel = lo.Times(genNumGroups, func(i int) string { return fmt.Sprintf("g%d-%s", i, run) })
	np := test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
		ConsolidateAfter: v1.MustParseNillableDuration("Never"), Budgets: []v1.Budget{{Nodes: "100%"}},
	}}})
	np.Spec.Template.Spec.Requirements = append(np.Spec.Template.Spec.Requirements, v1.NodeSelectorRequirementWithMinValues{
		Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: zones,
	})
	ExpectApplied(ctx, env.Client, np)
	ExpectReconcileSucceeded(e.ctx, e.npCtrl, client.ObjectKeyFromObject(np))
	gc.pools = []*v1.NodePool{np}
	rs := test.ReplicaSet()
	ExpectApplied(ctx, env.Client, rs)
	base := env.Clock.Now().Add(-time.Hour)
	specs := []struct {
		name, zone     string
		drifted        bool
		tscCPU, filler string
	}{
		{"a1", zones[0], true, "2", ""},
		{"b1", zones[1], true, "1", ""},
		{"e-a", zones[0], false, "1", "1900m"},
		{"e-b", zones[1], false, "1", "900m"},
		{"c1", zones[2], false, "1", "2900m"},
		{"c2", zones[2], false, "1", "2900m"},
	}
	var nodeClaims []*v1.NodeClaim
	var nodes []*corev1.Node
	var pods []*corev1.Pod
	sel := map[string]string{genGroupLabel: gc.groupSel[genGroupTSC]}
	owner := []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true)}}
	for i, ns := range specs {
		nc, _ := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey: np.Name, corev1.LabelInstanceTypeStable: it.Name, v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand,
				corev1.LabelTopologyZone: ns.zone, corev1.LabelArchStable: "amd64", corev1.LabelOSStable: "linux", genIdxLabel: ns.name,
			}},
			Status: v1.NodeClaimStatus{ProviderID: test.RandomProviderID(), Allocatable: it.Allocatable(), Capacity: it.Capacity},
		})
		for _, c := range []string{v1.ConditionTypeLaunched, v1.ConditionTypeRegistered, v1.ConditionTypeInitialized, v1.ConditionTypeConsolidatable} {
			nc.StatusConditions().SetTrue(c)
		}
		if ns.drifted {
			nc.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
			setDriftedTime(nc, base.Add(time.Duration(i)*time.Second))
		}
		n := test.NodeClaimLinkedNode(nc)
		n.Spec.Taints = lo.Reject(n.Spec.Taints, func(t corev1.Taint, _ int) bool { return t.MatchTaint(&v1.UnregisteredNoExecuteTaint) })
		n.Labels[v1.NodeRegisteredLabelKey] = "true"
		n.Labels[v1.NodeInitializedLabelKey] = "true"
		n.Labels[corev1.LabelHostname] = n.Name
		n.Status.Phase = corev1.NodeRunning
		n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			LastHeartbeatTime: metav1.NewTime(env.Clock.Now()), LastTransitionTime: metav1.NewTime(env.Clock.Now())}}
		nc.Status.NodeName = n.Name
		nodeClaims, nodes = append(nodeClaims, nc), append(nodes, n)
		gc.nodeIdx[nc.Status.ProviderID] = ns.name
		gc.nodeZone[n.Name] = ns.zone
		pods = append(pods, test.Pod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: lo.Assign(sel, map[string]string{genPodLabel: "tsc-" + ns.name}), OwnerReferences: owner},
			NodeName:   n.Name, Phase: corev1.PodRunning,
			ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(ns.tscCPU)}},
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{MaxSkew: 1, TopologyKey: corev1.LabelTopologyZone,
				WhenUnsatisfiable: corev1.DoNotSchedule, LabelSelector: &metav1.LabelSelector{MatchLabels: sel}}},
		}))
		if ns.filler != "" {
			pods = append(pods, test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{genGroupLabel: gc.groupSel[0], genPodLabel: "filler-" + ns.name}, OwnerReferences: owner},
				NodeName:   n.Name, Phase: corev1.PodRunning,
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(ns.filler)}},
			}))
		}
	}
	for i := range nodes {
		coreApply(ctx, env.Client, nodeClaims[i])
		taints := nodes[i].Spec.Taints
		coreApply(ctx, env.Client, nodes[i])
		nodes[i].Spec.Taints = taints
		Expect(env.Client.Update(ctx, nodes[i])).To(Succeed())
	}
	for _, p := range pods {
		ExpectApplied(ctx, env.Client, p)
	}
	e.hydrate(len(pods), len(nodes), 0, nodeClaims, nodes, pods)
	return gc
}

// fakeSimulator fails the pods of the candidates in fail (by providerID), and the pod of a deleting node when crowd
// is set and more than crowd candidates are simulated together.
type fakeSimulator struct {
	fail    map[string]bool
	crowd   int
	sleep   time.Duration
	calls   [][]string
	deleted *corev1.Pod
}

func (f *fakeSimulator) Simulate(ctx context.Context, _ disruption.SimOptions, cands ...*disruption.Candidate) (disruption.Outcome, error) {
	f.calls = append(f.calls, lo.Map(cands, func(c *disruption.Candidate, _ int) string { return c.ProviderID() }))
	if f.sleep > 0 {
		select {
		case <-time.After(f.sleep):
		case <-ctx.Done():
			return disruption.Outcome{}, fmt.Errorf("scheduling pods, %w", ctx.Err())
		}
	}
	r := pscheduling.Results{PodErrors: map[*corev1.Pod]error{}}
	for _, c := range cands {
		if f.fail[c.ProviderID()] {
			r.PodErrors[podOf(c)] = errors.New("no fit")
		}
	}
	if f.crowd > 0 && len(cands) > f.crowd {
		r.PodErrors[f.deleted] = errors.New("crowded out")
	}
	return disruption.Outcome{Results: r, Took: time.Duration(len(cands)) * time.Millisecond}, nil
}

var candidatePods = map[string]*corev1.Pod{}

func podOf(c *disruption.Candidate) *corev1.Pod { return candidatePods[c.ProviderID()] }

func bareCandidates(n int) []*disruption.Candidate {
	np := test.NodePool()
	return lo.Times(n, func(i int) *disruption.Candidate {
		id := fmt.Sprintf("c%02d", i)
		p := test.Pod(test.PodOptions{NodeName: "node-" + id})
		p.UID = types.UID(uuid.NewString())
		candidatePods[id] = p
		return disruption.NewBareCandidate(id, np, p)
	})
}

func ids(cs []*disruption.Candidate) []string {
	return lo.Map(cs, func(c *disruption.Candidate, _ int) string { return c.ProviderID() })
}

var _ = Describe("SimulateBatch", func() {
	It("accepts the whole batch when every pod schedules", func() {
		cs := bareCandidates(5)
		sim := &fakeSimulator{}
		out, err := disruption.SimulateBatch(ctx, sim, disruption.BatchOptions{K: 4}, cs...)
		Expect(err).ToNot(HaveOccurred())
		Expect(ids(out.Accepted)).To(Equal(ids(cs[:4])))
		Expect(out.Size).To(Equal(4))
		Expect(out.Rounds).To(Equal(1))
	})
	It("drops blocked candidates and tops the batch up once", func() {
		cs := bareCandidates(6)
		sim := &fakeSimulator{fail: map[string]bool{"c01": true, "c04": true}}
		out, err := disruption.SimulateBatch(ctx, sim, disruption.BatchOptions{K: 3}, cs...)
		Expect(err).ToNot(HaveOccurred())
		// Round 1: c00 c01 c02, c01 dropped, topped up with c03. Round 2: c00 c02 c03.
		Expect(sim.calls).To(Equal([][]string{{"c00", "c01", "c02"}, {"c00", "c02", "c03"}}))
		Expect(ids(out.Accepted)).To(Equal([]string{"c00", "c02", "c03"}))
		Expect(out.Dropped).To(Equal(map[string]disruption.DropReason{"c01": disruption.DropPodsUnschedulable}))
	})
	It("ignores pending pods", func() {
		cs := bareCandidates(2)
		sim := &fakeSimulator{crowd: 1, deleted: test.UnschedulablePod()}
		out, err := disruption.SimulateBatch(ctx, sim, disruption.BatchOptions{K: 2}, cs...)
		Expect(err).ToNot(HaveOccurred())
		Expect(out.Accepted).To(HaveLen(2))
	})
	It("halves the batch when a pod of no candidate fails", func() {
		cs := bareCandidates(8)
		sim := &fakeSimulator{crowd: 2, deleted: test.Pod(test.PodOptions{NodeName: "deleting"})}
		out, err := disruption.SimulateBatch(ctx, sim, disruption.BatchOptions{K: 8}, cs...)
		Expect(err).ToNot(HaveOccurred())
		Expect(ids(out.Accepted)).To(Equal(ids(cs[:2])))
		Expect(out.Halvings).To(Equal(2))
		Expect(out.Dropped).To(HaveLen(6))
	})
	It("gives up below the minimum size", func() {
		cs := bareCandidates(3)
		sim := &fakeSimulator{crowd: 1, deleted: test.Pod(test.PodOptions{NodeName: "deleting"})}
		out, err := disruption.SimulateBatch(ctx, sim, disruption.BatchOptions{K: 3}, cs...)
		Expect(err).ToNot(HaveOccurred())
		Expect(out.Accepted).To(BeEmpty())
	})
	It("stops after MaxRounds", func() {
		cs := bareCandidates(10)
		fail := map[string]bool{}
		for _, c := range cs {
			fail[c.ProviderID()] = true
		}
		sim := &fakeSimulator{fail: fail}
		out, err := disruption.SimulateBatch(ctx, sim, disruption.BatchOptions{K: 2}, cs...)
		Expect(err).ToNot(HaveOccurred())
		Expect(out.Accepted).To(BeEmpty())
		Expect(len(sim.calls)).To(BeNumerically("<=", disruption.MaxBatchRounds))
	})
	It("abandons a simulation that runs past twice the deadline", func() {
		cs := bareCandidates(4)
		sim := &fakeSimulator{sleep: time.Minute}
		out, err := disruption.SimulateBatch(ctx, sim, disruption.BatchOptions{K: 4, Deadline: 10 * time.Millisecond}, cs...)
		Expect(err).ToNot(HaveOccurred())
		Expect(out.TimedOut).To(BeTrue())
		Expect(out.Accepted).To(BeEmpty())
	})
	It("returns errors of the caller's context", func() {
		cs := bareCandidates(4)
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		_, err := disruption.SimulateBatch(cctx, &fakeSimulator{sleep: time.Minute}, disruption.BatchOptions{K: 4, Deadline: time.Minute}, cs...)
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("BatchSizer", func() {
	It("starts at FirstBatchMax and the limit", func() {
		s := disruption.NewBatchSizer(2 * time.Second)
		Expect(s.Size(1000)).To(Equal(disruption.FirstBatchMax))
		Expect(s.Size(7)).To(Equal(7))
		Expect(s.Size(0)).To(Equal(1))
	})
	It("grows at most twofold toward the deadline and shrinks at most by half", func() {
		s := disruption.NewBatchSizer(2 * time.Second)
		accepted := func(n int, took time.Duration) disruption.BatchOutcome {
			return disruption.BatchOutcome{Size: n, Accepted: bareCandidates(n), Outcome: disruption.Outcome{Took: took}}
		}
		s.Observe(accepted(50, 500*time.Millisecond)) // 10ms per candidate: 200 fit in 2s
		Expect(s.Size(1000)).To(Equal(100))
		s.Observe(accepted(100, time.Second))
		Expect(s.Size(1000)).To(Equal(200))
		s.Observe(accepted(200, 8*time.Second)) // 40ms per candidate: 50 fit, clamped to 100
		Expect(s.Size(1000)).To(Equal(100))
		Expect(s.Size(30)).To(Equal(30))
	})
	It("halves after a timeout or a halved batch", func() {
		s := disruption.NewBatchSizer(2 * time.Second)
		s.Observe(disruption.BatchOutcome{Size: 40, TimedOut: true})
		Expect(s.Size(1000)).To(Equal(20))
		s.Observe(disruption.BatchOutcome{Size: 20, Halvings: 2, Accepted: bareCandidates(5)})
		Expect(s.Size(1000)).To(Equal(5))
	})
})

var _ = Describe("Topology coupling", func() {
	sel := func(app string) *metav1.LabelSelector {
		return &metav1.LabelSelector{MatchLabels: map[string]string{"app": app}}
	}
	pod := func(app string, mutate ...func(*corev1.Pod)) *corev1.Pod {
		p := test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": app}}})
		for _, m := range mutate {
			m(p)
		}
		return p
	}
	tsc := func(key, app string) func(*corev1.Pod) {
		return func(p *corev1.Pod) {
			p.Spec.TopologySpreadConstraints = append(p.Spec.TopologySpreadConstraints, corev1.TopologySpreadConstraint{
				MaxSkew: 1, TopologyKey: key, WhenUnsatisfiable: corev1.DoNotSchedule, LabelSelector: sel(app)})
		}
	}
	anti := func(key, app string) func(*corev1.Pod) {
		return func(p *corev1.Pod) {
			p.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{TopologyKey: key, LabelSelector: sel(app)}}}}
		}
	}
	It("couples pods selected by a zone topology spread constraint, in either direction", func() {
		a := []*corev1.Pod{pod("web", tsc(corev1.LabelTopologyZone, "web"))}
		Expect(disruption.TopologyCoupled(a, []*corev1.Pod{pod("web")})).To(BeTrue())
		Expect(disruption.TopologyCoupled([]*corev1.Pod{pod("web")}, a)).To(BeTrue())
		Expect(disruption.TopologyCoupled(a, []*corev1.Pod{pod("db")})).To(BeFalse())
	})
	It("ignores hostname-keyed terms", func() {
		a := []*corev1.Pod{pod("web", tsc(corev1.LabelHostname, "web"), anti(corev1.LabelHostname, "web"))}
		Expect(disruption.TopologyCoupled(a, []*corev1.Pod{pod("web")})).To(BeFalse())
	})
	It("couples zone anti-affinity", func() {
		a := []*corev1.Pod{pod("web", anti(corev1.LabelTopologyZone, "db"))}
		Expect(disruption.TopologyCoupled(a, []*corev1.Pod{pod("db")})).To(BeTrue())
		Expect(disruption.TopologyCoupled(a, []*corev1.Pod{pod("web")})).To(BeFalse())
	})
	It("respects the namespace of a topology spread constraint", func() {
		a := []*corev1.Pod{pod("web", tsc(corev1.LabelTopologyZone, "web"))}
		other := pod("web")
		other.Namespace = "other-" + a[0].Namespace
		Expect(disruption.TopologyCoupled(a, []*corev1.Pod{other})).To(BeFalse())
	})
	It("couples pods with DRA claims", func() {
		claim := func(p *corev1.Pod) { p.Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: "gpu"}} }
		Expect(disruption.TopologyCoupled([]*corev1.Pod{pod("a", claim)}, []*corev1.Pod{pod("b", claim)})).To(BeTrue())
		Expect(disruption.TopologyCoupled([]*corev1.Pod{pod("a", claim)}, []*corev1.Pod{pod("b")})).To(BeFalse())
	})
})

var _ = Describe("Split", func() {
	It("connects candidates on shared new NodeClaims only and keeps pending-only claims on the first piece", func() {
		cs := bareCandidates(4)
		pending := test.UnschedulablePod()
		nc := func(pods ...*corev1.Pod) *pscheduling.NodeClaim { return &pscheduling.NodeClaim{Pods: pods} }
		results := pscheduling.Results{NewNodeClaims: []*pscheduling.NodeClaim{
			nc(podOf(cs[1]), podOf(cs[3])), nc(pending), nc(podOf(cs[2])),
		}}
		pieces := disruption.Split(cs, results, disruption.KeepOnFirst)
		Expect(lo.Map(pieces, func(p disruption.Piece, _ int) []string { return ids(p.Candidates) })).To(Equal([][]string{{"c00"}, {"c01", "c03"}, {"c02"}}))
		Expect(pieces[0].Results.NewNodeClaims).To(Equal([]*pscheduling.NodeClaim{results.NewNodeClaims[1]}))
		Expect(pieces[1].Results.NewNodeClaims).To(Equal([]*pscheduling.NodeClaim{results.NewNodeClaims[0]}))
		dropped := disruption.Split(cs, results, disruption.DropPendingOnly)
		Expect(dropped[0].Results.NewNodeClaims).To(BeEmpty())
		Expect(lo.SumBy(dropped, func(p disruption.Piece) int { return len(p.Results.NewNodeClaims) })).To(Equal(2))
	})
	It("sorts candidates into pieces in candidate order", func() {
		cs := bareCandidates(3)
		slices.Reverse(cs)
		pieces := disruption.Split(cs, pscheduling.Results{}, disruption.KeepOnFirst)
		Expect(lo.Map(pieces, func(p disruption.Piece, _ int) string { return p.Candidates[0].ProviderID() })).To(Equal(ids(cs)))
	})
})

// chunkSimulator places every candidate's pod on one new NodeClaim per call, of a 16 CPU instance type, and records the
// options of every call.
// With pending set, every simulation also opens a NodeClaim that holds only that pending pod, as the scheduler does
// for pending pods in every simulation.
type chunkSimulator struct {
	opts    []disruption.SimOptions
	it      *cloudprovider.InstanceType
	pending *corev1.Pod
}

func (f *chunkSimulator) Simulate(_ context.Context, opts disruption.SimOptions, cands ...*disruption.Candidate) (disruption.Outcome, error) {
	f.opts = append(f.opts, opts)
	nc := &pscheduling.NodeClaim{Pods: lo.Map(cands, func(c *disruption.Candidate, _ int) *corev1.Pod { return podOf(c) })}
	nc.NodePoolName = cands[0].NodePool.Name
	nc.InstanceTypeOptions = []*cloudprovider.InstanceType{f.it}
	claims := []*pscheduling.NodeClaim{nc}
	if f.pending != nil {
		pc := &pscheduling.NodeClaim{Pods: []*corev1.Pod{f.pending}}
		pc.NodePoolName, pc.InstanceTypeOptions = nc.NodePoolName, nc.InstanceTypeOptions
		claims = append(claims, pc)
	}
	return disruption.Outcome{Results: pscheduling.Results{NewNodeClaims: claims, PodErrors: map[*corev1.Pod]error{}}}, nil
}

var _ = Describe("batchWindow", func() {
	in := func(names ...string) func(*disruption.Candidate) bool {
		return func(c *disruption.Candidate) bool { return lo.Contains(names, c.ProviderID()) }
	}
	all := func(*disruption.Candidate) bool { return true }
	It("never plans past a candidate only the single path plans", func() {
		cs := bareCandidates(5)
		budgets := map[string]int{cs[0].NodePool.Name: 10}
		first, window := disruption.BatchWindow(cs, budgets, nil, in("c00", "c03"), all)
		// c00 ranks first and is left to the single path; the batch is c01 c02 and ends at c03.
		Expect(first).To(Equal(1))
		Expect(ids(window)).To(Equal([]string{"c01", "c02"}))
		first, window = disruption.BatchWindow(cs, budgets, nil, in("c02"), all)
		Expect(first).To(Equal(0))
		Expect(ids(window)).To(Equal([]string{"c00", "c01"}))
	})
	It("passes over skipped candidates and those beyond the budget", func() {
		cs := bareCandidates(5)
		budgets := map[string]int{cs[0].NodePool.Name: 2}
		first, window := disruption.BatchWindow(cs, budgets, in("c00"), func(*disruption.Candidate) bool { return false }, in("c01", "c02", "c03", "c04"))
		Expect(first).To(Equal(1))
		Expect(ids(window)).To(Equal([]string{"c01", "c02"}))
		Expect(budgets[cs[0].NodePool.Name]).To(Equal(2))
		// Uncharged candidates don't need budget.
		_, window = disruption.BatchWindow(cs, map[string]int{}, nil, func(*disruption.Candidate) bool { return false }, func(*disruption.Candidate) bool { return false })
		Expect(window).To(HaveLen(5))
	})
})

var _ = Describe("CapPieces", func() {
	It("keeps pieces of at most ComponentCap candidates", func() {
		cs := bareCandidates(disruption.ComponentCap)
		sim := &chunkSimulator{}
		pieces := []disruption.Piece{{Candidates: cs}}
		capped, deferred, chunked := disruption.CapPieces(ctx, sim, disruption.SimOptions{}, cs, pieces, disruption.KeepOnFirst)
		Expect(capped).To(Equal(pieces))
		Expect(deferred).To(BeEmpty())
		Expect(chunked).To(BeFalse())
		Expect(sim.opts).To(BeEmpty())
	})
	It("plans a larger piece again in chunks, counting what stays and what earlier chunks launch against limits", func() {
		cs := bareCandidates(disruption.ComponentCap + 5)
		np := cs[0].NodePool.Name
		sim := &chunkSimulator{it: fake.NewInstanceType("chunk-16cpu", fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("16")}))}
		capped, deferred, chunked := disruption.CapPieces(ctx, sim, disruption.SimOptions{}, cs, []disruption.Piece{{Candidates: cs}}, disruption.KeepOnFirst)
		Expect(chunked).To(BeTrue())
		Expect(deferred).To(BeEmpty())
		Expect(lo.Map(capped, func(p disruption.Piece, _ int) int { return len(p.Candidates) })).To(Equal([]int{disruption.ComponentCap, 5}))
		Expect(sim.opts).To(HaveLen(2))
		// Every simulation excludes the whole batch as destinations. Only the first schedules the pods of nodes that are
		// already being disrupted.
		for _, o := range sim.opts {
			Expect(o.ExcludeDestinations.UnsortedList()).To(ConsistOf(ids(cs)))
		}
		Expect(lo.Map(sim.opts, func(o disruption.SimOptions, _ int) bool { return o.SkipDeletingNodePods })).To(Equal([]bool{false, true}))
		// Chunk 1: the 5 candidates of chunk 2 stay, 8 CPUs each. Chunk 2: chunk 1's 16 CPU replacement.
		cpu := func(rl corev1.ResourceList) string { return lo.ToPtr(rl[corev1.ResourceCPU]).String() }
		Expect(cpu(sim.opts[0].LimitsUsed[np])).To(Equal("40"))
		Expect(cpu(sim.opts[1].LimitsUsed[np])).To(Equal("16"))
	})
	It("plans a pending pod's NodeClaim once, in the first chunk", func() {
		cs := bareCandidates(disruption.ComponentCap*2 + 1)
		sim := &chunkSimulator{it: fake.NewInstanceType("chunk-16cpu", fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("16")})),
			pending: test.UnschedulablePod()}
		pendingOnly := func(p disruption.Piece) int {
			return lo.CountBy(p.Results.NewNodeClaims, func(nc *pscheduling.NodeClaim) bool {
				return len(nc.Pods) == 1 && nc.Pods[0] == sim.pending
			})
		}
		for _, pending := range []disruption.PendingClaims{disruption.KeepOnFirst, disruption.DropPendingOnly} {
			sim.opts = nil
			capped, deferred, chunked := disruption.CapPieces(ctx, sim, disruption.SimOptions{}, cs, []disruption.Piece{{Candidates: cs}}, pending)
			Expect(chunked).To(BeTrue())
			Expect(deferred).To(BeEmpty())
			Expect(sim.opts).To(HaveLen(3))
			Expect(capped).To(HaveLen(3))
			// KeepOnFirst keeps it on the first chunk's piece; DropPendingOnly leaves it to the provisioner. No later
			// chunk launches it again.
			Expect(lo.Map(capped, func(p disruption.Piece, _ int) int { return pendingOnly(p) })).To(Equal(
				[]int{lo.Ternary(pending == disruption.KeepOnFirst, 1, 0), 0, 0}))
		}
	})
	It("defers the rest after a chunk whose pods don't all schedule", func() {
		cs := bareCandidates(25)
		sim := &fakeSimulator{fail: map[string]bool{cs[12].ProviderID(): true}}
		capped, deferred, _ := disruption.CapPieces(ctx, sim, disruption.SimOptions{}, cs, []disruption.Piece{{Candidates: cs}}, disruption.KeepOnFirst)
		Expect(lo.SumBy(capped, func(p disruption.Piece) int { return len(p.Candidates) })).To(Equal(disruption.ComponentCap))
		Expect(ids(deferred)).To(Equal(ids(cs[disruption.ComponentCap:])))
	})
})
