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

// Flow-prefilter experiment.
//
// Tests whether a cheap max-flow feasibility check can SOUNDLY pre-reject doomed
// candidate removal sets before the expensive real SimulateScheduling call.
//
// Model (strict relaxation of the reschedule step for a fixed removal set S):
//   - Displaced pods = reschedulable pods on S's nodes.
//   - Targets = every cluster node NOT in S, headroom = StateNode.Available() per dim.
//   - One generous virtual replacement node, capacity = max instance-type allocatable
//     per dim (models Karpenter's m->1: at most ONE new node).
//   - Per binding resource dimension {cpu, mem} we solve a TRANSPORTATION max-flow:
//     source -> pod (cap = pod demand) -> {legal targets, replacement} -> sink
//     (cap = headroom / replacement size). "Feasible in dim d" = flow saturates all
//     displaced demand. Set is flow-feasible iff EVERY dim saturates.
//
// Why this is SOUND (flow-feasible is a SUPERSET of oracle-feasible, so a
// flow REJECT is provably a true infeasible => zero false negatives):
//   - Splittable (fractional) flow only relaxes the integral pod assignment.
//   - Per-dimension flow is a necessary condition of the joint multi-dim packing.
//   - The replacement is universally reachable and as large as the biggest instance,
//     so we never under-provision relative to what the oracle could launch.
//   - We EXCLUDE only provably-illegal edges (hostname anti-affinity: a target that
//     keeps a same-group peer). We do NOT model TSC/affinity/soft terms -> that only
//     makes flow MORE permissive (accept more), never wrongly reject.
// A flow ACCEPT is merely necessary, not sufficient -> fall through to the oracle.

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// ---------------------------------------------------------------------------
// Dinic max-flow (int64 capacities)
// ---------------------------------------------------------------------------

const flowINF = int64(1) << 60

type edge struct {
	to, rev int
	cap     int64
}

type maxflow struct {
	g          [][]edge
	level, iter []int
}

func newMaxflow(n int) *maxflow {
	return &maxflow{g: make([][]edge, n), level: make([]int, n), iter: make([]int, n)}
}

func (m *maxflow) addEdge(from, to int, cap int64) {
	m.g[from] = append(m.g[from], edge{to: to, rev: len(m.g[to]), cap: cap})
	m.g[to] = append(m.g[to], edge{to: from, rev: len(m.g[from]) - 1, cap: 0})
}

func (m *maxflow) bfs(s int) {
	for i := range m.level {
		m.level[i] = -1
	}
	q := []int{s}
	m.level[s] = 0
	for len(q) > 0 {
		v := q[0]
		q = q[1:]
		for _, e := range m.g[v] {
			if e.cap > 0 && m.level[e.to] < 0 {
				m.level[e.to] = m.level[v] + 1
				q = append(q, e.to)
			}
		}
	}
}

func (m *maxflow) dfs(v, t int, f int64) int64 {
	if v == t {
		return f
	}
	for ; m.iter[v] < len(m.g[v]); m.iter[v]++ {
		e := &m.g[v][m.iter[v]]
		if e.cap > 0 && m.level[v] < m.level[e.to] {
			d := m.dfs(e.to, t, min64(f, e.cap))
			if d > 0 {
				e.cap -= d
				m.g[e.to][e.rev].cap += d
				return d
			}
		}
	}
	return 0
}

func (m *maxflow) run(s, t int) int64 {
	var flow int64
	for {
		m.bfs(s)
		if m.level[t] < 0 {
			return flow
		}
		for i := range m.iter {
			m.iter[i] = 0
		}
		for {
			f := m.dfs(s, t, flowINF)
			if f == 0 {
				break
			}
			flow += f
		}
	}
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// ---------------------------------------------------------------------------
// flowFeasible: the sound prefilter
// ---------------------------------------------------------------------------

type dim struct {
	name string
	get  func(corev1.ResourceList) int64
}

var dims = []dim{
	{"cpu", func(rl corev1.ResourceList) int64 { q := rl[corev1.ResourceCPU]; return q.MilliValue() }},
	{"mem", func(rl corev1.ResourceList) int64 { q := rl[corev1.ResourceMemory]; return q.Value() }},
}

// flowFeasible returns true if the displaced pods of S *might* be re-homeable
// (fall through to the oracle), false if provably not (sound reject).
func (h *harness) flowFeasible(t *testing.T, S []*disruption.Candidate) bool {
	sNames := map[string]bool{}
	for _, c := range S {
		sNames[c.Name()] = true
	}

	// Displaced reschedulable pods on S.
	var displaced []*corev1.Pod
	for _, c := range S {
		for _, p := range h.podsOn(t, c.Name()) {
			if podutils.IsReschedulable(p) {
				displaced = append(displaced, p)
			}
		}
	}
	if len(displaced) == 0 {
		return true // nothing to re-home
	}

	// Targets = all cluster state nodes not in S. headroom = Available() per dim.
	var targets []*state.StateNode
	for _, sn := range h.cluster.DeepCopyNodes() {
		if sn.Node == nil && sn.NodeClaim == nil {
			continue
		}
		if !sNames[sn.Name()] {
			targets = append(targets, sn)
		}
	}

	// Staying same-group peers per hostname-anti-affinity group: (group -> target node names).
	stayGroupNode := map[string]map[string]bool{}
	for _, tn := range targets {
		for _, p := range h.podsOn(t, tn.Name()) {
			if hasHostAnti(p) {
				if app := p.Labels["app"]; app != "" {
					if stayGroupNode[app] == nil {
						stayGroupNode[app] = map[string]bool{}
					}
					stayGroupNode[app][tn.Name()] = true
				}
			}
		}
	}

	// Generous replacement: the largest instance-type allocatable per dim.
	replCap := map[string]int64{}
	for _, d := range dims {
		var mx int64
		for _, it := range h.cloudProvider.InstanceTypes {
			if v := d.get(it.Capacity); v > mx {
				mx = v
			}
		}
		replCap[d.name] = mx
	}

	// Solve one transportation flow per dimension; all must saturate.
	for _, d := range dims {
		// total demand
		var total int64
		demand := make([]int64, len(displaced))
		for i, p := range displaced {
			demand[i] = d.get(resources.RequestsForPods(p))
			total += demand[i]
		}
		if total == 0 {
			continue // dimension not binding for these pods
		}
		// node ids: 0=source, 1..P=pods, P+1..P+T=targets, P+T+1=replacement, P+T+2=sink
		P, T := len(displaced), len(targets)
		src, repl, sink := 0, P+T+1, P+T+2
		mf := newMaxflow(P + T + 3)
		for i, dem := range demand {
			mf.addEdge(src, 1+i, dem)
		}
		for j, tn := range targets {
			mf.addEdge(P+1+j, sink, d.get(tn.Available()))
		}
		mf.addEdge(repl, sink, replCap[d.name])
		for i, p := range displaced {
			// replacement always reachable (empty node, no peers)
			mf.addEdge(1+i, repl, flowINF)
			app := p.Labels["app"]
			isHA := hasHostAnti(p) && app != ""
			for j, tn := range targets {
				if isHA && stayGroupNode[app][tn.Name()] {
					continue // provably illegal: target keeps a same-group peer
				}
				mf.addEdge(1+i, P+1+j, flowINF)
			}
		}
		if mf.run(src, sink) < total {
			return false // provably cannot re-home in this dimension
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Experiment
// ---------------------------------------------------------------------------

type confusion struct {
	total          int
	flowReject     int
	oracleNoOp     int
	oracleConsol   int
	correctPrune   int // flow reject & oracle no-op
	falseNeg       int // flow reject & oracle consolidatable  (MUST be 0)
	aggReject      int // cheap O(P+T) aggregate check rejects
	aggFalseNeg    int // agg reject & oracle consolidatable   (MUST be 0)
	flowNotAgg     int // flow rejects & agg accepts (flow's marginal value over the cheap check)
	flowNanos      int64
	oracleNanos    int64
}

func (c *confusion) pruneRate() float64 {
	if c.total == 0 {
		return 0
	}
	return float64(c.flowReject) / float64(c.total)
}
func (c *confusion) recallOnInfeasible() float64 {
	if c.oracleNoOp == 0 {
		return 0
	}
	return float64(c.correctPrune) / float64(c.oracleNoOp)
}

// evalPair runs BOTH the flow prefilter and the real oracle on S and updates the matrix.
func (h *harness) evalPair(t *testing.T, S []*disruption.Candidate, c *confusion) {
	c.total++
	// Gather once (client access), then run both the flow and the cheap aggregate
	// check on the SAME inputs so the comparison is exact.
	in := h.flowGather(t, S)
	tf := time.Now()
	flowOK := flowSolve(in)
	c.flowNanos += time.Since(tf).Nanoseconds()
	aggOK := aggSolve(in)

	to := time.Now()
	r := h.evalSet(t, S)
	c.oracleNanos += time.Since(to).Nanoseconds()
	consolidatable := r.decision != "no-op"

	if consolidatable {
		c.oracleConsol++
	} else {
		c.oracleNoOp++
	}
	if !flowOK { // flow rejects
		c.flowReject++
		if consolidatable {
			c.falseNeg++
			t.Errorf("FALSE NEGATIVE (flow): rejected a consolidatable set (decision=%s savings=%.3f, size=%d)",
				r.decision, r.savings, len(S))
		} else {
			c.correctPrune++
		}
	}
	if !aggOK { // cheap aggregate check rejects
		c.aggReject++
		if consolidatable {
			c.aggFalseNeg++
			t.Errorf("FALSE NEGATIVE (agg): rejected a consolidatable set (decision=%s savings=%.3f, size=%d)",
				r.decision, r.savings, len(S))
		}
	}
	if !flowOK && aggOK {
		c.flowNotAgg++ // flow caught it, the cheap check missed it => flow's marginal value
	}
}

// enumerateSubsets yields all subsets of cands with size in [2, maxSize].
func enumerateSubsets(cands []*disruption.Candidate, maxSize int, fn func([]*disruption.Candidate)) {
	n := len(cands)
	for mask := 1; mask < (1 << n); mask++ {
		pc := popcount(mask)
		if pc < 2 || pc > maxSize {
			continue
		}
		var sub []*disruption.Candidate
		for i := 0; i < n; i++ {
			if mask&(1<<i) != 0 {
				sub = append(sub, cands[i])
			}
		}
		fn(sub)
	}
}

func popcount(x int) int {
	c := 0
	for x > 0 {
		c += x & 1
		x >>= 1
	}
	return c
}

func sampleSubsets(cands []*disruption.Candidate, sizes []int, perSize int, rng *rand.Rand, fn func([]*disruption.Candidate)) {
	n := len(cands)
	for _, sz := range sizes {
		if sz < 2 || sz > n {
			continue
		}
		for k := 0; k < perSize; k++ {
			idx := rng.Perm(n)[:sz]
			var sub []*disruption.Candidate
			for _, i := range idx {
				sub = append(sub, cands[i])
			}
			fn(sub)
		}
	}
}

func TestFlowPrefilter(t *testing.T) {
	type scenario struct {
		name  string
		build func(h *harness) []*disruption.Candidate
		run   func(h *harness, cands []*disruption.Candidate, c *confusion)
	}

	mixScenario := func(name string, reg regime) scenario {
		return scenario{
			name: name,
			build: func(h *harness) []*disruption.Candidate {
				rng := rand.New(rand.NewSource(11))
				h.genClusterMix(t, mixParams{
					numNodes: 8, podsPerNode: 2, podCPU: "2", instanceType: "large",
					fracHostAnti: reg.fracHostAnti, fracZonalTSC: reg.fracZonalTSC, fracAffinity: reg.fracAffin,
					groupSize: 3, constraintCPU: "2",
				}, rng)
				return h.allCandidates(t)
			},
			run: func(h *harness, cands []*disruption.Candidate, c *confusion) {
				enumerateSubsets(cands, 6, func(s []*disruption.Candidate) { h.evalPair(t, s, c) })
			},
		}
	}

	scenarios := []scenario{
		mixScenario("mix-none", regime{"none", 0, 0, 0}),
		mixScenario("mix-hostAnti", regime{"hostAnti", 0.3, 0, 0}),
		mixScenario("mix-zonalTSC", regime{"zonalTSC", 0, 0.3, 0}),
		mixScenario("mix-affinity", regime{"affinity", 0, 0, 0.3}),
		mixScenario("mix-mixed", regime{"mixed", 0.15, 0.15, 0.15}),
		{
			// TIGHT homogeneous: medium (8cpu) nodes each running one near-full 7cpu pod.
			// Removing many => 7cpu pods can't fit on ~1cpu-free neighbors + one replacement.
			name: "tight-homogeneous",
			build: func(h *harness) []*disruption.Candidate {
				h.genCluster(t, clusterParams{numNodes: 8, instanceType: "medium", podsPerNode: 1, podCPU: "7"})
				return h.allCandidates(t)
			},
			run: func(h *harness, cands []*disruption.Candidate, c *confusion) {
				enumerateSubsets(cands, 6, func(s []*disruption.Candidate) { h.evalPair(t, s, c) })
			},
		},
		{
			// #2434: 2 dedicated mergeable mediums + filler pools of near-full mediums.
			name: "issue-2434",
			build: func(h *harness) []*disruption.Candidate {
				h.genCluster2434(t, params2434{fillerPools: 4, fillerNodesPerPool: 2, dedicatedZone: "test-zone-1"})
				return h.allCandidates(t)
			},
			run: func(h *harness, cands []*disruption.Candidate, c *confusion) {
				// Up to size 6 so the enumeration includes oversized all-filler sets
				// (>=5 fillers = >=35cpu) that no single replacement can absorb.
				enumerateSubsets(cands, 6, func(s []*disruption.Candidate) { h.evalPair(t, s, c) })
			},
		},
		{
			// Scale: N=40 tight homogeneous, sampled subsets incl. deliberately-oversized.
			name: "scale-40-tight",
			build: func(h *harness) []*disruption.Candidate {
				h.genCluster(t, clusterParams{numNodes: 40, instanceType: "medium", podsPerNode: 1, podCPU: "7"})
				return h.allCandidates(t)
			},
			run: func(h *harness, cands []*disruption.Candidate, c *confusion) {
				rng := rand.New(rand.NewSource(40))
				sampleSubsets(cands, []int{2, 3, 5, 10, 20, 30, 38}, 40, rng,
					func(s []*disruption.Candidate) { h.evalPair(t, s, c) })
			},
		},
	}

	var b strings.Builder
	fmt.Fprintf(&b, "\nFLOW vs cheap AGGREGATE check vs REAL SimulateScheduling\n")
	fmt.Fprintf(&b, "%-20s %-6s %-8s %-8s %-9s %-8s %-14s %-7s %-7s\n",
		"scenario", "sets", "consol.", "flow%", "recall%", "agg%", "flowMarginal", "flowFN", "aggFN")
	fmt.Fprintln(&b, strings.Repeat("-", 96))

	totalFN, totalAggFN := 0, 0
	for _, sc := range scenarios {
		h := newHarness(t)
		cands := sc.build(h)
		c := &confusion{}
		sc.run(h, cands, c)
		totalFN += c.falseNeg
		totalAggFN += c.aggFalseNeg
		aggPct, margPct := 0.0, 0.0
		if c.total > 0 {
			aggPct = 100 * float64(c.aggReject) / float64(c.total)
			margPct = 100 * float64(c.flowNotAgg) / float64(c.total)
		}
		fmt.Fprintf(&b, "%-20s %-6d %-8d %-8.1f %-9.1f %-8.1f %-14s %-7d %-7d\n",
			sc.name, c.total, c.oracleConsol,
			100*c.pruneRate(), 100*c.recallOnInfeasible(), aggPct,
			fmt.Sprintf("%d (%.1f%%)", c.flowNotAgg, margPct), c.falseNeg, c.aggFalseNeg)
	}
	fmt.Fprintln(&b, strings.Repeat("-", 96))
	fmt.Fprintf(&b, "flow%%/agg%% = rejects / all sets. recall%% = flow-rejects / oracle-no-ops. flowMarginal = flow rejects that the O(P+T) agg check ACCEPTS (flow's added value).\n")
	fmt.Fprintf(&b, "false negatives MUST be 0 -- flow: %d, agg: %d\n", totalFN, totalAggFN)
	t.Log(b.String())

	if totalFN != 0 {
		t.Fatalf("flow model unsound: %d false negatives", totalFN)
	}
	if totalAggFN != 0 {
		t.Fatalf("aggregate check unsound: %d false negatives", totalAggFN)
	}
}
