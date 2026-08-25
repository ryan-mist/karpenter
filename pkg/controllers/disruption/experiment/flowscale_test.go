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

// Scaling benchmark for the flow prefilter: how does its cost grow with cluster
// size N, vs one SimulateScheduling call at the same N?
//
// flowFeasible bundles pod/target gathering (per-node client.List) with the
// max-flow solve. Here we split them: flowGather (client access, a harness
// artifact) vs flowSolve (graph build + Dinic — the true algorithmic cost).

import (
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// flowInputs is everything flowSolve needs, gathered once (client access hoisted
// out of the timed solve region).
type flowInputs struct {
	displaced     []*corev1.Pod
	targets       []*state.StateNode
	stayGroupNode map[string]map[string]bool
	replCap       map[string]int64
}

func (h *harness) flowGather(t *testing.T, S []*disruption.Candidate) flowInputs {
	t.Helper()
	sNames := map[string]bool{}
	for _, c := range S {
		sNames[c.Name()] = true
	}
	var displaced []*corev1.Pod
	for _, c := range S {
		for _, p := range h.podsOn(t, c.Name()) {
			if podutils.IsReschedulable(p) {
				displaced = append(displaced, p)
			}
		}
	}
	var targets []*state.StateNode
	for _, sn := range h.cluster.DeepCopyNodes() {
		if sn.Node == nil && sn.NodeClaim == nil {
			continue
		}
		if !sNames[sn.Name()] {
			targets = append(targets, sn)
		}
	}
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
	return flowInputs{displaced, targets, stayGroupNode, replCap}
}

// flowSolve is the pure algorithmic cost: build the per-dimension transportation
// graph and run Dinic. Identical logic to flowFeasible's solve loop, no client access.
func flowSolve(in flowInputs) bool {
	for _, d := range dims {
		var total int64
		demand := make([]int64, len(in.displaced))
		for i, p := range in.displaced {
			demand[i] = d.get(resources.RequestsForPods(p))
			total += demand[i]
		}
		if total == 0 {
			continue
		}
		P, T := len(in.displaced), len(in.targets)
		src, repl, sink := 0, P+T+1, P+T+2
		mf := newMaxflow(P + T + 3)
		for i, dem := range demand {
			mf.addEdge(src, 1+i, dem)
		}
		for j, tn := range in.targets {
			mf.addEdge(P+1+j, sink, d.get(tn.Available()))
		}
		mf.addEdge(repl, sink, in.replCap[d.name])
		for i, p := range in.displaced {
			mf.addEdge(1+i, repl, flowINF)
			app := p.Labels["app"]
			isHA := hasHostAnti(p) && app != ""
			for j, tn := range in.targets {
				if isHA && in.stayGroupNode[app][tn.Name()] {
					continue
				}
				mf.addEdge(1+i, P+1+j, flowINF)
			}
		}
		if mf.run(src, sink) < total {
			return false
		}
	}
	return true
}

// aggSolve is the trivial O(P+T) aggregate-capacity check: reject iff, for some
// binding dimension, total displaced demand exceeds total target headroom + the
// generous replacement. It is SOUND (aggregate demand > aggregate capacity =>
// truly infeasible) and a STRICT WEAKENING of flowSolve: flowSolve additionally
// enforces per-pod legality/anti-affinity edges, so flow rejects a SUPERSET of
// what agg rejects. With no anti-affinity, all pods can reach all capacity (INF
// edges) and max-flow == aggregate check exactly — so flow's only marginal value
// over agg is where anti-affinity (or other edge removal) shrinks reachability.
func aggSolve(in flowInputs) bool {
	for _, d := range dims {
		var demand, capacity int64
		for _, p := range in.displaced {
			demand += d.get(resources.RequestsForPods(p))
		}
		if demand == 0 {
			continue
		}
		for _, tn := range in.targets {
			capacity += d.get(tn.Available())
		}
		capacity += in.replCap[d.name]
		if demand > capacity {
			return false // provably cannot re-home: not enough aggregate room
		}
	}
	return true
}

func avgMicros(reps int, fn func()) int64 {
	if reps < 1 {
		reps = 1
	}
	start := time.Now()
	for i := 0; i < reps; i++ {
		fn()
	}
	return time.Since(start).Microseconds() / int64(reps)
}

func TestFlowScaling(t *testing.T) {
	type variant struct {
		name   string
		params func(n int) clusterParams
	}
	variants := []variant{
		// LOOSE: very underutilized 32-CPU nodes -> flow ACCEPTS (saturates all pods).
		{"loose", func(n int) clusterParams {
			return clusterParams{numNodes: n, instanceType: "large", podsPerNode: 2, podCPU: "2"}
		}},
		// TIGHT: near-full 8-CPU nodes (7-CPU pod each) -> flow REJECTS (runs to exhaustion).
		{"tight", func(n int) clusterParams {
			return clusterParams{numNodes: n, instanceType: "medium", podsPerNode: 1, podCPU: "7"}
		}},
	}
	Ns := []int{50, 100, 200, 500, 1000}

	var b strings.Builder
	fmt.Fprintf(&b, "\nFLOW PREFILTER SCALING (avg over reps; wall-clock)\n")
	fmt.Fprintf(&b, "%-7s %-5s %-8s %-9s %-12s %-12s %-14s %-12s %-8s\n",
		"variant", "N", "targets", "displ.", "flowSolve us", "flowFull us", "Simulate us", "setup ms", "verdict")
	fmt.Fprintln(&b, strings.Repeat("-", 100))

	for _, v := range variants {
		for _, n := range Ns {
			h := newHarness(t)
			ts := time.Now()
			h.genCluster(t, v.params(n))
			cands := h.allCandidates(t)
			setupMs := time.Since(ts).Milliseconds()
			if len(cands) == 0 {
				t.Fatalf("%s N=%d: no candidates", v.name, n)
			}
			k := min(len(cands), 40)
			S := cands[:k]

			in := h.flowGather(t, S) // client access hoisted out of the timed solve
			verdict := flowSolve(in)

			solveUs := avgMicros(8, func() { _ = flowSolve(in) })
			fullUs := avgMicros(8, func() { _ = h.flowFeasible(t, S) })
			simUs := avgMicros(3, func() { _ = h.evalSet(t, S) })

			vs := "accept"
			if !verdict {
				vs = "REJECT"
			}
			fmt.Fprintf(&b, "%-7s %-5d %-8d %-9d %-12d %-12d %-14d %-12d %-8s\n",
				v.name, n, len(in.targets), len(in.displaced), solveUs, fullUs, simUs, setupMs, vs)
		}
		fmt.Fprintln(&b, strings.Repeat("-", 100))
	}
	t.Log(b.String())
}
