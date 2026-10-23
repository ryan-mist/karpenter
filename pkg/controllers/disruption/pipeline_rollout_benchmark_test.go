//go:build test_performance

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

// A drift rollout through the disruption pipeline (gate on, its production async runners) on a scale-bench cluster,
// with replacement NodeClaims that register only after a delay, as on KWOK and real clouds. The scale benchmark
// launches replacements instantly, which hid that the pipeline wasn't budget-bound at scale (KWOK run 2). This spec
// measures budget utilization: nodes in flight (MarkedForDeletion) over the drift budget allowed, sampled while more
// drifted nodes remain than the budget allows.
//
// Opt-in, like the scale benchmark:
//
//	KARPENTER_PIPELINE_ROLLOUT=1 \
//	KARPENTER_PIPELINE_ROLLOUT_NODES=1000 \
//	KARPENTER_PIPELINE_ROLLOUT_DELAY=30s \
//	KARPENTER_PIPELINE_ROLLOUT_MIN_UTILIZATION=0.9 \
//	KARPENTER_DISRUPTION_BENCH_OUT=/tmp/rollout.jsonl \
//	go test -tags test_performance ./pkg/controllers/disruption/ -run TestAPIs -timeout 2h \
//	  --ginkgo.focus='Pipeline drift rollout'
//
// The fake clock runs at wall-clock speed (benchEnv.stepClock), so the registration delay and the pipeline's pacing are
// in real time. A kwok-like loop launches replacements after the delay, the queue terminates candidates whose
// replacements are initialized, and their pods are re-created on the nodes the command planned them onto.

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/lifecycle"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// runnableRecorder is a manager that only records Add.
type runnableRecorder struct {
	manager.Manager
	added []manager.Runnable
}

func (m *runnableRecorder) Add(r manager.Runnable) error {
	m.added = append(m.added, r)
	return nil
}

func rolloutEnv(name string, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

type rolloutSample struct {
	at                time.Duration
	inFlight, allowed int
	remaining         int
	synced            bool
	loop              time.Duration
}

var _ = Describe("Pipeline drift rollout", Label("benchmark"), func() {
	var b *benchEnv
	BeforeEach(func() {
		if os.Getenv("KARPENTER_PIPELINE_ROLLOUT") == "" {
			Skip("set KARPENTER_PIPELINE_ROLLOUT=1 to run the pipeline drift rollout (see pipeline_rollout_benchmark_test.go)")
		}
		b = newBenchEnv(ctx)
	})
	AfterEach(func() {
		if b != nil {
			b.stop()
		}
	})
	It("keeps the drift budget in use while replacements register late", func() {
		n := lo.Must(strconv.Atoi(rolloutEnv("KARPENTER_PIPELINE_ROLLOUT_NODES", "1000")))
		delay := lo.Must(time.ParseDuration(rolloutEnv("KARPENTER_PIPELINE_ROLLOUT_DELAY", "30s")))
		limit := lo.Must(time.ParseDuration(rolloutEnv("KARPENTER_PIPELINE_ROLLOUT_TIMEOUT", "40m")))
		minUtil := lo.Must(strconv.ParseFloat(rolloutEnv("KARPENTER_PIPELINE_ROLLOUT_MIN_UTILIZATION", "0"), 64))
		bc := b.buildCluster(clusterSpec{nodes: n, podCPU: "3", podMem: "5Gi", drifted: true, budget: "10%", capacity: v1.CapacityTypeSpot, consolAftr: "Never"})
		emit(map[string]any{"scenario": "rollout-setup", "nodes": n, "pods": bc.pods, "setupMs": ms(bc.setup), "registrationDelay": delay.String()})
		original := lo.SliceToMap(bc.nodeClaims, func(nc *v1.NodeClaim) (string, bool) { return nc.Name, true })
		// The pipeline's runners always wait on the clock, so benchEnv's stepper (which jumps 15 s whenever something
		// waits) would race the fake clock ahead; here it only follows wall time.
		b.wallClockOnly()

		c := pipeline.NewController(b.ctx, env.Clock, b.client, b.prov, cloudProvider, b.recorder, b.cluster, b.queue, b.clusterCost)
		m := &runnableRecorder{}
		Expect(c.Register(b.ctx, m)).To(Succeed())
		runCtx, cancel := context.WithCancel(b.ctx)
		defer cancel()
		for _, r := range m.added {
			go func() {
				defer GinkgoRecover()
				Expect(r.Start(runCtx)).To(Succeed())
			}()
		}

		launchedAt := map[string]time.Time{}
		moved := map[types.UID]bool{}
		var samples []rolloutSample
		start, startClock := time.Now(), env.Clock.Now()
		var firstCommand time.Duration
		for time.Since(start) < limit {
			iter := time.Now()
			b.registerLate(launchedAt, delay)
			b.completeCommands(moved)
			numNodes, _ := disruption.NodePoolStats(b.cluster, v1.DisruptionReasonDrifted)
			allowed := lo.Must(bc.nodePool.GetAllowedDisruptionsByReason(env.Clock, numNodes[bc.nodePool.Name], v1.DisruptionReasonDrifted))
			nodes := b.cluster.DeepCopyNodes()
			inFlight := lo.CountBy(nodes, func(n *state.StateNode) bool { return n.MarkedForDeletion() })
			remaining := lo.CountBy(nodes, func(n *state.StateNode) bool { return n.NodeClaim != nil && original[n.NodeClaim.Name] })
			if inFlight > 0 && firstCommand == 0 {
				firstCommand = env.Clock.Since(startClock)
			}
			samples = append(samples, rolloutSample{at: env.Clock.Since(startClock), inFlight: inFlight, allowed: allowed, remaining: remaining,
				synced: b.cluster.Synced(b.ctx), loop: time.Since(iter)})
			if remaining == 0 {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		cancel()

		// Utilization while more drifted nodes remain than the budget allows, from the first command on.
		steady := lo.Filter(samples, func(s rolloutSample, _ int) bool {
			return s.at >= firstCommand && s.remaining > s.allowed && s.allowed > 0
		})
		utils := lo.Map(steady, func(s rolloutSample, _ int) float64 { return float64(s.inFlight) / float64(s.allowed) })
		sort.Float64s(utils)
		mean := lo.Ternary(len(utils) > 0, lo.Sum(utils)/float64(max(len(utils), 1)), 0)
		elapsed := samples[len(samples)-1].at
		replaced := n - samples[len(samples)-1].remaining
		steadyMean := func(f func(rolloutSample) float64) float64 {
			return lo.SumBy(steady, f) / float64(max(len(steady), 1))
		}
		emit(map[string]any{"scenario": "rollout", "nodes": n, "registrationDelay": delay.String(), "replaced": replaced,
			"elapsedS": elapsed.Seconds(), "nodesPerMin": float64(replaced) / max(elapsed.Minutes(), 1e-9), "firstCommandS": firstCommand.Seconds(),
			"meanUtilization": mean, "p10Utilization": quantile(utils, 0.1), "p50Utilization": quantile(utils, 0.5), "steadySamples": len(utils),
			// Why budget went unused: cluster state unsynced, the harness's own loop time, and the budget itself.
			"unsyncedShare": steadyMean(func(s rolloutSample) float64 { return lo.Ternary(s.synced, 0.0, 1.0) }),
			"meanLoopMs":    steadyMean(func(s rolloutSample) float64 { return float64(s.loop.Milliseconds()) }),
			"meanAllowed":   steadyMean(func(s rolloutSample) float64 { return float64(s.allowed) }),
			"pipeline":      pipelineCounters(),
			// [seconds, in flight, allowed, original nodes remaining, synced] per sample.
			"series": lo.Map(samples, func(s rolloutSample, _ int) []any {
				return []any{s.at.Seconds(), s.inFlight, s.allowed, s.remaining, s.synced}
			}),
		})
		if minUtil > 0 {
			Expect(mean).To(BeNumerically(">=", minUtil), "mean budget utilization while drift is budget-bound")
		}
	})
})

// wallClockOnly replaces benchEnv's clock stepper with one that only keeps the fake clock at wall-clock speed.
func (b *benchEnv) wallClockOnly() {
	close(b.stopStep)
	<-b.stepDone
	b.stopStep, b.stepDone = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(b.stepDone)
		last := time.Now()
		for {
			select {
			case <-b.stopStep:
				return
			case <-time.After(5 * time.Millisecond):
			}
			now := time.Now()
			env.Clock.Step(now.Sub(last))
			last = now
		}
	}()
}

// registerLate is KWOK: a new NodeClaim is launched at once (its providerID resolved, so cluster state stays synced),
// and its Node appears, registered and initialized, only delay later.
func (b *benchEnv) registerLate(launchedAt map[string]time.Time, delay time.Duration) {
	GinkgoHelper()
	ncs := &v1.NodeClaimList{}
	Expect(env.Client.List(ctx, ncs)).To(Succeed())
	for i := range ncs.Items {
		nc := &ncs.Items[i]
		if !nc.DeletionTimestamp.IsZero() || nc.StatusConditions().Get(v1.ConditionTypeInitialized).IsTrue() {
			continue
		}
		if nc.Status.ProviderID == "" {
			resolved, err := cloudProvider.Create(ctx, nc)
			Expect(err).ToNot(HaveOccurred())
			nc = lifecycle.PopulateNodeClaimDetails(nc, resolved)
			nc.StatusConditions().SetTrue(v1.ConditionTypeLaunched)
			Expect(env.Client.Status().Update(ctx, nc)).To(Succeed())
			b.cluster.UpdateNodeClaim(nc)
			launchedAt[nc.Name] = env.Clock.Now()
			continue
		}
		at, ok := launchedAt[nc.Name]
		if !ok || env.Clock.Since(at) < delay {
			continue
		}
		nc.StatusConditions().SetTrue(v1.ConditionTypeRegistered)
		n := test.NodeClaimLinkedNode(nc)
		n.Spec.Taints = lo.Reject(n.Spec.Taints, func(t corev1.Taint, _ int) bool { return t.MatchTaint(&v1.UnregisteredNoExecuteTaint) })
		n.Labels[v1.NodeRegisteredLabelKey] = "true"
		nc.Status.NodeName = n.Name
		ExpectApplied(ctx, env.Client, nc, n)
		ncs, ns := []*v1.NodeClaim{nc}, []*corev1.Node{n}
		ExpectMakeNodeClaimsInitialized(b.ctx, env.Client, env.Clock, ncs...)
		ExpectMakeNodesInitialized(b.ctx, env.Client, env.Clock, ns...)
		b.cluster.UpdateNodeClaim(ncs[0])
		Expect(b.cluster.UpdateNode(b.ctx, ns[0])).To(Succeed())
	}
}

// completeCommands runs the queue for every command whose replacements are initialized: it deletes the candidates,
// whose nodes and NodeClaims then go away, and their pods are re-created on the nodes the command planned them onto.
func (b *benchEnv) completeCommands(moved map[types.UID]bool) {
	GinkgoHelper()
	for _, cmd := range b.queue.GetCommands() {
		ready := lo.EveryBy(cmd.Replacements, func(r *disruption.Replacement) bool {
			nc := &v1.NodeClaim{}
			return env.Client.Get(ctx, client.ObjectKey{Name: r.Name}, nc) == nil && nc.StatusConditions().Get(v1.ConditionTypeInitialized).IsTrue()
		})
		if !ready {
			continue
		}
		ExpectObjectReconciled(b.ctx, env.Client, b.queue, cmd.Candidates[0].NodeClaim)
		dest := plannedDestinations(cmd)
		for _, cand := range cmd.Candidates {
			for _, p := range cand.ReschedulablePods() {
				if moved[p.UID] {
					continue
				}
				moved[p.UID] = true
				b.movePod(p, dest[p.UID])
			}
			nc := &v1.NodeClaim{}
			if err := env.Client.Get(ctx, client.ObjectKeyFromObject(cand.NodeClaim), nc); err == nil {
				ExpectFinalizersRemoved(ctx, env.Client, nc)
			}
			if n := cand.Node; n != nil {
				if err := env.Client.Delete(ctx, n); err == nil || errors.IsNotFound(err) {
					ExpectFinalizersRemoved(ctx, env.Client, n)
				}
				ExpectReconcileSucceeded(b.ctx, b.nodeCtrl, client.ObjectKeyFromObject(n))
			}
			ExpectReconcileSucceeded(b.ctx, b.ncCtrl, client.ObjectKeyFromObject(cand.NodeClaim))
		}
	}
}

// plannedDestinations maps each of the command's pods to the node it was planned onto: an existing node, or the node of
// the replacement NodeClaim it was planned onto ("" if unknown).
func plannedDestinations(cmd *disruption.Command) map[types.UID]string {
	out := map[types.UID]string{}
	for _, n := range cmd.Results.ExistingNodes {
		for _, p := range n.Pods {
			out[p.UID] = n.Name()
		}
	}
	for _, r := range cmd.Replacements {
		nc := &v1.NodeClaim{}
		if env.Client.Get(ctx, client.ObjectKey{Name: r.Name}, nc) != nil {
			continue
		}
		for _, p := range r.Pods {
			out[p.UID] = nc.Status.NodeName
		}
	}
	return out
}

// movePod deletes p and re-creates it bound to node, as its controller and the scheduler would.
func (b *benchEnv) movePod(p *corev1.Pod, node string) {
	GinkgoHelper()
	if err := env.Client.Delete(ctx, p); err != nil && !errors.IsNotFound(err) {
		Expect(err).ToNot(HaveOccurred())
	}
	ExpectReconcileSucceeded(b.ctx, b.podCtrl, client.ObjectKeyFromObject(p))
	if node == "" {
		return
	}
	np := p.DeepCopy()
	np.ResourceVersion, np.UID, np.Spec.NodeName = "", "", node
	np.Name = p.Name + "-m"
	np.Status.Phase = corev1.PodRunning
	applyFast(ctx, env.Client, np)
	ExpectReconcileSucceeded(b.ctx, b.podCtrl, client.ObjectKeyFromObject(np))
}

func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[min(len(sorted)-1, int(q*float64(len(sorted))))]
}

// pipelineCounters are the pipeline's and the joint simulations' counters, and its histograms' sums and counts, by series.
func pipelineCounters() map[string]float64 {
	out := map[string]float64{}
	for _, mf := range lo.Must(crmetrics.Registry.Gather()) {
		name := mf.GetName()
		if !strings.Contains(name, "_pipeline_") && !strings.Contains(name, "_batch_") || strings.Contains(name, "_shadow_") {
			continue
		}
		for _, m := range mf.Metric {
			if m.Counter != nil {
				out[fmt.Sprint(name, m.GetLabel())] = m.GetCounter().GetValue()
			}
			if h := m.GetHistogram(); h != nil {
				out[fmt.Sprint(name, "_sum", m.GetLabel())] = h.GetSampleSum()
				out[fmt.Sprint(name, "_count", m.GetLabel())] = float64(h.GetSampleCount())
			}
		}
	}
	return out
}
