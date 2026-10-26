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

// A drift rollout by the disruption controller (gate off) with the pipeline's shadow mode at rate 1, on the
// late-registration harness of the pipeline drift rollout: the shadow soak of KWOK, in envtest. It reports the shadow's
// comparisons and mismatches by level, planner, kind and cause, and the mismatch details it logged.
//
//	KARPENTER_PIPELINE_SHADOW_ROLLOUT=1 KARPENTER_PIPELINE_ROLLOUT_NODES=300 KARPENTER_PIPELINE_ROLLOUT_DELAY=10s \
//	KARPENTER_DISRUPTION_BENCH_OUT=/tmp/shadow.jsonl \
//	go test -tags test_performance ./pkg/controllers/disruption/ -run TestAPIs -timeout 2h --ginkgo.focus='Shadow drift rollout'

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Shadow drift rollout", Label("benchmark"), func() {
	var b *benchEnv
	BeforeEach(func() {
		if os.Getenv("KARPENTER_PIPELINE_SHADOW_ROLLOUT") == "" {
			Skip("set KARPENTER_PIPELINE_SHADOW_ROLLOUT=1 to run the shadow drift rollout")
		}
		b = newBenchEnv(ctx)
	})
	AfterEach(func() {
		if b != nil {
			b.stop()
		}
	})
	It("compares the shadow with the disruption controller through a rollout", func() {
		n := lo.Must(strconv.Atoi(rolloutEnv("KARPENTER_PIPELINE_ROLLOUT_NODES", "300")))
		delay := lo.Must(time.ParseDuration(rolloutEnv("KARPENTER_PIPELINE_ROLLOUT_DELAY", "10s")))
		limit := lo.Must(time.ParseDuration(rolloutEnv("KARPENTER_PIPELINE_ROLLOUT_TIMEOUT", "20m")))
		bc := b.buildCluster(clusterSpec{nodes: n, podCPU: "3", podMem: "5Gi", drifted: true, budget: "10%", capacity: v1.CapacityTypeSpot, consolAftr: "Never"})
		original := lo.SliceToMap(bc.nodeClaims, func(nc *v1.NodeClaim) (string, bool) { return nc.Name, true })
		b.wallClockOnly()
		var mu sync.Mutex
		var logged []string
		sctx := log.IntoContext(b.ctx, funcr.New(func(prefix, args string) {
			mu.Lock()
			defer mu.Unlock()
			logged = append(logged, prefix+" "+args)
		}, funcr.Options{}))
		for _, m := range []interface{ Reset() }{pipeline.ShadowComparisonsTotal, pipeline.ShadowMismatchesTotal} {
			m.Reset()
		}
		shadow := pipeline.NewShadow(sctx, env.Clock, b.client, b.prov, cloudProvider, b.cluster, b.queue, b.clusterCost, 1)
		legacy := disruption.NewController(b.ctx, env.Clock, b.client, b.prov, cloudProvider, b.recorder, b.cluster, b.queue, b.clusterCost,
			disruption.WithComputeObserver(shadow))
		launchedAt := map[string]time.Time{}
		moved := map[types.UID]bool{}
		start := time.Now()
		passes := 0
		for time.Since(start) < limit {
			b.registerLate(launchedAt, delay)
			b.completeCommands(moved)
			ExpectSingletonReconciled(b.ctx, legacy)
			passes++
			remaining := lo.CountBy(b.cluster.DeepCopyNodes(), func(n *state.StateNode) bool { return n.NodeClaim != nil && original[n.NodeClaim.Name] })
			if remaining == 0 {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		// Let the last sample's goroutine finish (WaitIdle is internal to the pipeline package).
		time.Sleep(30 * time.Second)
		mu.Lock()
		defer mu.Unlock()
		emit(map[string]any{"scenario": "shadow-rollout", "nodes": n, "registrationDelay": delay.String(), "passes": passes,
			"elapsedS": time.Since(start).Seconds(), "shadow": shadowCounters(),
			"mismatches": lo.Filter(logged, func(l string, _ int) bool { return strings.Contains(l, "shadow mismatch") })})
	})
})

// shadowCounters are the shadow's comparison and mismatch counters, by series.
func shadowCounters() map[string]float64 {
	out := map[string]float64{}
	for _, mf := range lo.Must(crmetrics.Registry.Gather()) {
		if !strings.Contains(mf.GetName(), "_shadow_") {
			continue
		}
		for _, m := range mf.Metric {
			if m.Counter != nil {
				out[fmt.Sprint(mf.GetName(), m.GetLabel())] = m.GetCounter().GetValue()
			}
		}
	}
	return out
}
