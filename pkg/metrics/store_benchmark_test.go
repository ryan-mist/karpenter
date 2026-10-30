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

package metrics_test

import (
	"fmt"
	"maps"
	"math/rand"
	"testing"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

const (
	benchGauges        = 5
	benchResourceTypes = 5 // benchGauges x benchResourceTypes = 25 series per key, like metrics.node
	benchNodeLabels    = 38
)

// benchGeneration builds one ReplaceAll input shaped like the metrics.node controller's: per node, 25 series with 39
// labels each (38 node labels plus resource_type), and freshly allocated label maps as the controller builds per tick.
// Series are emitted in a random order per tick (the controller iterates maps). changed[node] bumps that node's phase
// label so all of its series are replaced.
func benchGeneration(r *rand.Rand, gauges []opmetrics.GaugeMetric, nodes int, changed map[int]bool) map[string][]*metrics.StoreMetric {
	res := make(map[string][]*metrics.StoreMetric, nodes)
	for n := range nodes {
		base := map[string]string{"node_name": fmt.Sprintf("node-%d", n), "node_phase": lo.Ternary(changed[n], "Changed", "Running")}
		for l := len(base); l < benchNodeLabels; l++ {
			base[fmt.Sprintf("label_%d", l)] = fmt.Sprintf("value-%d-%d", l, n%7)
		}
		var ms []*metrics.StoreMetric
		for _, g := range gauges {
			for rt := range benchResourceTypes {
				labels := maps.Clone(base)
				labels["resource_type"] = fmt.Sprintf("resource_%d", rt)
				ms = append(ms, &metrics.StoreMetric{GaugeMetric: g, Value: float64(rt), Labels: labels})
			}
		}
		r.Shuffle(len(ms), func(i, j int) { ms[i], ms[j] = ms[j], ms[i] })
		res[fmt.Sprintf("default/node-%d", n)] = ms
	}
	return res
}

func BenchmarkStoreReplaceAll(b *testing.B) {
	for _, nodes := range []int{1000, 5000} {
		for _, churn := range []bool{false, true} {
			b.Run(fmt.Sprintf("nodes=%d/churn=%s", nodes, lo.Ternary(churn, "1pct", "none")), func(b *testing.B) {
				registry := prometheus.NewRegistry()
				labelNames := append([]opmetrics.Label{{Name: "node_name"}, {Name: "node_phase"}, {Name: "resource_type"}},
					lo.Times(benchNodeLabels-2, func(i int) opmetrics.Label { return opmetrics.Label{Name: fmt.Sprintf("label_%d", i+2)} })...)
				gauges := lo.Times(benchGauges, func(i int) opmetrics.GaugeMetric {
					return opmetrics.NewPrometheusGauge(registry, prometheus.GaugeOpts{Name: fmt.Sprintf("bench_gauge_%d", i)}, labelNames, opmetrics.Alpha)
				})
				// Alternate between two generations; with churn, 1% of nodes differ between them so each ReplaceAll
				// replaces ~1% of series
				r := rand.New(rand.NewSource(0))
				changed := map[int]bool{}
				if churn {
					for _, n := range r.Perm(nodes)[:nodes/100] {
						changed[n] = true
					}
				}
				generations := []map[string][]*metrics.StoreMetric{
					benchGeneration(r, gauges, nodes, nil),
					benchGeneration(r, gauges, nodes, changed),
				}
				store := metrics.NewStore()
				store.ReplaceAll(generations[1])
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					store.ReplaceAll(generations[i%2])
				}
			})
		}
	}
}
