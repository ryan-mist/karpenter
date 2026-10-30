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
	"math/rand"
	"slices"
	"sort"
	"strings"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/api/equality"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

// Well-formed label sets always carry their store key, so no two keys own the same series: ReplaceAll visits keys in
// map order, which would otherwise make the final state of a series shared between keys nondeterministic
var diffLabelNames = []string{"a", "b", "c", "key"}

// recordingGauge logs every Set/Delete it receives and forwards well-formed calls to a real gauge, so that both the call
// sequence and the gathered output can be compared
type recordingGauge struct {
	opmetrics.GaugeMetric
	name string
	log  *[]string
}

func (g *recordingGauge) Set(v float64, labels map[string]string) {
	*g.log = append(*g.log, fmt.Sprintf("set %s %v %s", g.name, v, formatLabels(labels)))
	if len(labels) == len(diffLabelNames) {
		g.GaugeMetric.Set(v, labels)
	}
}

func (g *recordingGauge) Delete(labels map[string]string) {
	*g.log = append(*g.log, fmt.Sprintf("delete %s %s", g.name, formatLabels(labels)))
	if len(labels) == len(diffLabelNames) {
		g.GaugeMetric.Delete(labels)
	}
}

func formatLabels(labels map[string]string) string {
	if labels == nil {
		return "nil"
	}
	keys := lo.Keys(labels)
	sort.Strings(keys)
	return strings.Join(lo.Map(keys, func(k string, _ int) string { return k + "=" + labels[k] }), ",")
}

// referenceStore is the previous O(old x new) implementation of metrics.Store, kept to check the keyed diff against
type referenceStore struct {
	store map[string][]*metrics.StoreMetric
}

func (s *referenceStore) update(key string, ms []*metrics.StoreMetric) {
	for _, metric := range ms {
		metric.Set(metric.Value, metric.Labels)
	}
	if oldMetrics, ok := s.store[key]; ok {
		for _, oldMetric := range oldMetrics {
			if _, ok = lo.Find(ms, func(m *metrics.StoreMetric) bool {
				return oldMetric.GaugeMetric == m.GaugeMetric && equality.Semantic.DeepEqual(oldMetric.Labels, m.Labels)
			}); !ok {
				oldMetric.Delete(oldMetric.Labels)
			}
		}
	}
	s.store[key] = ms
}

func (s *referenceStore) ReplaceAll(newStore map[string][]*metrics.StoreMetric) {
	for k, v := range newStore {
		s.update(k, v)
	}
	for k := range s.store {
		if _, ok := newStore[k]; !ok {
			s.Delete(k)
		}
	}
}

func (s *referenceStore) Delete(key string) {
	if ms, ok := s.store[key]; ok {
		for _, metric := range ms {
			metric.Delete(metric.Labels)
		}
		delete(s.store, key)
	}
}

type diffWorld struct {
	registry *prometheus.Registry
	gauges   []opmetrics.GaugeMetric
	log      []string
}

func newDiffWorld() *diffWorld {
	w := &diffWorld{registry: prometheus.NewRegistry()}
	for i := range 3 {
		name := fmt.Sprintf("diff_gauge_%d", i)
		w.gauges = append(w.gauges, &recordingGauge{
			GaugeMetric: opmetrics.NewPrometheusGauge(w.registry, prometheus.GaugeOpts{Name: name}, lo.Map(diffLabelNames, func(n string, _ int) opmetrics.Label { return opmetrics.Label{Name: n} }), opmetrics.Alpha),
			name:        name,
			log:         &w.log,
		})
	}
	return w
}

// gather returns the registry's full gathered output, one string per metric family
func (w *diffWorld) gather() []string {
	families, err := w.registry.Gather()
	Expect(err).ToNot(HaveOccurred())
	return lo.Map(families, func(f *dto.MetricFamily, _ int) string { return f.String() })
}

type metricSpec struct {
	gauge  int
	value  float64
	labels map[string]string
}

func (w *diffWorld) build(specs []metricSpec) []*metrics.StoreMetric {
	return lo.Map(specs, func(s metricSpec, _ int) *metrics.StoreMetric {
		return &metrics.StoreMetric{GaugeMetric: w.gauges[s.gauge], Value: s.value, Labels: s.labels}
	})
}

// randomLabels draws from a small space so that equal, duplicate, nil, empty and partial label sets are all common
func randomLabels(r *rand.Rand, key string) map[string]string {
	switch r.Intn(10) {
	case 0:
		return nil
	case 1:
		return map[string]string{}
	case 2:
		return map[string]string{diffLabelNames[r.Intn(3)]: []string{"", "x"}[r.Intn(2)]}
	}
	labels := lo.SliceToMap(diffLabelNames[:3], func(n string) (string, string) { return n, []string{"", "x", "y"}[r.Intn(3)] })
	labels["key"] = key
	return labels
}

func randomSpecs(r *rand.Rand, key string) []metricSpec {
	specs := make([]metricSpec, r.Intn(8))
	for i := range specs {
		specs[i] = metricSpec{gauge: r.Intn(3), value: float64(r.Intn(4)), labels: randomLabels(r, key)}
		// Share a label map between metrics sometimes
		if i > 0 && r.Intn(8) == 0 {
			specs[i].labels = specs[r.Intn(i)].labels
		}
	}
	return specs
}

var _ = Describe("Store differential", func() {
	It("should make the same Set and Delete calls as the reference implementation", func() {
		for seed := range int64(200) {
			r := rand.New(rand.NewSource(seed))
			ref, refWorld := &referenceStore{store: map[string][]*metrics.StoreMetric{}}, newDiffWorld()
			store, storeWorld := metrics.NewStore(), newDiffWorld()
			keys := []string{"k0", "k1", "k2", "k3"}

			for step := range 50 {
				refWorld.log, storeWorld.log = nil, nil
				ordered := true
				switch op := r.Intn(4); op {
				case 0, 1:
					key := keys[r.Intn(len(keys))]
					specs := randomSpecs(r, key)
					ref.update(key, refWorld.build(specs))
					store.Update(key, storeWorld.build(specs))
				case 2:
					// ReplaceAll iterates a map, so calls across keys are unordered
					ordered = false
					specsByKey := map[string][]metricSpec{}
					for _, k := range keys {
						if r.Intn(3) != 0 {
							specsByKey[k] = randomSpecs(r, k)
						}
					}
					ref.ReplaceAll(lo.MapValues(specsByKey, func(s []metricSpec, _ string) []*metrics.StoreMetric { return refWorld.build(s) }))
					store.ReplaceAll(lo.MapValues(specsByKey, func(s []metricSpec, _ string) []*metrics.StoreMetric { return storeWorld.build(s) }))
				case 3:
					key := keys[r.Intn(len(keys))]
					ref.Delete(key)
					store.Delete(key)
				}
				refLog, storeLog := slices.Clone(refWorld.log), slices.Clone(storeWorld.log)
				if !ordered {
					sort.Strings(refLog)
					sort.Strings(storeLog)
				}
				Expect(storeLog).To(Equal(refLog), "seed %d step %d", seed, step)
				Expect(storeWorld.gather()).To(Equal(refWorld.gather()), "seed %d step %d", seed, step)
			}
		}
	})
	It("should not delete a series whose labels are nil in one update and empty in the next", func() {
		w := newDiffWorld()
		store := metrics.NewStore()
		store.Update("k", w.build([]metricSpec{{gauge: 0, labels: nil}}))
		store.Update("k", w.build([]metricSpec{{gauge: 0, labels: map[string]string{}}}))
		Expect(w.log).To(Equal([]string{"set diff_gauge_0 0 nil", "set diff_gauge_0 0 "}))
	})
	It("should delete duplicate stale series once per old entry", func() {
		w := newDiffWorld()
		store := metrics.NewStore()
		labels := map[string]string{"a": "x", "b": "x", "c": "x", "key": "k"}
		store.Update("k", w.build([]metricSpec{{gauge: 0, labels: labels}, {gauge: 0, labels: labels}}))
		w.log = nil
		store.Update("k", w.build([]metricSpec{{gauge: 1, labels: labels}}))
		Expect(w.log).To(Equal([]string{"set diff_gauge_1 0 a=x,b=x,c=x,key=k", "delete diff_gauge_0 a=x,b=x,c=x,key=k", "delete diff_gauge_0 a=x,b=x,c=x,key=k"}))
	})
})
