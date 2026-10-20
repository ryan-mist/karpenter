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

package topoindex

import (
	"fmt"
	"runtime"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

type benchCluster struct {
	pods   []*corev1.Pod
	specs  []GroupSpec
	nodes  int
	groups int
}

// newBenchCluster builds deployments of 30 pods spread over the nodes and zones, with one zone spread group and, for
// every fifth deployment, one hostname group per deployment (like KWOK config T).
func newBenchCluster(pods, nodes, namespaces int) *benchCluster {
	c := &benchCluster{nodes: nodes}
	deployments := pods / 30
	for d := range deployments {
		ns := fmt.Sprintf("ns-%d", d%namespaces)
		l := map[string]string{"app": fmt.Sprintf("app-%d", d), "pod-template-hash": fmt.Sprintf("h%d", d), "team": fmt.Sprint(d % 7)}
		for i := range 30 {
			n := (d*30 + i) % nodes
			c.pods = append(c.pods, &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: types.UID(fmt.Sprintf("uid-%d-%d", d, i)), Name: fmt.Sprintf("p-%d-%d", d, i), Namespace: ns, Labels: l, ResourceVersion: "1"},
				Spec:       corev1.PodSpec{NodeName: fmt.Sprintf("node-%d", n)},
				Status:     corev1.PodStatus{Phase: corev1.PodRunning},
			})
		}
		selector := labels.SelectorFromSet(labels.Set{"app": l["app"]})
		c.specs = append(c.specs, GroupSpec{TopologyKey: corev1.LabelTopologyZone, Namespaces: []string{ns}, Selector: selector})
		if d%5 == 0 {
			c.specs = append(c.specs, GroupSpec{TopologyKey: corev1.LabelHostname, Namespaces: []string{ns}, Selector: selector})
		}
	}
	return c
}

func (c *benchCluster) index(b *testing.B, groups int) *Index {
	b.Helper()
	ix := New()
	for i := range c.nodes {
		ix.ObserveNode(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node-%d", i), ResourceVersion: "1",
			Labels: map[string]string{corev1.LabelTopologyZone: fmt.Sprintf("zone-%d", i%3), corev1.LabelHostname: fmt.Sprintf("node-%d", i)}}})
	}
	for _, p := range c.pods {
		ix.ObservePod(p)
	}
	for _, spec := range c.specs[:min(groups, len(c.specs))] {
		ix.Register(spec)
	}
	return ix
}

var sizes = []struct{ pods, nodes, namespaces int }{{3000, 150, 10}, {150_000, 15_000, 500}}

// BenchmarkEvent measures a real diff (a pod moving to another node) with 500 registered groups, publishing a
// snapshot every 16 events as a lazily publishing index would under 167 events/s and 5.5 simulations/s.
func BenchmarkEvent(b *testing.B) {
	for _, size := range sizes {
		b.Run(fmt.Sprintf("pods=%d", size.pods), func(b *testing.B) {
			c := newBenchCluster(size.pods, size.nodes, size.namespaces)
			ix := c.index(b, 500)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				p := c.pods[i%len(c.pods)].DeepCopy()
				p.ResourceVersion = fmt.Sprint(i + 2)
				p.Spec.NodeName = fmt.Sprintf("node-%d", (i*7)%size.nodes)
				ix.ObservePod(p)
				if i%16 == 0 {
					ix.Snapshot()
				}
			}
		})
	}
}

// BenchmarkNoopEvent measures a periodic resync: the same resource version.
func BenchmarkNoopEvent(b *testing.B) {
	c := newBenchCluster(150_000, 15_000, 500)
	ix := c.index(b, 500)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		ix.ObservePod(c.pods[i%len(c.pods)])
	}
}

// BenchmarkSnapshot measures acquiring a snapshot when nothing changed and when one event happened since the last one.
func BenchmarkSnapshot(b *testing.B) {
	c := newBenchCluster(150_000, 15_000, 500)
	ix := c.index(b, 500)
	b.Run("unchanged", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			ix.Snapshot()
		}
	})
	b.Run("after-one-event", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			p := c.pods[i%len(c.pods)].DeepCopy()
			p.ResourceVersion = fmt.Sprint(i + 2)
			// alternate the label on every pass over the pods, so every event is a real diff
			p.Labels = map[string]string{"app": p.Labels["app"], "pod-template-hash": p.Labels["pod-template-hash"], "team": fmt.Sprint(i / len(c.pods) % 2)}
			ix.ObservePod(p)
			ix.Snapshot()
		}
	})
}

// BenchmarkRegister measures the first-use scan of a group.
func BenchmarkRegister(b *testing.B) {
	c := newBenchCluster(150_000, 15_000, 500)
	ix := c.index(b, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		ix.mu.Lock()
		g := ix.registerLocked(newGroupSpec(c.specs[i%len(c.specs)]))
		ix.unregisterLocked(g)
		ix.mu.Unlock()
	}
}

// TestIndexMemory reports the heap retained by an index of 150k pods / 15k nodes / 500 groups.
func TestIndexMemory(t *testing.T) {
	c := newBenchCluster(150_000, 15_000, 500)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	ix := c.index(&testing.B{}, 500)
	ix.Snapshot()
	runtime.GC()
	runtime.ReadMemStats(&after)
	retained := after.HeapAlloc - before.HeapAlloc
	t.Logf("index retains %.1f MB, %.0f B/pod (%d label sets, %d groups)", float64(retained)/1e6, float64(retained)/float64(len(c.pods)), ix.sets.Len(), len(ix.groups))
	runtime.KeepAlive(ix)
}
