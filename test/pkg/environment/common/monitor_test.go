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

package common

import (
	"context"
	"fmt"
	"math"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

type clusterFixture struct {
	t      *testing.T
	client client.Client
	pods   int
}

func newClusterFixture(t *testing.T) *clusterFixture {
	return &clusterFixture{t: t, client: fake.NewClientBuilder().Build()}
}

func (f *clusterFixture) create(obj client.Object) {
	if err := f.client.Create(context.Background(), obj); err != nil {
		f.t.Fatal(err)
	}
}

// node creates a node with the given allocatable CPU. Launched nodes carry the NodePool label and
// run a small DaemonSet pod, as every node in a cluster with DaemonSets does.
func (f *clusterFixture) node(name, cpu string, launched bool) string {
	labels := map[string]string{}
	if launched {
		labels[v1.NodePoolLabelKey] = "default"
	}
	f.create(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse(cpu),
		}},
	})
	if launched {
		f.pod(name, "100m", func(p *corev1.Pod) {
			p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: "ds", UID: "ds"}}
		})
	}
	return name
}

func (f *clusterFixture) pod(nodeName, cpu string, opts ...func(*corev1.Pod)) {
	f.pods++
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d", f.pods), Namespace: "default"},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{{
				Name:      "app",
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}},
			}},
		},
	}
	for _, opt := range opts {
		opt(p)
	}
	f.create(p)
}

func (f *clusterFixture) deleteNode(name string) {
	if err := f.client.Delete(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
		f.t.Fatal(err)
	}
}

func (f *clusterFixture) monitor() *Monitor {
	return NewMonitor(context.Background(), f.client)
}

func expectNear(t *testing.T, what string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}

// packedCluster binds the workload onto twelve 4-CPU launched nodes: six run 3.5 CPU of workload pods
// and six run 1.8 CPU. Each also runs a 100m DaemonSet pod. A node that wasn't launched by Karpenter
// is ignored by every metric.
func packedCluster(t *testing.T) *clusterFixture {
	f := newClusterFixture(t)
	for i := range 6 {
		f.pod(f.node(fmt.Sprintf("full-%d", i), "4", true), "3500m")
		n := f.node(fmt.Sprintf("half-%d", i), "4", true)
		f.pod(n, "900m")
		f.pod(n, "900m")
	}
	f.pod(f.node("unmanaged", "64", false), "1")
	return f
}

// Per-node utilization of the twelve non-empty nodes, DaemonSet pod included.
var packedNonEmptyCPU = (6*3.6/4 + 6*1.9/4) / 12

func TestNonEmptyUtilizationIgnoresEmptyNodes(t *testing.T) {
	f := packedCluster(t)
	// Two launched nodes running only per-node infrastructure: a DaemonSet pod (added by node), a
	// static pod, and a completed pod.
	f.node("empty-0", "4", true)
	f.pod(f.node("empty-1", "4", true), "100m", func(p *corev1.Pod) {
		p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: "empty-1", UID: "empty-1"}}
	})
	f.pod("empty-1", "100m", func(p *corev1.Pod) { p.Status.Phase = corev1.PodSucceeded })
	m := f.monitor()

	if got := m.EmptyNodeCount(); got != 2 {
		t.Errorf("EmptyNodeCount = %d, want 2", got)
	}
	expectNear(t, "AvgNonEmptyUtilization", m.AvgNonEmptyUtilization(corev1.ResourceCPU), packedNonEmptyCPU)
	// The all-node mean counts the empty nodes' DaemonSet, static and completed pod requests over
	// fourteen nodes: two empty nodes out of fourteen pull it down by ~0.08.
	wantAll := (12*packedNonEmptyCPU + 0.1/4 + 0.3/4) / 14
	expectNear(t, "AvgUtilization", m.AvgUtilization(corev1.ResourceCPU), wantAll)
	if packedNonEmptyCPU-wantAll < 0.08 {
		t.Errorf("expected empty nodes to pull the all-node mean down, got %v vs %v", wantAll, packedNonEmptyCPU)
	}
}

// After every pod is running, empty nodes can still register (launched for pods that were bound
// elsewhere) or be consolidated away, so the number present depends on when the report is taken. The
// non-empty mean is the same at every one of those instants.
func TestNonEmptyUtilizationIsStableWhileEmptyNodesSettle(t *testing.T) {
	f := packedCluster(t)
	m := f.monitor()
	var allNode []float64
	sample := func(wantEmpty int) {
		t.Helper()
		if got := m.EmptyNodeCount(); got != wantEmpty {
			t.Errorf("EmptyNodeCount = %d, want %d", got, wantEmpty)
		}
		expectNear(t, fmt.Sprintf("AvgNonEmptyUtilization with %d empty nodes", wantEmpty), m.AvgNonEmptyUtilization(corev1.ResourceCPU), packedNonEmptyCPU)
		allNode = append(allNode, m.AvgUtilization(corev1.ResourceCPU))
	}

	sample(0)
	f.node("late-0", "4", true)
	sample(1)
	f.node("late-1", "8", true)
	sample(2)
	f.deleteNode("late-0")
	f.deleteNode("late-1")
	sample(0)

	if allNode[0] != allNode[3] || allNode[1] >= allNode[0] || allNode[2] >= allNode[1] {
		t.Errorf("expected the all-node mean to fall as empty nodes register and recover when they are removed, got %v", allNode)
	}
}

func TestNonEmptyUtilizationWithoutWorkloadPods(t *testing.T) {
	f := newClusterFixture(t)
	f.node("empty", "4", true)
	f.pod(f.node("unmanaged", "64", false), "1")
	m := f.monitor()
	if got := m.AvgNonEmptyUtilization(corev1.ResourceCPU); got != 0 {
		t.Errorf("AvgNonEmptyUtilization = %v, want 0", got)
	}
	if got := m.EmptyNodeCount(); got != 1 {
		t.Errorf("EmptyNodeCount = %d, want 1", got)
	}
}
