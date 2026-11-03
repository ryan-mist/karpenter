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

type utilizationFixture struct {
	objects []client.Object
	pods    int
}

func (f *utilizationFixture) node(name, cpu string, launched bool) string {
	labels := map[string]string{}
	if launched {
		labels[v1.NodePoolLabelKey] = "default"
	}
	f.objects = append(f.objects, &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse(cpu),
		}},
	})
	return name
}

func (f *utilizationFixture) pod(nodeName, cpu string) {
	f.pods++
	f.objects = append(f.objects, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d", f.pods), Namespace: "default"},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{{
				Name:      "app",
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}},
			}},
		},
	})
}

func (f *utilizationFixture) monitor() *Monitor {
	return NewMonitor(context.Background(), fake.NewClientBuilder().WithObjects(f.objects...).Build())
}

// mixedSizeCluster builds a cluster with mixed node sizes: many small and medium nodes each running
// one pod, plus one very large node running many pods. The "extra" pods are bound either one per
// small node (which still has room for one more) or all onto the large node.
func mixedSizeCluster(extraOnLargeNode bool) *Monitor {
	f := &utilizationFixture{}
	var small []string
	for i := range 26 {
		small = append(small, f.node(fmt.Sprintf("small-%d", i), "2", true))
		f.pod(small[i], "900m")
	}
	for i := range 24 {
		f.pod(f.node(fmt.Sprintf("medium-%d", i), "4", true), "3500m")
	}
	large := f.node("large", "384", true)
	for range 26 {
		f.pod(large, "3500m")
	}
	for i := range 10 {
		if extraOnLargeNode {
			f.pod(large, "900m")
		} else {
			f.pod(small[i], "900m")
		}
	}
	// Nodes that Karpenter didn't launch are ignored by both metrics.
	f.pod(f.node("unmanaged", "64", false), "100m")
	return f.monitor()
}

func TestUtilizationWithMixedNodeSizes(t *testing.T) {
	spread := mixedSizeCluster(false)
	packed := mixedSizeCluster(true)

	// The same pods on the same nodes: 26*0.9 + 24*3.5 + 26*3.5 + 10*0.9 = 207.4 cores requested
	// over 26*2 + 24*4 + 384 = 532 cores allocatable.
	want := 207.4 / 532
	for name, m := range map[string]*Monitor{"spread": spread, "packed": packed} {
		if got := m.TotalUtilization(corev1.ResourceCPU); math.Abs(got-want) > 1e-9 {
			t.Errorf("%s: TotalUtilization = %v, want %v", name, got, want)
		}
	}

	// The unweighted mean moves by ~0.08 purely because of where the ten extra pods were bound.
	spreadAvg := spread.AvgUtilization(corev1.ResourceCPU)
	packedAvg := packed.AvgUtilization(corev1.ResourceCPU)
	wantSpreadAvg := (10*1.8/2 + 16*0.9/2 + 24*3.5/4 + 26*3.5/384) / 51
	wantPackedAvg := (26*0.9/2 + 24*3.5/4 + (26*3.5+10*0.9)/384) / 51
	if math.Abs(spreadAvg-wantSpreadAvg) > 1e-9 || math.Abs(packedAvg-wantPackedAvg) > 1e-9 {
		t.Errorf("AvgUtilization = %v (spread), %v (packed), want %v, %v", spreadAvg, packedAvg, wantSpreadAvg, wantPackedAvg)
	}
	if spreadAvg-packedAvg < 0.08 {
		t.Errorf("expected the unweighted mean to depend on pod placement, got %v (spread) vs %v (packed)", spreadAvg, packedAvg)
	}
}

func TestTotalUtilizationWithoutLaunchedNodes(t *testing.T) {
	f := &utilizationFixture{}
	f.pod(f.node("unmanaged", "64", false), "1")
	if got := f.monitor().TotalUtilization(corev1.ResourceCPU); got != 0 {
		t.Errorf("TotalUtilization = %v, want 0", got)
	}
}
