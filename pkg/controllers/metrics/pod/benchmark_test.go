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

package pod_test

import (
	"context"
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/metrics/pod"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/test"
)

const (
	benchPods  = 10_000
	benchNodes = 250
	// benchWindow is the simulated time window. Every pod is reconciled once at the start, as on controller start, and
	// then again whenever its reconcile asks for a requeue that falls inside the window.
	benchWindow = 5 * time.Minute
)

var (
	benchOnce    sync.Once
	benchEnv     *test.Environment
	benchPending []*corev1.Pod
	benchAll     []types.NamespacedName
)

// benchNode is a Node with the labels, capacity, conditions and images of a real node, since the controller reads it
// from the informer cache on every reconcile of a bound pod
func benchNode(i int) *corev1.Node {
	labels := map[string]string{
		corev1.LabelTopologyZone:       fmt.Sprintf("test-zone-%d", i%3),
		corev1.LabelArchStable:         "amd64",
		corev1.LabelOSStable:           "linux",
		v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
		corev1.LabelInstanceTypeStable: "m5.2xlarge",
		v1.NodePoolLabelKey:            fmt.Sprintf("nodepool-%d", i%10),
		corev1.LabelHostname:           fmt.Sprintf("bench-node-%d", i),
	}
	for j := range 28 {
		labels[fmt.Sprintf("example.com/instance-label-%d", j)] = fmt.Sprintf("value-%d", j)
	}
	resources := corev1.ResourceList{
		corev1.ResourceCPU:              resource.MustParse("8"),
		corev1.ResourceMemory:           resource.MustParse("32Gi"),
		corev1.ResourcePods:             resource.MustParse("58"),
		corev1.ResourceEphemeralStorage: resource.MustParse("100Gi"),
	}
	node := test.Node(test.NodeOptions{
		ObjectMeta:  metav1.ObjectMeta{Name: fmt.Sprintf("bench-node-%d", i), Labels: labels},
		ProviderID:  fmt.Sprintf("fake:///bench-node-%d", i),
		Allocatable: resources,
	})
	for _, t := range []corev1.NodeConditionType{corev1.NodeMemoryPressure, corev1.NodeDiskPressure, corev1.NodePIDPressure, corev1.NodeNetworkUnavailable} {
		node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{Type: t, Status: corev1.ConditionFalse, Reason: "KubeletHasSufficient", Message: "kubelet has sufficient resources"})
	}
	// The kubelet reports up to 50 images by default
	for j := range 50 {
		node.Status.Images = append(node.Status.Images, corev1.ContainerImage{
			Names:     []string{fmt.Sprintf("registry.example.com/team/image-%d@sha256:%064d", j, j), fmt.Sprintf("registry.example.com/team/image-%d:v1.2.3", j)},
			SizeBytes: 100_000_000,
		})
	}
	node.Status.Addresses = []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.1"}, {Type: corev1.NodeHostName, Address: node.Name}}
	return node
}

// benchPod is a Pod from a ReplicaSet in the given state. Pending pods are 15% of pods (two thirds unbound), terminal
// pods are 10%, and the rest are running and ready.
func benchPod(i int) *corev1.Pod {
	now := metav1.Now()
	p := test.Pod(test.PodOptions{
		ObjectMeta: metav1.ObjectMeta{
			Name:            fmt.Sprintf("bench-pod-%d", i),
			Namespace:       "default",
			Labels:          map[string]string{"app": fmt.Sprintf("app-%d", i%100), "pod-template-hash": "abcdef"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: fmt.Sprintf("app-%d-abcdef", i%100), UID: "uid"}},
		},
		NodeName:             fmt.Sprintf("bench-node-%d", i%benchNodes),
		ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")}},
	})
	scheduled := corev1.PodCondition{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: now}
	ready := corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: now}
	switch {
	case i%20 < 2: // 10% pending and unbound
		p.Spec.NodeName = ""
		p.Status.Phase = corev1.PodPending
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable", LastTransitionTime: now}}
	case i%20 < 3: // 5% pending and bound
		p.Status.Phase = corev1.PodPending
		p.Status.Conditions = []corev1.PodCondition{scheduled}
	case i%20 < 4: // 5% succeeded
		p.Status.Phase = corev1.PodSucceeded
		p.Status.Conditions = []corev1.PodCondition{scheduled}
	case i%20 < 5: // 5% failed
		p.Status.Phase = corev1.PodFailed
		p.Status.Conditions = []corev1.PodCondition{scheduled}
	default: // 75% running and ready
		p.Status.Phase = corev1.PodRunning
		p.Status.Conditions = []corev1.PodCondition{scheduled, ready}
	}
	return p
}

func setupBenchEnv(b *testing.B) {
	benchOnce.Do(func() {
		ctx := context.Background()
		benchEnv = test.NewEnvironment(test.WithFieldIndexers(test.NodeProviderIDFieldIndexer(ctx)))
		writer := lo.Must(client.New(benchEnv.Config, client.Options{Scheme: scheme.Scheme}))
		create := func(objs []client.Object) {
			work := make(chan client.Object)
			wg := sync.WaitGroup{}
			for range 32 {
				wg.Go(func() {
					for o := range work {
						stored := o.DeepCopyObject().(client.Object)
						lo.Must0(writer.Create(ctx, o))
						switch s := stored.(type) {
						case *corev1.Pod:
							o.(*corev1.Pod).Status = s.Status
						case *corev1.Node:
							o.(*corev1.Node).Status = s.Status
						}
						lo.Must0(writer.Status().Update(ctx, o))
					}
				})
			}
			for _, o := range objs {
				work <- o
			}
			close(work)
			wg.Wait()
		}
		create(lo.Times(benchNodes, func(i int) client.Object { return benchNode(i) }))
		pods := lo.Times(benchPods, benchPod)
		create(lo.Map(pods, func(p *corev1.Pod, _ int) client.Object { return p }))
		benchPending = lo.Filter(pods, func(p *corev1.Pod, _ int) bool { return p.Spec.NodeName == "" })
		benchAll = lo.Map(pods, func(p *corev1.Pod, _ int) types.NamespacedName { return client.ObjectKeyFromObject(p) })
		// Wait for the informer cache to observe every pod with its status
		for {
			list := &corev1.PodList{}
			lo.Must0(benchEnv.Client.List(ctx, list, client.InNamespace("default")))
			if lo.CountBy(list.Items, func(p corev1.Pod) bool { return p.Status.Phase != "" }) == benchPods {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	})
	if benchEnv == nil {
		b.Fatal("benchmark environment failed to start")
	}
}

func cpuTime() time.Duration {
	ru := syscall.Rusage{}
	lo.Must0(syscall.Getrusage(syscall.RUSAGE_SELF, &ru))
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// BenchmarkReconcileWindow simulates the pod metrics controller over a 5 minute window without pod or node events: one
// op is the initial reconcile of every pod plus every requeue that falls inside the window, on a single goroutine as
// with the operator's single worker. It reports the number of reconciles per window and the process CPU time per window.
func BenchmarkReconcileWindow(b *testing.B) {
	setupBenchEnv(b)
	ctx := context.Background()
	reconciles := 0
	cpu := time.Duration(0)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		cloudProvider := fake.NewCloudProvider()
		cluster := state.NewCluster(benchEnv.Clock, benchEnv.Client, cloudProvider)
		cluster.AckPods(benchPending...)
		half := benchPending[:len(benchPending)/2]
		cluster.MarkPodSchedulingDecisions(ctx, map[*corev1.Pod]error{}, map[string][]*corev1.Pod{"np": half}, map[string][]*corev1.Pod{"nc": half})
		controller := pod.NewController(benchEnv.Client, cluster)
		startCPU := cpuTime()
		b.StartTimer()
		// Every requeue is 30s, so the window is simulated in rounds of 30s
		due := benchAll
		for elapsed := time.Duration(0); elapsed <= benchWindow && len(due) > 0; elapsed += 30 * time.Second {
			var next []types.NamespacedName
			for _, nn := range due {
				result := lo.Must(controller.Reconcile(ctx, reconcile.Request{NamespacedName: nn}))
				reconciles++
				if result.RequeueAfter > 0 && elapsed+result.RequeueAfter <= benchWindow {
					next = append(next, nn)
				}
			}
			due = next
		}
		b.StopTimer()
		cpu += cpuTime() - startCPU
		b.StartTimer()
	}
	b.ReportMetric(float64(reconciles)/float64(b.N), "reconciles/op")
	b.ReportMetric(float64(cpu.Milliseconds())/float64(b.N), "cpu-ms/op")
}
