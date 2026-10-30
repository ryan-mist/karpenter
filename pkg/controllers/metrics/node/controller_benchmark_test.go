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

package node

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

// awsWellKnownLabels are the labels the AWS provider adds to v1.WellKnownLabels at init
// (karpenter-provider-aws pkg/apis/v1/labels.go), which every node metric series carries.
var awsWellKnownLabels = []string{
	"karpenter.k8s.aws/capacity-reservation-id",
	"karpenter.k8s.aws/capacity-reservation-type",
	"karpenter.k8s.aws/capacity-reservation-interruptible",
	"karpenter.k8s.aws/instance-hypervisor",
	"karpenter.k8s.aws/instance-encryption-in-transit-supported",
	"karpenter.k8s.aws/instance-category",
	"karpenter.k8s.aws/instance-capability-flex",
	"karpenter.k8s.aws/instance-family",
	"karpenter.k8s.aws/instance-generation",
	"karpenter.k8s.aws/instance-size",
	"karpenter.k8s.aws/instance-local-nvme",
	"karpenter.k8s.aws/instance-cpu",
	"karpenter.k8s.aws/instance-cpu-manufacturer",
	"karpenter.k8s.aws/instance-cpu-sustained-clock-speed-mhz",
	"karpenter.k8s.aws/instance-memory",
	"karpenter.k8s.aws/instance-ebs-bandwidth",
	"karpenter.k8s.aws/instance-network-bandwidth",
	"karpenter.k8s.aws/instance-gpu-name",
	"karpenter.k8s.aws/instance-gpu-manufacturer",
	"karpenter.k8s.aws/instance-gpu-count",
	"karpenter.k8s.aws/instance-gpu-memory",
	"karpenter.k8s.aws/instance-accelerator-name",
	"karpenter.k8s.aws/instance-accelerator-manufacturer",
	"karpenter.k8s.aws/instance-accelerator-count",
	"topology.k8s.aws/zone-id",
	"karpenter.k8s.aws/instance-tenancy",
	corev1.LabelWindowsBuild,
	"karpenter.k8s.aws/placement-group-id",
	"karpenter.k8s.aws/placement-group-partition",
}

var registerAWSWellKnownLabels = sync.OnceFunc(func() {
	v1.WellKnownLabels.Insert(awsWellKnownLabels...)
})

// newIsolatedController creates a Controller with the AWS well-known labels registered and its metrics in a fresh
// registry, so that it can be created more than once in a test binary.
func newIsolatedController(cluster *state.Cluster) (*Controller, *prometheus.Registry) {
	registerAWSWellKnownLabels()
	registry := prometheus.NewRegistry()
	defaultRegistry := crmetrics.Registry
	crmetrics.Registry = registry
	defer func() { crmetrics.Registry = defaultRegistry }()
	return NewController(cluster), registry
}

func testContext() context.Context {
	return options.ToContext(context.Background(), test.Options())
}

func newTestCluster() *state.Cluster {
	kubeClient := fakecr.NewClientBuilder().WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
		return []string{o.(*corev1.Pod).Spec.NodeName}
	}).Build()
	return state.NewCluster(clock.RealClock{}, kubeClient, nil)
}

type testInstanceType struct {
	name, arch string
	labels     map[string]string
	capacity   corev1.ResourceList
}

var testInstanceTypes = []testInstanceType{
	{
		name: "m5.2xlarge", arch: "amd64",
		labels: map[string]string{
			"karpenter.k8s.aws/instance-hypervisor": "nitro", "karpenter.k8s.aws/instance-encryption-in-transit-supported": "false",
			"karpenter.k8s.aws/instance-category": "m", "karpenter.k8s.aws/instance-family": "m5", "karpenter.k8s.aws/instance-generation": "5",
			"karpenter.k8s.aws/instance-size": "2xlarge", "karpenter.k8s.aws/instance-cpu": "8", "karpenter.k8s.aws/instance-cpu-manufacturer": "intel",
			"karpenter.k8s.aws/instance-cpu-sustained-clock-speed-mhz": "3100", "karpenter.k8s.aws/instance-memory": "32768",
			"karpenter.k8s.aws/instance-ebs-bandwidth": "4750", "karpenter.k8s.aws/instance-network-bandwidth": "2500",
			"karpenter.k8s.aws/instance-tenancy": "default", "karpenter.k8s.aws/instance-capability-flex": "false",
		},
		capacity: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("32Gi"),
			corev1.ResourcePods: resource.MustParse("58"), corev1.ResourceEphemeralStorage: resource.MustParse("20Gi"),
			"hugepages-1Gi": resource.MustParse("0"), "hugepages-2Mi": resource.MustParse("0")},
	},
	{
		name: "c7g.xlarge", arch: "arm64",
		labels: map[string]string{
			"karpenter.k8s.aws/instance-hypervisor": "nitro", "karpenter.k8s.aws/instance-encryption-in-transit-supported": "true",
			"karpenter.k8s.aws/instance-category": "c", "karpenter.k8s.aws/instance-family": "c7g", "karpenter.k8s.aws/instance-generation": "7",
			"karpenter.k8s.aws/instance-size": "xlarge", "karpenter.k8s.aws/instance-cpu": "4", "karpenter.k8s.aws/instance-cpu-manufacturer": "aws",
			"karpenter.k8s.aws/instance-cpu-sustained-clock-speed-mhz": "2600", "karpenter.k8s.aws/instance-memory": "8192",
			"karpenter.k8s.aws/instance-ebs-bandwidth": "10000", "karpenter.k8s.aws/instance-network-bandwidth": "1876",
			"karpenter.k8s.aws/instance-tenancy": "default", "karpenter.k8s.aws/instance-capability-flex": "false",
		},
		capacity: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("8Gi"),
			corev1.ResourcePods: resource.MustParse("58"), corev1.ResourceEphemeralStorage: resource.MustParse("20Gi"),
			"hugepages-1Gi": resource.MustParse("0"), "hugepages-2Mi": resource.MustParse("0")},
	},
	{
		name: "g5.12xlarge", arch: "amd64",
		labels: map[string]string{
			"karpenter.k8s.aws/instance-hypervisor": "nitro", "karpenter.k8s.aws/instance-encryption-in-transit-supported": "true",
			"karpenter.k8s.aws/instance-category": "g", "karpenter.k8s.aws/instance-family": "g5", "karpenter.k8s.aws/instance-generation": "5",
			"karpenter.k8s.aws/instance-size": "12xlarge", "karpenter.k8s.aws/instance-cpu": "48", "karpenter.k8s.aws/instance-cpu-manufacturer": "amd",
			"karpenter.k8s.aws/instance-cpu-sustained-clock-speed-mhz": "3300", "karpenter.k8s.aws/instance-memory": "196608",
			"karpenter.k8s.aws/instance-ebs-bandwidth": "16000", "karpenter.k8s.aws/instance-network-bandwidth": "40000",
			"karpenter.k8s.aws/instance-local-nvme": "3800", "karpenter.k8s.aws/instance-gpu-name": "a10g",
			"karpenter.k8s.aws/instance-gpu-manufacturer": "nvidia", "karpenter.k8s.aws/instance-gpu-count": "4",
			"karpenter.k8s.aws/instance-gpu-memory": "24576", "karpenter.k8s.aws/instance-tenancy": "default",
		},
		capacity: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("48"), corev1.ResourceMemory: resource.MustParse("192Gi"),
			corev1.ResourcePods: resource.MustParse("234"), corev1.ResourceEphemeralStorage: resource.MustParse("100Gi"),
			"hugepages-1Gi": resource.MustParse("0"), "hugepages-2Mi": resource.MustParse("0"), "nvidia.com/gpu": resource.MustParse("4")},
	},
}

// testNode returns the i-th node of a fixture, and its NodeClaim when managed. Nodes are sized like real ones
// (conditions, images, addresses) since they are part of the cluster state the metrics controller reads.
func testNode(i int, managed bool) (*corev1.Node, *v1.NodeClaim) {
	it := testInstanceTypes[i%len(testInstanceTypes)]
	zone := []string{"a", "b", "c"}[i%3]
	name := fmt.Sprintf("ip-10-0-%d-%d.us-west-2.compute.internal", i/250, i%250)
	providerID := fmt.Sprintf("aws:///us-west-2%s/i-%017x", zone, i)
	labels := map[string]string{
		corev1.LabelInstanceTypeStable: it.name,
		corev1.LabelArchStable:         it.arch,
		corev1.LabelOSStable:           "linux",
		corev1.LabelTopologyZone:       "us-west-2" + zone,
		corev1.LabelTopologyRegion:     "us-west-2",
		corev1.LabelHostname:           name,
		"topology.k8s.aws/zone-id":     "usw2-az" + zone,
		"k8s.io/cloud-provider-aws":    "abc123",
	}
	for k, v := range it.labels {
		labels[k] = v
	}
	if managed {
		labels[v1.NodePoolLabelKey] = fmt.Sprintf("nodepool-%d", i%10)
		labels[v1.CapacityTypeLabelKey] = []string{v1.CapacityTypeOnDemand, v1.CapacityTypeSpot}[i%2]
		labels[v1.NodeRegisteredLabelKey] = "true"
		labels[v1.NodeInitializedLabelKey] = "true"
	}
	allocatable := corev1.ResourceList{}
	for k, v := range it.capacity {
		allocatable[k] = v.DeepCopy()
	}
	allocatable[corev1.ResourceCPU] = resource.MustParse(fmt.Sprintf("%dm", it.capacity.Cpu().MilliValue()-90))
	allocatable[corev1.ResourceMemory] = resource.MustParse(fmt.Sprintf("%dMi", it.capacity.Memory().Value()/(1<<20)-1024))
	allocatable[corev1.ResourceEphemeralStorage] = resource.MustParse("18Gi")
	created := metav1.NewTime(time.Now().Add(-time.Duration(i+1) * time.Minute))
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			UID:               types.UID(fmt.Sprintf("node-uid-%d", i)),
			CreationTimestamp: created,
			Labels:            labels,
			Annotations: map[string]string{
				"alpha.kubernetes.io/provided-node-ip":                   "10.0.0.1",
				"node.alpha.kubernetes.io/ttl":                           "0",
				"volumes.kubernetes.io/controller-managed-attach-detach": "true",
				"csi.volume.kubernetes.io/nodeid":                        `{"ebs.csi.aws.com":"i-0123456789abcdef0"}`,
			},
		},
		Spec: corev1.NodeSpec{ProviderID: providerID},
		Status: corev1.NodeStatus{
			Phase:       corev1.NodeRunning,
			Capacity:    it.capacity,
			Allocatable: allocatable,
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: "10.0.0.1"},
				{Type: corev1.NodeHostName, Address: name},
				{Type: corev1.NodeInternalDNS, Address: name},
			},
			NodeInfo: corev1.NodeSystemInfo{KubeletVersion: "v1.34.1-eks-abcdef", ContainerRuntimeVersion: "containerd://1.7.27",
				KernelVersion: "6.1.134-152.225.amzn2023.x86_64", OSImage: "Amazon Linux 2023.7.20250512", OperatingSystem: "linux", Architecture: it.arch},
		},
	}
	for _, c := range []corev1.NodeConditionType{corev1.NodeMemoryPressure, corev1.NodeDiskPressure, corev1.NodePIDPressure, corev1.NodeReady} {
		node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{
			Type: c, Status: corev1.ConditionFalse, LastHeartbeatTime: created, LastTransitionTime: created, Reason: "KubeletHasSufficient", Message: "kubelet is healthy",
		})
	}
	for j := range 20 {
		node.Status.Images = append(node.Status.Images, corev1.ContainerImage{
			Names:     []string{fmt.Sprintf("123456789012.dkr.ecr.us-west-2.amazonaws.com/app-%d@sha256:%064x", j, j), fmt.Sprintf("123456789012.dkr.ecr.us-west-2.amazonaws.com/app-%d:v1.2.%d", j, j)},
			SizeBytes: int64(j+1) << 24,
		})
	}
	if !managed {
		return node, nil
	}
	nodeClaim := &v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("nodeclaim-%d", i), Labels: labels, CreationTimestamp: created},
		Status: v1.NodeClaimStatus{
			ProviderID:  providerID,
			NodeName:    name,
			Capacity:    it.capacity,
			Allocatable: allocatable,
		},
	}
	return node, nodeClaim
}

// testPods returns count pods bound to the node, the first daemonSetPods of which are owned by a DaemonSet.
func testPods(node *corev1.Node, count, daemonSetPods int) []*corev1.Pod {
	var pods []*corev1.Pod
	for j := range count {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-pod-%d", node.Name, j), Namespace: "default", UID: types.UID(fmt.Sprintf("%s-pod-%d", node.UID, j))},
			Spec: corev1.PodSpec{
				NodeName: node.Name,
				Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(fmt.Sprintf("%dm", 100+j*10)), corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dMi", 128+j*64))},
					Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dMi", 256+j*64))},
				}}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		if j < daemonSetPods {
			pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: fmt.Sprintf("ds-%d", j), UID: types.UID(fmt.Sprintf("ds-%d", j)), Controller: lo.ToPtr(true)}}
		}
		pods = append(pods, pod)
	}
	return pods
}

func addNode(ctx context.Context, tb testing.TB, cluster *state.Cluster, node *corev1.Node, nodeClaim *v1.NodeClaim, pods []*corev1.Pod) {
	if nodeClaim != nil {
		cluster.UpdateNodeClaim(nodeClaim)
	}
	if err := cluster.UpdateNode(ctx, node); err != nil {
		tb.Fatal(err)
	}
	for _, pod := range pods {
		if err := cluster.UpdatePod(ctx, pod); err != nil {
			tb.Fatal(err)
		}
	}
}

var benchmarkClusters = map[int]*state.Cluster{}

// benchmarkCluster returns a cluster of managed, AWS-labelled nodes with 10 pods each (2 of them DaemonSet pods),
// which gives 25 series per node.
func benchmarkCluster(b *testing.B, nodes int) *state.Cluster {
	if cluster, ok := benchmarkClusters[nodes]; ok {
		return cluster
	}
	ctx := testContext()
	cluster := newTestCluster()
	for i := range nodes {
		node, nodeClaim := testNode(i, true)
		addNode(ctx, b, cluster, node, nodeClaim, testPods(node, 10, 2))
	}
	benchmarkClusters[nodes] = cluster
	return cluster
}

func BenchmarkReconcile(b *testing.B) {
	for _, nodes := range []int{1000, 5000} {
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			ctx := testContext()
			c, _ := newIsolatedController(benchmarkCluster(b, nodes))
			// The first reconcile creates every series, later ones (measured) update them
			if _, err := c.Reconcile(ctx); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := c.Reconcile(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
