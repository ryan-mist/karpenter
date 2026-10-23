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

import (
	"context"
	"fmt"
	"sync"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// instanceTypeCallCounter records GetInstanceTypes calls per NodePool name. When copyInstanceTypes is set it returns a
// freshly allocated InstanceType (with its own offerings slice) per call, like providers that inject per-call offerings.
type instanceTypeCallCounter struct {
	*fake.CloudProvider
	copyInstanceTypes bool

	mu    sync.Mutex
	calls map[string]int
}

func newInstanceTypeCallCounter(cp *fake.CloudProvider) *instanceTypeCallCounter {
	return &instanceTypeCallCounter{CloudProvider: cp, calls: map[string]int{}}
}

func (c *instanceTypeCallCounter) GetInstanceTypes(ctx context.Context, np *v1.NodePool) ([]*cloudprovider.InstanceType, error) {
	c.mu.Lock()
	c.calls[np.Name]++
	c.mu.Unlock()
	its, err := c.CloudProvider.GetInstanceTypes(ctx, np)
	if err != nil || !c.copyInstanceTypes {
		return its, err
	}
	return lo.Map(its, func(it *cloudprovider.InstanceType, _ int) *cloudprovider.InstanceType {
		return &cloudprovider.InstanceType{
			Name:                   it.Name,
			Requirements:           it.Requirements,
			Offerings:              append(cloudprovider.Offerings(nil), it.Offerings...),
			Capacity:               it.Capacity,
			VolumeAttachmentLimits: it.VolumeAttachmentLimits,
			DynamicResources:       it.DynamicResources,
			Overhead:               it.Overhead,
		}
	}), nil
}

func (c *instanceTypeCallCounter) Calls() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return lo.Assign(c.calls)
}

func (c *instanceTypeCallCounter) TotalCalls() int {
	return lo.Sum(lo.Values(c.Calls()))
}

var _ = Describe("NodePool Map", func() {
	var counter *instanceTypeCallCounter
	var ownerNodePool, idleNodePool, failingNodePool *v1.NodePool
	var nodeClaim *v1.NodeClaim
	var node *corev1.Node
	BeforeEach(func() {
		counter = newInstanceTypeCallCounter(cloudProvider)
		ownerNodePool = test.NodePool()
		idleNodePool = test.NodePool()
		failingNodePool = test.NodePool()
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            ownerNodePool.Name,
					corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
					corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
				},
			},
		})
		ExpectApplied(ctx, env.Client, ownerNodePool, idleNodePool, failingNodePool, nodeClaim, node)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
	})
	It("should only resolve instance types for NodePools that own a node when building candidates", func() {
		// An error for a NodePool without nodes must not surface since its instance types are never requested.
		cloudProvider.ErrorsForNodePool[failingNodePool.Name] = fmt.Errorf("unable to fetch instance types")
		candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, env.Clock, counter,
			func(context.Context, *disruption.Candidate) bool { return true }, disruption.GracefulDisruptionClass, queue)
		Expect(err).ToNot(HaveOccurred())
		Expect(candidates).To(HaveLen(1))
		Expect(candidates[0].NodePool.Name).To(Equal(ownerNodePool.Name))
		Expect(counter.Calls()).To(Equal(map[string]int{ownerNodePool.Name: 1}))
	})
	It("should keep every NodePool in the NodePool map but only resolve instance types for owners", func() {
		nodePoolMap, instanceTypeMap, err := disruption.BuildNodePoolMapForNodes(ctx, env.Client, counter, cluster.DeepCopyNodes())
		Expect(err).ToNot(HaveOccurred())
		Expect(nodePoolMap).To(HaveKey(ownerNodePool.Name))
		Expect(nodePoolMap).To(HaveKey(idleNodePool.Name))
		Expect(nodePoolMap).To(HaveKey(failingNodePool.Name))
		Expect(instanceTypeMap).To(HaveLen(1))
		Expect(instanceTypeMap[ownerNodePool.Name]).To(HaveKey(mostExpensiveInstance.Name))
		Expect(counter.Calls()).To(Equal(map[string]int{ownerNodePool.Name: 1}))
	})
	It("should resolve instance types for every NodePool with BuildNodePoolMap", func() {
		nodePoolMap, instanceTypeMap, err := disruption.BuildNodePoolMap(ctx, env.Client, counter)
		Expect(err).ToNot(HaveOccurred())
		for _, np := range []*v1.NodePool{ownerNodePool, idleNodePool, failingNodePool} {
			Expect(nodePoolMap).To(HaveKey(np.Name))
			Expect(instanceTypeMap).To(HaveKey(np.Name))
			Expect(counter.Calls()).To(HaveKeyWithValue(np.Name, 1))
		}
	})
	It("should not consider a candidate whose owning NodePool fails to resolve instance types", func() {
		cloudProvider.ErrorsForNodePool[ownerNodePool.Name] = fmt.Errorf("unable to fetch instance types")
		candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, env.Clock, counter,
			func(context.Context, *disruption.Candidate) bool { return true }, disruption.GracefulDisruptionClass, queue)
		Expect(err).ToNot(HaveOccurred())
		Expect(candidates).To(BeEmpty())
		Expect(counter.Calls()).To(Equal(map[string]int{ownerNodePool.Name: 1}))
		Expect(recorder.DetectedEvent(fmt.Sprintf("NodePool not found (NodePool=%s)", ownerNodePool.Name))).To(BeTrue())
	})
})

func benchmarkNodePoolMapFixture(b *testing.B, nodePools, nodes, instanceTypes int) (client.Client, *instanceTypeCallCounter, []*state.StateNode) {
	b.Helper()
	cp := fake.NewCloudProvider()
	cp.InstanceTypes = fake.InstanceTypes(instanceTypes)
	counter := newInstanceTypeCallCounter(cp)
	counter.copyInstanceTypes = true
	objs := make([]client.Object, 0, nodePools)
	for i := range nodePools {
		objs = append(objs, test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("nodepool-%d", i)}}))
	}
	kubeClient := crfake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objs...).Build()
	stateNodes := make([]*state.StateNode, 0, nodes)
	for i := range nodes {
		stateNodes = append(stateNodes, &state.StateNode{Node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("node-%d", i),
			Labels: map[string]string{
				v1.NodePoolLabelKey:            fmt.Sprintf("nodepool-%d", i%nodePools),
				corev1.LabelInstanceTypeStable: cp.InstanceTypes[0].Name,
			},
		}}})
	}
	return kubeClient, counter, stateNodes
}

// BenchmarkBuildNodePoolMap compares resolving instance types for every NodePool (all) with resolving them only for
// NodePools that own a node (owned), with 500 NodePools and nodes spread over 3 or 150 of them.
func BenchmarkBuildNodePoolMap(b *testing.B) {
	ctx := context.Background()
	for _, nodes := range []int{3, 150} {
		for _, mode := range []string{"all", "owned"} {
			b.Run(fmt.Sprintf("nodepools=500/nodes=%d/mode=%s", nodes, mode), func(b *testing.B) {
				kubeClient, counter, stateNodes := benchmarkNodePoolMapFixture(b, 500, nodes, 500)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					var err error
					if mode == "all" {
						_, _, err = disruption.BuildNodePoolMap(ctx, kubeClient, counter)
					} else {
						_, _, err = disruption.BuildNodePoolMapForNodes(ctx, kubeClient, counter, stateNodes)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(counter.TotalCalls())/float64(b.N), "GetInstanceTypes/op")
			})
		}
	}
}
