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

package static_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Static Provisioning Launch Capacity", func() {
	// reservedOnlyPool pins a static pool to a single instance type that only has the given reserved offering (or none).
	reservedOnlyPool := func(replicas int64, reservation *cloudprovider.Offering) *v1.NodePool {
		offerings := []cloudprovider.Offering{{Available: true, Requirements: scheduling.NewLabelRequirements(map[string]string{
			v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: "test-zone-1",
		})}}
		if reservation != nil {
			offerings = append(offerings, *reservation)
		}
		cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{fake.NewInstanceType("reserved-type", fake.WithOfferings(offerings...))}
		return test.StaticNodePool(v1.NodePool{Spec: v1.NodePoolSpec{
			Replicas: lo.ToPtr(replicas),
			Template: v1.NodeClaimTemplate{Spec: v1.NodeClaimTemplateSpec{Requirements: []v1.NodeSelectorRequirementWithMinValues{
				{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeReserved}},
			}}},
		}})
	}
	reservation := func(capacity int) *cloudprovider.Offering {
		return &cloudprovider.Offering{Available: true, ReservationCapacity: capacity, Requirements: scheduling.NewLabelRequirements(map[string]string{
			v1.CapacityTypeLabelKey: v1.CapacityTypeReserved, corev1.LabelTopologyZone: "test-zone-1", cloudprovider.ReservationIDLabel: "r-1",
		})}
	}
	nodeClaimCount := func() int {
		ncs := &v1.NodeClaimList{}
		Expect(env.Client.List(ctx, ncs)).To(Succeed())
		return len(ncs.Items)
	}

	It("does not create NodeClaims for a reserved-only template whose reservation is gone", func() {
		nodePool := reservedOnlyPool(3, nil)
		ExpectApplied(ctx, env.Client, nodePool)

		result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)

		Expect(nodeClaimCount()).To(BeZero())
		Expect(result.RequeueAfter).To(Equal(30 * time.Second))
		ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 0)
	})

	It("does not create NodeClaims while the reservation is full", func() {
		nodePool := reservedOnlyPool(3, reservation(0))
		ExpectApplied(ctx, env.Client, nodePool)

		result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)

		Expect(nodeClaimCount()).To(BeZero())
		Expect(result.RequeueAfter).To(Equal(30 * time.Second))
	})

	It("creates only as many NodeClaims as the reservation has free slots", func() {
		nodePool := reservedOnlyPool(3, reservation(2))
		ExpectApplied(ctx, env.Client, nodePool)

		ExpectObjectReconciled(ctx, env.Client, controller, nodePool)

		Expect(nodeClaimCount()).To(Equal(2))
		ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 0, 0)
	})

	It("resumes provisioning once capacity becomes launchable again", func() {
		nodePool := reservedOnlyPool(1, reservation(0))
		ExpectApplied(ctx, env.Client, nodePool)
		ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
		Expect(nodeClaimCount()).To(BeZero())

		cloudProvider.InstanceTypes[0].Offerings[1].ReservationCapacity = 1
		ExpectObjectReconciled(ctx, env.Client, controller, nodePool)

		Expect(nodeClaimCount()).To(Equal(1))
	})

	It("still provisions an on-demand static pool", func() {
		nodePool := test.StaticNodePool(v1.NodePool{Spec: v1.NodePoolSpec{Replicas: lo.ToPtr(int64(2))}})
		ExpectApplied(ctx, env.Client, nodePool)

		ExpectObjectReconciled(ctx, env.Client, controller, nodePool)

		Expect(nodeClaimCount()).To(Equal(2))
	})
})
