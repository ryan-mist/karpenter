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
	"github.com/awslabs/operatorpkg/status"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// These tests exercise the terminate-first disruption path (RFC #3203). The reserved offering convention
// used here treats Offering.Available as HEALTH and ReservationCapacity as the remaining count. A
// full-but-healthy reservation is (Available=true, ReservationCapacity=0); an unhealthy reservation is
// (Available=false). This convention is modeled directly in the fake instance type fixtures below.
var _ = Describe("TerminateFirst", func() {
	const reservationID = "r-terminate-first"
	const zone = "test-zone-1"
	var pinNodePoolName string
	// When true, the reschedulable pod is pinned to pinNodePoolName via spec.nodeSelector (matchLabels)
	// instead of a required nodeAffinity, exercising the nodeSelector widening path.
	var pinViaNodeSelector bool

	// reservedOffering builds a reserved offering in test-zone-1 with explicit health/capacity.
	reservedOffering := func(available bool, capacity int) cloudprovider.Offering {
		return cloudprovider.Offering{
			Available:           available,
			ReservationCapacity: capacity,
			Price:               1.0,
			Requirements: scheduling.NewLabelRequirements(map[string]string{
				v1.CapacityTypeLabelKey:     v1.CapacityTypeReserved,
				corev1.LabelTopologyZone:    zone,
				v1alpha1.LabelReservationID: reservationID,
			}),
		}
	}

	onDemandOffering := func() cloudprovider.Offering {
		return cloudprovider.Offering{
			Available: true,
			Price:     2.0,
			Requirements: scheduling.NewLabelRequirements(map[string]string{
				v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
				corev1.LabelTopologyZone: zone,
			}),
		}
	}

	// newInstanceType builds a fully-formed instance type with explicit offerings and capacity-type
	// requirements. We construct the struct directly (rather than via fake.NewInstanceType) so that the
	// capacity-type requirement is stable even when the reserved offering is unhealthy (Available=false).
	newInstanceType := func(name string, capacityTypes []string, offerings ...cloudprovider.Offering) *cloudprovider.InstanceType {
		return &cloudprovider.InstanceType{
			Name: name,
			Requirements: scheduling.NewRequirements(
				scheduling.NewRequirement(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, name),
				scheduling.NewRequirement(corev1.LabelArchStable, corev1.NodeSelectorOpIn, v1.ArchitectureAmd64),
				scheduling.NewRequirement(corev1.LabelOSStable, corev1.NodeSelectorOpIn, string(corev1.Linux)),
				scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zone),
				scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, capacityTypes...),
			),
			Offerings: lo.ToSlicePtr(offerings),
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("16"),
				corev1.ResourceMemory: resource.MustParse("32Gi"),
				corev1.ResourcePods:   resource.MustParse("110"),
			},
			Overhead: &cloudprovider.InstanceTypeOverhead{
				KubeReserved: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("10Mi"),
				},
			},
		}
	}

	// setup applies the pools, a drifted reserved candidate on reservedIT, and a reschedulable pod, then
	// syncs cluster state. poolITs maps each pool name to the instance types the fake cloud provider should
	// return for it. candidatePool is the pool that owns the drifted candidate node.
	setup := func(candidatePool *v1.NodePool, reservedIT *cloudprovider.InstanceType, pools []*v1.NodePool, poolITs map[string][]*cloudprovider.InstanceType) (*v1.NodeClaim, *corev1.Node) {
		for name, its := range poolITs {
			cloudProvider.InstanceTypesForNodePool[name] = its
		}
		nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            candidatePool.Name,
					corev1.LabelInstanceTypeStable: reservedIT.Name,
					v1.CapacityTypeLabelKey:        v1.CapacityTypeReserved,
					corev1.LabelTopologyZone:       zone,
					v1alpha1.LabelReservationID:    reservationID,
				},
			},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
				Allocatable: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:  resource.MustParse("16"),
					corev1.ResourcePods: resource.MustParse("110"),
				},
			},
		})
		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeDrifted)

		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs)
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(rs), rs)).To(Succeed())
		var pinReqs []corev1.NodeSelectorRequirement
		var pinSelector map[string]string
		if pinNodePoolName != "" {
			if pinViaNodeSelector {
				pinSelector = map[string]string{v1.NodePoolLabelKey: pinNodePoolName}
			} else {
				pinReqs = []corev1.NodeSelectorRequirement{{Key: v1.NodePoolLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{pinNodePoolName}}}
			}
		}
		pod := test.Pod(test.PodOptions{
			NodeSelector:     pinSelector,
			NodeRequirements: pinReqs,
			ObjectMeta: metav1.ObjectMeta{
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion:         "apps/v1",
					Kind:               "ReplicaSet",
					Name:               rs.Name,
					UID:                rs.UID,
					Controller:         new(true),
					BlockOwnerDeletion: new(true),
				}},
			},
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
			},
		})

		objs := []client.Object{nodeClaim, node, pod}
		for _, p := range pools {
			p.StatusConditions().SetTrue(status.ConditionReady)
			objs = append(objs, p)
		}
		ExpectApplied(ctx, env.Client, objs...)
		ExpectManualBinding(ctx, env.Client, pod, node)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		return nodeClaim, node
	}

	// computeDriftDecision builds candidates, runs Drift.ComputeCommands, and returns the commands.
	computeDriftDecision := func() []disruption.Command {
		d := disruption.NewDrift(env.Client, cluster, prov, recorder, env.Clock)
		candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, env.Clock, cloudProvider, d.ShouldDisrupt, d.Class(), queue)
		Expect(err).ToNot(HaveOccurred())
		Expect(candidates).To(HaveLen(1))
		budgets, err := disruption.BuildDisruptionBudgetMapping(ctx, cluster, env.Clock, env.Client, cloudProvider, recorder, d.Reason())
		Expect(err).ToNot(HaveOccurred())
		cmds, err := d.ComputeCommands(ctx, budgets, candidates...)
		Expect(err).ToNot(HaveOccurred())
		return cmds
	}

	BeforeEach(func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{
			FeatureGates: test.FeatureGates{
				ReservedCapacity: lo.ToPtr(true),
				TerminateFirst:   lo.ToPtr(true),
			},
		}))
		pinNodePoolName = ""
		pinViaNodeSelector = false
	})

	It("A) replaces onto a lower-weight on-demand pool when the reserved pool is full", func() {
		reservedIT := newInstanceType("reserved-it", []string{v1.CapacityTypeReserved}, reservedOffering(true, 0))
		odIT := fake.NewInstanceType("od-it", fake.WithOfferings(onDemandOffering()))

		reservedPool := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "reserved-pool"}, Spec: v1.NodePoolSpec{Weight: lo.ToPtr(int32(100))}})
		odPool := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "od-pool"}, Spec: v1.NodePoolSpec{Weight: lo.ToPtr(int32(10))}})

		setup(reservedPool, reservedIT, []*v1.NodePool{reservedPool, odPool}, map[string][]*cloudprovider.InstanceType{
			reservedPool.Name: {reservedIT},
			odPool.Name:       {odIT},
		})

		cmds := computeDriftDecision()
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Decision()).To(Equal(disruption.ReplaceDecision))
		Expect(cmds[0].Replacements).To(HaveLen(1))
		Expect(cmds[0].Replacements[0].Requirements.Get(v1.CapacityTypeLabelKey).Has(v1.CapacityTypeOnDemand)).To(BeTrue())
	})

	It("B) issues a delete-only command for a full reserved-only pool with no fallback", func() {
		reservedIT := newInstanceType("reserved-it", []string{v1.CapacityTypeReserved}, reservedOffering(true, 0))
		reservedPool := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "reserved-pool"}})

		setup(reservedPool, reservedIT, []*v1.NodePool{reservedPool}, map[string][]*cloudprovider.InstanceType{
			reservedPool.Name: {reservedIT},
		})

		cmds := computeDriftDecision()
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Decision()).To(Equal(disruption.DeleteDecision))
		Expect(cmds[0].Replacements).To(HaveLen(0))
	})

	It("F) terminate-first fires for a pod pinned to the real NodePool name via nodeAffinity (requirement widened in the sim)", func() {
		pinNodePoolName = "reserved-pool"
		reservedIT := newInstanceType("reserved-it", []string{v1.CapacityTypeReserved}, reservedOffering(true, 0))
		reservedPool := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "reserved-pool"}})

		setup(reservedPool, reservedIT, []*v1.NodePool{reservedPool}, map[string][]*cloudprovider.InstanceType{
			reservedPool.Name: {reservedIT},
		})

		cmds := computeDriftDecision()
		// The pod requires karpenter.sh/nodepool In [reserved-pool]. During the terminate-first sim the
		// requirement is widened to In [reserved-pool, reserved-pool-synthetic-freed-slot], so the pod lands on
		// the synthetic freed-slot pool. Deleting the candidate is the only way to make room -> delete-only.
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Decision()).To(Equal(disruption.DeleteDecision))
		Expect(cmds[0].Replacements).To(HaveLen(0))
	})

	It("F2) terminate-first fires for a pod pinned to the real NodePool name via nodeSelector (promoted to widened nodeAffinity in the sim)", func() {
		pinNodePoolName = "reserved-pool"
		pinViaNodeSelector = true
		reservedIT := newInstanceType("reserved-it", []string{v1.CapacityTypeReserved}, reservedOffering(true, 0))
		reservedPool := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "reserved-pool"}})

		setup(reservedPool, reservedIT, []*v1.NodePool{reservedPool}, map[string][]*cloudprovider.InstanceType{
			reservedPool.Name: {reservedIT},
		})

		cmds := computeDriftDecision()
		// The pod pins nodepool=reserved-pool via spec.nodeSelector. In the sim that exact-match entry is
		// promoted to a required nodeAffinity In [reserved-pool, reserved-pool-synthetic-freed-slot], so the pod
		// lands on the synthetic freed-slot pool -> delete-only, no replacements.
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Decision()).To(Equal(disruption.DeleteDecision))
		Expect(cmds[0].Replacements).To(HaveLen(0))
	})

	It("C) does not terminate-first for an unhealthy full reserved-only pool (blocked)", func() {
		reservedIT := newInstanceType("reserved-it", []string{v1.CapacityTypeReserved}, reservedOffering(false, 0))
		reservedPool := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "reserved-pool"}})

		setup(reservedPool, reservedIT, []*v1.NodePool{reservedPool}, map[string][]*cloudprovider.InstanceType{
			reservedPool.Name: {reservedIT},
		})

		cmds := computeDriftDecision()
		Expect(cmds).To(HaveLen(0))
	})

	It("D) replaces onto on-demand within a mixed pool whose reservation is full", func() {
		reservedIT := newInstanceType("mixed-it", []string{v1.CapacityTypeReserved, v1.CapacityTypeOnDemand}, reservedOffering(true, 0), onDemandOffering())
		mixedPool := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "mixed-pool"}})

		setup(mixedPool, reservedIT, []*v1.NodePool{mixedPool}, map[string][]*cloudprovider.InstanceType{
			mixedPool.Name: {reservedIT},
		})

		cmds := computeDriftDecision()
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Decision()).To(Equal(disruption.ReplaceDecision))
		Expect(cmds[0].Replacements).To(HaveLen(1))
		Expect(cmds[0].Replacements[0].Requirements.Get(v1.CapacityTypeLabelKey).Has(v1.CapacityTypeOnDemand)).To(BeTrue())
	})

	It("E) replaces onto reserved when the reservation has a spare slot", func() {
		reservedIT := newInstanceType("reserved-it", []string{v1.CapacityTypeReserved}, reservedOffering(true, 1))
		reservedPool := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "reserved-pool"}})

		setup(reservedPool, reservedIT, []*v1.NodePool{reservedPool}, map[string][]*cloudprovider.InstanceType{
			reservedPool.Name: {reservedIT},
		})

		cmds := computeDriftDecision()
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Decision()).To(Equal(disruption.ReplaceDecision))
	})
})
