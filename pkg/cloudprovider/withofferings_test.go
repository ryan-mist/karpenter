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

package cloudprovider_test

import (
	"reflect"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// mapID returns the identity of a map or slice so tests can assert that copies share (or don't share) it.
func mapID(m any) uintptr {
	return uintptr(reflect.ValueOf(m).UnsafePointer())
}

func copyOfferings(it *cloudprovider.InstanceType) cloudprovider.Offerings {
	return append(cloudprovider.Offerings(nil), it.Offerings...)
}

var _ = Describe("InstanceType WithOfferings", func() {
	var base *cloudprovider.InstanceType
	BeforeEach(func() {
		base = fake.NewInstanceType("base-type", fake.WithResourceSliceTemplates(fake.ResourceSliceTemplate("gpu.example.com", "pool", fake.Devices("gpu-0", "gpu-1")...)))
	})

	It("should return a copy with the new offerings and the receiver's other fields", func() {
		offerings := cloudprovider.Offerings{base.Offerings[0]}
		c := base.WithOfferings(offerings)
		Expect(c).ToNot(BeIdenticalTo(base))
		Expect(c.Name).To(Equal(base.Name))
		Expect(c.Offerings).To(HaveLen(1))
		Expect(c.Offerings[0]).To(BeIdenticalTo(base.Offerings[0]))
		Expect(mapID(c.Requirements)).To(Equal(mapID(base.Requirements)))
		Expect(c.Overhead).To(BeIdenticalTo(base.Overhead))
		Expect(mapID(c.Capacity)).To(Equal(mapID(base.Capacity)))
		Expect(c.VolumeAttachmentLimits).To(Equal(base.VolumeAttachmentLimits))
		Expect(c.DynamicResources.ResourceSliceTemplates).To(HaveLen(1))
		Expect(mapID(c.DynamicResources.ResourceSliceTemplates)).To(Equal(mapID(base.DynamicResources.ResourceSliceTemplates)))
		Expect(c.IsCapacityOverlayApplied()).To(BeFalse())
		Expect(len(base.Offerings)).To(BeNumerically(">", 1))
	})
	It("should group only the copy's available offerings", func() {
		c := base.WithOfferings(cloudprovider.Offerings{base.Offerings[0], base.Offerings[1]})
		groups := c.AllocatableOfferingsList()
		Expect(groups).To(HaveLen(1))
		Expect(groups[0].Offerings).To(ConsistOf(base.Offerings[0], base.Offerings[1]))
		Expect(base.AllocatableOfferingsList()[0].Offerings).To(HaveLen(len(base.Offerings.Available())))
	})
	It("should share the base allocatable when the receiver has already computed it", func() {
		want := base.Allocatable()
		c := base.WithOfferings(copyOfferings(base))
		Expect(mapID(c.Allocatable())).To(Equal(mapID(want)))
	})
	It("should share the base allocatable across copies created before it is computed, including copies of copies", func() {
		a := base.WithOfferings(copyOfferings(base))
		b := a.WithOfferings(copyOfferings(a))
		Expect(mapID(b.Allocatable())).To(Equal(mapID(a.Allocatable())))
		Expect(mapID(base.Allocatable())).To(Equal(mapID(a.Allocatable())))
	})
	It("should compute the same allocatable as an equivalent struct literal", func() {
		literal := &cloudprovider.InstanceType{Name: base.Name, Requirements: base.Requirements, Offerings: copyOfferings(base), Capacity: base.Capacity, Overhead: base.Overhead}
		c := base.WithOfferings(copyOfferings(base))
		Expect(c.Allocatable()).To(Equal(literal.Allocatable()))
	})
	It("should share the base allocatable when Requirements are reassigned on the copy", func() {
		c := base.WithOfferings(copyOfferings(base))
		c.Requirements = scheduling.NewRequirements(append(base.Requirements.Values(), scheduling.NewRequirement("example.com/label", corev1.NodeSelectorOpIn, "value"))...)
		Expect(mapID(c.Allocatable())).To(Equal(mapID(base.Allocatable())))
		Expect(base.Requirements.Has("example.com/label")).To(BeFalse())
	})
	It("should share the base allocatable group when the copy has override offerings", func() {
		override := &cloudprovider.Offering{
			Requirements:     scheduling.NewRequirements(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand)),
			Available:        true,
			CapacityOverride: corev1.ResourceList{"example.com/device": resource.MustParse("4")},
			OverheadOverride: &cloudprovider.InstanceTypeOverhead{SystemReserved: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}},
		}
		c := base.WithOfferings(append(copyOfferings(base), override))
		groups := c.AllocatableOfferingsList()
		Expect(groups).To(HaveLen(2))
		Expect(mapID(groups[0].Allocatable)).To(Equal(mapID(base.Allocatable())))
		Expect(groups[0].Offerings).To(HaveLen(len(base.Offerings.Available())))
		Expect(groups[1].Offerings).To(ConsistOf(override))
		device := groups[1].Allocatable["example.com/device"]
		Expect(device.Value()).To(BeNumerically("==", 4))
		baseMem, overrideMem := groups[0].Allocatable[corev1.ResourceMemory], groups[1].Allocatable[corev1.ResourceMemory]
		Expect(overrideMem.Cmp(baseMem)).To(Equal(-1))
		// The receiver is unaffected by the copy's override offerings
		Expect(base.AllocatableOfferingsList()).To(HaveLen(1))
		Expect(base.Allocatable()).ToNot(HaveKey(corev1.ResourceName("example.com/device")))
	})
	It("should stop sharing the base allocatable when a capacity overlay is applied to the copy", func() {
		baseAlloc := base.Allocatable()
		c := base.WithOfferings(copyOfferings(base))
		c.ApplyCapacityOverlay(corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Ti"), "example.com/device": resource.MustParse("2")})
		Expect(c.IsCapacityOverlayApplied()).To(BeTrue())
		Expect(mapID(c.Allocatable())).ToNot(Equal(mapID(baseAlloc)))
		Expect(c.Allocatable()).To(HaveKeyWithValue(corev1.ResourceName("example.com/device"), resource.MustParse("2")))
		got, was := c.Allocatable()[corev1.ResourceMemory], baseAlloc[corev1.ResourceMemory]
		Expect(got.Cmp(was)).To(Equal(1))
		// The receiver is unaffected
		Expect(base.IsCapacityOverlayApplied()).To(BeFalse())
		Expect(mapID(base.Allocatable())).To(Equal(mapID(baseAlloc)))
		Expect(base.Allocatable()).ToNot(HaveKey(corev1.ResourceName("example.com/device")))
	})
	It("should share the overlaid allocatable with copies of an overlaid copy", func() {
		overlaid := base.WithOfferings(copyOfferings(base))
		overlaid.ApplyCapacityOverlay(corev1.ResourceList{"example.com/device": resource.MustParse("2")})
		c := overlaid.WithOfferings(copyOfferings(overlaid))
		Expect(c.IsCapacityOverlayApplied()).To(BeTrue())
		Expect(mapID(c.Allocatable())).To(Equal(mapID(overlaid.Allocatable())))
		Expect(c.Allocatable()).To(HaveKey(corev1.ResourceName("example.com/device")))
		Expect(mapID(c.Allocatable())).ToNot(Equal(mapID(base.Allocatable())))
	})
	It("should keep sharing the base allocatable between other copies when one copy has a capacity overlay", func() {
		c := base.WithOfferings(copyOfferings(base))
		c.ApplyCapacityOverlay(corev1.ResourceList{"example.com/device": resource.MustParse("2")})
		d := base.WithOfferings(copyOfferings(base))
		Expect(mapID(d.Allocatable())).To(Equal(mapID(base.Allocatable())))
		Expect(d.Allocatable()).ToNot(HaveKey(corev1.ResourceName("example.com/device")))
		Expect(c.Allocatable()).To(HaveKey(corev1.ResourceName("example.com/device")))
	})
	It("should not use the shared base allocatable when Capacity is reassigned on the copy", func() {
		baseAlloc := base.Allocatable()
		c := base.WithOfferings(copyOfferings(base))
		c.Capacity = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1000"), corev1.ResourceMemory: resource.MustParse("1Ti"), corev1.ResourcePods: resource.MustParse("10")}
		Expect(mapID(c.Allocatable())).ToNot(Equal(mapID(baseAlloc)))
		got, was := c.Allocatable()[corev1.ResourceCPU], baseAlloc[corev1.ResourceCPU]
		Expect(got.Cmp(was)).To(Equal(1))
		Expect(mapID(base.Allocatable())).To(Equal(mapID(baseAlloc)))
	})
	It("should not use the shared base allocatable when Overhead is reassigned on the copy", func() {
		baseAlloc := base.Allocatable()
		c := base.WithOfferings(copyOfferings(base))
		c.Overhead = &cloudprovider.InstanceTypeOverhead{KubeReserved: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}
		Expect(mapID(c.Allocatable())).ToNot(Equal(mapID(baseAlloc)))
		got, was := c.Allocatable()[corev1.ResourceCPU], baseAlloc[corev1.ResourceCPU]
		Expect(got.Cmp(was)).To(Equal(-1))
	})
	It("should not share the allocatable with a DeepCopy", func() {
		baseAlloc := base.Allocatable()
		c := base.WithOfferings(copyOfferings(base))
		dc := c.DeepCopy()
		Expect(mapID(dc.Allocatable())).ToNot(Equal(mapID(baseAlloc)))
		Expect(dc.Allocatable()).To(Equal(baseAlloc))
	})
	It("should share the base allocatable when copies are created and used concurrently", func() {
		copies := make([]*cloudprovider.InstanceType, 64)
		var wg sync.WaitGroup
		for i := range copies {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				src := base
				if i%2 == 1 {
					// Exercise copies of copies racing with the receiver's own computation
					src = base.WithOfferings(copyOfferings(base))
				}
				copies[i] = src.WithOfferings(copyOfferings(base))
				_ = copies[i].AllocatableOfferingsList()
				if i%4 == 0 {
					_ = base.Allocatable()
				}
				if i%8 == 3 {
					overlaid := base.WithOfferings(copyOfferings(base))
					overlaid.ApplyCapacityOverlay(corev1.ResourceList{"example.com/device": resource.MustParse("1")})
					Expect(overlaid.Allocatable()).To(HaveKey(corev1.ResourceName("example.com/device")))
				}
			}()
		}
		wg.Wait()
		want := mapID(base.Allocatable())
		for _, c := range copies {
			Expect(mapID(c.Allocatable())).To(Equal(want))
			Expect(c.AllocatableOfferingsList()[0].Offerings).To(HaveLen(len(base.Offerings.Available())))
		}
	})
})
