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
	"testing"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
)

// BenchmarkInstanceTypeCopyAllocatable models a cloud provider that builds a fresh copy of each cached base
// InstanceType (with its own offerings) on every GetInstanceTypes call, after which the scheduler reads each copy's
// allocatable. One op is one GetInstanceTypes call over 400 instance types.
//
//	structLiteral: copies built with a struct literal, so every copy computes its own allocatable
//	withOfferings: copies built with InstanceType.WithOfferings, so copies share the base allocatable
func BenchmarkInstanceTypeCopyAllocatable(b *testing.B) {
	base := fake.InstanceTypes(400)
	for _, it := range base {
		_ = it.Allocatable()
	}
	for _, mode := range []struct {
		name string
		copy func(*cloudprovider.InstanceType, cloudprovider.Offerings) *cloudprovider.InstanceType
	}{
		{name: "structLiteral", copy: func(it *cloudprovider.InstanceType, offerings cloudprovider.Offerings) *cloudprovider.InstanceType {
			return &cloudprovider.InstanceType{
				Name:                   it.Name,
				Requirements:           it.Requirements,
				Offerings:              offerings,
				Capacity:               it.Capacity,
				Overhead:               it.Overhead,
				VolumeAttachmentLimits: it.VolumeAttachmentLimits,
				DynamicResources:       it.DynamicResources,
			}
		}},
		{name: "withOfferings", copy: func(it *cloudprovider.InstanceType, offerings cloudprovider.Offerings) *cloudprovider.InstanceType {
			return it.WithOfferings(offerings)
		}},
	} {
		b.Run("copy="+mode.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for _, it := range base {
					c := mode.copy(it, it.Offerings)
					_ = c.AllocatableOfferingsList()
				}
			}
		})
	}
}
