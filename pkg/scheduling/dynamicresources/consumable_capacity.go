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

package dynamicresources

import (
	"fmt"

	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// computeConsumedCapacity computes the consumed capacity for all dimensions on a device given
// the request's capacity requirements. Returns nil if the device has no capacity dimensions.
// Returns an error if a requested dimension doesn't exist on the device or violates policy.
func computeConsumedCapacity(
	capacityRequests map[resourcev1.QualifiedName]resource.Quantity,
	deviceCapacity map[resourcev1.QualifiedName]resourcev1.DeviceCapacity,
) (map[resourcev1.QualifiedName]resource.Quantity, error) {
	if len(deviceCapacity) == 0 {
		return nil, nil
	}
	consumed := make(map[resourcev1.QualifiedName]resource.Quantity, len(deviceCapacity))
	for name, cap := range deviceCapacity {
		var requestedVal *resource.Quantity
		if capacityRequests != nil {
			if rv, ok := capacityRequests[name]; ok {
				requestedVal = &rv
			}
		}
		c := calculateConsumedCapacity(requestedVal, cap)
		if violatesPolicy(c, cap.RequestPolicy) {
			return nil, fmt.Errorf("capacity request violates policy for dimension %s", name)
		}
		consumed[name] = c
	}
	return consumed, nil
}

// addCapacity adds the quantities from src into dst, initializing dst if nil.
func addCapacity(dst, src map[resourcev1.QualifiedName]resource.Quantity) map[resourcev1.QualifiedName]resource.Quantity {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		dst = make(map[resourcev1.QualifiedName]resource.Quantity, len(src))
	}
	for name, qty := range src {
		existing := dst[name]
		existing.Add(qty)
		dst[name] = existing
	}
	return dst
}

// subCapacity subtracts the quantities in src from dst.
func subCapacity(dst, src map[resourcev1.QualifiedName]resource.Quantity) map[resourcev1.QualifiedName]resource.Quantity {
	if len(src) == 0 {
		return dst
	}
	for name, qty := range src {
		existing := dst[name]
		existing.Sub(qty)
		dst[name] = existing
	}
	return dst
}

// requestsContainNonExistCapacity returns true if the request references capacity dimensions
// that don't exist on the device.
// Note: equivalent to upstream requestsContainNonExistCapacity in k8s.io/dynamic-resource-allocation
func requestsContainNonExistCapacity(
	capacityRequests map[resourcev1.QualifiedName]resource.Quantity,
	deviceCapacity map[resourcev1.QualifiedName]resourcev1.DeviceCapacity,
) bool {
	for name := range capacityRequests {
		if _, ok := deviceCapacity[name]; !ok {
			return true
		}
	}
	return false
}

// calculateConsumedCapacity returns valid capacity to be consumed regarding the requested capacity and device capacity policy.
// If no requestPolicy, return capacity.Value. If no requestVal, fill the quantity by fillEmptyRequest function
// Otherwise, use requestPolicy to calculate the consumed capacity from request if applicable.
// Note: equivalent to upstream calculateConsumedCapacity in k8s.io/dynamic-resource-allocation
func calculateConsumedCapacity(requestedVal *resource.Quantity, capacity resourcev1.DeviceCapacity) resource.Quantity {
	if requestedVal == nil {
		return fillEmptyRequest(capacity)
	}
	if capacity.RequestPolicy == nil {
		return requestedVal.DeepCopy()
	}
	switch {
	case capacity.RequestPolicy.ValidRange != nil && capacity.RequestPolicy.ValidRange.Min != nil:
		return roundUpRange(requestedVal, capacity.RequestPolicy.ValidRange)
	case capacity.RequestPolicy.ValidValues != nil:
		return roundUpValidValues(requestedVal, capacity.RequestPolicy.ValidValues)
	}
	return requestedVal.DeepCopy()
}

// fillEmptyRequest returns RequestPolicy.Default if defined, otherwise the full device capacity.
// Note: equivalent to upstream fillEmptyRequest in k8s.io/dynamic-resource-allocation
func fillEmptyRequest(capacity resourcev1.DeviceCapacity) resource.Quantity {
	if capacity.RequestPolicy != nil && capacity.RequestPolicy.Default != nil {
		return capacity.RequestPolicy.Default.DeepCopy()
	}
	return capacity.Value.DeepCopy()
}

// roundUpRange rounds requestedVal up to fit within the specified validRange.
// If requestedVal < Min, returns Min.
// If Step is specified, rounds up to the nearest Min + N*Step.
// If no Step is specified and requestedVal >= Min, it returns requestedVal as is.
// Note: equivalent to upstream roundUpRange in k8s.io/dynamic-resource-allocation
func roundUpRange(requestedVal *resource.Quantity, validRange *resourcev1.CapacityRequestPolicyRange) resource.Quantity {
	if requestedVal.Cmp(*validRange.Min) < 0 {
		return validRange.Min.DeepCopy()
	}
	if validRange.Step == nil {
		return requestedVal.DeepCopy()
	}
	requestedInt64 := requestedVal.Value()
	step := validRange.Step.Value()
	min := validRange.Min.Value()
	added := requestedInt64 - min
	n := added / step
	if added%step != 0 {
		n++
	}
	return *resource.NewQuantity(min+step*n, resource.BinarySI)
}

// roundUpValidValues returns the first valid value >= requestedVal. If none exists, returns requestedVal.
// Note: equivalent to upstream roundUpValidValues in k8s.io/dynamic-resource-allocation
func roundUpValidValues(requestedVal *resource.Quantity, validValues []resource.Quantity) resource.Quantity {
	// validValues must be sorted ascending (enforced by API validation).
	for _, validValue := range validValues {
		if requestedVal.Cmp(validValue) <= 0 {
			return validValue.DeepCopy()
		}
	}
	return requestedVal.DeepCopy()
}

// violatesPolicy checks whether a consumed value violates the device's request policy.
// Note: equivalent to upstream violatesPolicy in k8s.io/dynamic-resource-allocation
func violatesPolicy(consumedVal resource.Quantity, policy *resourcev1.CapacityRequestPolicy) bool {
	if policy == nil {
		return false
	}
	if policy.Default != nil && consumedVal.Cmp(*policy.Default) == 0 {
		return false
	}
	switch {
	case policy.ValidRange != nil:
		return violateValidRange(consumedVal, *policy.ValidRange)
	case len(policy.ValidValues) > 0:
		return violateValidValues(consumedVal, policy.ValidValues)
	}
	return false
}

// Note: equivalent to upstream violateValidRange in k8s.io/dynamic-resource-allocation
func violateValidRange(val resource.Quantity, validRange resourcev1.CapacityRequestPolicyRange) bool {
	if validRange.Max != nil && val.Cmp(*validRange.Max) > 0 {
		return true
	}
	if validRange.Step != nil {
		requestedInt64 := val.Value()
		step := validRange.Step.Value()
		min := validRange.Min.Value()
		if (requestedInt64-min)%step != 0 {
			return true
		}
	}
	return false
}

// Note: equivalent to upstream violateValidValues in k8s.io/dynamic-resource-allocation
func violateValidValues(val resource.Quantity, validValues []resource.Quantity) bool {
	for i := range validValues {
		if val.Cmp(validValues[i]) == 0 {
			return false
		}
	}
	return true
}
