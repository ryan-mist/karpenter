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

package topology_test

import (
	"hash/fnv"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/scheduling/topology"
)

// fakePredicate is a NodePredicate that matches nodes labeled with its name
type fakePredicate string

func (p fakePredicate) Hash() uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(p))
	return h.Sum64()
}
func (p fakePredicate) Equal(o topology.NodePredicate) bool { return o == topology.NodePredicate(p) }
func (p fakePredicate) Keys() []string                      { return []string{string(p)} }
func (p fakePredicate) ReadsTaints() bool                   { return false }
func (p fakePredicate) Matches(labels map[string]string, _ []corev1.Taint) bool {
	_, ok := labels[string(p)]
	return ok
}
