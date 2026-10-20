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

package scheduling

import (
	"github.com/mitchellh/hashstructure/v2"
	"github.com/samber/lo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

type TopologyType byte

const (
	TopologyTypeSpread TopologyType = iota
	TopologyTypePodAffinity
	TopologyTypePodAntiAffinity
)

func (t TopologyType) String() string {
	switch t {
	case TopologyTypeSpread:
		return "topology spread"
	case TopologyTypePodAffinity:
		return "pod affinity"
	case TopologyTypePodAntiAffinity:
		return "pod anti-affinity"
	}
	return ""
}

// hashSelector is a specialized hash function for a metav1.LabelSelector. Due to https://github.com/mitchellh/hashstructure/issues/36
// repeated requirements inside a label selector can result in hash collisions when using SlicesAsSets. This function provides the same
// behavior while avoiding that bug by storing the individual expression hashes in a set, ensuring there aren't repeated elements.
//
// NOTE: Although repeated elements typically won't occur, they can occur on k8s 1.34+ when using matchLabelKeys since both Karpenter
// and the API server inject an expression.
func hashSelector(selector *metav1.LabelSelector) uint64 {
	expressionHashes := sets.New[uint64]()
	var selectorHash uint64
	if selector != nil {
		for i := range selector.MatchExpressions {
			expressionHashes.Insert(lo.Must(hashstructure.Hash(selector.MatchExpressions[i], hashstructure.FormatV2, &hashstructure.HashOptions{SlicesAsSets: true})))
		}
		selectorHash = lo.Must(hashstructure.Hash(selector.MatchLabels, hashstructure.FormatV2, nil))
	}
	return lo.Must(hashstructure.Hash([]any{expressionHashes, selectorHash}, hashstructure.FormatV2, nil))
}
