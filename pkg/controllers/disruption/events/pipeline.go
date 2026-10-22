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

package events

import (
	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
)

// NodePoolNoAction explains why a disruption pipeline planner did nothing for a NodePool, e.g. reason
// events.DisruptionBudgetExhausted. The pipeline publishes it only when the explanation for that planner and NodePool
// changes.
func NodePoolNoAction(nodePool *v1.NodePool, reason, planner, msg string) events.Event {
	return events.Event{
		InvolvedObject: nodePool,
		Type:           corev1.EventTypeNormal,
		Reason:         reason,
		Message:        msg,
		DedupeValues:   []string{string(nodePool.UID), planner, reason},
	}
}
