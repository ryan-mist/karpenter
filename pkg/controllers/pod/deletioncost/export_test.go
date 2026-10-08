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

package deletioncost

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator"
)

// ReconcileKey exposes the reconciler that Register wires into the controller, so tests can drive the queue the same
// way the controller does.
func (q *Queue) ReconcileKey(ctx context.Context, key terminator.QueueKey) (reconcile.Result, error) {
	return q.reconcileKey(ctx, key)
}
