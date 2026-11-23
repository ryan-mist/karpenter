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

package disruption

import (
	"context"

	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

// commitReboot hands the candidate to the reboot controller and returns false if it is already rebooting or deleting.
// The reason is written as the DisruptionReason condition, so reboot drain evictions and metrics are attributed to the
// method that committed the reboot, and the message is written on the Rebooting condition. The drain bound is the
// candidate's TerminationGracePeriod, falling back to the NodeClaim's; no bound means an unbounded drain.
func commitReboot(ctx context.Context, kubeClient client.Client, cluster *state.Cluster, clk clock.Clock, candidate *Candidate,
	reason v1.DisruptionReason, message string) (bool, error) {
	// Re-read in case cluster state lags the API server.
	nodeClaim := &v1.NodeClaim{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(candidate.NodeClaim), nodeClaim); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	if nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting).IsTrue() || !nodeClaim.DeletionTimestamp.IsZero() {
		return false, nil
	}
	// Fall back to the NodeClaim TGP; no TGP means an unbounded drain.
	tgp := candidate.TerminationGracePeriod
	if tgp == nil && nodeClaim.Spec.TerminationGracePeriod != nil {
		tgp = &nodeClaim.Spec.TerminationGracePeriod.Duration
	}
	stored := nodeClaim.DeepCopy()
	if tgp != nil {
		nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{
			v1.RebootTerminationGracePeriodAnnotationKey: tgp.String(),
		})
	} else {
		// A previous reboot may have left a bound behind.
		delete(nodeClaim.Annotations, v1.RebootTerminationGracePeriodAnnotationKey)
	}
	if !equality.Semantic.DeepEqual(stored.Annotations, nodeClaim.Annotations) {
		if err := kubeClient.Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			return false, err
		}
	}
	stored = nodeClaim.DeepCopy()
	// Attribute reboot drain evictions to the committing method.
	nodeClaim.StatusConditions(status.WithClock(clk)).SetTrueWithReason(v1.ConditionTypeDisruptionReason, string(reason), string(reason))
	nodeClaim.StatusConditions(status.WithClock(clk)).SetTrueWithReason(v1.ConditionTypeRebooting, v1.RebootReasonRequested, message)
	if err := kubeClient.Status().Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, err
	}
	// Make the reboot visible before the informer catches up, so the next pass counts it against the budget and
	// excludes it from candidates.
	cluster.UpdateNodeClaim(nodeClaim)
	log.FromContext(ctx).WithValues("NodeClaim", klog.KObj(nodeClaim), "reason", reason).Info("committed node reboot")
	return true, nil
}
