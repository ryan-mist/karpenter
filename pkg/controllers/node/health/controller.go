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

package health

import (
	"context"
	"fmt"
	"time"

	"github.com/awslabs/operatorpkg/reasonable"
	"github.com/awslabs/operatorpkg/serrors"
	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	utilscontroller "sigs.k8s.io/karpenter/pkg/utils/controller"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
)

// breakerRequeue is how long a node blocked by the NodePool circuit breaker waits before it is re-evaluated.
const breakerRequeue = 5 * time.Minute

// Controller repairs registered nodes that are unhealthy before they initialize. The repair disruption method only
// considers initialized nodes and the NodeClaim liveness controller only considers unregistered ones, so without this
// controller a node that registers but never becomes healthy is stranded.
//
// Disruption budgets don't apply: they exist to protect running workload, and they already exclude uninitialized nodes
// from both the node count and the in-flight disruptions. Repair still halts for a NodePool once more than
// UnhealthyThreshold of its nodes are unhealthy, so a correlated failure (e.g. a bad image) doesn't churn the pool.
type Controller struct {
	clock         clock.Clock
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	recorder      events.Recorder
	matcher       *RepairPolicyMatcher
}

// NewController compiles the provider's repair policy set with MustNewRepairPolicyMatcher, so it accepts exactly the
// policy sets the repair disruption method does. Whatever action a matching policy selects, this controller terminates:
// the reboot controller bounds recovery from the Initialized condition's transition to Unknown, which never happens on a
// node that never initialized, so a reboot here would immediately time out and escalate to replacement anyway.
func NewController(clk clock.Clock, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider, recorder events.Recorder) *Controller {
	return &Controller{
		clock:         clk,
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
		recorder:      recorder,
		matcher:       MustNewRepairPolicyMatcher(cloudProvider),
	}
}

func (c *Controller) Name() string {
	return "node.health"
}

// Register watches Nodes and the NodeClaims that own them. Eligibility depends on state from both objects, and several
// transitions change only one of them: registration labels the Node before it marks the NodeClaim Registered, and a
// reboot reaches its terminal outcome on the NodeClaim alone. Each watch fires on every input to eligibility that can
// change without the other firing; toleration and the circuit breaker are covered by requeues.
func (c *Controller) Register(ctx context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("node.health").
		For(&corev1.Node{}, builder.WithPredicates(nodePredicates(c.cloudProvider)...)).
		Watches(&v1.NodeClaim{}, nodeutils.NodeClaimEventHandler(c.kubeClient), builder.WithPredicates(nodeClaimPredicates(c.cloudProvider)...)).
		WithOptions(controller.Options{
			RateLimiter:             reasonable.RateLimiter(),
			MaxConcurrentReconciles: utilscontroller.LinearScaleReconciles(utilscontroller.CPUCount(ctx), 10, 100),
		}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}

//nolint:gocyclo
func (c *Controller) Reconcile(ctx context.Context, node *corev1.Node) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())
	if !options.FromContext(ctx).FeatureGates.NodeRepair {
		return reconcile.Result{}, nil
	}
	nodeClaim, err := nodeutils.NodeClaimForNode(ctx, c.kubeClient, node)
	if err != nil {
		return reconcile.Result{}, nodeutils.IgnoreDuplicateNodeClaimError(nodeutils.IgnoreNodeClaimNotFoundError(err))
	}
	ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("NodeClaim", klog.KObj(nodeClaim)))
	if !owns(node, nodeClaim) {
		return reconcile.Result{}, nil
	}
	// do-not-repair is the operator's escape hatch; do-not-disrupt is deliberately ignored, as in the disruption method.
	if node.Annotations[v1.DoNotRepairAnnotationKey] == "true" || nodeClaim.Annotations[v1.DoNotRepairAnnotationKey] == "true" {
		return reconcile.Result{}, nil
	}
	// Repair is scoped to NodePool-owned nodes, matching the repair disruption method.
	nodePoolName := nodeClaim.Labels[v1.NodePoolLabelKey]
	if nodePoolName == "" {
		return reconcile.Result{}, nil
	}
	now := c.clock.Now()
	result := c.matcher.EvaluateSince(node, now, rebootFinishedAt(nodeClaim))
	if result.Action == "" {
		if !result.NextEligibleAt.IsZero() {
			return reconcile.Result{RequeueAfter: result.NextEligibleAt.Sub(now)}, nil
		}
		return reconcile.Result{}, nil
	}
	tripped, err := TrippedNodePools(ctx, c.kubeClient, c.matcher, client.MatchingLabels{v1.NodePoolLabelKey: nodePoolName})
	if err != nil {
		return reconcile.Result{}, err
	}
	if tripped[nodePoolName] {
		nodePool := &v1.NodePool{}
		if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: nodePoolName}, nodePool); err != nil {
			return reconcile.Result{}, client.IgnoreNotFound(err)
		}
		c.recorder.Publish(disruptionevents.NodeRepairBlocked(node, nodeClaim, nodePool,
			fmt.Sprintf("more than %s of nodes in nodepool %q are unhealthy", UnhealthyThreshold, nodePoolName))...)
		return reconcile.Result{RequeueAfter: breakerRequeue}, nil
	}
	// A deleting NodeClaim that is still eligible only needs its termination deadline. This retries a stamp that failed
	// after this controller's delete succeeded and, like v1.14 node repair, also forces an eligible node that something
	// else is deleting.
	if !nodeClaim.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, c.stampTerminationDeadline(ctx, nodeClaim)
	}
	return c.delete(ctx, node, nodeClaim, result)
}

// owns reports whether the node is registered but not initialized, and not mid-reboot. A node counts as initialized
// when either the NodeClaim condition or the Node label says so: the initialization controller writes the label before
// the condition, and the repair disruption method keys off the label, so requiring both to be unset keeps the two repair
// paths disjoint. Rebooting nodes are left to the reboot controller, whose recovery deadline escalates to replacement;
// once the reboot is terminal, a node that goes unhealthy before re-initializing is repaired here. Conditions are read
// with WithObservedOnly so a cached NodeClaim is never mutated.
func owns(node *corev1.Node, nodeClaim *v1.NodeClaim) bool {
	conditions := nodeClaim.StatusConditions(status.WithObservedOnly())
	return conditions.Get(v1.ConditionTypeRegistered).IsTrue() &&
		!conditions.Get(v1.ConditionTypeInitialized).IsTrue() &&
		node.Labels[v1.NodeInitializedLabelKey] != "true" &&
		!conditions.Get(v1.ConditionTypeRebooting).IsTrue()
}

// rebootFinishedAt returns when the NodeClaim's most recent reboot reached a terminal outcome, or zero if it was never
// rebooted. The reboot controller's terminal write flips Rebooting from True to False, which stamps its transition time;
// owns excludes NodeClaims that are still rebooting, so any Rebooting condition seen here is terminal.
// Unhealthy time is only counted from then: a condition reported before or during the reboot (e.g. by an agent that
// hasn't re-reported since the node came back) says nothing about the rebooted node, so the node gets a full
// toleration to re-initialize, and return to the repair disruption method's reboot escalation, or clear the condition.
func rebootFinishedAt(nodeClaim *v1.NodeClaim) time.Time {
	rebooting := nodeClaim.StatusConditions(status.WithObservedOnly()).Get(v1.ConditionTypeRebooting)
	if rebooting == nil {
		return time.Time{}
	}
	return rebooting.LastTransitionTime.Time
}

// delete requests NodeClaim deletion, preconditioned on the evaluated ResourceVersion so a NodeClaim that initialized
// or started rebooting since it was read is re-evaluated instead of deleted. Termination is forceful, matching v1.14
// node repair: drainable pods are force-deleted with a minimal grace period, bypassing PDBs and do-not-disrupt. That
// includes workload pods, which can land on a node that is Ready but never initializes (e.g. a requested extended
// resource never registers). The deadline is stamped only after the delete succeeds: stamping first would bump the
// ResourceVersion the delete is preconditioned on, and would leave a stale deadline on a NodeClaim that escapes repair.
func (c *Controller) delete(ctx context.Context, node *corev1.Node, nodeClaim *v1.NodeClaim, result RepairResult) (reconcile.Result, error) {
	if err := c.kubeClient.Delete(ctx, nodeClaim, client.Preconditions{ResourceVersion: lo.ToPtr(nodeClaim.ResourceVersion)}); err != nil {
		if errors.IsConflict(err) {
			return reconcile.Result{Requeue: true}, nil
		}
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	log.FromContext(ctx).WithValues(
		"condition", result.Condition,
		"status", result.ConditionStatus,
		"reason", result.Reason,
		"eligible-at", result.SelectedEligibleAt,
	).Info("deleting unhealthy node that never initialized")
	labels := map[string]string{
		metrics.ReasonLabel:              metrics.UnhealthyReason,
		metrics.NodePoolLabel:            nodeClaim.Labels[v1.NodePoolLabelKey],
		metrics.CapacityTypeLabel:        nodeClaim.Labels[v1.CapacityTypeLabelKey],
		metrics.ConsolidationPolicyLabel: "",
		metrics.TerminationModeLabel:     metrics.TerminationModeForceful,
	}
	metrics.NodeClaimsDisruptedTotal.Inc(labels)
	// Errors don't fail the reconcile; the metric reports 0.
	reschedulablePods, err := nodeutils.ReschedulablePods(ctx, c.kubeClient, node.Name)
	if err != nil {
		log.FromContext(ctx).V(1).Info("listing reschedulable pods for disruption metric", "error", err.Error())
	}
	metrics.PodsDisruptionInitiatedTotal.Add(float64(len(reschedulablePods)), labels)
	NodeClaimsUnhealthyDisruptedTotal.Inc(map[string]string{
		ConditionLabel:               pretty.ToSnakeCase(string(result.Condition)),
		metrics.NodePoolLabel:        nodeClaim.Labels[v1.NodePoolLabelKey],
		metrics.CapacityTypeLabel:    nodeClaim.Labels[v1.CapacityTypeLabelKey],
		ImageIDLabel:                 nodeClaim.Status.ImageID,
		metrics.TerminationModeLabel: metrics.TerminationModeForceful,
	})
	if err := c.stampTerminationDeadline(ctx, nodeClaim); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

// stampTerminationDeadline tightens the NodeClaim's termination deadline to now. It never extends an earlier deadline.
func (c *Controller) stampTerminationDeadline(ctx context.Context, nodeClaim *v1.NodeClaim) error {
	deadline := c.clock.Now()
	return retry.OnError(retry.DefaultBackoff, errors.IsConflict, func() error {
		stored := &v1.NodeClaim{}
		if err := c.kubeClient.Get(ctx, client.ObjectKeyFromObject(nodeClaim), stored); err != nil {
			return client.IgnoreNotFound(err)
		}
		if value, ok := stored.Annotations[v1.NodeClaimTerminationTimestampAnnotationKey]; ok {
			if existing, err := time.Parse(time.RFC3339, value); err == nil && !existing.After(deadline) {
				return nil
			}
		}
		if err := nodeclaimutils.PatchTerminationTimestampAnnotation(ctx, c.kubeClient, stored, deadline); err != nil {
			if client.IgnoreNotFound(err) == nil {
				return nil
			}
			return serrors.Wrap(fmt.Errorf("patching termination timestamp, %w", err), "NodeClaim", klog.KObj(nodeClaim))
		}
		return nil
	})
}

// nodePredicates admit Node events that can change eligibility: creation, becoming managed, registration, losing the
// initialized label (a reboot strips it), any condition change, and do-not-repair changes. Initialized nodes belong
// to the repair disruption method and are filtered out.
func nodePredicates(cloudProvider cloudprovider.CloudProvider) []predicate.Predicate {
	return []predicate.Predicate{
		nodeutils.IsManagedPredicateFuncs(cloudProvider),
		predicate.NewPredicateFuncs(func(o client.Object) bool {
			return o.GetLabels()[v1.NodeInitializedLabelKey] != "true"
		}),
		predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
			oldNode, newNode := e.ObjectOld.(*corev1.Node), e.ObjectNew.(*corev1.Node)
			return !nodeutils.IsManaged(oldNode, cloudProvider) ||
				oldNode.Labels[v1.NodeRegisteredLabelKey] != newNode.Labels[v1.NodeRegisteredLabelKey] ||
				oldNode.Labels[v1.NodeInitializedLabelKey] != newNode.Labels[v1.NodeInitializedLabelKey] ||
				oldNode.Annotations[v1.DoNotRepairAnnotationKey] != newNode.Annotations[v1.DoNotRepairAnnotationKey] ||
				conditionsChanged(oldNode, newNode)
		}},
	}
}

// nodeClaimPredicates admit NodeClaim updates that can change eligibility without a Node event: the Registered
// condition changing status (registration labels the Node before it marks the NodeClaim Registered), the Rebooting
// condition changing status (a reboot reaches its terminal outcome on the NodeClaim alone), and do-not-repair changes.
// Initialized needs no trigger: it only returns to Unknown when a reboot is issued, which also strips the Node's
// initialized label. Node events cover creation, so NodeClaim creation, deletion, and generic events are dropped.
// Conditions are read with WithObservedOnly because predicates receive the shared cached object.
func nodeClaimPredicates(cloudProvider cloudprovider.CloudProvider) []predicate.Predicate {
	return []predicate.Predicate{
		nodeclaimutils.IsManagedPredicateFuncs(cloudProvider),
		predicate.Funcs{
			CreateFunc:  func(event.CreateEvent) bool { return false },
			DeleteFunc:  func(event.DeleteEvent) bool { return false },
			GenericFunc: func(event.GenericEvent) bool { return false },
			UpdateFunc: func(e event.UpdateEvent) bool {
				oldNodeClaim, newNodeClaim := e.ObjectOld.(*v1.NodeClaim), e.ObjectNew.(*v1.NodeClaim)
				oldConditions := oldNodeClaim.StatusConditions(status.WithObservedOnly())
				newConditions := newNodeClaim.StatusConditions(status.WithObservedOnly())
				return lo.SomeBy([]string{v1.ConditionTypeRegistered, v1.ConditionTypeRebooting}, func(t string) bool {
					return conditionStatus(oldConditions.Get(t)) != conditionStatus(newConditions.Get(t))
				}) || oldNodeClaim.Annotations[v1.DoNotRepairAnnotationKey] != newNodeClaim.Annotations[v1.DoNotRepairAnnotationKey]
			},
		},
	}
}

func conditionStatus(condition *status.Condition) metav1.ConditionStatus {
	if condition == nil {
		return ""
	}
	return condition.Status
}

// conditionsChanged reports whether any Node condition was added, removed, or changed status, reason, or transition
// time. Reason matters because repair policies can match on it, and it can change without a transition.
func conditionsChanged(oldNode, newNode *corev1.Node) bool {
	if len(oldNode.Status.Conditions) != len(newNode.Status.Conditions) {
		return true
	}
	for _, oldCond := range oldNode.Status.Conditions {
		newCond := nodeutils.GetCondition(newNode, oldCond.Type)
		if newCond.Type == "" || oldCond.LastTransitionTime != newCond.LastTransitionTime || oldCond.Status != newCond.Status || oldCond.Reason != newCond.Reason {
			return true
		}
	}
	return false
}
