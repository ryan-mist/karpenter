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
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
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

// NewController validates and compiles the provider's repair policy set the same way the repair disruption method
// does. It panics when the provider defines no policies or the set is invalid.
func NewController(clk clock.Clock, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider, recorder events.Recorder) *Controller {
	policies := cloudProvider.RepairPolicies()
	if len(policies) == 0 {
		panic("node repair requires the cloud provider to define RepairPolicies, but it defines none")
	}
	// Accept the same actions as the repair disruption method so both paths validate the provider set identically. Any
	// selected action replaces here: rebooting a node that never initialized can't restore a healthy prior state.
	matcher, err := NewRepairPolicyMatcher(policies, sets.New(cloudprovider.ReplaceNode, cloudprovider.RebootNode))
	if err != nil {
		panic(fmt.Sprintf("node repair requires valid RepairPolicies: %v", err))
	}
	return &Controller{
		clock:         clk,
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
		recorder:      recorder,
		matcher:       matcher,
	}
}

func (c *Controller) Name() string {
	return "node.health"
}

func (c *Controller) Register(ctx context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("node.health").
		For(&corev1.Node{}, builder.WithPredicates(
			nodeutils.IsManagedPredicateFuncs(c.cloudProvider),
			predicate.NewPredicateFuncs(func(o client.Object) bool {
				// Initialized nodes belong to the repair disruption method.
				return o.GetLabels()[v1.NodeInitializedLabelKey] != "true"
			}),
			predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
				oldNode, newNode := e.ObjectOld.(*corev1.Node), e.ObjectNew.(*corev1.Node)
				return oldNode.Labels[v1.NodeRegisteredLabelKey] != newNode.Labels[v1.NodeRegisteredLabelKey] ||
					oldNode.Labels[v1.NodeInitializedLabelKey] != newNode.Labels[v1.NodeInitializedLabelKey] ||
					conditionsChanged(oldNode, newNode)
			}},
		)).
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
	if !c.owns(node, nodeClaim) {
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
	result := c.matcher.Evaluate(node, now)
	if result.Action == "" {
		if next, ok := c.matcher.NextEligibleAt(node, now); ok {
			return reconcile.Result{RequeueAfter: next.Sub(now)}, nil
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
	return c.delete(ctx, node, nodeClaim, result)
}

// owns reports whether the node is registered but not initialized, and not mid-reboot. A node counts as initialized
// when either the NodeClaim condition or the Node label says so: the initialization controller writes the label before
// the condition, and the repair disruption method keys off the label, so requiring both to be unset keeps the two repair
// paths disjoint. Rebooting nodes are left to the reboot controller, whose recovery deadline escalates to replacement;
// once the reboot is terminal, a node that goes unhealthy before re-initializing is repaired here.
func (c *Controller) owns(node *corev1.Node, nodeClaim *v1.NodeClaim) bool {
	return nodeClaim.DeletionTimestamp.IsZero() &&
		nodeClaim.StatusConditions().Get(v1.ConditionTypeRegistered).IsTrue() &&
		!nodeClaim.StatusConditions().Get(v1.ConditionTypeInitialized).IsTrue() &&
		node.Labels[v1.NodeInitializedLabelKey] != "true" &&
		!nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting).IsTrue()
}

// delete requests NodeClaim deletion, preconditioned on the evaluated ResourceVersion so a NodeClaim that initialized
// or started rebooting since it was read is re-evaluated instead of deleted. The node never served workload and its
// unhealthy kubelet can't complete a graceful drain, so termination is forceful. The deadline is stamped only after the
// delete succeeds so a NodeClaim that escapes repair never carries a stale deadline into a later disruption.
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

// conditionsChanged reports whether any Node condition was added, removed, or changed status or transition time.
func conditionsChanged(oldNode, newNode *corev1.Node) bool {
	if len(oldNode.Status.Conditions) != len(newNode.Status.Conditions) {
		return true
	}
	for _, oldCond := range oldNode.Status.Conditions {
		newCond := nodeutils.GetCondition(newNode, oldCond.Type)
		if newCond.Type == "" || oldCond.LastTransitionTime != newCond.LastTransitionTime || oldCond.Status != newCond.Status {
			return true
		}
	}
	return false
}
