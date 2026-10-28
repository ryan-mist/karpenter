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

package node

import (
	"context"
	"maps"
	"strconv"
	"strings"
	"time"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/singleton"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

const (
	nodeName  = "node_name"
	nodePhase = "phase"
	managed   = "managed"
)

// Fixed node metric dimensions. The remaining dimensions are the cloud
// provider's well-known labels, which are self-describing and wrapped without
// help text (see nodeLabelNames).
var (
	nodeNameLabel  = opmetrics.Label{Name: nodeName, Help: "The name of the node."}
	nodePhaseLabel = opmetrics.Label{Name: nodePhase, Help: "The node's lifecycle phase, e.g. `Pending`, `Running`."}
	managedLabel   = opmetrics.Label{Name: managed, Help: "Whether the node is managed by Karpenter.", Values: metrics.BoolValues}
)

var (
	Allocatable         opmetrics.GaugeMetric
	TotalPodRequests    opmetrics.GaugeMetric
	TotalPodLimits      opmetrics.GaugeMetric
	TotalDaemonRequests opmetrics.GaugeMetric
	TotalDaemonLimits   opmetrics.GaugeMetric
	SystemOverhead      opmetrics.GaugeMetric
	Lifetime            opmetrics.GaugeMetric
	ClusterUtilization  opmetrics.GaugeMetric
)

// Initialize metrics at runtime to ensure cloud provider's well-known labels are properly
// injected, preventing race conditions in dependency ordering during label injection for global variable. .
func initializeMetrics(wellKnownLabels map[string]string) {
	Allocatable = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeSubsystem,
			Name:      "allocatable",
			Help:      "Node allocatable are the resources allocatable by nodes.",
		},
		nodeLabelNamesWithResourceType(wellKnownLabels),
		opmetrics.Beta,
	)
	TotalPodRequests = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeSubsystem,
			Name:      "total_pod_requests",
			Help:      "Node total pod requests are the resources requested by pods bound to nodes, including the DaemonSet pods.",
		},
		nodeLabelNamesWithResourceType(wellKnownLabels),
		opmetrics.Beta,
	)
	TotalPodLimits = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeSubsystem,
			Name:      "total_pod_limits",
			Help:      "Node total pod limits are the resources specified by pod limits, including the DaemonSet pods.",
		},
		nodeLabelNamesWithResourceType(wellKnownLabels),
		opmetrics.Beta,
	)
	TotalDaemonRequests = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeSubsystem,
			Name:      "total_daemon_requests",
			Help:      "Node total daemon requests are the resource requested by DaemonSet pods bound to nodes.",
		},
		nodeLabelNamesWithResourceType(wellKnownLabels),
		opmetrics.Beta,
	)
	TotalDaemonLimits = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeSubsystem,
			Name:      "total_daemon_limits",
			Help:      "Node total daemon limits are the resources specified by DaemonSet pod limits.",
		},
		nodeLabelNamesWithResourceType(wellKnownLabels),
		opmetrics.Beta,
	)
	SystemOverhead = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeSubsystem,
			Name:      "system_overhead",
			Help:      "Node system daemon overhead are the resources reserved for system overhead, the difference between the node's capacity and allocatable values are reported by the status.",
		},
		nodeLabelNamesWithResourceType(wellKnownLabels),
		opmetrics.Beta,
	)
	Lifetime = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeSubsystem,
			Name:      "current_lifetime_seconds",
			Help:      "Node age in seconds",
		},
		nodeLabelNames(wellKnownLabels),
		opmetrics.Alpha,
	)
	ClusterUtilization = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: "cluster",
			Name:      "utilization_percent",
			Help:      "Utilization of allocatable resources by pod requests",
		},
		[]opmetrics.Label{metrics.ResourceType},
		opmetrics.Alpha,
	)
}

func nodeLabelNamesWithResourceType(wellKnownLabels map[string]string) []opmetrics.Label {
	return append(
		nodeLabelNames(wellKnownLabels),
		metrics.ResourceType,
	)
}

func nodeLabelNames(wellKnownLabels map[string]string) []opmetrics.Label {
	return append(
		// WellKnownLabels includes the nodepool label, so we don't need to add it as its own item here.
		// If we do, prometheus will panic since there would be duplicate labels.
		// The well-known labels are self-describing, so they are wrapped without help text.
		lo.Map(sets.New(lo.Values(wellKnownLabels)...).UnsortedList(), func(l string, _ int) opmetrics.Label {
			return opmetrics.Label{Name: l}
		}),
		nodeNameLabel,
		nodePhaseLabel,
		managedLabel,
	)
}

type Controller struct {
	cluster     *state.Cluster
	metricStore *metrics.Store
	// wellKnownLabels maps each well-known node label key to its metric label name. It is computed once, together with
	// the metrics' label names, since the gauges can't accept a different label set after they are registered.
	wellKnownLabels map[string]string
}

func NewController(cluster *state.Cluster) *Controller {
	wellKnownLabels := getWellKnownLabels()
	initializeMetrics(wellKnownLabels)
	return &Controller{
		cluster:         cluster,
		metricStore:     metrics.NewStore(),
		wellKnownLabels: wellKnownLabels,
	}
}

func (c *Controller) Reconcile(ctx context.Context) (reconciler.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name()) //nolint:ineffassign,staticcheck

	nodes, allocatable, podRequests := c.readNodes()

	// Build per-node metrics
	metricsMap := make(map[string][]*metrics.StoreMetric, len(nodes)+1)
	for _, n := range nodes {
		metricsMap[n.key] = buildMetrics(n)
	}

	// Build cluster level metric
	metricsMap["clusterUtilization"] = buildClusterUtilizationMetric(allocatable, podRequests)

	c.metricStore.ReplaceAll(metricsMap)

	return reconciler.Result{RequeueAfter: time.Second * 5}, nil
}

func (c *Controller) Name() string {
	return "metrics.node"
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}

// nodeMetricsState is what a node's metrics are built from. It is read from cluster state under the cluster's lock,
// and owns its resource lists so that it can be used after the lock is released.
type nodeMetricsState struct {
	key               string
	labels            prometheus.Labels
	creationTimestamp time.Time
	resources         map[opmetrics.GaugeMetric]corev1.ResourceList
}

// readNodes reads what the metrics need from the nodes in cluster state, rather than deep copying every state node
// with the resources of each of its pods. It also returns the cluster's allocatable resources and pod requests.
func (c *Controller) readNodes() (nodes []nodeMetricsState, allocatable, podRequests corev1.ResourceList) {
	allocatable, podRequests = corev1.ResourceList{}, corev1.ResourceList{}
	// Nodes() holds the cluster's read lock, so nothing in this loop may call back into the cluster.
	for n := range c.cluster.Nodes() {
		if n.Node == nil {
			continue
		}
		// The sums are new lists of new Quantities, but the Node's own lists must be copied: Subtract does arithmetic
		// on the Quantities of its first argument in place, and the Node is shared cluster state.
		nodePodRequests := n.PodRequests()
		resources.MergeInto(allocatable, n.Allocatable())
		resources.MergeInto(podRequests, nodePodRequests)
		nodes = append(nodes, nodeMetricsState{
			key:               client.ObjectKeyFromObject(n.Node).String(),
			labels:            getNodeLabels(n, c.wellKnownLabels),
			creationTimestamp: n.Node.GetCreationTimestamp().Time,
			resources: map[opmetrics.GaugeMetric]corev1.ResourceList{
				SystemOverhead:      resources.Subtract(n.Node.Status.Capacity.DeepCopy(), n.Node.Status.Allocatable),
				TotalPodRequests:    nodePodRequests,
				TotalPodLimits:      n.PodLimits(),
				TotalDaemonRequests: n.DaemonSetRequests(),
				TotalDaemonLimits:   n.DaemonSetLimits(),
				Allocatable:         n.Node.Status.Allocatable.DeepCopy(),
			},
		})
	}
	return nodes, allocatable, podRequests
}

func buildClusterUtilizationMetric(allocatableAggregate, utilizedAggregate corev1.ResourceList) []*metrics.StoreMetric {
	res := make([]*metrics.StoreMetric, 0, len(allocatableAggregate))

	for resourceName, allocatableResource := range allocatableAggregate {

		if allocatableResource.Value() == 0 {
			// This zero check may be unnecessary. I'm erring towards caution.
			continue
		}

		utilizedResource := utilizedAggregate[resourceName]

		// Typecast to float before the calculation to maximize resolution
		utilizationPercentage := 100 * lo.Ternary(
			resourceName == corev1.ResourceCPU,
			float64(utilizedResource.MilliValue())/float64(allocatableResource.MilliValue()),
			float64(utilizedResource.Value())/float64(allocatableResource.Value()))

		res = append(res, &metrics.StoreMetric{
			GaugeMetric: ClusterUtilization,
			Value:       utilizationPercentage,
			Labels:      map[string]string{metrics.ResourceTypeLabel: resourceNameToString(resourceName)},
		})
	}

	return res
}

func buildMetrics(n nodeMetricsState) (res []*metrics.StoreMetric) {
	// Every series of a node shares the same node labels, so they are built once and the per-resource labels derived
	for gaugeMetric, resourceList := range n.resources {
		for resourceName, quantity := range resourceList {
			res = append(res, &metrics.StoreMetric{
				GaugeMetric: gaugeMetric,
				Value:       lo.Ternary(resourceName == corev1.ResourceCPU, float64(quantity.MilliValue())/float64(1000), float64(quantity.Value())),
				Labels:      withResourceType(n.labels, resourceNameToString(resourceName)),
			})
		}
	}
	return append(res,
		&metrics.StoreMetric{
			GaugeMetric: Lifetime,
			Value:       time.Since(n.creationTimestamp).Seconds(),
			Labels:      n.labels,
		})
}

// withResourceType returns a copy of the node labels with the resource type label added
func withResourceType(nodeLabels prometheus.Labels, resourceTypeName string) prometheus.Labels {
	metricLabels := make(prometheus.Labels, len(nodeLabels)+1)
	maps.Copy(metricLabels, nodeLabels)
	metricLabels[metrics.ResourceTypeLabel] = resourceTypeName
	return metricLabels
}

func getNodeLabels(n *state.StateNode, wellKnownLabels map[string]string) prometheus.Labels {
	node := n.Node
	metricLabels := make(prometheus.Labels, len(wellKnownLabels)+3)
	metricLabels[nodeName] = node.Name
	metricLabels[nodePhase] = string(node.Status.Phase)
	metricLabels[managed] = strconv.FormatBool(n.Managed())

	// Populate well known labels
	for wellKnownLabel, label := range wellKnownLabels {
		metricLabels[label] = node.Labels[wellKnownLabel]
	}
	return metricLabels
}

func getWellKnownLabels() map[string]string {
	labels := make(map[string]string)
	for wellKnownLabel := range v1.WellKnownLabels {
		if parts := strings.Split(wellKnownLabel, "/"); len(parts) == 2 {
			label := parts[1]
			// Reformat label names to be consistent with Prometheus naming conventions (snake_case)
			label = strings.ReplaceAll(strings.ToLower(label), "-", "_")
			labels[wellKnownLabel] = label
		}
	}
	return labels
}

func resourceNameToString(resourceName corev1.ResourceName) string {
	return strings.ReplaceAll(strings.ToLower(string(resourceName)), "-", "_")
}
