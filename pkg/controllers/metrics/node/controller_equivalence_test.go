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
	"bytes"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// referenceController emits the node metrics the way the controller did before its label and cluster state reads
// were optimized, into its own registry, so that the output of both can be compared.
type referenceController struct {
	cluster     *state.Cluster
	metricStore *metrics.Store
	gauges      map[opmetrics.GaugeMetric]opmetrics.GaugeMetric
}

func newReferenceController(cluster *state.Cluster) (*referenceController, *prometheus.Registry) {
	registry := prometheus.NewRegistry()
	nodeLabels := append(lo.Map(sets.New(lo.Values(referenceWellKnownLabels())...).UnsortedList(), func(l string, _ int) opmetrics.Label {
		return opmetrics.Label{Name: l}
	}), nodeNameLabel, nodePhaseLabel, managedLabel)
	gauge := func(subsystem, name string, labels []opmetrics.Label) opmetrics.GaugeMetric {
		return opmetrics.NewPrometheusGauge(registry, prometheus.GaugeOpts{Namespace: metrics.Namespace, Subsystem: subsystem, Name: name, Help: name}, labels, opmetrics.Beta)
	}
	withResourceType := append(lo.Clone(nodeLabels), metrics.ResourceType)
	return &referenceController{
		cluster:     cluster,
		metricStore: metrics.NewStore(),
		gauges: map[opmetrics.GaugeMetric]opmetrics.GaugeMetric{
			Allocatable:         gauge(metrics.NodeSubsystem, "allocatable", withResourceType),
			TotalPodRequests:    gauge(metrics.NodeSubsystem, "total_pod_requests", withResourceType),
			TotalPodLimits:      gauge(metrics.NodeSubsystem, "total_pod_limits", withResourceType),
			TotalDaemonRequests: gauge(metrics.NodeSubsystem, "total_daemon_requests", withResourceType),
			TotalDaemonLimits:   gauge(metrics.NodeSubsystem, "total_daemon_limits", withResourceType),
			SystemOverhead:      gauge(metrics.NodeSubsystem, "system_overhead", withResourceType),
			Lifetime:            gauge(metrics.NodeSubsystem, "current_lifetime_seconds", nodeLabels),
			ClusterUtilization:  gauge("cluster", "utilization_percent", []opmetrics.Label{metrics.ResourceType}),
		},
	}, registry
}

func (c *referenceController) Reconcile() {
	nodes := lo.Reject(c.cluster.DeepCopyNodes(), func(n *state.StateNode, _ int) bool {
		return n.Node == nil
	})
	metricsMap := lo.SliceToMap(nodes, func(n *state.StateNode) (string, []*metrics.StoreMetric) {
		return client.ObjectKeyFromObject(n.Node).String(), referenceBuildMetrics(n)
	})
	metricsMap["clusterUtilization"] = referenceBuildClusterUtilizationMetric(nodes)
	for _, storeMetrics := range metricsMap {
		for _, m := range storeMetrics {
			m.GaugeMetric = c.gauges[m.GaugeMetric]
		}
	}
	c.metricStore.ReplaceAll(metricsMap)
}

func referenceBuildClusterUtilizationMetric(nodes state.StateNodes) []*metrics.StoreMetric {
	allocatableAggregate, utilizedAggregate := corev1.ResourceList{}, corev1.ResourceList{}
	for _, node := range nodes {
		resources.MergeInto(allocatableAggregate, node.Allocatable())
		resources.MergeInto(utilizedAggregate, node.PodRequests())
	}
	res := make([]*metrics.StoreMetric, 0, len(allocatableAggregate))
	for resourceName, allocatableResource := range allocatableAggregate {
		if allocatableResource.Value() == 0 {
			continue
		}
		utilizedResource := utilizedAggregate[resourceName]
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

func referenceBuildMetrics(n *state.StateNode) (res []*metrics.StoreMetric) {
	for gaugeMetric, resourceList := range map[opmetrics.GaugeMetric]corev1.ResourceList{
		SystemOverhead:      resources.Subtract(n.Node.Status.Capacity, n.Node.Status.Allocatable),
		TotalPodRequests:    n.PodRequests(),
		TotalPodLimits:      n.PodLimits(),
		TotalDaemonRequests: n.DaemonSetRequests(),
		TotalDaemonLimits:   n.DaemonSetLimits(),
		Allocatable:         n.Node.Status.Allocatable,
	} {
		for resourceName, quantity := range resourceList {
			metricLabels := referenceNodeLabels(n)
			metricLabels[metrics.ResourceTypeLabel] = resourceNameToString(resourceName)
			res = append(res, &metrics.StoreMetric{
				GaugeMetric: gaugeMetric,
				Value:       lo.Ternary(resourceName == corev1.ResourceCPU, float64(quantity.MilliValue())/float64(1000), float64(quantity.Value())),
				Labels:      metricLabels,
			})
		}
	}
	return append(res, &metrics.StoreMetric{
		GaugeMetric: Lifetime,
		Value:       time.Since(n.Node.GetCreationTimestamp().Time).Seconds(),
		Labels:      referenceNodeLabels(n),
	})
}

func referenceNodeLabels(n *state.StateNode) prometheus.Labels {
	metricLabels := map[string]string{}
	metricLabels[nodeName] = n.Node.Name
	metricLabels[nodePhase] = string(n.Node.Status.Phase)
	metricLabels[managed] = strconv.FormatBool(n.Managed())
	for wellKnownLabel, label := range referenceWellKnownLabels() {
		metricLabels[label] = n.Node.Labels[wellKnownLabel]
	}
	return metricLabels
}

func referenceWellKnownLabels() map[string]string {
	labels := make(map[string]string)
	for wellKnownLabel := range v1.WellKnownLabels {
		if parts := strings.Split(wellKnownLabel, "/"); len(parts) == 2 {
			labels[wellKnownLabel] = strings.ReplaceAll(strings.ToLower(parts[1]), "-", "_")
		}
	}
	return labels
}

var lifetimeSample = regexp.MustCompile(`(?m)^(karpenter_nodes_current_lifetime_seconds\{.*\}) \S+$`)

// gather returns the registry's samples in the text exposition format, without HELP and TYPE lines. Node lifetimes
// depend on when they are computed, so they are only checked to be set.
func gather(t *testing.T, registry *prometheus.Registry) string {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	for _, family := range families {
		if err := expfmt.NewEncoder(&buf, expfmt.NewFormat(expfmt.TypeTextPlain)).Encode(family); err != nil {
			t.Fatal(err)
		}
	}
	for _, sample := range lifetimeSample.FindAllString(buf.String(), -1) {
		if lifetime, err := strconv.ParseFloat(sample[strings.LastIndex(sample, " ")+1:], 64); err != nil || lifetime < 60 {
			t.Fatalf("unexpected node lifetime sample %q", sample)
		}
	}
	return strings.Join(lo.Reject(strings.Split(lifetimeSample.ReplaceAllString(buf.String(), "$1 <lifetime>"), "\n"), func(line string, _ int) bool {
		return strings.HasPrefix(line, "#")
	}), "\n")
}

// TestReconcileMatchesReference checks that the controller emits exactly the series (names, labels and values) of the
// reference implementation, and deletes the same ones, across a sequence of cluster state changes.
func TestReconcileMatchesReference(t *testing.T) {
	ctx := testContext()
	cluster := newTestCluster()
	controller, registry := newIsolatedController(cluster)
	reference, referenceRegistry := newReferenceController(cluster)

	reconcile := func(step string, minSeries int) {
		t.Helper()
		if _, err := controller.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		reference.Reconcile()
		got, want := gather(t, registry), gather(t, referenceRegistry)
		if got != want {
			t.Fatalf("%s: emitted metrics differ from the reference\n--- got\n%s\n--- want\n%s", step, got, want)
		}
		if series := strings.Count(got, "\n"); series < minSeries {
			t.Fatalf("%s: expected at least %d series, got %d", step, minSeries, series)
		}
	}

	// A varied cluster: managed and unmanaged nodes over several instance types, NodePools and zones
	var nodes []*corev1.Node
	var nodeClaims []*v1.NodeClaim
	for i := range 12 {
		node, nodeClaim := testNode(i, i%4 != 3)
		nodes, nodeClaims = append(nodes, node), append(nodeClaims, nodeClaim)
	}
	// Missing labels, including some of the AWS ones and the NodePool's capacity type
	for _, key := range []string{corev1.LabelTopologyZone, "topology.k8s.aws/zone-id", "karpenter.k8s.aws/instance-family", v1.CapacityTypeLabelKey} {
		delete(nodes[1].Labels, key)
	}
	nodes[2].Labels["karpenter.k8s.aws/capacity-reservation-id"] = "cr-0123456789"
	nodes[2].Labels["karpenter.k8s.aws/capacity-reservation-type"] = "default"
	nodes[2].Labels["karpenter.k8s.aws/placement-group-id"] = "pg-0123456789"
	nodes[2].Labels["karpenter.k8s.aws/placement-group-partition"] = "2"
	nodes[3].Status.Phase = ""
	nodes[5].Status.Phase = corev1.NodePending
	// A capacity in the decimal (inf.Dec) form, which arithmetic on a shared Quantity would mutate in place
	nodes[4].Status.Capacity = nodes[4].Status.Capacity.DeepCopy()
	decimalMemory := resource.MustParse("32Gi")
	nodes[4].Status.Capacity[corev1.ResourceMemory] = *decimalMemory.ToDec()
	// An uninitialized managed node whose zero allocatable is reported from its NodeClaim for cluster utilization
	delete(nodes[6].Labels, v1.NodeInitializedLabelKey)
	nodes[6].Status.Allocatable = nodes[6].Status.Allocatable.DeepCopy()
	nodes[6].Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("0")
	for i, node := range nodes {
		addNode(ctx, t, cluster, node, nodeClaims[i], testPods(node, i%5, i%3))
	}
	// A launched NodeClaim without a Node has no node metrics
	_, launched := testNode(100, true)
	cluster.UpdateNodeClaim(launched)

	reconcile("initial", 12*20)
	reconcile("unchanged", 12*20)

	// Pods come and go, a node changes instance type and labels, one loses its NodeClaim, one is deleted, one is added
	for _, pod := range testPods(nodes[0], 8, 3)[5:] {
		if err := cluster.UpdatePod(ctx, pod); err != nil {
			t.Fatal(err)
		}
	}
	cluster.DeletePod(types.NamespacedName{Namespace: "default", Name: nodes[4].Name + "-pod-0"})
	completed := testPods(nodes[7], 3, 0)[2]
	completed.Status.Phase = corev1.PodSucceeded
	if err := cluster.UpdatePod(ctx, completed); err != nil {
		t.Fatal(err)
	}
	relabelled := nodes[8].DeepCopy()
	for k, v := range testInstanceTypes[2].labels {
		relabelled.Labels[k] = v
	}
	relabelled.Labels[corev1.LabelInstanceTypeStable] = testInstanceTypes[2].name
	delete(relabelled.Labels, corev1.LabelTopologyZone)
	relabelled.Status.Phase = corev1.NodePending
	if err := cluster.UpdateNode(ctx, relabelled); err != nil {
		t.Fatal(err)
	}
	cluster.DeleteNodeClaim(nodeClaims[9].Name)
	cluster.DeleteNode(nodes[10].Name)
	added, addedNodeClaim := testNode(12, true)
	addNode(ctx, t, cluster, added, addedNodeClaim, testPods(added, 4, 1))

	reconcile("updated", 12*20)
	reconcile("unchanged after update", 12*20)
}
