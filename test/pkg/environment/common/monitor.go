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

package common

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// Monitor is used to monitor the cluster state during a running test
type Monitor struct {
	ctx        context.Context
	kubeClient client.Client

	mu sync.RWMutex

	nodesAtReset map[string]*corev1.Node
	// restartsAtReset snapshots each pod's container restart counts at the last Reset()
	restartsAtReset map[types.UID]map[string]int32 // pod UID -> container name -> restart count
}

type state struct {
	pods         corev1.PodList
	nodes        map[string]*corev1.Node        // node name -> node
	nodePods     map[string][]*corev1.Pod       // node name -> pods bound to the node
	nodeRequests map[string]corev1.ResourceList // node name -> sum of pod resource requests
}

func NewMonitor(ctx context.Context, kubeClient client.Client) *Monitor {
	m := &Monitor{
		ctx:             ctx,
		kubeClient:      kubeClient,
		nodesAtReset:    map[string]*corev1.Node{},
		restartsAtReset: map[types.UID]map[string]int32{},
	}
	m.Reset()
	return m
}

// Reset resets the cluster monitor prior to running a test.
func (m *Monitor) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	st := m.poll()
	m.nodesAtReset = deepCopyMap(st.nodes)
	m.restartsAtReset = map[types.UID]map[string]int32{}
	for _, pod := range st.pods.Items {
		containers := map[string]int32{}
		for _, cs := range pod.Status.ContainerStatuses {
			containers[cs.Name] = cs.RestartCount
		}
		m.restartsAtReset[pod.UID] = containers
	}
}

// RestartCount returns the containers and number of restarts observed since the last Reset() for that container
// for all containers in the pods in the given namespace.
func (m *Monitor) RestartCount(namespace string) map[string]int {
	st := m.poll()

	m.mu.RLock()
	defer m.mu.RUnlock()
	restarts := map[string]int{}
	for _, pod := range st.pods.Items {
		if pod.Namespace != namespace {
			continue
		}
		// A pod with no baseline (UID absent) is new since the lastReset(); its counts are
		// diffed against a zero value.
		baseline := m.restartsAtReset[pod.UID]
		for _, cs := range pod.Status.ContainerStatuses {
			if delta := int(cs.RestartCount - baseline[cs.Name]); delta > 0 {
				restarts[fmt.Sprintf("%s/%s", pod.Name, cs.Name)] = delta
			}
		}
	}
	return restarts
}

// NodeCount returns the current number of nodes
func (m *Monitor) NodeCount() int {
	return len(m.poll().nodes)
}

// NodeCountAtReset returns the number of nodes that were running when the monitor was last reset, typically at the
// beginning of a test
func (m *Monitor) NodeCountAtReset() int {
	return len(m.NodesAtReset())
}

// CreatedNodeCount returns the number of nodes created since the last reset
func (m *Monitor) CreatedNodeCount() int {
	return m.NodeCount() - m.NodeCountAtReset()
}

// NodesAtReset returns a slice of nodes that the monitor saw at the last reset
func (m *Monitor) NodesAtReset() []*corev1.Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return deepCopySlice(lo.Values(m.nodesAtReset))
}

// Nodes returns all the nodes on the cluster
func (m *Monitor) Nodes() []*corev1.Node {
	st := m.poll()
	return lo.Values(st.nodes)
}

// CreatedNodes returns the nodes that have been created since the last reset (essentially Nodes - NodesAtReset)
func (m *Monitor) CreatedNodes() []*corev1.Node {
	resetNodeNames := sets.NewString(lo.Map(m.NodesAtReset(), func(n *corev1.Node, _ int) string { return n.Name })...)
	return lo.Filter(m.Nodes(), func(n *corev1.Node, _ int) bool { return !resetNodeNames.Has(n.Name) })
}

// DeletedNodes returns the nodes that have been deleted since the last reset (essentially NodesAtReset - Nodes)
func (m *Monitor) DeletedNodes() []*corev1.Node {
	currentNodeNames := sets.NewString(lo.Map(m.Nodes(), func(n *corev1.Node, _ int) string { return n.Name })...)
	return lo.Filter(m.NodesAtReset(), func(n *corev1.Node, _ int) bool { return !currentNodeNames.Has(n.Name) })
}

// PendingPods returns the number of pending pods matching the given selector
func (m *Monitor) PendingPods(selector labels.Selector) []*corev1.Pod {
	var pods []*corev1.Pod
	for _, pod := range m.poll().pods.Items {
		if pod.Status.Phase != corev1.PodPending {
			continue
		}
		if selector.Matches(labels.Set(pod.Labels)) {
			pods = append(pods, &pod)
		}
	}
	return pods
}

func (m *Monitor) PendingPodsCount(selector labels.Selector) int {
	return len(m.PendingPods(selector))
}

// RunningPods returns the number of running pods matching the given selector
func (m *Monitor) RunningPods(selector labels.Selector) []*corev1.Pod {
	var pods []*corev1.Pod
	for _, pod := range m.poll().pods.Items {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		if selector.Matches(labels.Set(pod.Labels)) {
			pods = append(pods, &pod)
		}
	}
	return pods
}

func (m *Monitor) RunningPodsCount(selector labels.Selector) int {
	return len(m.RunningPods(selector))
}

func (m *Monitor) poll() state {
	var nodes corev1.NodeList
	if err := m.kubeClient.List(m.ctx, &nodes); err != nil {
		log.FromContext(m.ctx).Error(err, "failed listing nodes")
	}
	var pods corev1.PodList
	if err := m.kubeClient.List(m.ctx, &pods); err != nil {
		log.FromContext(m.ctx).Error(err, "failing listing pods")
	}
	st := state{
		nodes:        map[string]*corev1.Node{},
		pods:         pods,
		nodePods:     map[string][]*corev1.Pod{},
		nodeRequests: map[string]corev1.ResourceList{},
	}
	for i := range nodes.Items {
		st.nodes[nodes.Items[i].Name] = &nodes.Items[i]
	}
	// collect pods per node
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Spec.NodeName == "" {
			continue
		}
		st.nodePods[pod.Spec.NodeName] = append(st.nodePods[pod.Spec.NodeName], pod)
	}

	for _, n := range nodes.Items {
		st.nodeRequests[n.Name] = resources.RequestsForPods(st.nodePods[n.Name]...)
	}
	return st
}

// AvgUtilization returns the unweighted mean, over Karpenter-launched nodes, of each node's
// requested/allocatable ratio for the resource. Nodes without workload pods count as (near) zero, so
// the result also moves with how many empty nodes exist at the moment it is sampled; see
// AvgNonEmptyUtilization and EmptyNodeCount.
func (m *Monitor) AvgUtilization(resource corev1.ResourceName) float64 {
	utilization := nodeUtilization(m.poll(), resource, false)
	return lo.Sum(utilization) / float64(len(utilization))
}

// AvgNonEmptyUtilization is AvgUtilization over only the Karpenter-launched nodes that are running at
// least one workload pod (see hasWorkloadPods). Requests of every pod bound to those nodes, including
// DaemonSet pods, still count. Once the expected pods are running this depends only on how those pods
// were packed onto nodes, not on how many empty nodes have registered or have already been
// consolidated away when it is sampled. It returns 0 when no launched node runs a workload pod.
func (m *Monitor) AvgNonEmptyUtilization(resource corev1.ResourceName) float64 {
	utilization := nodeUtilization(m.poll(), resource, true)
	if len(utilization) == 0 {
		return 0
	}
	return lo.Sum(utilization) / float64(len(utilization))
}

// EmptyNodeCount returns the number of Karpenter-launched nodes that are not running any workload pod
// (see hasWorkloadPods), e.g. nodes launched for pods that were bound elsewhere, or nodes vacated by a
// scale-down that consolidation has not yet removed.
func (m *Monitor) EmptyNodeCount() int {
	st := m.poll()
	return len(lo.Filter(lo.Values(st.nodes), func(n *corev1.Node, _ int) bool {
		return isLaunched(n) && !hasWorkloadPods(st, n.Name)
	}))
}

func (m *Monitor) MinUtilization(resource corev1.ResourceName) float64 {
	min := math.MaxFloat64
	for _, v := range nodeUtilization(m.poll(), resource, false) {
		min = math.Min(v, min)
	}
	return min
}

// nodeUtilization returns requested/allocatable for the resource on each Karpenter-launched node with
// allocatable for it. If nonEmptyOnly is set, nodes without workload pods are skipped.
func nodeUtilization(st state, resource corev1.ResourceName, nonEmptyOnly bool) []float64 {
	var utilization []float64
	for nodeName, requests := range st.nodeRequests {
		node := st.nodes[nodeName]
		allocatable := node.Status.Allocatable[resource]
		// skip any nodes we didn't launch
		if !isLaunched(node) || allocatable.IsZero() {
			continue
		}
		if nonEmptyOnly && !hasWorkloadPods(st, nodeName) {
			continue
		}
		requested := requests[resource]
		utilization = append(utilization, requested.AsApproximateFloat64()/allocatable.AsApproximateFloat64())
	}
	return utilization
}

func isLaunched(node *corev1.Node) bool {
	return node.Labels[v1.NodePoolLabelKey] != ""
}

// hasWorkloadPods reports whether the node is running a pod other than per-node infrastructure:
// DaemonSet and static pods run on every node regardless of the workload, and terminal pods hold no
// capacity.
func hasWorkloadPods(st state, nodeName string) bool {
	return lo.ContainsBy(st.nodePods[nodeName], func(p *corev1.Pod) bool {
		return !podutils.IsTerminal(p) && !podutils.IsOwnedByDaemonSet(p) && !podutils.IsOwnedByNode(p)
	})
}

type copyable[T any] interface {
	DeepCopy() T
}

func deepCopyMap[K comparable, V copyable[V]](m map[K]V) map[K]V {
	ret := map[K]V{}
	for k, v := range m {
		ret[k] = v.DeepCopy()
	}
	return ret
}

func deepCopySlice[T copyable[T]](s []T) []T {
	var ret []T
	for _, elem := range s {
		ret = append(ret, elem.DeepCopy())
	}
	return ret
}
