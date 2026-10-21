//go:build test_performance

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

package scheduling_test

import (
	"fmt"
	"testing"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/utils/clock"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/topoindex"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

// BenchmarkNewTopologyIndex measures NewTopology for a disruption-style simulation (each workload's pending pod, plus
// the bound pods of one node being excluded) with existing pods counted by listing them (source=list) or from cluster
// state's topology index (source=index, overlay engine only: the legacy topology always lists). Each workload has a zone and a hostname spread.
func BenchmarkNewTopologyIndex(b *testing.B) {
	for _, size := range []struct{ nodes, workloads, podsPerWorkload int }{{1000, 100, 30}, {15000, 500, 300}} {
		f := buildIndexBenchFixture(b, size.nodes, size.workloads, size.podsPerWorkload)
		for _, engine := range []string{"legacy", "overlay"} {
			for _, source := range []string{"list", "index"} {
				// the legacy topology always lists pods
				if engine == "legacy" && source == "index" {
					continue
				}
				cluster := lo.Ternary(source == "index", f.indexed, f.cluster)
				name := fmt.Sprintf("pods=%d/groups=%d/engine=%s/source=%s", size.workloads*size.podsPerWorkload, 2*size.workloads, engine, source)
				b.Run(name, func(b *testing.B) {
					ctx := benchCtx()
					// the first simulation registers the index's groups
					if _, err := scheduling.NewTopology(ctx, f.client, cluster, f.stateNodes, f.nodePools, f.itsByNP, f.pods, scheduling.TopologyEngine(karpopts.TopologyEngine(engine))); err != nil {
						b.Fatalf("creating topology: %s", err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if _, err := scheduling.NewTopology(ctx, f.client, cluster, f.stateNodes, f.nodePools, f.itsByNP, f.pods, scheduling.TopologyEngine(karpopts.TopologyEngine(engine))); err != nil {
							b.Fatalf("creating topology: %s", err)
						}
					}
				})
			}
		}
	}
}

type indexBenchFixture struct {
	countDomainsFixture
	// indexed is a cluster state with a topology index hydrated from the client
	indexed *state.Cluster
}

func buildIndexBenchFixture(b *testing.B, nodes, workloads, podsPerWorkload int) *indexBenchFixture {
	b.Helper()
	ctx := benchCtx()
	cp := fake.NewCloudProvider()
	instanceTypes := fake.InstanceTypes(20)
	cp.InstanceTypes = instanceTypes
	c := fakecr.NewFakeClient()
	nodePools := benchNodePools(1)
	stateNodes := benchStateNodes(nodes, nodePools)
	for i, sn := range stateNodes {
		sn.Node.Labels[corev1.LabelTopologyZone] = fmt.Sprintf("test-zone-%d", i%3+1)
		if err := c.Create(ctx, sn.Node); err != nil {
			b.Fatalf("creating node: %s", err)
		}
	}
	var pods []*corev1.Pod
	var candidate []*corev1.Pod
	for w := range workloads {
		ns := fmt.Sprintf("bench-ns-%d", w%50)
		labels := map[string]string{"app": fmt.Sprintf("bench-app-%d", w)}
		pods = append(pods, test.UnschedulablePod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Labels: labels, UID: uuid.NewUUID()},
			TopologySpreadConstraints: lo.Map([]string{corev1.LabelTopologyZone, corev1.LabelHostname}, func(key string, _ int) corev1.TopologySpreadConstraint {
				return corev1.TopologySpreadConstraint{MaxSkew: 1, TopologyKey: key, WhenUnsatisfiable: corev1.DoNotSchedule, LabelSelector: &metav1.LabelSelector{MatchLabels: labels}}
			}),
		}))
		for j := range podsPerWorkload {
			node := stateNodes[(w*podsPerWorkload+j)%nodes].Node.Name
			bound := test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Labels: labels, UID: uuid.NewUUID()}, NodeName: node, Phase: corev1.PodRunning})
			if err := c.Create(ctx, bound); err != nil {
				b.Fatalf("creating pod: %s", err)
			}
			if node == stateNodes[0].Node.Name {
				candidate = append(candidate, bound)
			}
		}
	}
	itsByNP := map[string][]*cloudprovider.InstanceType{nodePools[0].Name: instanceTypes}
	f := &indexBenchFixture{countDomainsFixture: countDomainsFixture{client: c, cluster: state.NewCluster(&clock.RealClock{}, c, cp), stateNodes: stateNodes[1:],
		nodePools: nodePools, itsByNP: itsByNP, pods: append(pods, candidate...)}}

	f.indexed = state.NewCluster(&clock.RealClock{}, c, cp)
	index := topoindex.New()
	f.indexed.SetTopologyIndex(index)
	podList, nodeList, namespaceList := &corev1.PodList{}, &corev1.NodeList{}, &corev1.NamespaceList{}
	if err := c.List(ctx, podList); err != nil {
		b.Fatalf("listing pods: %s", err)
	}
	if err := c.List(ctx, nodeList); err != nil {
		b.Fatalf("listing nodes: %s", err)
	}
	if err := c.List(ctx, namespaceList); err != nil {
		b.Fatalf("listing namespaces: %s", err)
	}
	index.Hydrate(lo.ToSlicePtr(podList.Items), lo.ToSlicePtr(nodeList.Items), lo.ToSlicePtr(namespaceList.Items))
	return f
}
