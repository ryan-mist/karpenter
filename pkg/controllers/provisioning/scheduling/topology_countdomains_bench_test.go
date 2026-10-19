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
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/test"
)

const (
	countDomainsBoundPodsPerWorkload = 10
	countDomainsClassLabel           = "bench.example.com/class"
	countDomainsTaintKey             = "bench.example.com/dedicated"
)

// BenchmarkNewTopologyCountDomains measures NewTopology with many existing nodes, where countDomains runs once per
// distinct TopologyGroup. Each workload is one pending pod with a zone and a hostname topology spread (two groups),
// plus a handful of bound pods in its own namespace. Workloads rotate between a nodeSelector filter, a taint-honoring
// filter, and no filter, so the node filter is exercised on every node for every group.
func BenchmarkNewTopologyCountDomains(b *testing.B) {
	for _, n := range []int{1000, 5000, 15000} {
		b.Run(fmt.Sprintf("vector=nodes/n=%d", n), func(b *testing.B) {
			benchmarkNewTopologyCountDomains(b, n, 100)
		})
	}
	for _, w := range []int{10, 100} {
		b.Run(fmt.Sprintf("vector=workloads/w=%d", w), func(b *testing.B) {
			benchmarkNewTopologyCountDomains(b, 15000, w)
		})
	}
}

type countDomainsFixture struct {
	client     client.Client
	cluster    *state.Cluster
	stateNodes []*state.StateNode
	nodePools  []*v1.NodePool
	itsByNP    map[string][]*cloudprovider.InstanceType
	pods       []*corev1.Pod
}

var countDomainsFixtures = map[[2]int]*countDomainsFixture{}

func benchmarkNewTopologyCountDomains(b *testing.B, nodes, workloads int) {
	ctx := benchCtx()
	f := buildCountDomainsFixture(b, nodes, workloads)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := scheduling.NewTopology(ctx, f.client, f.cluster, f.stateNodes, f.nodePools, f.itsByNP, f.pods); err != nil {
			b.Fatalf("creating topology: %s", err)
		}
	}
}

func buildCountDomainsFixture(b *testing.B, nodes, workloads int) *countDomainsFixture {
	b.Helper()
	if f, ok := countDomainsFixtures[[2]int{nodes, workloads}]; ok {
		return f
	}
	ctx := benchCtx()
	cp := fake.NewCloudProvider()
	instanceTypes := fake.InstanceTypes(20)
	cp.InstanceTypes = instanceTypes
	c := fakecr.NewFakeClient()
	cl := state.NewCluster(&clock.RealClock{}, c, cp)
	nodePools := benchNodePools(1)
	itsByNP := map[string][]*cloudprovider.InstanceType{nodePools[0].Name: instanceTypes}

	stateNodes := benchStateNodes(nodes, nodePools)
	for i, sn := range stateNodes {
		sn.Node.Labels[corev1.LabelTopologyZone] = fmt.Sprintf("test-zone-%d", i%3+1)
		sn.Node.Labels[countDomainsClassLabel] = fmt.Sprintf("class-%d", i%4)
		if i%10 == 0 {
			sn.Node.Spec.Taints = []corev1.Taint{{Key: countDomainsTaintKey, Value: "true", Effect: corev1.TaintEffectNoSchedule}}
		}
		if err := c.Create(ctx, sn.Node); err != nil {
			b.Fatalf("creating node: %s", err)
		}
	}

	var pending []*corev1.Pod
	for w := 0; w < workloads; w++ {
		ns := fmt.Sprintf("bench-ns-%d", w)
		labels := map[string]string{"app": fmt.Sprintf("bench-app-%d", w)}
		opts := test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Labels: labels, UID: uuid.NewUUID()},
			TopologySpreadConstraints: lo.Map([]string{corev1.LabelTopologyZone, corev1.LabelHostname}, func(key string, _ int) corev1.TopologySpreadConstraint {
				return corev1.TopologySpreadConstraint{
					MaxSkew:           1,
					TopologyKey:       key,
					WhenUnsatisfiable: corev1.DoNotSchedule,
					LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				}
			}),
		}
		switch w % 3 {
		case 0:
			opts.NodeSelector = map[string]string{countDomainsClassLabel: fmt.Sprintf("class-%d", w%4)}
		case 1:
			opts.Tolerations = []corev1.Toleration{{Key: countDomainsTaintKey, Operator: corev1.TolerationOpExists}}
			for i := range opts.TopologySpreadConstraints {
				opts.TopologySpreadConstraints[i].NodeTaintsPolicy = lo.ToPtr(corev1.NodeInclusionPolicyHonor)
			}
		}
		pending = append(pending, test.UnschedulablePod(opts))

		for j := 0; j < countDomainsBoundPodsPerWorkload; j++ {
			bound := test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Labels: labels, UID: uuid.NewUUID()},
				NodeName:   stateNodes[(w*countDomainsBoundPodsPerWorkload+j)%nodes].Node.Name,
				Phase:      corev1.PodRunning,
			})
			if err := c.Create(ctx, bound); err != nil {
				b.Fatalf("creating pod: %s", err)
			}
		}
	}
	f := &countDomainsFixture{client: c, cluster: cl, stateNodes: stateNodes, nodePools: nodePools, itsByNP: itsByNP, pods: pending}
	countDomainsFixtures[[2]int{nodes, workloads}] = f
	return f
}
