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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/utils/clock"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/test"
)

func BenchmarkNewTopology(b *testing.B) {
	for _, np := range []int{1, 5, 20, 50} {
		b.Run(fmt.Sprintf("vector=nodepools/np=%d", np), func(b *testing.B) {
			ctx := benchCtx()
			pods := makeDiversePods(1000)

			cp := fake.NewCloudProvider()
			instanceTypes := fake.InstanceTypes(400)
			cp.InstanceTypes = instanceTypes

			client := fakecr.NewFakeClient()
			clk := &clock.RealClock{}
			cl := state.NewCluster(clk, client, cp)

			nodePools := benchNodePools(np)
			itsByNP := map[string][]*cloudprovider.InstanceType{}
			for _, pool := range nodePools {
				itsByNP[pool.Name] = instanceTypes
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := scheduling.NewTopology(ctx, client, cl, nil, nodePools, itsByNP, pods); err != nil {
					b.Fatalf("creating topology: %s", err)
				}
			}
		})
	}
}

// BenchmarkNewTopologySharedConstraints models pods that come from a handful of deployments, so pods in the same
// deployment share their topology spread and pod (anti-)affinity terms and dedupe to the same topology groups.
func BenchmarkNewTopologySharedConstraints(b *testing.B) {
	for _, np := range []int{1, 5, 20, 50} {
		b.Run(fmt.Sprintf("vector=nodepools/np=%d", np), func(b *testing.B) {
			ctx := benchCtx()
			pods := makeDeploymentPods(20, 50)

			cp := fake.NewCloudProvider()
			instanceTypes := fake.InstanceTypes(400)
			cp.InstanceTypes = instanceTypes

			client := fakecr.NewFakeClient()
			clk := &clock.RealClock{}
			cl := state.NewCluster(clk, client, cp)

			nodePools := benchNodePools(np)
			itsByNP := map[string][]*cloudprovider.InstanceType{}
			for _, pool := range nodePools {
				itsByNP[pool.Name] = instanceTypes
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := scheduling.NewTopology(ctx, client, cl, nil, nodePools, itsByNP, pods); err != nil {
					b.Fatalf("creating topology: %s", err)
				}
			}
		})
	}
}

// makeDeploymentPods returns deployments*podsPerDeployment pods. Each deployment's pods share a zonal topology spread,
// a zonal pod affinity and a hostname pod anti-affinity, all selecting the deployment's own label.
func makeDeploymentPods(deployments, podsPerDeployment int) []*corev1.Pod {
	var pods []*corev1.Pod
	for d := 0; d < deployments; d++ {
		labels := map[string]string{"app": fmt.Sprintf("deployment-%d", d)}
		for i := 0; i < podsPerDeployment; i++ {
			pods = append(pods, test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					UID:    uuid.NewUUID(),
				},
				TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
					MaxSkew:           1,
					TopologyKey:       corev1.LabelTopologyZone,
					WhenUnsatisfiable: corev1.DoNotSchedule,
					LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				}},
				PodRequirements: []corev1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{MatchLabels: labels},
					TopologyKey:   corev1.LabelTopologyZone,
				}},
				PodAntiRequirements: []corev1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{MatchLabels: labels},
					TopologyKey:   corev1.LabelHostname,
				}},
			}))
		}
	}
	return pods
}

func BenchmarkForEachDomain(b *testing.B) {
	cases := []struct {
		domains     int
		taintGroups int
	}{
		{100, 1}, {400, 1}, {1000, 1}, {400, 5}, {1000, 5},
	}
	// reject maens that pod tolerates no taint; tolerate means that pod tolerates every taint
	for _, mode := range []struct {
		name      string
		tolerates bool
	}{{"reject", false}, {"tolerate", true}} {
		for _, tc := range cases {
			b.Run(fmt.Sprintf("%s/vector=domains/d=%d/t=%d", mode.name, tc.domains, tc.taintGroups), func(b *testing.B) {
				benchmarkForEachDomain(b, tc.domains, tc.taintGroups, mode.tolerates)
			})
		}
	}
}

func benchmarkForEachDomain(b *testing.B, domains, taintGroupsPerDomain int, tolerates bool) {
	dg := scheduling.NewTopologyDomainGroup()
	for d := 0; d < domains; d++ {
		domain := fmt.Sprintf("test-zone-%d", d)
		for t := 0; t < taintGroupsPerDomain; t++ {
			dg.Insert(domain, corev1.Taint{
				Key:    fmt.Sprintf("bench.example.com/taint-%d", t),
				Value:  "true",
				Effect: corev1.TaintEffectNoSchedule,
			})
		}
	}
	pod := test.Pod()
	if tolerates {
		for t := 0; t < taintGroupsPerDomain; t++ {
			pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{
				Key:      fmt.Sprintf("bench.example.com/taint-%d", t),
				Operator: corev1.TolerationOpExists,
			})
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		dg.ForEachDomain(pod, corev1.NodeInclusionPolicyHonor, func(domain string) {
			count++
		})
		_ = count
	}
}
