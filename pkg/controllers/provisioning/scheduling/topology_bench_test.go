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
	"math/rand"
	"strings"
	"testing"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/utils/clock"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	pscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
)

func BenchmarkNewTopology(b *testing.B) {
	for _, fixture := range []struct {
		name          string
		nodePools     func(count int) []*v1.NodePool
		instanceTypes []*cloudprovider.InstanceType
	}{
		{"", benchNodePools, fake.InstanceTypes(400)},
		{"untainted/", untainted(benchNodePools), fake.InstanceTypes(400)},
		{"aws/", awsLikeNodePools, awsLikeInstanceTypes(800)},
		{"aws-untainted/", untainted(awsLikeNodePools), awsLikeInstanceTypes(800)},
	} {
		for _, np := range []int{1, 5, 20, 50} {
			b.Run(fmt.Sprintf("%svector=nodepools/np=%d", fixture.name, np), func(b *testing.B) {
				benchmarkNewTopology(b, fixture.nodePools(np), fixture.instanceTypes)
			})
		}
	}
}

func benchmarkNewTopology(b *testing.B, nodePools []*v1.NodePool, instanceTypes []*cloudprovider.InstanceType) {
	ctx := benchCtx()
	pods := makeDiversePods(1000)

	cp := fake.NewCloudProvider()
	cp.InstanceTypes = instanceTypes

	client := fakecr.NewFakeClient()
	clk := &clock.RealClock{}
	cl := state.NewCluster(clk, client, cp)

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
}

func untainted(nodePools func(count int) []*v1.NodePool) func(count int) []*v1.NodePool {
	return func(count int) []*v1.NodePool {
		nps := nodePools(count)
		for _, np := range nps {
			np.Spec.Template.Spec.Taints = nil
		}
		return nps
	}
}

const awsLabelPrefix = "karpenter.k8s.aws/"

var awsLikeZones = []string{"us-west-2a", "us-west-2b", "us-west-2c", "us-west-2d"}

// awsLikeNodePools returns tainted NodePools shaped like typical AWS provider NodePools: capacity type, architecture,
// instance category and generation requirements, a team label, and a zone restriction on a third of them.
func awsLikeNodePools(count int) []*v1.NodePool {
	nps := benchNodePools(count)
	for i, np := range nps {
		np.Spec.Template.Labels = map[string]string{"team": fmt.Sprintf("team-%d", i%7)}
		np.Spec.Template.Spec.Requirements = []v1.NodeSelectorRequirementWithMinValues{
			{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeSpot, v1.CapacityTypeOnDemand}},
			{Key: corev1.LabelArchStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"amd64", "arm64"}[:1+i%2]},
			{Key: awsLabelPrefix + "instance-category", Operator: corev1.NodeSelectorOpIn, Values: []string{"c", "m", "r"}},
			{Key: awsLabelPrefix + "instance-generation", Operator: corev1.NodeSelectorOpGt, Values: []string{"4"}},
		}
		if i%3 == 1 {
			np.Spec.Template.Spec.Requirements = append(np.Spec.Template.Spec.Requirements,
				v1.NodeSelectorRequirementWithMinValues{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: awsLikeZones[:3]})
		}
	}
	return nps
}

// awsLikeInstanceTypes returns instance types whose requirements have the same keys as the AWS provider's (33 keys,
// with DoesNotExist requirements for the keys that don't apply), most offered in every zone.
func awsLikeInstanceTypes(count int) []*cloudprovider.InstanceType {
	r := rand.New(rand.NewSource(1))
	zoneIDs := []string{"usw2-az1", "usw2-az2", "usw2-az3", "usw2-az4"}
	categories := []string{"c", "m", "r", "t", "i", "d", "x", "z", "g", "p"}
	sizes := []string{"medium", "large", "xlarge", "2xlarge", "4xlarge", "8xlarge", "12xlarge", "16xlarge", "24xlarge", "32xlarge", "48xlarge", "metal"}
	optionalKeys := []string{"instance-local-nvme", "instance-gpu-name", "instance-gpu-manufacturer", "instance-gpu-count", "instance-gpu-memory",
		"instance-accelerator-name", "instance-accelerator-manufacturer", "instance-accelerator-count", "capacity-reservation-type", "capacity-reservation-interruptible"}
	instanceTypes := make([]*cloudprovider.InstanceType, 0, count)
	for i := 0; i < count; i++ {
		category, generation, size := categories[i%len(categories)], 3+(i/len(categories))%7, sizes[(i/70)%len(sizes)]
		family := fmt.Sprintf("%s%d%s", category, generation, []string{"", "a", "g", "i", "d", "n", "gd", "in"}[(i/7)%8])
		name := fmt.Sprintf("%s.%s-%d", family, size, i)
		zones, ids := awsLikeZones, zoneIDs
		if r.Intn(4) == 0 {
			perm := r.Perm(len(awsLikeZones))[:2+r.Intn(2)]
			zones, ids = lo.Map(perm, func(p int, _ int) string { return awsLikeZones[p] }), lo.Map(perm, func(p int, _ int) string { return zoneIDs[p] })
		}
		capacityTypes := []string{v1.CapacityTypeOnDemand, v1.CapacityTypeSpot}
		reservationID := pscheduling.NewRequirement(cloudprovider.ReservationIDLabel, corev1.NodeSelectorOpDoesNotExist)
		if r.Intn(33) == 0 {
			capacityTypes = append(capacityTypes, v1.CapacityTypeReserved)
			reservationID = pscheduling.NewRequirement(cloudprovider.ReservationIDLabel, corev1.NodeSelectorOpIn, fmt.Sprintf("cr-%08x", r.Int31()))
		}
		cpu := 1 << ((i / 70) % len(sizes))
		requirements := pscheduling.NewRequirements(
			pscheduling.NewRequirement(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, name),
			pscheduling.NewRequirement(corev1.LabelArchStable, corev1.NodeSelectorOpIn, lo.Ternary(strings.Contains(family, "g"), "arm64", "amd64")),
			pscheduling.NewRequirement(corev1.LabelOSStable, corev1.NodeSelectorOpIn, "linux", "windows"),
			pscheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zones...),
			pscheduling.NewRequirement(corev1.LabelTopologyRegion, corev1.NodeSelectorOpIn, "us-west-2"),
			pscheduling.NewRequirement(corev1.LabelWindowsBuild, corev1.NodeSelectorOpDoesNotExist),
			pscheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, capacityTypes...),
			reservationID,
			pscheduling.NewRequirement(awsLabelPrefix+"topology-zone-id", corev1.NodeSelectorOpIn, ids...),
			pscheduling.NewRequirement(awsLabelPrefix+"instance-category", corev1.NodeSelectorOpIn, category),
			pscheduling.NewRequirement(awsLabelPrefix+"instance-family", corev1.NodeSelectorOpIn, family),
			pscheduling.NewRequirement(awsLabelPrefix+"instance-generation", corev1.NodeSelectorOpIn, fmt.Sprint(generation)),
			pscheduling.NewRequirement(awsLabelPrefix+"instance-size", corev1.NodeSelectorOpIn, size),
			pscheduling.NewRequirement(awsLabelPrefix+"instance-cpu", corev1.NodeSelectorOpIn, fmt.Sprint(cpu)),
			pscheduling.NewRequirement(awsLabelPrefix+"instance-cpu-manufacturer", corev1.NodeSelectorOpIn, []string{"intel", "amd", "aws"}[r.Intn(3)]),
			pscheduling.NewRequirement(awsLabelPrefix+"instance-cpu-sustained-clock-speed-mhz", corev1.NodeSelectorOpIn, fmt.Sprint(2500+100*r.Intn(10))),
			pscheduling.NewRequirement(awsLabelPrefix+"instance-memory", corev1.NodeSelectorOpIn, fmt.Sprint(cpu*[]int{2048, 4096, 8192}[r.Intn(3)])),
			pscheduling.NewRequirement(awsLabelPrefix+"instance-network-bandwidth", corev1.NodeSelectorOpIn, fmt.Sprint(750*cpu)),
			pscheduling.NewRequirement(awsLabelPrefix+"instance-ebs-bandwidth", corev1.NodeSelectorOpIn, fmt.Sprint(650*cpu)),
			pscheduling.NewRequirement(awsLabelPrefix+"instance-hypervisor", corev1.NodeSelectorOpIn, []string{"nitro", "xen", ""}[r.Intn(3)]),
			pscheduling.NewRequirement(awsLabelPrefix+"instance-encryption-in-transit-supported", corev1.NodeSelectorOpIn, fmt.Sprint(r.Intn(2) == 0)),
			pscheduling.NewRequirement(awsLabelPrefix+"instance-capability-flex", corev1.NodeSelectorOpIn, "false"),
			pscheduling.NewRequirement(awsLabelPrefix+"instance-tenancy", corev1.NodeSelectorOpIn, "default", "dedicated"),
		)
		for _, key := range optionalKeys {
			requirements.Add(pscheduling.NewRequirement(awsLabelPrefix+key, corev1.NodeSelectorOpDoesNotExist))
		}
		instanceTypes = append(instanceTypes, &cloudprovider.InstanceType{Name: name, Requirements: requirements})
	}
	return instanceTypes
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

// BenchmarkTopologyGroups measures the per-pod topology work done for each candidate (AddRequirements) and each
// placement (Record) as the number of topology groups grows. Each Deployment has a zone spread and every 4th also has
// hostname self anti-affinity, so the groups include owned, selecting and inverse anti-affinity groups.
func BenchmarkTopologyGroups(b *testing.B) {
	for _, deployments := range []int{10, 100, 500} {
		topology, pods := benchTopologyGroups(b, deployments)
		podRequirements := pscheduling.NewRequirements()
		nodeRequirements := pscheduling.NewRequirements(
			pscheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "test-zone-1"),
			pscheduling.NewRequirement(corev1.LabelHostname, corev1.NodeSelectorOpIn, "bench-host"),
		)
		b.Run(fmt.Sprintf("op=AddRequirements/deployments=%d", deployments), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				if _, err := topology.AddRequirements(pods[i%len(pods)], nil, podRequirements, nodeRequirements); err != nil {
					b.Fatalf("adding requirements: %s", err)
				}
			}
		})
		b.Run(fmt.Sprintf("op=Record/deployments=%d", deployments), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				topology.Record(pods[i%len(pods)], nil, nodeRequirements)
			}
		})
	}
}

func benchTopologyGroups(b *testing.B, deployments int) (*scheduling.Topology, []*corev1.Pod) {
	ctx := benchCtx()
	cp := fake.NewCloudProvider()
	client := fakecr.NewFakeClient()
	cl := state.NewCluster(&clock.RealClock{}, client, cp)
	nodePools := benchNodePools(1)
	itsByNP := map[string][]*cloudprovider.InstanceType{nodePools[0].Name: fake.InstanceTypes(10)}

	var pods []*corev1.Pod
	for d := 0; d < deployments; d++ {
		labels := map[string]string{"app": fmt.Sprintf("deployment-%d", d)}
		opts := test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: labels, UID: uuid.NewUUID()},
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
				MaxSkew:           1,
				TopologyKey:       corev1.LabelTopologyZone,
				WhenUnsatisfiable: corev1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
			}},
		}
		if d%4 == 0 {
			opts.PodAntiRequirements = []corev1.PodAffinityTerm{{
				TopologyKey:   corev1.LabelHostname,
				LabelSelector: &metav1.LabelSelector{MatchLabels: labels},
			}}
		}
		pods = append(pods, test.Pod(opts))
	}
	topology, err := scheduling.NewTopology(ctx, client, cl, nil, nodePools, itsByNP, pods)
	if err != nil {
		b.Fatalf("creating topology: %s", err)
	}
	return topology, pods
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

