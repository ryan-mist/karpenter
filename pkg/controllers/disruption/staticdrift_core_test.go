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

package disruption_test

// Differential test of the static drift core (StaticDrift.Compute): on generated clusters of static NodePools,
// StaticDrift.ComputeCommands (the core with the controller's options) and a verbatim copy of ComputeCommands before
// the core existed (disruption.LegacyStaticDriftComputeCommands) produce the same commands, publish the same events
// and reserve the same limits.nodes slots. NodePools are handled in map order, so commands and events are compared as
// multisets. The generator covers budgets, node limits (none, at the node count, above it), NodePools that are still
// scaling down, a NotReady NodePool, full and open capacity reservations, and TerminateFirstDrift on and off. Between
// steps the commands are started and their replacements launched. Seeds as in drift_core_test.go.

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

const staticReservationID = "r-gen-static"

type staticSpec struct {
	Seed           int64  `json:"seed"`
	Pools          int    `json:"pools"`
	Budget         string `json:"budget"`
	DriftFrac      float64
	Limit          string `json:"limit"` // none | at | above
	ScaleDown      bool   `json:"scaleDown"`
	NotReady       bool   `json:"notReady"`
	Reserved       bool   `json:"reserved"`
	ReservationCap int    `json:"reservationCapacity"`
	TerminateFirst bool   `json:"terminateFirst"`
}

func randomStaticSpec(seed int64) staticSpec {
	r := rand.New(rand.NewSource(seed))
	return staticSpec{
		Seed:           seed,
		Pools:          1 + r.Intn(3),
		Budget:         []string{"1", "2", "20%", "50%", "100%"}[r.Intn(5)],
		DriftFrac:      []float64{0.3, 0.6, 1}[r.Intn(3)],
		Limit:          []string{"none", "at", "above"}[r.Intn(3)],
		ScaleDown:      r.Intn(4) == 0,
		NotReady:       r.Intn(4) == 0,
		Reserved:       r.Intn(3) == 0,
		ReservationCap: r.Intn(2),
		TerminateFirst: r.Intn(2) == 0,
	}
}

func staticInstanceTypes(reservationCapacity int) []*cloudprovider.InstanceType {
	its := genInstanceTypes(genZones(1))
	res := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("32Gi"), corev1.ResourcePods: resource.MustParse("110")}
	reserved := fake.NewInstanceType("gen-reserved-8cpu", fake.WithResources(res), fake.WithOfferings(cloudprovider.Offering{
		Available: true, Price: fake.PriceFromResources(res) / 100, ReservationCapacity: reservationCapacity,
		Requirements: scheduling.NewLabelRequirements(map[string]string{
			v1.CapacityTypeLabelKey: v1.CapacityTypeReserved, corev1.LabelTopologyZone: genZones(1)[0], cloudprovider.ReservationIDLabel: staticReservationID,
		}),
	}))
	reserved.Requirements.Add(scheduling.NewRequirement(cloudprovider.ReservationIDLabel, corev1.NodeSelectorOpIn, staticReservationID))
	reserved.Requirements.Get(v1.CapacityTypeLabelKey).Insert(v1.CapacityTypeReserved)
	return append(its, reserved)
}

// buildStatic builds the static NodePools of g, each with its replicas as nodes (one more if it is scaling down).
//
//nolint:gocyclo
func (e *coreEnv) buildStatic(g staticSpec) *genCluster {
	GinkgoHelper()
	r := rand.New(rand.NewSource(g.Seed*7919 + 23))
	zone := genZones(1)[0]
	run := uuid.NewString()[:6]
	gc := &genCluster{spec: genSpec{Seed: g.Seed, Zones: 1}, prefix: "s" + run[:5], nodeIdx: map[string]string{}, nodeZone: map[string]string{}}
	gc.groupSel = lo.Times(genNumGroups, func(i int) string { return fmt.Sprintf("g%d-%s", i, run) })
	rs := test.ReplicaSet()
	ExpectApplied(ctx, env.Client, rs)
	var nodeClaims []*v1.NodeClaim
	var nodes []*corev1.Node
	var pods []*corev1.Pod
	base := env.Clock.Now().Add(-time.Hour)
	for p := range g.Pools {
		replicas := int64(2 + r.Intn(7))
		count := replicas + lo.Ternary(g.ScaleDown && p == 0, int64(1), int64(0))
		reserved := g.Reserved && p == g.Pools-1
		np := test.StaticNodePool(v1.NodePool{Spec: v1.NodePoolSpec{
			Replicas:   lo.ToPtr(replicas),
			Disruption: v1.Disruption{Budgets: []v1.Budget{{Nodes: g.Budget}}},
		}})
		switch g.Limit {
		case "at":
			np.Spec.Limits = v1.Limits{resources.Node: *resource.NewQuantity(count, resource.DecimalSI)}
		case "above":
			np.Spec.Limits = v1.Limits{resources.Node: *resource.NewQuantity(count+int64(1+r.Intn(3)), resource.DecimalSI)}
		}
		itName := fmt.Sprintf("gen-%dcpu", genNodeITCPU)
		capacityType := v1.CapacityTypeOnDemand
		if reserved {
			itName, capacityType = "gen-reserved-8cpu", v1.CapacityTypeReserved
			np.Spec.Template.Spec.Requirements = []v1.NodeSelectorRequirementWithMinValues{
				{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn, Values: []string{itName}},
				{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeReserved}},
			}
		}
		if g.NotReady && p == min(1, g.Pools-1) {
			np.StatusConditions().SetFalse(v1.ConditionTypeValidationSucceeded, "NotReady", "NotReady")
		}
		ExpectApplied(ctx, env.Client, np)
		ExpectReconcileSucceeded(e.ctx, e.npCtrl, client.ObjectKeyFromObject(np))
		gc.pools = append(gc.pools, np)
		it, _ := lo.Find(cloudProvider.InstanceTypes, func(i *cloudprovider.InstanceType) bool { return i.Name == itName })
		for range count {
			idx := len(nodes)
			labels := map[string]string{
				v1.NodePoolLabelKey: np.Name, corev1.LabelInstanceTypeStable: it.Name, v1.CapacityTypeLabelKey: capacityType,
				corev1.LabelTopologyZone: zone, corev1.LabelArchStable: "amd64", corev1.LabelOSStable: "linux", genIdxLabel: strconv.Itoa(idx),
			}
			if reserved {
				labels[cloudprovider.ReservationIDLabel] = staticReservationID
			}
			nc, _ := test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-nc%04d", gc.prefix, idx), Labels: labels},
				Status:     v1.NodeClaimStatus{ProviderID: test.RandomProviderID(), Allocatable: it.Allocatable(), Capacity: it.Capacity},
			})
			for _, c := range []string{v1.ConditionTypeLaunched, v1.ConditionTypeRegistered, v1.ConditionTypeInitialized} {
				nc.StatusConditions().SetTrue(c)
			}
			if r.Float64() < g.DriftFrac {
				nc.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
				setDriftedTime(nc, base.Add(time.Duration(idx)*time.Second))
			}
			n := test.NodeClaimLinkedNode(nc)
			n.Name = fmt.Sprintf("%s-n%04d", gc.prefix, idx)
			n.Spec.Taints = lo.Reject(n.Spec.Taints, func(t corev1.Taint, _ int) bool { return t.MatchTaint(&v1.UnregisteredNoExecuteTaint) })
			n.Labels[v1.NodeRegisteredLabelKey] = "true"
			n.Labels[v1.NodeInitializedLabelKey] = "true"
			n.Labels[corev1.LabelHostname] = n.Name
			n.Status.Phase = corev1.NodeRunning
			n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue,
				LastHeartbeatTime: metav1.NewTime(env.Clock.Now()), LastTransitionTime: metav1.NewTime(env.Clock.Now())}}
			nc.Status.NodeName = n.Name
			nodeClaims, nodes = append(nodeClaims, nc), append(nodes, n)
			gc.nodeIdx[nc.Status.ProviderID] = fmt.Sprintf("n%d", idx)
			gc.nodeZone[n.Name] = zone
			for j := range r.Intn(3) {
				o := test.PodOptions{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{genGroupLabel: gc.groupSel[0], genPodLabel: fmt.Sprintf("%d-%d", idx, j)},
						OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true)}}},
					NodeName: n.Name, Phase: corev1.PodRunning,
					ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")}},
				}
				if r.Intn(10) == 0 {
					o.Annotations = map[string]string{v1.DoNotDisruptAnnotationKey: "true"}
				}
				pods = append(pods, test.Pod(o))
			}
		}
	}
	coreParallel(len(nodes), func(i int) {
		coreApply(ctx, env.Client, nodeClaims[i])
		taints := nodes[i].Spec.Taints
		coreApply(ctx, env.Client, nodes[i])
		nodes[i].Spec.Taints = taints
		Expect(env.Client.Update(ctx, nodes[i])).To(Succeed())
	})
	coreParallel(len(pods), func(i int) { coreApply(ctx, env.Client, pods[i]) })
	e.hydrate(len(pods), len(nodes), 0, nodeClaims, nodes, pods)
	return gc
}

var _ = Describe("Static Drift Core", func() {
	var e *coreEnv
	AfterEach(func() {
		if e != nil {
			e.stop()
			e = nil
		}
	})
	for seed := int64(3001); seed <= int64(3000+coreDiffEnv("KARPENTER_CORE_DIFF_SEEDS", 12)); seed++ {
		spec := randomStaticSpec(seed)
		It(fmt.Sprintf("matches ComputeCommands before the core (seed=%d %+v)", seed, spec), func() {
			e = newCoreEnv(ctx, staticInstanceTypes(spec.ReservationCap))
			gc := e.buildStatic(spec)
			sd := coreMethod[*disruption.StaticDrift](e)
			gctx := options.ToContext(e.ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{TerminateFirstDrift: lo.ToPtr(spec.TerminateFirst)}}))
			// Candidates arrive in cluster-state order, which is random, and each NodePool takes the first ones it may
			// disrupt; so the reference itself picks a random subset of a NodePool's candidates. ties is the outcome
			// without node identities: per NodePool, the decisions, and the events without their dedupe values.
			type result struct {
				cmds   []string
				events []string
				ties   []string
			}
			key := func(r result) string { return fmt.Sprint(r.cmds, r.events) }
			// run computes one variant and releases the limits.nodes slots it reserved (one per static replacement), so
			// every variant starts from the same reservations.
			run := func(f func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error)) ([]disruption.Command, result) {
				var res result
				cmds, _ := e.compute(sd, func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
					e.recorder.Reset()
					defer func() { res.events = eventStrings(e.recorder.Events()); sort.Strings(res.events) }()
					return f(budgets, cands)
				})
				res.cmds = lo.Map(cmds, func(c disruption.Command, _ int) string { return gc.cmdSignature(c) })
				sort.Strings(res.cmds)
				res.ties = lo.Map(cmds, func(c disruption.Command, _ int) string {
					return fmt.Sprintf("%s %s %d", c.Candidates[0].NodePool.Name, c.Decision(), len(c.Replacements))
				})
				res.ties = append(res.ties, lo.Map(e.recorder.Events(), func(ev events.Event, _ int) string { return ev.Reason + " " + ev.Message })...)
				sort.Strings(res.ties)
				for _, c := range cmds {
					e.cluster.NodePoolState.ReleaseNodeCount(c.Candidates[0].NodePool.Name, int64(len(c.Replacements)))
				}
				return cmds, res
			}
			legacy := func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
				return disruption.LegacyStaticDriftComputeCommands(gctx, sd, budgets, cands...)
			}
			current := func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error) {
				return sd.ComputeCommands(gctx, budgets, cands...)
			}
			for step := range 4 {
				ref, want := run(legacy)
				_, got := run(current)
				upstream := map[string]bool{key(want): true}
				for i := 0; i < 16 && !upstream[key(got)]; i++ {
					_, again := run(legacy)
					upstream[key(again)] = true
				}
				exact := upstream[key(got)]
				emitCoreDiff(map[string]any{"test": "staticdrift", "spec": spec, "step": step, "refCommands": len(ref),
					"refDecisions": lo.Map(ref, func(c disruption.Command, _ int) string { return string(c.Decision()) }),
					"equal":        exact, "equalUpToTies": fmt.Sprint(got.ties) == fmt.Sprint(want.ties), "upstreamOutcomes": len(upstream)})
				if !exact {
					Expect(got.ties).To(Equal(want.ties), "static drift core differs from upstream beyond the choice of candidates: %v", got.cmds)
					break // the state paths diverge from here
				}
				if len(ref) == 0 {
					break
				}
				// Start the reference commands as the controller would, holding their reservations again.
				for _, c := range ref {
					if len(c.Replacements) > 0 {
						Expect(e.cluster.NodePoolState.ReserveNodeCount(c.Candidates[0].NodePool.Name, 1<<40, 1)).To(BeEquivalentTo(1))
					}
				}
				started, _ := e.startCommands(sd, ref)
				e.genLaunch(gc, started)
			}
		})
	}
})
