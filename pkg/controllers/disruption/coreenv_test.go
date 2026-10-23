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

// Generated clusters for the method-core differential and invariant tests (drift_core_test.go).
//
// coreEnv is the disruption stack on a cache-backed client with the operator's field indexers, so that clusters of a
// few hundred nodes build and simulate quickly. genSpec draws a cluster from a seed: NodePools, zones, budgets,
// limits, workload groups with zone topology spread, hostname anti-affinity, blocking PDBs, do-not-disrupt pods, and
// pending pods. Builds of one spec are comparable: node names have a fixed-length per-build prefix (the scheduler
// orders existing nodes by name), each pod has its own memory request (the scheduler's last pod-order tie-break is the
// UID), and drift times are distinct. cmdSignature reduces a command to what a decision is made of, with objects
// mapped back to generator indices. checkEndState checks a placement against the hard constraints without trusting
// the scheduler.

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/dynamicresources/deviceallocation"
	"sigs.k8s.io/karpenter/pkg/controllers/node/health"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/lifecycle"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	"sigs.k8s.io/karpenter/pkg/state/prediction"
	"sigs.k8s.io/karpenter/pkg/state/virtualpods"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

type coreEnv struct {
	ctx         context.Context
	cancel      context.CancelFunc
	client      client.Client
	cluster     *state.Cluster
	clusterCost *cost.ClusterCost
	prov        *provisioning.Provisioner
	queue       *disruption.Queue
	methods     []disruption.Method
	recorder    *test.EventRecorder
	nodeCtrl    *informer.NodeController
	ncCtrl      *informer.NodeClaimController
	podCtrl     *informer.PodController
	npCtrl      *informer.NodePoolController
	stopClock   func()
}

// newCoreEnv builds the disruption stack for its. With repairPolicies, node repair is enabled with those policies, as
// the operator does at startup.
func newCoreEnv(parent context.Context, its []*cloudprovider.InstanceType, repairPolicies ...cloudprovider.RepairPolicy) *coreEnv {
	GinkgoHelper()
	cloudProvider.InstanceTypes = its
	var clusterOpts []option.Function[state.ClusterOptions]
	if len(repairPolicies) > 0 {
		cloudProvider.RepairPolicy = repairPolicies
		parent = options.ToContext(parent, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodeRepair: lo.ToPtr(true)}}))
		clusterOpts = append(clusterOpts, state.WithRepairPolicyMatcher(lo.Must(health.NewRepairPolicyMatcher(parent, cloudProvider))))
	}
	ctx, cancel := context.WithCancel(log.IntoContext(parent, logr.Discard()))
	ctx = injection.WithControllerName(ctx, "disruption")
	c := lo.Must(cache.New(env.Config, cache.Options{Scheme: scheme.Scheme}))
	idx := func(obj client.Object, field string, f func(client.Object) []string) {
		lo.Must0(c.IndexField(ctx, obj, field, f))
	}
	idx(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string { return []string{o.(*corev1.Pod).Spec.NodeName} })
	idx(&corev1.Node{}, "spec.providerID", func(o client.Object) []string { return []string{o.(*corev1.Node).Spec.ProviderID} })
	idx(&storagev1.VolumeAttachment{}, "spec.nodeName", func(o client.Object) []string { return []string{o.(*storagev1.VolumeAttachment).Spec.NodeName} })
	idx(&v1.NodeClaim{}, "status.providerID", func(o client.Object) []string { return []string{o.(*v1.NodeClaim).Status.ProviderID} })
	idx(&v1.NodeClaim{}, "spec.nodeClassRef.group", func(o client.Object) []string { return []string{o.(*v1.NodeClaim).Spec.NodeClassRef.Group} })
	idx(&v1.NodeClaim{}, "spec.nodeClassRef.kind", func(o client.Object) []string { return []string{o.(*v1.NodeClaim).Spec.NodeClassRef.Kind} })
	idx(&v1.NodeClaim{}, "spec.nodeClassRef.name", func(o client.Object) []string { return []string{o.(*v1.NodeClaim).Spec.NodeClassRef.Name} })
	idx(&v1.NodePool{}, "spec.template.spec.nodeClassRef.group", func(o client.Object) []string {
		return []string{o.(*v1.NodePool).Spec.Template.Spec.NodeClassRef.Group}
	})
	idx(&v1.NodePool{}, "spec.template.spec.nodeClassRef.kind", func(o client.Object) []string {
		return []string{o.(*v1.NodePool).Spec.Template.Spec.NodeClassRef.Kind}
	})
	idx(&v1.NodePool{}, "spec.template.spec.nodeClassRef.name", func(o client.Object) []string {
		return []string{o.(*v1.NodePool).Spec.Template.Spec.NodeClassRef.Name}
	})
	go func() { _ = c.Start(ctx) }()
	Expect(c.WaitForCacheSync(ctx)).To(BeTrue())
	kc := lo.Must(client.New(env.Config, client.Options{Scheme: scheme.Scheme, Cache: &client.CacheOptions{Reader: c}}))

	e := &coreEnv{ctx: ctx, cancel: cancel, client: kc, recorder: test.NewEventRecorder()}
	e.clusterCost = cost.NewClusterCost(ctx, cloudProvider, kc)
	ExpectSingletonReconciled(ctx, informer.NewPricingController(kc, cloudProvider, e.clusterCost))
	e.cluster = state.NewCluster(env.Clock, kc, cloudProvider, clusterOpts...)
	e.nodeCtrl = informer.NewNodeController(kc, e.cluster)
	e.ncCtrl = informer.NewNodeClaimController(kc, cloudProvider, e.cluster, e.clusterCost)
	e.podCtrl = informer.NewPodController(kc, e.cluster)
	e.npCtrl = informer.NewNodePoolController(kc, cloudProvider, e.cluster, e.clusterCost)
	st := prediction.NewStore(kc)
	metaScheme := runtime.NewScheme()
	Expect(metav1.AddMetaToScheme(metaScheme)).To(Succeed())
	Expect(st.RegisterTargetKinds(ctx, &informertest.FakeInformers{Scheme: metaScheme}, prediction.SupportedTargets.UnsortedList()...)).To(Succeed())
	e.prov = provisioning.NewProvisioner(kc, e.recorder, cloudProvider, e.cluster, env.Clock, deviceallocation.NewController(kc), virtualpods.NewVirtualPodCache(kc), st)
	e.queue = disruption.NewQueue(kc, e.recorder, e.cluster, env.Clock, e.prov)
	e.methods = disruption.NewMethods(ctx, env.Clock, e.cluster, kc, e.prov, cloudProvider, e.recorder, e.queue)
	return e
}

func (e *coreEnv) stop() {
	if e.stopClock != nil {
		e.stopClock()
	}
	e.cancel()
}

// stepClock elides validation waits until stop: whenever something waits on the fake clock (the 15 s command
// validation), the clock steps forward by 15 s.
func (e *coreEnv) stepClock() {
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-done:
				return
			case <-time.After(200 * time.Microsecond):
			}
			if env.Clock.HasWaiters() {
				env.Clock.Step(15 * time.Second)
			}
		}
	}()
	e.stopClock = func() { close(done); <-stopped; e.stopClock = nil }
}

func coreMethod[T disruption.Method](e *coreEnv) T {
	GinkgoHelper()
	m, ok := lo.Find(e.methods, func(m disruption.Method) bool { _, ok := m.(T); return ok })
	Expect(ok).To(BeTrue())
	return m.(T)
}

func (e *coreEnv) drift() *disruption.Drift { return coreMethod[*disruption.Drift](e) }

// coreParallel runs f(i) for i in [0,n) with a fixed worker pool.
func coreParallel(n int, f func(i int)) {
	workqueue.ParallelizeUntil(context.Background(), 32, n, func(i int) {
		defer GinkgoRecover()
		f(i)
	})
}

// coreApply creates obj and then writes its status, without the read-backs of ExpectApplied.
func coreApply(ctx context.Context, c client.Client, obj client.Object) {
	GinkgoHelper()
	statusCopy := obj.DeepCopyObject().(client.Object)
	Expect(c.Create(ctx, obj)).To(Succeed())
	statusCopy.SetResourceVersion(obj.GetResourceVersion())
	if err := c.Status().Update(ctx, statusCopy); err == nil {
		obj.SetResourceVersion(statusCopy.GetResourceVersion())
	}
}

// coreSimCount is the number of scheduling simulations (scheduler Solve calls) so far.
func coreSimCount() uint64 {
	var count uint64
	for _, mf := range lo.Must(crmetrics.Registry.Gather()) {
		if mf.GetName() != "karpenter_scheduler_scheduling_duration_seconds" {
			continue
		}
		for _, m := range mf.Metric {
			count += m.GetHistogram().GetSampleCount()
		}
	}
	return count
}

const (
	genIdxLabel   = "gen.karpenter.sh/idx"
	genPodLabel   = "gen.karpenter.sh/pod"
	genGroupLabel = "gen.karpenter.sh/group"
	genNodeITCPU  = 8
	genNumGroups  = 6
	genGroupTSC   = 2
	genGroupAnti  = 3
	genGroupPDB0  = 4
	genGroupPDB1  = 5
)

var genAllZones = []string{"test-zone-1", "test-zone-2", "test-zone-3"}

// genSpec is one generated cluster. Every field is derived from Seed, so two builds of a spec are identical up to
// object names and UIDs, which the comparisons map back to indices.
type genSpec struct {
	Seed           int64   `json:"seed"`
	Nodes          int     `json:"nodes"`
	MaxPodsPerNode int     `json:"maxPodsPerNode"`
	Pools          int     `json:"pools"`
	Zones          int     `json:"zones"`
	Budget         string  `json:"budget"`
	DriftFrac      float64 `json:"driftFrac"`
	EmptyFrac      float64 `json:"emptyFrac"`
	ZoneTSC        bool    `json:"zoneTSC"`
	AntiAffinity   bool    `json:"antiAffinity"`
	PDB            bool    `json:"pdb"`
	DoNotDisrupt   bool    `json:"doNotDisrupt"`
	Limits         bool    `json:"limits"`
	Pending        int     `json:"pending"`
	Util           float64 `json:"util"`
	// Reserved makes the last NodePool a capacity-reservation pool (instance type gen-reserved-8cpu, one reservation
	// per zone with ReservationCapacity free slots); half of its nodes' pods require reserved capacity.
	Reserved            bool `json:"reserved,omitempty"`
	ReservationCapacity int  `json:"reservationCapacity,omitempty"`
	// Pinned is the fraction of pods that select their own node by label, so they can't be rescheduled.
	Pinned float64 `json:"pinned,omitempty"`
	// MixedTypes gives each node one of the 4, 8 and 16 CPU instance types instead of 8, so nodes differ in price.
	MixedTypes bool `json:"mixedTypes,omitempty"`
	// ConsolidateAfter of every NodePool; empty means Never.
	ConsolidateAfter string `json:"consolidateAfter,omitempty"`
	// ConsolidationPolicy of every NodePool; empty means WhenEmptyOrUnderutilized.
	ConsolidationPolicy v1.ConsolidationPolicy `json:"consolidationPolicy,omitempty"`
}

func randomGenSpec(seed int64, nodeChoices []int, budgetFor func(nodes int, r *rand.Rand) string) genSpec {
	r := rand.New(rand.NewSource(seed))
	g := genSpec{Seed: seed}
	g.Nodes = nodeChoices[r.Intn(len(nodeChoices))]
	g.MaxPodsPerNode = []int{8, 16, 30}[r.Intn(3)]
	g.Pools = 1 + r.Intn(3)
	g.Zones = 1 + r.Intn(3)
	g.Budget = budgetFor(g.Nodes, r)
	g.DriftFrac = []float64{0.5, 0.8, 1}[r.Intn(3)]
	g.EmptyFrac = []float64{0, 0, 0.1}[r.Intn(3)]
	g.ZoneTSC = r.Intn(2) == 0
	g.AntiAffinity = r.Intn(2) == 0
	g.PDB = r.Intn(2) == 0
	g.DoNotDisrupt = r.Intn(3) == 0
	g.Limits = r.Intn(4) == 0
	g.Pending = []int{0, 0, 2, 6}[r.Intn(4)]
	g.Util = []float64{0.6, 0.9, 1, 1}[r.Intn(4)]
	return g
}

// setDriftedTime sets the Drifted condition's LastTransitionTime in place: ConditionSet.Set overwrites it with now.
func setDriftedTime(nc *v1.NodeClaim, t time.Time) {
	for i := range nc.Status.Conditions {
		if nc.Status.Conditions[i].Type == v1.ConditionTypeDrifted {
			nc.Status.Conditions[i].LastTransitionTime = metav1.NewTime(t)
		}
	}
}

func genZones(n int) []string { return genAllZones[:n] }

func genInstanceTypes(zones []string) []*cloudprovider.InstanceType {
	return genInstanceTypesOf(zones, v1.CapacityTypeOnDemand)
}

// genInstanceTypesOf returns instance types of 2 to 32 CPUs with an offering per zone and capacity type. Spot
// offerings cost 70% of on-demand.
func genInstanceTypesOf(zones []string, capacityTypes ...string) []*cloudprovider.InstanceType {
	var its []*cloudprovider.InstanceType
	for _, cpu := range []int{2, 4, 8, 16, 32} {
		res := corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(strconv.Itoa(cpu)),
			corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", cpu*4)),
			corev1.ResourcePods:   resource.MustParse("110"),
		}
		its = append(its, fake.NewInstanceType(fmt.Sprintf("gen-%dcpu", cpu), fake.WithResources(res),
			fake.WithOfferings(lo.FlatMap(zones, func(z string, _ int) []cloudprovider.Offering {
				return lo.Map(capacityTypes, func(ct string, _ int) cloudprovider.Offering {
					price := fake.PriceFromResources(res) * lo.Ternary(ct == v1.CapacityTypeSpot, 0.7, 1.0)
					return cloudprovider.Offering{Available: true, Price: price, Requirements: scheduling.NewLabelRequirements(map[string]string{
						v1.CapacityTypeLabelKey: ct, corev1.LabelTopologyZone: z,
					})}
				})
			})...)))
	}
	return its
}

const genReservedIT = "gen-reserved-8cpu"

func genReservationID(zone string) string { return "r-gen-" + zone }

// genReservedInstanceTypes returns gen-reserved-8cpu, offered as reserved capacity in each zone with capacity free
// slots, and as on-demand.
func genReservedInstanceTypes(zones []string, capacity int) []*cloudprovider.InstanceType {
	res := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("32Gi"), corev1.ResourcePods: resource.MustParse("110")}
	var offerings []cloudprovider.Offering
	for _, z := range zones {
		offerings = append(offerings,
			cloudprovider.Offering{Available: true, Price: fake.PriceFromResources(res) / 100, ReservationCapacity: capacity,
				Requirements: scheduling.NewLabelRequirements(map[string]string{
					v1.CapacityTypeLabelKey: v1.CapacityTypeReserved, corev1.LabelTopologyZone: z, cloudprovider.ReservationIDLabel: genReservationID(z),
				})},
			cloudprovider.Offering{Available: true, Price: fake.PriceFromResources(res),
				Requirements: scheduling.NewLabelRequirements(map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: z})})
	}
	it := fake.NewInstanceType(genReservedIT, fake.WithResources(res), fake.WithOfferings(offerings...))
	it.Requirements.Add(scheduling.NewRequirement(cloudprovider.ReservationIDLabel, corev1.NodeSelectorOpIn, lo.Map(zones, func(z string, _ int) string { return genReservationID(z) })...))
	it.Requirements.Get(v1.CapacityTypeLabelKey).Insert(v1.CapacityTypeReserved)
	return []*cloudprovider.InstanceType{it}
}

type genCluster struct {
	spec     genSpec
	prefix   string // fixed-length per-build name prefix, so name order is the same in every build
	launched int
	pools    []*v1.NodePool
	nodeIdx  map[string]string // providerID -> "n<i>"
	nodeZone map[string]string // node name -> zone
	groupSel []string          // group label values
	podCPU   []resource.Quantity
}

//nolint:gocyclo
func (e *coreEnv) buildGen(g genSpec) *genCluster {
	GinkgoHelper()
	r := rand.New(rand.NewSource(g.Seed*7919 + 17))
	zones := genZones(g.Zones)
	it, _ := lo.Find(cloudProvider.InstanceTypes, func(i *cloudprovider.InstanceType) bool { return i.Name == fmt.Sprintf("gen-%dcpu", genNodeITCPU) })
	// Each node's instance type: genNodeITCPU, or with MixedTypes one of 4, 8, 16 CPUs (from its own source, so other
	// specs build the same clusters as before).
	nodeIT := make([]*cloudprovider.InstanceType, g.Nodes)
	rt := rand.New(rand.NewSource(g.Seed*15485863 + 5))
	for i := range nodeIT {
		nodeIT[i] = it
		if g.MixedTypes {
			name := fmt.Sprintf("gen-%dcpu", []int{4, 8, 16}[rt.Intn(3)])
			nodeIT[i], _ = lo.Find(cloudProvider.InstanceTypes, func(i *cloudprovider.InstanceType) bool { return i.Name == name })
		}
	}
	run := uuid.NewString()[:6]
	gc := &genCluster{spec: g, prefix: "x" + run[:5], nodeIdx: map[string]string{}, nodeZone: map[string]string{}}

	// Workload groups.
	cpuChoices := []string{"250m", "500m", "1", "2"}
	rss := make([]*appsv1.ReplicaSet, genNumGroups)
	for i := range genNumGroups {
		rss[i] = test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rss[i])
		gc.groupSel = append(gc.groupSel, fmt.Sprintf("g%d-%s", i, run))
		gc.podCPU = append(gc.podCPU, resource.MustParse(cpuChoices[r.Intn(len(cpuChoices))]))
	}
	kind := func(grp int) string {
		switch {
		case grp == genGroupTSC && g.ZoneTSC:
			return "tsc"
		case grp == genGroupAnti && g.AntiAffinity:
			return "anti"
		case grp == genGroupPDB0 && g.PDB:
			return "pdb0"
		case grp == genGroupPDB1 && g.PDB:
			return "pdb1"
		}
		return "plain"
	}

	// NodePools.
	poolCPU := make([]int64, g.Pools)
	for i := range g.Nodes {
		poolCPU[i%g.Pools] += nodeIT[i].Capacity.Cpu().MilliValue()
	}
	for p := range g.Pools {
		np := test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{
			Disruption: v1.Disruption{
				ConsolidateAfter:    v1.MustParseNillableDuration(lo.Ternary(g.ConsolidateAfter == "", "Never", g.ConsolidateAfter)),
				ConsolidationPolicy: lo.Ternary(g.ConsolidationPolicy == "", v1.ConsolidationPolicyWhenEmptyOrUnderutilized, g.ConsolidationPolicy),
				Budgets:             []v1.Budget{{Nodes: g.Budget}},
			},
		}})
		np.Spec.Template.Spec.Requirements = append(np.Spec.Template.Spec.Requirements, v1.NodeSelectorRequirementWithMinValues{
			Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: zones,
		})
		if g.Reserved && p == g.Pools-1 {
			np.Spec.Template.Spec.Requirements = append(np.Spec.Template.Spec.Requirements,
				v1.NodeSelectorRequirementWithMinValues{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn, Values: []string{genReservedIT}},
				v1.NodeSelectorRequirementWithMinValues{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeReserved}})
		}
		if g.Limits {
			// Exactly the pool's current capacity: replacements fit only because candidates are excluded from usage.
			np.Spec.Limits = v1.Limits(corev1.ResourceList{corev1.ResourceCPU: *resource.NewMilliQuantity(poolCPU[p], resource.DecimalSI)})
		}
		ExpectApplied(ctx, env.Client, np)
		ExpectReconcileSucceeded(e.ctx, e.npCtrl, client.ObjectKeyFromObject(np))
		gc.pools = append(gc.pools, np)
	}

	// Nodes.
	nodeClaims := make([]*v1.NodeClaim, g.Nodes)
	nodes := make([]*corev1.Node, g.Nodes)
	empty := make([]bool, g.Nodes)
	base := env.Clock.Now().Add(-time.Hour)
	reservedNode := func(i int) bool { return g.Reserved && i%g.Pools == g.Pools-1 }
	for i := range g.Nodes {
		zone := zones[i%len(zones)]
		labels := map[string]string{
			v1.NodePoolLabelKey:            gc.pools[i%g.Pools].Name,
			corev1.LabelInstanceTypeStable: nodeIT[i].Name,
			v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
			corev1.LabelTopologyZone:       zone,
			corev1.LabelArchStable:         "amd64",
			corev1.LabelOSStable:           "linux",
			genIdxLabel:                    strconv.Itoa(i),
		}
		if reservedNode(i) {
			labels[corev1.LabelInstanceTypeStable] = genReservedIT
			labels[v1.CapacityTypeLabelKey] = v1.CapacityTypeReserved
			labels[cloudprovider.ReservationIDLabel] = genReservationID(zone)
		}
		nc, _ := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-nc%04d", gc.prefix, i), Labels: labels},
			Status:     v1.NodeClaimStatus{ProviderID: test.RandomProviderID(), Allocatable: nodeIT[i].Allocatable(), Capacity: nodeIT[i].Capacity},
		})
		nc.StatusConditions().SetTrue(v1.ConditionTypeLaunched)
		nc.StatusConditions().SetTrue(v1.ConditionTypeRegistered)
		nc.StatusConditions().SetTrue(v1.ConditionTypeInitialized)
		nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		if r.Float64() < g.DriftFrac {
			nc.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
			setDriftedTime(nc, base.Add(time.Duration(i)*time.Second))
		}
		empty[i] = r.Float64() < g.EmptyFrac
		n := test.NodeClaimLinkedNode(nc)
		n.Name = fmt.Sprintf("%s-n%04d", gc.prefix, i)
		n.Spec.Taints = lo.Reject(n.Spec.Taints, func(t corev1.Taint, _ int) bool { return t.MatchTaint(&v1.UnregisteredNoExecuteTaint) })
		n.Labels[v1.NodeRegisteredLabelKey] = "true"
		n.Labels[v1.NodeInitializedLabelKey] = "true"
		n.Labels[corev1.LabelHostname] = n.Name
		n.Status.Phase = corev1.NodeRunning
		n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			LastHeartbeatTime: metav1.NewTime(env.Clock.Now()), LastTransitionTime: metav1.NewTime(env.Clock.Now())}}
		nc.Status.NodeName = n.Name
		nodeClaims[i], nodes[i] = nc, n
		gc.nodeIdx[nc.Status.ProviderID] = fmt.Sprintf("n%d", i)
		gc.nodeZone[n.Name] = zone
	}
	coreParallel(g.Nodes, func(i int) {
		coreApply(ctx, env.Client, nodeClaims[i])
		// The apiserver's TaintNodesByCondition admission adds the not-ready taint on create; drop it again.
		taints := nodes[i].Spec.Taints
		coreApply(ctx, env.Client, nodes[i])
		nodes[i].Spec.Taints = taints
		Expect(env.Client.Update(ctx, nodes[i])).To(Succeed())
	})

	// Pod placement: fill each non-empty node with up to MaxPodsPerNode pods, at most one anti-affinity pod per node.
	type placed struct{ node, group int }
	var placements []placed
	for i := range g.Nodes {
		if empty[i] {
			continue
		}
		used := int64(0)
		nodeAlloc := nodeIT[i].Allocatable()
		target := int64(float64(nodeAlloc.Cpu().MilliValue()) * g.Util)
		hasAnti := false
		for range g.MaxPodsPerNode {
			grp := r.Intn(genNumGroups)
			if kind(grp) == "anti" && hasAnti {
				grp = 0
			}
			cpu := gc.podCPU[grp].MilliValue()
			if used+cpu > target {
				// Top up with the smallest plain group that still fits.
				grp = lo.Ternary(gc.podCPU[0].Cmp(gc.podCPU[1]) <= 0, 0, 1)
				cpu = gc.podCPU[grp].MilliValue()
				if used+cpu > target {
					break
				}
			}
			used += cpu
			hasAnti = hasAnti || kind(grp) == "anti"
			placements = append(placements, placed{node: i, group: grp})
		}
	}
	// Make the zone TSC group satisfy maxSkew=1 initially by trimming over-full zones.
	if g.ZoneTSC {
		counts := map[string]int{}
		for _, z := range zones {
			counts[z] = 0
		}
		for _, p := range placements {
			if p.group == genGroupTSC {
				counts[zones[p.node%len(zones)]]++
			}
		}
		minCount := lo.Min(lo.Values(counts))
		keep := map[string]int{}
		placements = lo.Filter(placements, func(p placed, _ int) bool {
			if p.group != genGroupTSC {
				return true
			}
			z := zones[p.node%len(zones)]
			keep[z]++
			return keep[z] <= minCount+1
		})
	}
	podOpts := func(i, grp int) test.PodOptions {
		sel := map[string]string{genGroupLabel: gc.groupSel[grp]}
		o := test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{genGroupLabel: gc.groupSel[grp], genPodLabel: strconv.Itoa(i)},
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rss[grp].Name, UID: rss[grp].UID,
					Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true)}},
			},
			ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU: gc.podCPU[grp],
				// A per-pod memory offset gives the scheduler's pod order a deterministic tie-break.
				corev1.ResourceMemory: *resource.NewQuantity(gc.podCPU[grp].MilliValue()*2*1024*1024+int64(i)*1024, resource.BinarySI),
			}},
		}
		switch kind(grp) {
		case "tsc":
			o.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{MaxSkew: 1, TopologyKey: corev1.LabelTopologyZone,
				WhenUnsatisfiable: corev1.DoNotSchedule, LabelSelector: &metav1.LabelSelector{MatchLabels: sel}}}
		case "anti":
			o.PodAntiRequirements = []corev1.PodAffinityTerm{{TopologyKey: corev1.LabelHostname, LabelSelector: &metav1.LabelSelector{MatchLabels: sel}}}
		}
		if g.DoNotDisrupt && r.Float64() < 0.03 {
			o.Annotations = map[string]string{v1.DoNotDisruptAnnotationKey: "true"}
		}
		return o
	}
	var pods []*corev1.Pod
	groupCount := make([]int, genNumGroups)
	// A separate source, so specs without Reserved or Pinned build the same clusters as before these existed.
	r2 := rand.New(rand.NewSource(g.Seed*104729 + 3))
	for i, p := range placements {
		o := podOpts(i, p.group)
		o.NodeName = nodes[p.node].Name
		if reservedNode(p.node) && r2.Float64() < 0.5 {
			o.NodeSelector = lo.Assign(o.NodeSelector, map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeReserved})
		}
		if g.Pinned > 0 && r2.Float64() < g.Pinned {
			o.NodeSelector = lo.Assign(o.NodeSelector, map[string]string{genIdxLabel: strconv.Itoa(p.node)})
		}
		o.Phase = corev1.PodRunning
		pods = append(pods, test.Pod(o))
		groupCount[p.group]++
	}
	for j := range g.Pending {
		pods = append(pods, test.UnschedulablePod(podOpts(len(placements)+j, j%2))) // plain groups only
	}
	coreParallel(len(pods), func(i int) { coreApply(ctx, env.Client, pods[i]) })
	if g.PDB {
		for _, grp := range []int{genGroupPDB0, genGroupPDB1} {
			allowed := lo.Ternary(grp == genGroupPDB0, 0, 1)
			ExpectApplied(ctx, env.Client, test.PodDisruptionBudget(test.PDBOptions{
				Labels:         map[string]string{genGroupLabel: gc.groupSel[grp]},
				MaxUnavailable: lo.ToPtr(intstr.FromInt32(int32(allowed))),
				Status: &policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: int32(allowed), CurrentHealthy: int32(groupCount[grp]),
					DesiredHealthy: int32(max(0, groupCount[grp]-allowed)), ExpectedPods: int32(groupCount[grp])},
			}))
		}
	}
	e.hydrate(len(pods), g.Nodes, lo.Ternary(g.PDB, 2, 0), nodeClaims, nodes, pods)
	return gc
}

// hydrate waits for the informer cache to hold the objects, then updates cluster state as the informer controllers
// would.
func (e *coreEnv) hydrate(numPods, numNodes, numPDBs int, nodeClaims []*v1.NodeClaim, nodes []*corev1.Node, pods []*corev1.Pod) {
	GinkgoHelper()
	Eventually(func(gm Gomega) {
		pl := &corev1.PodList{}
		gm.Expect(e.client.List(ctx, pl)).To(Succeed())
		gm.Expect(len(pl.Items)).To(BeNumerically(">=", numPods))
		nl := &corev1.NodeList{}
		gm.Expect(e.client.List(ctx, nl)).To(Succeed())
		gm.Expect(len(nl.Items)).To(BeNumerically(">=", numNodes))
		ncl := &v1.NodeClaimList{}
		gm.Expect(e.client.List(ctx, ncl)).To(Succeed())
		gm.Expect(len(ncl.Items)).To(BeNumerically(">=", numNodes))
		pdbs := &policyv1.PodDisruptionBudgetList{}
		gm.Expect(e.client.List(ctx, pdbs)).To(Succeed())
		gm.Expect(len(pdbs.Items)).To(BeNumerically(">=", numPDBs))
	}, 5*time.Minute, 50*time.Millisecond).Should(Succeed())
	coreParallel(len(nodes), func(i int) {
		ExpectReconcileSucceeded(e.ctx, e.ncCtrl, client.ObjectKeyFromObject(nodeClaims[i]))
		ExpectReconcileSucceeded(e.ctx, e.nodeCtrl, client.ObjectKeyFromObject(nodes[i]))
	})
	coreParallel(len(pods), func(i int) { ExpectReconcileSucceeded(e.ctx, e.podCtrl, client.ObjectKeyFromObject(pods[i])) })
	Expect(e.cluster.Synced(e.ctx)).To(BeTrue())
	// Start lazily-started informers (PDBs) before the first comparison.
	_, _, err := disruption.GetCandidatesWithTotals(e.ctx, e.cluster, e.client, e.recorder, env.Clock, cloudProvider,
		func(context.Context, *disruption.Candidate) bool { return true }, disruption.GracefulDisruptionClass, e.queue, e.clusterCost)
	Expect(err).ToNot(HaveOccurred())
}

// genLaunch launches the replacements of cmds, naming the new nodes deterministically (prefix-r<k>, after the
// original nodes) and giving them a hostname label, and initializes them in cluster state.
func (e *coreEnv) genLaunch(gc *genCluster, cmds []*disruption.Command) {
	GinkgoHelper()
	for _, cmd := range cmds {
		for _, r := range cmd.Replacements {
			nc := &v1.NodeClaim{}
			Expect(env.Client.Get(ctx, client.ObjectKey{Name: r.Name}, nc)).To(Succeed())
			if nc.Status.ProviderID != "" {
				continue
			}
			resolved, err := cloudProvider.Create(ctx, nc)
			Expect(err).ToNot(HaveOccurred())
			nc = lifecycle.PopulateNodeClaimDetails(nc, resolved)
			nc.StatusConditions().SetTrue(v1.ConditionTypeLaunched)
			nc.StatusConditions().SetTrue(v1.ConditionTypeRegistered)
			n := test.NodeClaimLinkedNode(nc)
			n.Name = fmt.Sprintf("%s-r%04d", gc.prefix, gc.launched)
			gc.launched++
			n.Spec.Taints = lo.Reject(n.Spec.Taints, func(t corev1.Taint, _ int) bool { return t.MatchTaint(&v1.UnregisteredNoExecuteTaint) })
			n.Labels[v1.NodeRegisteredLabelKey] = "true"
			n.Labels[corev1.LabelHostname] = n.Name
			nc.Status.NodeName = n.Name
			ExpectApplied(ctx, env.Client, nc, n)
			// The helpers replace the slice elements with the updated objects.
			ncs, ns := []*v1.NodeClaim{nc}, []*corev1.Node{n}
			ExpectMakeNodeClaimsInitialized(e.ctx, env.Client, env.Clock, ncs...)
			ExpectMakeNodesInitialized(e.ctx, env.Client, env.Clock, ns...)
			e.cluster.UpdateNodeClaim(ncs[0])
			Expect(e.cluster.UpdateNode(e.ctx, ns[0])).To(Succeed())
			gc.nodeZone[ns[0].Name] = ns[0].Labels[corev1.LabelTopologyZone]
		}
	}
}

func podKey(p *corev1.Pod) string { return "p" + p.Labels[genPodLabel] }

// cmdSignature canonicalizes a command: decision, candidates, and per replacement its requirements (without the
// per-NodeClaim hostname placeholder), instance type options and pods, plus the nominated existing nodes with the pods
// placed on them.
func (gc *genCluster) cmdSignature(cmd disruption.Command) string {
	cands := lo.Map(cmd.Candidates, func(c *disruption.Candidate, _ int) string { return gc.nodeIdx[c.ProviderID()] })
	sort.Strings(cands)
	reps := lo.Map(cmd.Results.NewNodeClaims, func(nc *pscheduling.NodeClaim, _ int) string {
		var reqs []string
		for _, k := range nc.Requirements.Keys().UnsortedList() {
			if k == corev1.LabelHostname {
				continue
			}
			req := nc.Requirements.Get(k)
			vals := req.Values()
			sort.Strings(vals)
			reqs = append(reqs, fmt.Sprintf("%s %s %v", k, req.Operator(), vals))
		}
		sort.Strings(reqs)
		its := lo.Map(nc.InstanceTypeOptions, func(it *cloudprovider.InstanceType, _ int) string { return it.Name })
		sort.Strings(its)
		pods := lo.Map(nc.Pods, func(p *corev1.Pod, _ int) string { return podKey(p) })
		sort.Strings(pods)
		return fmt.Sprintf("{reqs=%v its=%v pods=%v}", reqs, its, pods)
	})
	sort.Strings(reps)
	existing := lo.Map(cmd.Results.ExistingNodes, func(en *pscheduling.ExistingNode, _ int) string {
		pods := lo.Map(en.Pods, func(p *corev1.Pod, _ int) string { return podKey(p) })
		sort.Strings(pods)
		return fmt.Sprintf("%s:%v", gc.nodeIdx[en.ProviderID()], pods)
	})
	sort.Strings(existing)
	return fmt.Sprintf("decision=%s tf=%s candidates=%v replacements=%d %v existing=%v", cmd.Decision(), cmd.TerminateFirstReason, cands, len(cmd.Replacements), reps, existing)
}

func (gc *genCluster) cmdsSignature(cmds []disruption.Command) string {
	return strings.Join(lo.Map(cmds, func(c disruption.Command, _ int) string { return gc.cmdSignature(c) }), "\n")
}

// emitCoreDiff appends a JSON record to $KARPENTER_CORE_DIFF_OUT, if set.
func emitCoreDiff(record map[string]any) {
	GinkgoHelper()
	path := os.Getenv("KARPENTER_CORE_DIFF_OUT")
	if path == "" {
		return
	}
	f := lo.Must(os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644))
	defer f.Close()
	_, _ = f.Write(append(lo.Must(json.Marshal(record)), '\n'))
}

// coreSeeds returns the seeds of a generated table: first, first+1, ... (KARPENTER_CORE_DIFF_SEEDS of them, default
// def), or those of KARPENTER_CORE_DIFF_SEED_LIST in [first, first+1000).
func coreSeeds(first int64, def int) []int64 {
	if raw := os.Getenv("KARPENTER_CORE_DIFF_SEED_LIST"); raw != "" {
		return lo.Filter(lo.Map(strings.Split(raw, ","), func(s string, _ int) int64 { return int64(lo.Must(strconv.Atoi(strings.TrimSpace(s)))) }),
			func(s int64, _ int) bool { return s >= first && s < first+1000 })
	}
	return lo.Times(coreDiffEnv("KARPENTER_CORE_DIFF_SEEDS", def), func(i int) int64 { return first + int64(i) })
}

func coreDiffEnv(name string, def int) int {
	if raw := os.Getenv(name); raw != "" {
		return lo.Must(strconv.Atoi(raw))
	}
	return def
}

// compute builds m's candidates and budgets as the disruption controller does, runs f on them, and returns the commands
// that aren't no-ops and the number of candidates.
func (e *coreEnv) compute(m disruption.Method, f func(budgets map[string]int, cands []*disruption.Candidate) ([]disruption.Command, error)) ([]disruption.Command, int) {
	GinkgoHelper()
	cands, totals, err := disruption.GetCandidatesWithTotals(e.ctx, e.cluster, e.client, e.recorder, env.Clock, cloudProvider, m.ShouldDisrupt, m.Class(), e.queue, e.clusterCost)
	Expect(err).ToNot(HaveOccurred())
	if len(cands) == 0 {
		return nil, 0
	}
	if setter, ok := m.(disruption.NodePoolTotalsSetter); ok {
		setter.SetNodePoolTotals(totals)
	}
	budgets, err := disruption.BuildDisruptionBudgetMapping(e.ctx, e.cluster, env.Clock, e.client, cloudProvider, e.recorder, m.Reason())
	Expect(err).ToNot(HaveOccurred())
	cmds, err := f(budgets, cands)
	Expect(err).ToNot(HaveOccurred())
	return lo.Filter(cmds, func(c disruption.Command, _ int) bool { return c.Decision() != disruption.NoOpDecision }), len(cands)
}

// startCommands starts cmds as the disruption controller does and returns the ones that started. A failed start (for
// example the NodePool limit check in Provisioner.Create) is the command being rejected.
func (e *coreEnv) startCommands(m disruption.Method, cmds []disruption.Command) ([]*disruption.Command, int) {
	GinkgoHelper()
	var started []*disruption.Command
	failed := 0
	for i := range cmds {
		cmd := cmds[i]
		cmd.CreationTimestamp = env.Clock.Now()
		cmd.ID = uuid.New()
		cmd.Method = m
		if err := e.queue.StartCommand(e.ctx, &cmd); err != nil {
			failed++
			continue
		}
		started = append(started, &cmd)
	}
	return started, failed
}

// endState is the hard-constraint check of a placement: pods on non-deleting nodes stay, pods on deleting nodes (and
// pending pods) go where the placement says.
type endState struct {
	TSCMaxSkew        int `json:"tscMaxSkew"`
	TSCViolations     int `json:"tscViolations"`
	AntiViolations    int `json:"antiViolations"`
	CapacityOverbook  int `json:"capacityOverbook"`
	UnplacedMoving    int `json:"unplacedMoving"`
	UnplacedPending   int `json:"unplacedPending"`
	ExtraNodeClaims   int `json:"extraNodeClaims"`
	PendingNodeClaims int `json:"pendingNodeClaims"`
	BudgetOver        int `json:"budgetOver"`
	LimitOver         int `json:"limitOver"`
}

func (s endState) hardViolations() int {
	return s.TSCViolations + s.AntiViolations + s.CapacityOverbook + s.UnplacedMoving + s.BudgetOver + s.LimitOver
}

type location struct{ host, zone string }

//nolint:gocyclo
func (e *coreEnv) checkEndState(gc *genCluster, placement map[types.UID]location, extraNodeClaims, pendingNodeClaims int) endState {
	GinkgoHelper()
	var s endState
	s.ExtraNodeClaims, s.PendingNodeClaims = extraNodeClaims, pendingNodeClaims
	deleting := map[string]bool{}
	for _, sn := range e.cluster.DeepCopyNodes() {
		if sn.MarkedForDeletion() && sn.Node != nil {
			deleting[sn.Node.Name] = true
		}
	}
	nodeList := &corev1.NodeList{}
	Expect(env.Client.List(ctx, nodeList)).To(Succeed())
	allocCPU := map[string]int64{}
	for _, n := range nodeList.Items {
		allocCPU[n.Name] = n.Status.Allocatable.Cpu().MilliValue()
		gc.nodeZone[n.Name] = n.Labels[corev1.LabelTopologyZone]
	}
	podList := &corev1.PodList{}
	Expect(env.Client.List(ctx, podList, client.HasLabels{genPodLabel})).To(Succeed())
	used := map[string]int64{}
	tsc := map[string]int{}
	for _, z := range genZones(gc.spec.Zones) {
		tsc[z] = 0
	}
	anti := map[string]int{}
	for i := range podList.Items {
		p := &podList.Items[i]
		loc, moved := placement[p.UID]
		switch {
		case p.Spec.NodeName == "":
			if !moved {
				s.UnplacedPending++
				continue
			}
		case deleting[p.Spec.NodeName]:
			if !moved {
				s.UnplacedMoving++
				continue
			}
		default:
			loc = location{host: p.Spec.NodeName, zone: gc.nodeZone[p.Spec.NodeName]}
		}
		used[loc.host] += p.Spec.Containers[0].Resources.Requests.Cpu().MilliValue()
		switch p.Labels[genGroupLabel] {
		case gc.groupSel[genGroupTSC]:
			if gc.spec.ZoneTSC {
				tsc[loc.zone]++
			}
		case gc.groupSel[genGroupAnti]:
			if gc.spec.AntiAffinity {
				anti[loc.host]++
			}
		}
	}
	for host, u := range used {
		if a, ok := allocCPU[host]; ok && u > a {
			s.CapacityOverbook++
		}
	}
	if gc.spec.ZoneTSC {
		s.TSCMaxSkew = lo.Max(lo.Values(tsc)) - lo.Min(lo.Values(tsc))
		if s.TSCMaxSkew > 1 {
			s.TSCViolations = 1
		}
	}
	for _, c := range anti {
		if c > 1 {
			s.AntiViolations += c - 1
		}
	}
	// Budgets: nodes being disrupted per pool must not exceed what the drift budget allows for the pool's size now.
	reason := v1.DisruptionReasonDrifted
	numNodes, disrupting := disruption.NodePoolStats(e.cluster, reason)
	for _, np := range gc.pools {
		current := &v1.NodePool{}
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(np), current)).To(Succeed())
		allowed := lo.Must(current.GetAllowedDisruptionsByReason(env.Clock, numNodes[np.Name], reason))
		if disrupting[np.Name] > allowed {
			s.BudgetOver += disrupting[np.Name] - allowed
		}
		// Limits: capacity of the pool's non-deleting nodes.
		if lim, ok := np.Spec.Limits[corev1.ResourceCPU]; ok {
			var cpu int64
			for _, n := range nodeList.Items {
				if n.Labels[v1.NodePoolLabelKey] == np.Name && !deleting[n.Name] {
					cpu += n.Status.Capacity.Cpu().MilliValue()
				}
			}
			if cpu > lim.MilliValue() {
				s.LimitOver++
			}
		}
	}
	return s
}

// plannedPlacement is the latest placement Karpenter planned for each moving pod across the started commands.
func (e *coreEnv) plannedPlacement(gc *genCluster, cmds []*disruption.Command) map[types.UID]location {
	GinkgoHelper()
	out := map[types.UID]location{}
	for _, cmd := range cmds {
		for _, en := range cmd.Results.ExistingNodes {
			for _, p := range en.Pods {
				out[p.UID] = location{host: en.Node.Name, zone: gc.nodeZone[en.Node.Name]}
			}
		}
		for i, nc := range cmd.Results.NewNodeClaims {
			claim := &v1.NodeClaim{}
			Expect(env.Client.Get(ctx, client.ObjectKey{Name: cmd.Replacements[i].Name}, claim)).To(Succeed())
			for _, p := range nc.Pods {
				out[p.UID] = location{host: claim.Status.NodeName, zone: claim.Labels[corev1.LabelTopologyZone]}
			}
		}
	}
	return out
}

// provisionerPlacement asks the provisioner where it would put the pending pods and the pods of every deleting node
// now. NodeClaims it would launch for moving pods are capacity the disruption plan should have provided.
func (e *coreEnv) provisionerPlacement(gc *genCluster) (map[types.UID]location, int, int) {
	GinkgoHelper()
	results, err := e.prov.Schedule(e.ctx)
	Expect(err).ToNot(HaveOccurred())
	out := map[types.UID]location{}
	for _, en := range results.ExistingNodes {
		for _, p := range en.Pods {
			out[p.UID] = location{host: en.Node.Name, zone: gc.nodeZone[en.Node.Name]}
		}
	}
	extra, pendingOnly := 0, 0
	for i, nc := range results.NewNodeClaims {
		zone := "?"
		if z := nc.Requirements.Get(corev1.LabelTopologyZone); z.Len() == 1 {
			zone = z.Any()
		}
		for _, p := range nc.Pods {
			out[p.UID] = location{host: fmt.Sprintf("new-%d", i), zone: zone}
		}
		if lo.SomeBy(nc.Pods, func(p *corev1.Pod) bool { return p.Spec.NodeName != "" }) {
			extra++
		} else {
			pendingOnly++
		}
	}
	return out, extra, pendingOnly
}

func (e *coreEnv) evaluate(gc *genCluster, started []*disruption.Command) (planned, final endState) {
	GinkgoHelper()
	planned = e.checkEndState(gc, e.plannedPlacement(gc, started), 0, 0)
	pl, extra, pendingOnly := e.provisionerPlacement(gc)
	final = e.checkEndState(gc, pl, extra, pendingOnly)
	return planned, final
}
