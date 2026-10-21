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

package disruption_test

// Disruption Scale Benchmark: an opt-in Ginkgo spec that measures the disruption loop at N nodes.
//
// It is gated twice so that a plain `go test ./pkg/controllers/disruption/...` neither compiles nor runs it: the file
// needs the test_performance build tag (like the scheduling benchmarks), and the specs skip unless
// KARPENTER_DISRUPTION_BENCH=1, because a full run takes hours and needs a long -timeout. To run it:
//
//	KARPENTER_DISRUPTION_BENCH=1 \
//	KARPENTER_DISRUPTION_BENCH_SIZES=100,500 \
//	KARPENTER_DISRUPTION_BENCH_OUT=/tmp/bench.jsonl \
//	go test -tags=test_performance ./pkg/controllers/disruption/ -run TestAPIs -count=1 -timeout 4h \
//		--ginkgo.focus="Disruption Scale Benchmark"
//
// Optional settings:
//   - KARPENTER_DISRUPTION_BENCH_SIZES: comma-separated node counts (default 100,500,1000,2000).
//   - KARPENTER_DISRUPTION_BENCH_ITERS: maximum drift Reconcile iterations per size (default N/5+5, which is past the
//     10% budget; the loop stops at the first iteration that starts no new command).
//   - KARPENTER_DISRUPTION_BENCH_OUT: file that JSON lines are appended to. Records are always written to GinkgoWriter.
//   - KARPENTER_DISRUPTION_BENCH_PROFILE: directory for CPU profiles of the measured sections.
//
// Each record has a "scenario" and a "nodes" field. To compare two builds, run both with the same sizes on the same
// machine, preferably alternating, and compare records with the same (scenario, nodes) key.
//
// The benchmark builds its own cluster state, provisioner, queue and controller on top of a
// controller-runtime cache-backed client (with the same field indexers as the operator) so that reads
// are served from an informer cache as in production, rather than hitting the envtest API server for
// every per-node pod list, which is what the rest of this suite does.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	prometheusmodel "github.com/prometheus/client_model/go"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	"sigs.k8s.io/karpenter/pkg/state/prediction"
	"sigs.k8s.io/karpenter/pkg/state/virtualpods"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

const (
	benchInstanceCPU = 32
	benchInstanceMem = 64 // Gi
	benchPodsPerNode = 10
)

var benchZones = []string{"test-zone-1", "test-zone-2", "test-zone-3"}

type benchEnv struct {
	ctx         context.Context
	cancel      context.CancelFunc
	client      client.Client
	cluster     *state.Cluster
	clusterCost *cost.ClusterCost
	prov        *provisioning.Provisioner
	queue       *disruption.Queue
	ctrl        *disruption.Controller
	methods     []disruption.Method
	recorder    *test.EventRecorder
	nodeCtrl    *informer.NodeController
	ncCtrl      *informer.NodeClaimController
	podCtrl     *informer.PodController
	npCtrl      *informer.NodePoolController

	clockSteps atomic.Int64
	stopStep   chan struct{}
	stepDone   chan struct{}
}

// benchInstanceTypes mirrors fake.InstanceTypesAssorted (7 cpu x 8 mem x 3 zones x 2 capacity types x 2 OS x 2 arch =
// 1344 instance types, one offering each) but with a realistic 110 pod capacity instead of the fake default of 5.
func benchInstanceTypes() []*cloudprovider.InstanceType {
	var its []*cloudprovider.InstanceType
	for _, cpu := range []int{1, 2, 4, 8, 16, 32, 64} {
		for _, mem := range []int{1, 2, 4, 8, 16, 32, 64, 128} {
			for _, zone := range benchZones {
				for _, ct := range []string{v1.CapacityTypeSpot, v1.CapacityTypeOnDemand} {
					for _, osName := range []string{string(corev1.Linux), string(corev1.Windows)} {
						for _, arch := range []string{v1.ArchitectureAmd64, v1.ArchitectureArm64} {
							resources := corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse(strconv.Itoa(cpu)),
								corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", mem)),
								corev1.ResourcePods:   resource.MustParse("110"),
							}
							its = append(its, fake.NewInstanceType(
								fmt.Sprintf("%d-cpu-%d-mem-%s-%s-%s-%s", cpu, mem, arch, osName, zone, ct),
								fake.WithArchitecture(arch),
								fake.WithOperatingSystems(osName),
								fake.WithResources(resources),
								fake.WithOfferings(cloudprovider.Offering{
									Available: true,
									Requirements: scheduling.NewLabelRequirements(map[string]string{
										v1.CapacityTypeLabelKey:  ct,
										corev1.LabelTopologyZone: zone,
									}),
									Price: fake.PriceFromResources(resources),
								}),
							))
						}
					}
				}
			}
		}
	}
	return its
}

// newBenchEnv builds the disruption stack on a cache-backed client, mirroring the operator's indexers.
func newBenchEnv(parent context.Context) *benchEnv {
	GinkgoHelper()
	cloudProvider.InstanceTypes = benchInstanceTypes()
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

	b := &benchEnv{ctx: ctx, cancel: cancel, client: kc, recorder: test.NewEventRecorder(),
		stopStep: make(chan struct{}), stepDone: make(chan struct{})}
	b.clusterCost = cost.NewClusterCost(ctx, cloudProvider, kc)
	ExpectSingletonReconciled(ctx, informer.NewPricingController(kc, cloudProvider, b.clusterCost))
	b.cluster = state.NewCluster(env.Clock, kc, cloudProvider)
	b.nodeCtrl = informer.NewNodeController(kc, b.cluster)
	b.ncCtrl = informer.NewNodeClaimController(kc, cloudProvider, b.cluster, b.clusterCost)
	b.podCtrl = informer.NewPodController(kc, b.cluster)
	b.npCtrl = informer.NewNodePoolController(kc, cloudProvider, b.cluster, b.clusterCost)
	st := prediction.NewStore(kc)
	metaScheme := runtime.NewScheme()
	Expect(metav1.AddMetaToScheme(metaScheme)).To(Succeed())
	Expect(st.RegisterTargetKinds(ctx, &informertest.FakeInformers{Scheme: metaScheme}, prediction.SupportedTargets.UnsortedList()...)).To(Succeed())
	b.prov = provisioning.NewProvisioner(kc, b.recorder, cloudProvider, b.cluster, env.Clock, deviceallocation.NewController(kc), virtualpods.NewVirtualPodCache(kc), st)
	b.queue = disruption.NewQueue(kc, b.recorder, b.cluster, env.Clock, b.prov)
	// Production method set and order, with the real 15s validators. The fake clock is stepped by stepClock so
	// validation waits are elided (and counted) rather than slept.
	b.methods = disruption.NewMethods(ctx, env.Clock, b.cluster, kc, b.prov, cloudProvider, b.recorder, b.queue)
	b.ctrl = disruption.NewController(ctx, env.Clock, kc, b.prov, cloudProvider, b.recorder, b.cluster, b.queue, b.clusterCost, disruption.WithMethods(b.methods...))
	go b.stepClock()
	return b
}

// stepClock elides validation waits: whenever something blocks on the fake clock (EmptinessValidator /
// ConsolidationValidator use clock.After(15s)) it steps the clock forward by 15s.
func (b *benchEnv) stepClock() {
	defer close(b.stepDone)
	last := time.Now()
	for {
		select {
		case <-b.stopStep:
			return
		default:
		}
		// Keep the fake clock moving at wall-clock speed so clock-based deadlines (SingleNodeConsolidation's
		// 3m timeout) behave as in production.
		now := time.Now()
		env.Clock.Step(now.Sub(last))
		last = now
		if env.Clock.HasWaiters() {
			env.Clock.Step(15 * time.Second)
			b.clockSteps.Add(1)
		}
		time.Sleep(200 * time.Microsecond)
	}
}

func (b *benchEnv) stop() {
	close(b.stopStep)
	<-b.stepDone // the suite resets env.Clock after this spec
	b.cancel()
}

func (b *benchEnv) method(match func(disruption.Method) bool) disruption.Method {
	m, ok := lo.Find(b.methods, match)
	Expect(ok).To(BeTrue())
	return m
}

func isType[T any](m disruption.Method) bool { _, ok := m.(T); return ok }

// parallel runs f(i) for i in [0,n) with a fixed worker pool.
func parallel(n int, f func(i int)) {
	workqueue.ParallelizeUntil(context.Background(), 32, n, func(i int) {
		defer GinkgoRecover()
		f(i)
	})
}

func applyFast(ctx context.Context, c client.Client, obj client.Object) {
	GinkgoHelper()
	statusCopy := obj.DeepCopyObject().(client.Object)
	Expect(c.Create(ctx, obj)).To(Succeed())
	statusCopy.SetResourceVersion(obj.GetResourceVersion())
	err := c.Status().Update(ctx, statusCopy)
	if err == nil {
		obj.SetResourceVersion(statusCopy.GetResourceVersion())
	}
}

type clusterSpec struct {
	nodes      int
	podCPU     string
	podMem     string
	drifted    bool
	budget     string
	capacity   string
	consolAftr string
}

type builtCluster struct {
	nodePool   *v1.NodePool
	nodeClaims []*v1.NodeClaim
	nodes      []*corev1.Node
	pods       int
	setup      time.Duration
}

func (b *benchEnv) buildCluster(spec clusterSpec) *builtCluster {
	GinkgoHelper()
	start := time.Now()
	nodePool := test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{
		Limits: v1.Limits(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10000000")}),
		Disruption: v1.Disruption{
			ConsolidateAfter:    v1.MustParseNillableDuration(spec.consolAftr),
			ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
			Budgets:             []v1.Budget{{Nodes: spec.budget}},
		},
	}})
	ExpectApplied(ctx, env.Client, nodePool)
	ExpectReconcileSucceeded(b.ctx, b.npCtrl, client.ObjectKeyFromObject(nodePool))

	nodeClaims := make([]*v1.NodeClaim, spec.nodes)
	nodes := make([]*corev1.Node, spec.nodes)
	base := env.Clock.Now().Add(-time.Hour)
	for i := range spec.nodes {
		zone := benchZones[i%len(benchZones)]
		itName := fmt.Sprintf("%d-cpu-%d-mem-amd64-linux-%s-%s", benchInstanceCPU, benchInstanceMem, zone, spec.capacity)
		nc := test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: itName,
				v1.CapacityTypeLabelKey:        spec.capacity,
				corev1.LabelTopologyZone:       zone,
				corev1.LabelArchStable:         "amd64",
				corev1.LabelOSStable:           "linux",
			}},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
				Allocatable: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse(strconv.Itoa(benchInstanceCPU)),
					corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", benchInstanceMem)),
					corev1.ResourcePods:   resource.MustParse("110"),
				},
			},
		})
		nc.StatusConditions().SetTrue(v1.ConditionTypeLaunched)
		nc.StatusConditions().SetTrue(v1.ConditionTypeRegistered)
		nc.StatusConditions().SetTrue(v1.ConditionTypeInitialized)
		nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		if spec.drifted {
			nc.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
			// Distinct drift times so the drift sort order is well defined.
			cond := nc.StatusConditions().Get(v1.ConditionTypeDrifted)
			cond.LastTransitionTime = metav1.NewTime(base.Add(time.Duration(i) * time.Second))
			nc.StatusConditions().Set(*cond)
		}
		n := test.NodeClaimLinkedNode(nc)
		n.Spec.Taints = lo.Reject(n.Spec.Taints, func(t corev1.Taint, _ int) bool { return t.MatchTaint(&v1.UnregisteredNoExecuteTaint) })
		n.Labels[v1.NodeRegisteredLabelKey] = "true"
		n.Labels[v1.NodeInitializedLabelKey] = "true"
		n.Status.Phase = corev1.NodeRunning
		n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			LastHeartbeatTime: metav1.NewTime(env.Clock.Now()), LastTransitionTime: metav1.NewTime(env.Clock.Now())}}
		nc.Status.NodeName = n.Name
		nodeClaims[i], nodes[i] = nc, n
	}
	parallel(spec.nodes, func(i int) {
		applyFast(ctx, env.Client, nodeClaims[i])
		// The apiserver's TaintNodesByCondition admission adds node.kubernetes.io/not-ready on create; drop it as
		// the kubelet/node lifecycle controller would once the node is Ready.
		taints := nodes[i].Spec.Taints
		applyFast(ctx, env.Client, nodes[i])
		nodes[i].Spec.Taints = taints
		Expect(env.Client.Update(ctx, nodes[i])).To(Succeed())
	})

	// Workloads: 20 deployments (ReplicaSets), pods spread round-robin across nodes.
	const numRS = 20
	rss := make([]*appsv1.ReplicaSet, numRS)
	for i := range numRS {
		rss[i] = test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rss[i])
	}
	total := spec.nodes * benchPodsPerNode
	pods := make([]*corev1.Pod, total)
	for i := range total {
		rs := rss[i%numRS]
		pods[i] = test.Pod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": rs.Name},
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID,
					Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true)}}},
			NodeName: nodes[i%spec.nodes].Name,
			Phase:    corev1.PodRunning,
			ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(spec.podCPU),
				corev1.ResourceMemory: resource.MustParse(spec.podMem),
			}},
		})
	}
	parallel(total, func(i int) { applyFast(ctx, env.Client, pods[i]) })

	// Wait for the informer cache, then hydrate cluster state as the informer controllers would.
	Eventually(func(g Gomega) {
		pl := &corev1.PodList{}
		g.Expect(b.client.List(ctx, pl)).To(Succeed())
		g.Expect(len(pl.Items)).To(BeNumerically(">=", total))
		nl := &corev1.NodeList{}
		g.Expect(b.client.List(ctx, nl)).To(Succeed())
		g.Expect(len(nl.Items)).To(BeNumerically(">=", spec.nodes))
		ncl := &v1.NodeClaimList{}
		g.Expect(b.client.List(ctx, ncl)).To(Succeed())
		g.Expect(len(ncl.Items)).To(BeNumerically(">=", spec.nodes))
	}, 5*time.Minute, 100*time.Millisecond).Should(Succeed())
	parallel(spec.nodes, func(i int) {
		ExpectReconcileSucceeded(b.ctx, b.ncCtrl, client.ObjectKeyFromObject(nodeClaims[i]))
		ExpectReconcileSucceeded(b.ctx, b.nodeCtrl, client.ObjectKeyFromObject(nodes[i]))
	})
	parallel(total, func(i int) {
		ExpectReconcileSucceeded(b.ctx, b.podCtrl, client.ObjectKeyFromObject(pods[i]))
	})
	Expect(b.cluster.Synced(b.ctx)).To(BeTrue())
	// Warm lazily-started informers (e.g. PDBs) so the first measured call doesn't include informer sync.
	_, _, err := disruption.GetCandidatesWithTotals(b.ctx, b.cluster, b.client, b.recorder, env.Clock, cloudProvider,
		func(context.Context, *disruption.Candidate) bool { return true }, disruption.GracefulDisruptionClass, b.queue, b.clusterCost)
	Expect(err).ToNot(HaveOccurred())
	return &builtCluster{nodePool: nodePool, nodeClaims: nodeClaims, nodes: nodes, pods: total, setup: time.Since(start)}
}

// launchReplacements simulates the cloud provider launching the replacement NodeClaims of cmds and the
// resulting nodes registering/initializing, so cluster state is Synced again. Not timed.
func (b *benchEnv) launchReplacements(cmds []*disruption.Command) {
	GinkgoHelper()
	for _, cmd := range cmds {
		for _, r := range cmd.Replacements {
			nc := &v1.NodeClaim{}
			Expect(env.Client.Get(ctx, client.ObjectKey{Name: r.Name}, nc)).To(Succeed())
			if nc.Status.ProviderID != "" {
				continue
			}
			nc, n := ExpectNodeClaimDeployedAndStateUpdated(b.ctx, env.Client, b.cluster, cloudProvider, nc)
			ncs, ns := []*v1.NodeClaim{nc}, []*corev1.Node{n}
			ExpectMakeNodeClaimsInitialized(b.ctx, env.Client, env.Clock, ncs...)
			ExpectMakeNodesInitialized(b.ctx, env.Client, env.Clock, ns...)
			b.cluster.UpdateNodeClaim(ncs[0])
			Expect(b.cluster.UpdateNode(b.ctx, ns[0])).To(Succeed())
		}
	}
}

// simStats sums the scheduler Solve() histogram (one sample per scheduling simulation) across all label values.
func simStats() (count uint64, sum float64) {
	mfs := lo.Must(crmetrics.Registry.Gather())
	for _, mf := range mfs {
		if mf.GetName() != "karpenter_scheduler_scheduling_duration_seconds" {
			continue
		}
		for _, m := range mf.Metric {
			count += m.GetHistogram().GetSampleCount()
			sum += m.GetHistogram().GetSampleSum()
		}
	}
	return count, sum
}

type timed struct {
	Ms   float64 `json:"ms"`
	Sims uint64  `json:"sims"`
	// SimMs is the time spent inside scheduler.Solve for those sims (excludes NewScheduler/setup cost).
	SimMs float64 `json:"simMs"`
}

func measure(f func()) timed {
	c0, s0 := simStats()
	start := time.Now()
	f()
	d := time.Since(start)
	c1, s1 := simStats()
	return timed{Ms: ms(d), Sims: c1 - c0, SimMs: (s1 - s0) * 1000}
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := slices.Clone(xs)
	sort.Float64s(s)
	return s[len(s)/2]
}

func emit(record map[string]any) {
	GinkgoHelper()
	out := lo.Must(json.Marshal(record))
	fmt.Fprintln(GinkgoWriter, string(out))
	if path := os.Getenv("KARPENTER_DISRUPTION_BENCH_OUT"); path != "" {
		f := lo.Must(os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)) //nolint:gosec // path is set by the person running the benchmark
		defer f.Close()
		_, _ = f.Write(append(out, '\n'))
	}
}

// phaseResult holds the per-phase timings of one runPhases call.
type phaseResult struct {
	Candidates     int   `json:"candidates"`
	GetCandidates  timed `json:"getCandidates"`
	BudgetMapping  timed `json:"budgetMapping"`
	ComputeCmds    timed `json:"computeCommands"`
	StartCommand   timed `json:"startCommand"`
	Commands       int   `json:"commands"`
	ValidationWait int64 `json:"validationWaitsElided"`
}

// runPhases replays the body of Controller.disrupt for one method with a timer on each phase. If start is true, the
// resulting commands are started on the queue.
func (b *benchEnv) runPhases(m disruption.Method, start bool) phaseResult {
	GinkgoHelper()
	var r phaseResult
	var candidates []*disruption.Candidate
	var totals map[string]disruption.NodePoolTotals
	var err error
	r.GetCandidates = measure(func() {
		candidates, totals, err = disruption.GetCandidatesWithTotals(b.ctx, b.cluster, b.client, b.recorder, env.Clock, cloudProvider, m.ShouldDisrupt, m.Class(), b.queue, b.clusterCost)
	})
	Expect(err).ToNot(HaveOccurred())
	r.Candidates = len(candidates)
	if len(candidates) == 0 {
		return r
	}
	if setter, ok := m.(disruption.NodePoolTotalsSetter); ok {
		setter.SetNodePoolTotals(totals)
	}
	var budgets map[string]int
	r.BudgetMapping = measure(func() {
		budgets, err = disruption.BuildDisruptionBudgetMapping(b.ctx, b.cluster, env.Clock, b.client, cloudProvider, b.recorder, m.Reason())
	})
	Expect(err).ToNot(HaveOccurred())
	var cmds []disruption.Command
	steps0 := b.clockSteps.Load()
	r.ComputeCmds = measure(func() {
		cmds, err = m.ComputeCommands(b.ctx, budgets, candidates...)
	})
	r.ValidationWait = b.clockSteps.Load() - steps0
	Expect(err).ToNot(HaveOccurred())
	cmds = lo.Filter(cmds, func(c disruption.Command, _ int) bool { return c.Decision() != disruption.NoOpDecision })
	r.Commands = len(cmds)
	if !start || len(cmds) == 0 {
		return r
	}
	r.StartCommand = measure(func() {
		errs := make([]error, len(cmds))
		workqueue.ParallelizeUntil(b.ctx, len(cmds), len(cmds), func(i int) {
			cmds[i].CreationTimestamp = env.Clock.Now()
			cmds[i].ID = uuid.New()
			cmds[i].Method = m
			errs[i] = b.queue.StartCommand(b.ctx, &cmds[i])
		})
		for _, e := range errs {
			Expect(e).ToNot(HaveOccurred())
		}
	})
	return r
}

func (b *benchEnv) evalDurations() map[string]float64 {
	out := map[string]float64{}
	mfs := lo.Must(crmetrics.Registry.Gather())
	for _, mf := range mfs {
		if mf.GetName() != "karpenter_voluntary_disruption_decision_evaluation_duration_seconds" {
			continue
		}
		for _, m := range mf.Metric {
			key := strings.Join(lo.Map(m.Label, func(l *prometheusmodel.LabelPair, _ int) string { return l.GetValue() }), "/")
			out[key] = m.GetHistogram().GetSampleSum() * 1000
		}
	}
	return out
}

// eventSummary counts recorder events by reason and (truncated) message, and resets the recorder.
func (b *benchEnv) eventSummary() map[string]int {
	out := map[string]int{}
	b.recorder.ForEachEvent(func(e events.Event) {
		msg := e.Message
		if len(msg) > 160 {
			msg = msg[:160]
		}
		out[e.Reason+": "+msg]++
	})
	b.recorder.Reset()
	return out
}

// startProfile writes a CPU profile of the measured section to $KARPENTER_DISRUPTION_BENCH_PROFILE/<name>.pprof.
func startProfile(name string) func() {
	dir := os.Getenv("KARPENTER_DISRUPTION_BENCH_PROFILE")
	if dir == "" {
		return func() {}
	}
	f := lo.Must(os.Create(filepath.Join(dir, name+".pprof"))) //nolint:gosec // dir is set by the person running the benchmark
	lo.Must0(pprof.StartCPUProfile(f))
	return func() {
		pprof.StopCPUProfile()
		_ = f.Close()
	}
}

// topEvents collapses per-object event messages to their reason and keeps a total per reason.
func topEvents(counts map[string]int) map[string]int {
	out := map[string]int{}
	for k, v := range counts {
		key := k
		if i := strings.Index(k, ": "); i > 0 {
			reason, msg := k[:i], k[i+2:]
			if j := strings.IndexAny(msg, ",/"); j > 0 {
				msg = msg[:j]
			}
			key = reason + ": " + msg
		}
		out[key] += v
	}
	return out
}

func benchSizes() []int {
	raw := os.Getenv("KARPENTER_DISRUPTION_BENCH_SIZES")
	if raw == "" {
		raw = "100,500,1000,2000"
	}
	return lo.Map(strings.Split(raw, ","), func(s string, _ int) int { return lo.Must(strconv.Atoi(strings.TrimSpace(s))) })
}

func benchIters(n int) int {
	if raw := os.Getenv("KARPENTER_DISRUPTION_BENCH_ITERS"); raw != "" {
		return lo.Must(strconv.Atoi(raw))
	}
	return n/5 + 5 // well past the 10% budget; the loop stops at the first iteration without a new command
}

var _ = Describe("Disruption Scale Benchmark", Label("benchmark"), func() {
	var b *benchEnv
	BeforeEach(func() {
		if os.Getenv("KARPENTER_DISRUPTION_BENCH") == "" {
			Skip("set KARPENTER_DISRUPTION_BENCH=1 to run the disruption scale benchmark (see scale_benchmark_test.go)")
		}
		b = newBenchEnv(ctx)
	})
	AfterEach(func() {
		if b != nil {
			b.stop()
		}
	})

	for _, n := range benchSizes() {
		It(fmt.Sprintf("drift N=%d", n), func() {
			bc := b.buildCluster(clusterSpec{nodes: n, podCPU: "3", podMem: "5Gi", drifted: true, budget: "10%", capacity: v1.CapacityTypeSpot, consolAftr: "0s"})
			emit(map[string]any{"scenario": "setup", "nodes": n, "pods": bc.pods, "setupMs": ms(bc.setup)})

			// Candidate build cost, cached vs direct (uncached API server reads, as the rest of this suite uses).
			drift := b.method(isType[*disruption.Drift])
			var cached, direct []float64
			for range 5 {
				cached = append(cached, measure(func() {
					_, _, err := disruption.GetCandidatesWithTotals(b.ctx, b.cluster, b.client, b.recorder, env.Clock, cloudProvider, drift.ShouldDisrupt, drift.Class(), b.queue, b.clusterCost)
					Expect(err).ToNot(HaveOccurred())
				}).Ms)
			}
			for range 3 {
				direct = append(direct, measure(func() {
					_, _, err := disruption.GetCandidatesWithTotals(b.ctx, b.cluster, env.Client, b.recorder, env.Clock, cloudProvider, drift.ShouldDisrupt, drift.Class(), b.queue, b.clusterCost)
					Expect(err).ToNot(HaveOccurred())
				}).Ms)
			}
			var budgetMs []float64
			for range 5 {
				budgetMs = append(budgetMs, measure(func() {
					_, err := disruption.BuildDisruptionBudgetMapping(b.ctx, b.cluster, env.Clock, b.client, cloudProvider, b.recorder, v1.DisruptionReasonDrifted)
					Expect(err).ToNot(HaveOccurred())
				}).Ms)
			}
			emit(map[string]any{"scenario": "candidates", "nodes": n, "getCandidatesCachedMedianMs": median(cached), "getCandidatesDirectMedianMs": median(direct), "budgetMappingMedianMs": median(budgetMs)})

			// One instrumented drift pass (phase breakdown), including StartCommand. The first pass is a warmup.
			warm := b.runPhases(drift, false)
			emit(map[string]any{"scenario": "drift-phases-warmup", "nodes": n, "result": warm})
			p := b.runPhases(drift, true)
			emit(map[string]any{"scenario": "drift-phases", "nodes": n, "result": p})
			b.launchReplacements(b.queue.GetCommands())

			// Full Controller.Reconcile iterations until Drift stops producing commands (budget full), then that
			// fall-through iteration (where MultiNode/SingleNode consolidation run) is recorded too. Note the
			// budget grows as replacements join: allowed = 10% of (N + in-flight replacements).
			iters := benchIters(n)
			stopProfile := startProfile(fmt.Sprintf("drift-reconcile-%d", n))
			var iterMs []float64
			prevReplacements := 0
			fallThrough := -1.0
			// The test recorder retains every event (the real one dedupes/rate-limits), so drain it every iteration
			// to keep heap growth out of the timings.
			eventCounts := b.eventSummary()
			for i := range iters {
				before := len(b.queue.GetCommands())
				ev0 := b.evalDurations()
				steps0 := b.clockSteps.Load()
				var res reconciler.Result
				t := measure(func() {
					var err error
					res, err = b.ctrl.Reconcile(b.ctx)
					Expect(err).ToNot(HaveOccurred())
				})
				cmds := b.queue.GetCommands()
				syncedAfter := b.cluster.Synced(b.ctx)
				ev1 := b.evalDurations()
				perMethod := map[string]float64{}
				methodSum := 0.0
				for k, v := range ev1 {
					if d := v - ev0[k]; d > 0 {
						perMethod[k] = d
						methodSum += d
					}
				}
				replacements := lo.SumBy(cmds, func(c *disruption.Command) int { return len(c.Replacements) })
				emit(map[string]any{"scenario": "drift-reconcile", "nodes": n, "iter": i, "inFlight": before, "ms": t.Ms, "sims": t.Sims, "simMs": t.SimMs,
					"newCommands": len(cmds) - before, "newReplacements": replacements - prevReplacements, "syncedAfter": syncedAfter,
					"requeueAfterMs": ms(res.RequeueAfter), "perMethodMs": perMethod, "outsideMethodsMs": t.Ms - methodSum,
					"validationWaitsElided": b.clockSteps.Load() - steps0})
				prevReplacements = replacements
				for k, v := range b.eventSummary() {
					eventCounts[k] += v
				}
				iterMs = append(iterMs, t.Ms)
				b.launchReplacements(cmds)
				if len(cmds) == before {
					fallThrough = t.Ms
					break
				}
			}
			stopProfile()
			emit(map[string]any{"scenario": "drift-summary", "nodes": n, "iterations": len(iterMs), "medianIterMs": median(iterMs),
				"firstIterMs": iterMs[0], "lastFillIterMs": iterMs[max(0, len(iterMs)-2)], "fallThroughIterMs": fallThrough,
				"sumIterMs": lo.Sum(iterMs), "commandsStarted": len(b.queue.GetCommands()), "events": topEvents(eventCounts)})
		})
	}

	for _, n := range benchSizes() {
		It(fmt.Sprintf("consolidation underutilized N=%d", n), func() {
			// ~16% CPU utilization: lots of consolidation opportunity.
			bc := b.buildCluster(clusterSpec{nodes: n, podCPU: "500m", podMem: "1Gi", budget: "10%", capacity: v1.CapacityTypeSpot, consolAftr: "0s"})
			emit(map[string]any{"scenario": "setup-consolidation", "nodes": n, "pods": bc.pods, "setupMs": ms(bc.setup)})
			multi := b.method(isType[*disruption.MultiNodeConsolidation])
			single := b.method(isType[*disruption.SingleNodeConsolidation])
			b.cluster.MarkUnconsolidated()
			pm := b.runPhases(multi, false)
			emit(map[string]any{"scenario": "multi-underutilized", "nodes": n, "result": pm})
			b.cluster.MarkUnconsolidated()
			ps := b.runPhases(single, false)
			emit(map[string]any{"scenario": "single-underutilized", "nodes": n, "result": ps})
			// Nop-validator variants isolate ComputeCommands from validation work.
			c := disruption.MakeConsolidation(env.Clock, b.cluster, b.client, b.prov, cloudProvider, b.recorder, b.queue)
			b.cluster.MarkUnconsolidated()
			pmn := b.runPhases(disruption.NewMultiNodeConsolidation(c, disruption.WithValidator(NopValidator{})), false)
			emit(map[string]any{"scenario": "multi-underutilized-nopvalidator", "nodes": n, "result": pmn})
			b.cluster.MarkUnconsolidated()
			psn := b.runPhases(disruption.NewSingleNodeConsolidation(c, disruption.WithValidator(NopValidator{})), false)
			emit(map[string]any{"scenario": "single-underutilized-nopvalidator", "nodes": n, "result": psn, "events": b.eventSummary()})
		})
	}

	for _, n := range benchSizes() {
		It(fmt.Sprintf("consolidation packed N=%d", n), func() {
			// ~94% CPU utilization on spot: no consolidation is possible, which is SingleNode's worst case (one sim per candidate).
			bc := b.buildCluster(clusterSpec{nodes: n, podCPU: "3", podMem: "5Gi", budget: "10%", capacity: v1.CapacityTypeSpot, consolAftr: "0s"})
			emit(map[string]any{"scenario": "setup-packed", "nodes": n, "pods": bc.pods, "setupMs": ms(bc.setup)})
			c := disruption.MakeConsolidation(env.Clock, b.cluster, b.client, b.prov, cloudProvider, b.recorder, b.queue)
			b.cluster.MarkUnconsolidated()
			pmn := b.runPhases(disruption.NewMultiNodeConsolidation(c, disruption.WithValidator(NopValidator{})), false)
			emit(map[string]any{"scenario": "multi-packed", "nodes": n, "result": pmn})
			b.cluster.MarkUnconsolidated()
			stopProfile := startProfile(fmt.Sprintf("single-packed-%d", n))
			psn := b.runPhases(disruption.NewSingleNodeConsolidation(c, disruption.WithValidator(NopValidator{})), false)
			stopProfile()
			emit(map[string]any{"scenario": "single-packed", "nodes": n, "result": psn})
			// And one full Reconcile with nothing to do (all methods run, all return nothing).
			b.cluster.MarkUnconsolidated()
			ev0 := b.evalDurations()
			t := measure(func() {
				_, err := b.ctrl.Reconcile(b.ctx)
				Expect(err).ToNot(HaveOccurred())
			})
			ev1 := b.evalDurations()
			perMethod := map[string]float64{}
			for k, v := range ev1 {
				if d := v - ev0[k]; d > 0 {
					perMethod[k] = d
				}
			}
			emit(map[string]any{"scenario": "reconcile-packed-noop", "nodes": n, "ms": t.Ms, "sims": t.Sims, "simMs": t.SimMs, "perMethodMs": perMethod, "commands": len(b.queue.GetCommands()), "events": b.eventSummary()})
		})
	}
})
