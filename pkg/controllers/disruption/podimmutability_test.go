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

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/dynamicresources/deviceallocation"
	"sigs.k8s.io/karpenter/pkg/controllers/node/health"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	"sigs.k8s.io/karpenter/pkg/state/prediction"
	"sigs.k8s.io/karpenter/pkg/state/virtualpods"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// Candidates hold the informer cache's own pods (StateNode.ValidatePodsDisruptable lists them without a deep copy), so
// nothing in disruption may modify them. This spec runs disruption on an informer-cache-backed client, as the operator
// does (the rest of this suite reads from the API server, where every List returns fresh objects), through every
// method's candidate build, budgets and ComputeCommands (simulations with injected default topology spread constraints
// and preference relaxation, emptiness and consolidation validation), full Reconciles that start commands, and direct
// simulations, then requires every pod object in the informer's store to be unchanged. -race can't see such a write: it
// happens on one goroutine.
var _ = Describe("Pod Immutability", func() {
	It("never modifies a pod owned by the informer cache", func() {
		w := newImmutabilityWorld()
		defer w.stop()
		before := w.fingerprint()
		Expect(len(before)).To(BeNumerically(">=", 20))

		// The build really shares the cache's pods, so the comparison below guards something.
		cands, err := disruption.GetCandidates(w.ctx, w.cluster, w.client, w.recorder, env.Clock, cloudProvider,
			func(context.Context, *disruption.Candidate) bool { return true }, disruption.GracefulDisruptionClass, w.queue)
		Expect(err).ToNot(HaveOccurred())
		shared := 0
		for _, c := range cands {
			for _, p := range c.ReschedulablePods() {
				Expect(w.cached(p.UID)).ToNot(BeNil())
				Expect(sharesMemory(p, w.cached(p.UID))).To(BeTrue(), "candidate pod %s is not the cache's", p.Name)
				shared++
			}
		}
		Expect(shared).To(BeNumerically(">=", 15))

		// Every method, as Controller.disrupt runs it, without starting commands.
		ran := map[string]int{}
		for _, m := range w.methods {
			name := fmt.Sprintf("%T", m)
			candidates, totals, err := disruption.GetCandidatesWithTotals(w.ctx, w.cluster, w.client, w.recorder, env.Clock, cloudProvider,
				m.ShouldDisrupt, m.Class(), w.queue, w.clusterCost)
			Expect(err).ToNot(HaveOccurred())
			Expect(candidates).ToNot(BeEmpty(), "%s has no candidates", name)
			if setter, ok := m.(disruption.NodePoolTotalsSetter); ok {
				setter.SetNodePoolTotals(totals)
			}
			budgets, err := disruption.BuildDisruptionBudgetMapping(w.ctx, w.cluster, env.Clock, w.client, cloudProvider, w.recorder, m.Reason())
			Expect(err).ToNot(HaveOccurred())
			cmds, err := m.ComputeCommands(w.ctx, budgets, candidates...)
			Expect(err).ToNot(HaveOccurred())
			ran[name] = len(lo.Filter(cmds, func(c disruption.Command, _ int) bool { return c.Decision() != disruption.NoOpDecision }))
		}
		fmt.Fprintf(GinkgoWriter, "commands per method: %v, validation clock steps: %d\n", ran, w.clockSteps())
		Expect(ran).To(HaveLen(6))
		for _, name := range []string{"*disruption.Repair", "*disruption.Emptiness", "*disruption.Drift", "*disruption.StaticDrift"} {
			Expect(ran[name]).To(BeNumerically(">", 0), "%s computed no command", name)
		}
		Expect(ran["*disruption.MultiNodeConsolidation"] + ran["*disruption.SingleNodeConsolidation"]).To(BeNumerically(">", 0))
		Expect(w.clockSteps()).To(BeNumerically(">", 0), "no validation ran")

		// The production loop: shared snapshots, commands started on the queue.
		for range 3 {
			_, err := w.ctrl.Reconcile(w.ctx)
			Expect(err).ToNot(HaveOccurred())
		}
		w.expectUnchanged(before)

		// The scheduler gets its own copy of every candidate pod: the pinned pod can't be placed, so the scheduler
		// returns the pod object it was given as the key of its error. Default topology spread constraints were
		// injected into that object, and it shares no memory with the cache's pod.
		pinned := w.candidate(w.pinnedNode)
		results, err := disruption.SimulateScheduling(w.ctx, w.client, w.cluster, w.prov, env.Clock, w.recorder, nil, disruption.SimulationOptions{}, pinned)
		Expect(err).ToNot(HaveOccurred())
		given, ok := lo.Find(lo.Keys(results.PodErrors), func(p *corev1.Pod) bool { return p.UID == w.pinnedPod })
		Expect(ok).To(BeTrue(), "pinned pod scheduled")
		Expect(given.Spec.TopologySpreadConstraints).ToNot(BeEmpty(), "no default constraints were injected")
		Expect(sharesMemory(given, w.cached(w.pinnedPod))).To(BeFalse(), "the scheduler was given the informer cache's pod")

		w.expectUnchanged(before)
	})
})

type immutabilityWorld struct {
	ctx         context.Context
	cancel      context.CancelFunc
	client      client.Client
	store       toolscache.Store
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

	pinnedNode *corev1.Node
	pinnedPod  types.UID

	steps    atomic.Int64
	stopStep chan struct{}
	stepDone chan struct{}
}

type podFingerprint struct {
	obj  *corev1.Pod // the informer store's object
	copy *corev1.Pod
	json []byte
}

// newImmutabilityWorld builds the disruption stack on an informer-cache-backed client and a cluster on which every
// method computes a command.
//
//nolint:gocyclo
func newImmutabilityWorld() *immutabilityWorld {
	GinkgoHelper()
	opts := test.Options(test.OptionsFields{
		FeatureGates: test.FeatureGates{NodeRepair: lo.ToPtr(true)},
		SchedulerConfig: &options.SchedulerConfiguration{PodTopologySpread: &options.PodTopologySpreadConfig{
			DefaultConstraints: []corev1.TopologySpreadConstraint{{MaxSkew: 1, TopologyKey: corev1.LabelTopologyZone, WhenUnsatisfiable: corev1.ScheduleAnyway}},
		}},
	})
	wctx, cancel := context.WithCancel(injection.WithControllerName(options.ToContext(ctx, opts), "disruption"))
	c := lo.Must(cache.New(env.Config, cache.Options{Scheme: scheme.Scheme}))
	lo.Must0(c.IndexField(wctx, &corev1.Pod{}, "spec.nodeName", func(o client.Object) []string { return []string{o.(*corev1.Pod).Spec.NodeName} }))
	lo.Must0(c.IndexField(wctx, &corev1.Node{}, "spec.providerID", func(o client.Object) []string { return []string{o.(*corev1.Node).Spec.ProviderID} }))
	lo.Must0(c.IndexField(wctx, &v1.NodeClaim{}, "status.providerID", func(o client.Object) []string { return []string{o.(*v1.NodeClaim).Status.ProviderID} }))
	podInformer := lo.Must(c.GetInformer(wctx, &corev1.Pod{}))
	go func() { _ = c.Start(wctx) }()
	Expect(c.WaitForCacheSync(wctx)).To(BeTrue())
	kc := &test.CacheSyncingClient{Client: lo.Must(client.New(env.Config, client.Options{Scheme: scheme.Scheme, Cache: &client.CacheOptions{Reader: c}}))}

	w := &immutabilityWorld{ctx: wctx, cancel: cancel, client: kc, store: podInformer.(toolscache.SharedIndexInformer).GetStore(),
		recorder: test.NewEventRecorder(), stopStep: make(chan struct{}), stepDone: make(chan struct{})}
	w.clusterCost = cost.NewClusterCost(wctx, cloudProvider, kc)
	ExpectSingletonReconciled(wctx, informer.NewPricingController(kc, cloudProvider, w.clusterCost))
	w.cluster = state.NewCluster(env.Clock, kc, cloudProvider, state.WithRepairPolicyMatcher(lo.Must(health.NewRepairPolicyMatcher(wctx, cloudProvider))))
	w.nodeCtrl = informer.NewNodeController(kc, w.cluster)
	w.ncCtrl = informer.NewNodeClaimController(kc, cloudProvider, w.cluster, w.clusterCost)
	w.podCtrl = informer.NewPodController(kc, w.cluster)
	st := prediction.NewStore(kc)
	metaScheme := runtime.NewScheme()
	Expect(metav1.AddMetaToScheme(metaScheme)).To(Succeed())
	Expect(st.RegisterTargetKinds(wctx, &informertest.FakeInformers{Scheme: metaScheme}, prediction.SupportedTargets.UnsortedList()...)).To(Succeed())
	w.prov = provisioning.NewProvisioner(kc, w.recorder, cloudProvider, w.cluster, env.Clock, deviceallocation.NewController(kc), virtualpods.NewVirtualPodCache(kc), st)
	w.queue = disruption.NewQueue(kc, w.recorder, w.cluster, env.Clock, w.prov)
	// The production methods, with the real emptiness and consolidation validators.
	w.methods = disruption.NewMethods(wctx, env.Clock, w.cluster, kc, w.prov, cloudProvider, w.recorder, w.queue)
	Expect(w.methods).To(HaveLen(6))
	w.ctrl = disruption.NewController(wctx, env.Clock, kc, w.prov, cloudProvider, w.recorder, w.cluster, w.queue, w.clusterCost, disruption.WithMethods(w.methods...))
	go w.stepClock()

	dynamic := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "immutability"}, Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
		ConsolidateAfter: v1.MustParseNillableDuration("0s"), ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
		Budgets: []v1.Budget{{Nodes: "100%"}}}}})
	static := test.StaticNodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "immutability-static"}, Spec: v1.NodePoolSpec{
		Replicas: lo.ToPtr(int64(1)), Disruption: v1.Disruption{Budgets: []v1.Budget{{Nodes: "100%"}}}}})
	ExpectApplied(wctx, kc, dynamic, static)
	ExpectReconcileSucceeded(wctx, informer.NewNodePoolController(kc, cloudProvider, w.cluster, w.clusterCost), client.ObjectKeyFromObject(dynamic))
	ExpectReconcileSucceeded(wctx, informer.NewNodePoolController(kc, cloudProvider, w.cluster, w.clusterCost), client.ObjectKeyFromObject(static))
	// Pods of app=web get default topology spread constraints from this Service's selector, and their PDB is checked.
	ExpectApplied(wctx, kc, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default"},
		Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "web"}, Ports: []corev1.ServicePort{{Port: 80}}}})
	ExpectApplied(wctx, kc, test.PodDisruptionBudget(test.PDBOptions{Labels: map[string]string{"app": "web"}, MaxUnavailable: lo.ToPtr(intstr.FromString("100%")),
		Status: &policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 100, CurrentHealthy: 100, DesiredHealthy: 0, ExpectedPods: 100}}))

	it := mostExpensiveInstance
	of := mostExpensiveOffering
	newNode := func(np *v1.NodePool, mutate func(*v1.NodeClaim, *corev1.Node)) *corev1.Node {
		nc, node := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            np.Name,
				corev1.LabelInstanceTypeStable: it.Name,
				v1.CapacityTypeLabelKey:        of.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
				corev1.LabelTopologyZone:       of.Requirements.Get(corev1.LabelTopologyZone).Any(),
			}},
			Status: v1.NodeClaimStatus{Allocatable: it.Capacity},
		})
		mutate(nc, node)
		ExpectApplied(wctx, kc, nc, node)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(wctx, kc, env.Clock, w.nodeCtrl, w.ncCtrl, []*corev1.Node{node}, []*v1.NodeClaim{nc})
		return node
	}
	consolidatable := func(nc *v1.NodeClaim, _ *corev1.Node) { nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable) }
	drifted := func(nc *v1.NodeClaim, _ *corev1.Node) { nc.StatusConditions().SetTrue(v1.ConditionTypeDrifted) }
	newPod := func(node *corev1.Node, i int, po test.PodOptions) *corev1.Pod {
		po.ObjectMeta = metav1.ObjectMeta{
			Name:        fmt.Sprintf("%s-%d", node.Name, i),
			Labels:      lo.Assign(map[string]string{"app": "web", "tier": fmt.Sprint(i % 2)}, po.Labels),
			Annotations: map[string]string{"example.com/owner": "immutability"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web", UID: "web-rs",
				Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true)}},
		}
		po.NodeName = node.Name
		po.Phase = corev1.PodRunning
		po.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		po.ResourceRequirements = corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("64Mi")}}
		// A preference no node satisfies, which the scheduler relaxes.
		po.NodePreferences = []corev1.NodeSelectorRequirement{{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"no-such-zone"}}}
		p := test.Pod(po)
		ExpectApplied(wctx, kc, p)
		ExpectReconcileSucceeded(wctx, w.podCtrl, client.ObjectKeyFromObject(p))
		return p
	}
	withPods := func(node *corev1.Node, n int) {
		for i := range n {
			po := test.PodOptions{}
			if i%3 == 2 { // the pod's own constraint: not defaulted, relaxed instead
				po.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{MaxSkew: 1, TopologyKey: corev1.LabelHostname, WhenUnsatisfiable: corev1.ScheduleAnyway,
					LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}}}
			}
			newPod(node, i, po)
		}
	}

	newNode(dynamic, consolidatable) // empty: Emptiness
	for range 2 {                    // Drift
		withPods(newNode(dynamic, drifted), 3)
	}
	// Underutilized: MultiNode, SingleNode. These pods aren't spread (no Service selects them), so they fit elsewhere.
	for range 3 {
		node := newNode(dynamic, consolidatable)
		for i := range 2 {
			newPod(node, i, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "batch"}}})
		}
	}
	// Unhealthy past the fake repair policy's toleration (the clock is stepped below): Repair.
	unhealthy := ExpectExists(wctx, kc, newNode(dynamic, func(*v1.NodeClaim, *corev1.Node) {}))
	unhealthy.Status.Conditions = append(unhealthy.Status.Conditions, corev1.NodeCondition{Type: "BadNode", Status: corev1.ConditionFalse,
		LastTransitionTime: metav1.Time{Time: env.Clock.Now()}})
	ExpectApplied(wctx, kc, unhealthy)
	ExpectReconcileSucceeded(wctx, w.nodeCtrl, client.ObjectKeyFromObject(unhealthy))
	withPods(unhealthy, 3)
	withPods(newNode(static, drifted), 3) // StaticDrift
	// A pod that only fits its own node: every simulation that moves it reports it, as the object it was given.
	w.pinnedNode = newNode(dynamic, func(nc *v1.NodeClaim, node *corev1.Node) {
		consolidatable(nc, node)
		node.Labels["example.com/pinned"] = "true"
	})
	withPods(w.pinnedNode, 2)
	w.pinnedPod = newPod(w.pinnedNode, 9, test.PodOptions{NodeRequirements: []corev1.NodeSelectorRequirement{
		{Key: "example.com/pinned", Operator: corev1.NodeSelectorOpIn, Values: []string{"true"}}}}).UID
	env.Clock.Step(31 * time.Minute)
	w.cluster.MarkUnconsolidated()
	Expect(w.cluster.Synced(wctx)).To(BeTrue())
	return w
}

// stepClock elides the validators' 15s waits on the fake clock.
func (w *immutabilityWorld) stepClock() {
	defer close(w.stepDone)
	for {
		select {
		case <-w.stopStep:
			return
		case <-time.After(time.Millisecond):
		}
		if env.Clock.HasWaiters() {
			env.Clock.Step(15 * time.Second)
			w.steps.Add(1)
		}
	}
}

func (w *immutabilityWorld) clockSteps() int64 { return w.steps.Load() }

func (w *immutabilityWorld) stop() {
	close(w.stopStep)
	<-w.stepDone // the suite resets env.Clock after this spec
	w.cancel()
}

// fingerprint is every pod object in the informer's store, by UID, with a deep copy and serialization of it.
func (w *immutabilityWorld) fingerprint() map[types.UID]podFingerprint {
	out := map[types.UID]podFingerprint{}
	for _, o := range w.store.List() {
		p := o.(*corev1.Pod)
		out[p.UID] = podFingerprint{obj: p, copy: p.DeepCopy(), json: lo.Must(json.Marshal(p))}
	}
	return out
}

// expectUnchanged requires every pod object in the informer's store to be the object fingerprinted in before, with
// the same contents, field for field and byte for byte.
func (w *immutabilityWorld) expectUnchanged(before map[types.UID]podFingerprint) {
	GinkgoHelper()
	after := w.fingerprint()
	Expect(lo.Keys(after)).To(ConsistOf(lo.Keys(before)))
	for uid, b := range before {
		a := after[uid]
		Expect(a.obj).To(BeIdenticalTo(b.obj), "pod %s was replaced in the cache during the run, so it wasn't checked", b.copy.Name)
		Expect(reflect.DeepEqual(a.obj, b.copy)).To(BeTrue(), "pod %s in the informer cache was modified", b.copy.Name)
		Expect(string(lo.Must(json.Marshal(a.obj)))).To(Equal(string(b.json)), "pod %s in the informer cache was modified", b.copy.Name)
	}
}

func (w *immutabilityWorld) cached(uid types.UID) *corev1.Pod {
	p, _ := lo.Find(w.store.List(), func(o any) bool { return o.(*corev1.Pod).UID == uid })
	return lo.Ternary(p == nil, nil, p.(*corev1.Pod))
}

func (w *immutabilityWorld) candidate(node *corev1.Node) *disruption.Candidate {
	GinkgoHelper()
	cands, err := disruption.GetCandidates(w.ctx, w.cluster, w.client, w.recorder, env.Clock, cloudProvider,
		func(_ context.Context, c *disruption.Candidate) bool { return c.Name() == node.Name }, disruption.GracefulDisruptionClass, w.queue)
	Expect(err).ToNot(HaveOccurred())
	Expect(cands).To(HaveLen(1))
	return cands[0]
}

// sharesMemory reports whether a and b share the backing memory of their labels, annotations, managed fields,
// containers or affinity, i.e. whether writing through a can change b.
func sharesMemory(a, b *corev1.Pod) bool {
	samePtr := func(x, y any) bool {
		vx, vy := reflect.ValueOf(x), reflect.ValueOf(y)
		return !vx.IsNil() && !vy.IsNil() && vx.UnsafePointer() == vy.UnsafePointer()
	}
	return samePtr(a.Labels, b.Labels) || samePtr(a.Annotations, b.Annotations) || samePtr(a.ManagedFields, b.ManagedFields) ||
		samePtr(a.Spec.Containers, b.Spec.Containers) || samePtr(a.Spec.Affinity, b.Spec.Affinity)
}
