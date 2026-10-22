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
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
)

// Differential test for CandidateSnapshot: for random cluster states, every method's candidates from a shared
// snapshot must equal what one GetCandidatesWithTotals per method returned before snapshots were shared (the legacy
// oracle in candidatesnapshot_export_test.go). Compared per method: candidate ids and order, every Candidate field
// (TerminationGracePeriod, RepairPolicyResult, DisruptionCost, hasPodBlockers, ...), the StateNode, the events
// published (DisruptionBlocked, Unconsolidatable, pod do-not-disrupt events), and NodePoolTotals; per node and class,
// the rejection reason. Each method's Candidates are overwritten after use, so a method that writes to its Candidates
// (Repair) can't leak into a later one.
//
//	KARPENTER_SNAPSHOT_DIFF_SEEDS=100 KARPENTER_SNAPSHOT_DIFF_STEPS=40 [KARPENTER_SNAPSHOT_DIFF_FIRST_SEED=1] \
//	go test ./pkg/controllers/disruption/ --ginkgo.focus="Candidate Snapshot Differential" -v
var _ = Describe("Candidate Snapshot Differential", func() {
	seeds := snapshotDiffEnv("KARPENTER_SNAPSHOT_DIFF_SEEDS", 6)
	steps := snapshotDiffEnv("KARPENTER_SNAPSHOT_DIFF_STEPS", 20)
	firstSeed := snapshotDiffEnv("KARPENTER_SNAPSHOT_DIFF_FIRST_SEED", 1)
	for seed := firstSeed; seed < firstSeed+seeds; seed++ {
		It(fmt.Sprintf("matches one candidate build per method (seed=%d steps=%d)", seed, steps), func() {
			w := newSnapshotWorld(seed)
			w.check("initial")
			for range steps {
				w.check(w.randomOp())
			}
			counts := lo.Keys(w.nonEmpty)
			sort.Strings(counts)
			fmt.Fprintf(GinkgoWriter, "seed=%d checks=%d builds(controller rule)=%d methodRuns=%d nonEmpty=%v ops=%v\n",
				seed, w.checks, w.builds, w.methodRuns, lo.Map(counts, func(k string, _ int) string { return fmt.Sprintf("%s:%d", k, w.nonEmpty[k]) }), w.opCounts)
		})
	}
	It("scores candidates at the snapshot's now, however late a method reads them (#3406)", func() {
		w := newSnapshotWorld(1)
		s, err := w.build()
		Expect(err).ToNot(HaveOccurred())
		fields := func(cs []*disruption.Candidate) []disruption.CandidateFields {
			return lo.Map(cs, func(c *disruption.Candidate, _ int) disruption.CandidateFields { return c.Fields() })
		}
		before := fields(s.Candidates(ctx, disruption.GracefulDisruptionClass))
		Expect(before).ToNot(BeEmpty())
		env.Clock.Step(30 * time.Minute)
		Expect(fields(s.Candidates(ctx, disruption.GracefulDisruptionClass))).To(Equal(before))
		// A build at the new time scores differently (lifetime decay), so the comparison above is sensitive to now.
		fresh, _, err := disruption.LegacyGetCandidatesWithTotals(ctx, sortedNodes, env.Client, w.rec, env.Clock, cloudProvider,
			func(context.Context, *disruption.Candidate) bool { return true }, disruption.GracefulDisruptionClass, queue, clusterCost)
		Expect(err).ToNot(HaveOccurred())
		Expect(fields(fresh)).ToNot(Equal(before))
	})
})

func snapshotDiffEnv(name string, def int) int {
	if raw := os.Getenv(name); raw != "" {
		return lo.Must(strconv.Atoi(raw))
	}
	return def
}

// orderedRecorder keeps every published event, in order.
type orderedRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *orderedRecorder) Publish(evts ...events.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range evts {
		involved := "<nil>"
		if o, err := meta.Accessor(e.InvolvedObject); err == nil && !reflect.ValueOf(e.InvolvedObject).IsNil() {
			involved = fmt.Sprintf("%T/%s/%s", e.InvolvedObject, o.GetNamespace(), o.GetName())
		}
		r.events = append(r.events, fmt.Sprintf("%s %s %s %q dedupe=%v/%s", involved, e.Type, e.Reason, e.Message, e.DedupeValues, e.DedupeTimeout))
	}
}

func (r *orderedRecorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.events
	r.events = nil
	return out
}

type snapshotWorld struct {
	rng        *rand.Rand
	rec        *orderedRecorder
	podCtrl    *informer.PodController
	methods    []disruption.Method
	nodePools  []*v1.NodePool
	deletedNPs map[string]bool
	nodeClaims []*v1.NodeClaim
	nodes      []*corev1.Node
	pods       map[types.NamespacedName]bool
	pdbs       map[types.NamespacedName]bool
	its        []*cloudprovider.InstanceType
	removedIT  *cloudprovider.InstanceType

	checks, builds, methodRuns int
	nonEmpty                   map[string]int
	opCounts                   map[string]int
}

func newSnapshotWorld(seed int) *snapshotWorld {
	GinkgoHelper()
	ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodeRepair: lo.ToPtr(true)}}))
	w := &snapshotWorld{rng: rand.New(rand.NewSource(int64(seed))), rec: &orderedRecorder{}, deletedNPs: map[string]bool{},
		pods: map[types.NamespacedName]bool{}, pdbs: map[types.NamespacedName]bool{}, nonEmpty: map[string]int{}, opCounts: map[string]int{}}
	for i, cpu := range []int{2, 4, 8, 16} {
		var offerings []cloudprovider.Offering
		for _, zone := range []string{"test-zone-1", "test-zone-2"} {
			for _, ct := range []string{v1.CapacityTypeSpot, v1.CapacityTypeOnDemand} {
				offerings = append(offerings, cloudprovider.Offering{
					Available:    true,
					Requirements: scheduling.NewLabelRequirements(map[string]string{v1.CapacityTypeLabelKey: ct, corev1.LabelTopologyZone: zone}),
					Price:        float64(cpu) * lo.Ternary(ct == v1.CapacityTypeSpot, 0.3, 1.0) * (1 + 0.01*float64(i)),
				})
			}
		}
		w.its = append(w.its, fake.NewInstanceType(fmt.Sprintf("snap-%d-cpu", cpu),
			fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(strconv.Itoa(cpu)), corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", cpu*2)), corev1.ResourcePods: resource.MustParse("110")}),
			fake.WithOfferings(offerings...)))
	}
	cloudProvider.InstanceTypes = append([]*cloudprovider.InstanceType(nil), w.its...)
	w.podCtrl = informer.NewPodController(env.Client, cluster)
	w.methods = disruption.NewMethods(ctx, env.Clock, cluster, env.Client, prov, cloudProvider, w.rec, queue)
	Expect(w.methods).To(HaveLen(6))

	w.nodePools = []*v1.NodePool{
		test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "snap-underutilized"}, Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
			ConsolidateAfter: v1.MustParseNillableDuration("0s"), ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
			Budgets: []v1.Budget{{Nodes: "50%"}}}}}),
		test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "snap-empty"}, Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
			ConsolidateAfter: v1.MustParseNillableDuration("30s"), ConsolidationPolicy: v1.ConsolidationPolicyWhenEmpty,
			Budgets: []v1.Budget{{Nodes: "100%"}}}}}),
		test.StaticNodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "snap-static"}, Spec: v1.NodePoolSpec{Replicas: lo.ToPtr(int64(3)), Disruption: v1.Disruption{
			Budgets: []v1.Budget{{Nodes: "1"}}}}}),
	}
	for _, np := range w.nodePools {
		ExpectApplied(ctx, env.Client, np)
	}
	for i := range 8 + w.rng.Intn(6) {
		np := w.nodePools[i%len(w.nodePools)]
		it := w.its[w.rng.Intn(len(w.its))]
		itName := lo.Ternary(w.rng.Intn(10) == 0, "snap-unknown-type", it.Name)
		of := it.Offerings[w.rng.Intn(len(it.Offerings))]
		labels := map[string]string{
			v1.NodePoolLabelKey:            np.Name,
			corev1.LabelInstanceTypeStable: itName,
			v1.CapacityTypeLabelKey:        of.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
			corev1.LabelTopologyZone:       of.Requirements.Get(corev1.LabelTopologyZone).Any(),
		}
		if w.rng.Intn(12) == 0 {
			delete(labels, corev1.LabelTopologyZone)
		}
		spec := v1.NodeClaimSpec{ExpireAfter: v1.MustParseNillableDuration(lo.Ternary(w.rng.Intn(2) == 0, "2h", "Never"))}
		if w.rng.Intn(3) == 0 {
			spec.TerminationGracePeriod = &metav1.Duration{Duration: 5 * time.Minute}
		}
		nc, node := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: labels},
			Spec:       spec,
			Status:     v1.NodeClaimStatus{Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("32"), corev1.ResourcePods: resource.MustParse("110")}},
		})
		if w.rng.Intn(2) == 0 {
			nc.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
		}
		if w.rng.Intn(2) == 0 {
			nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		}
		if w.rng.Intn(4) == 0 { // unhealthy past the fake repair policy's 30m toleration
			node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{Type: "BadNode", Status: corev1.ConditionFalse,
				LastTransitionTime: metav1.Time{Time: env.Clock.Now().Add(-time.Hour)}})
		}
		ExpectApplied(ctx, env.Client, nc, node)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nc})
		w.nodeClaims = append(w.nodeClaims, nc)
		w.nodes = append(w.nodes, node)
	}
	// One unmanaged Node and one NodeClaim without a Node.
	un := test.Node(test.NodeOptions{ProviderID: fmt.Sprintf("fake://snap-unmanaged-%d", seed)})
	ExpectApplied(ctx, env.Client, un)
	ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(un))
	lone := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: w.nodePools[0].Name}}})
	ExpectApplied(ctx, env.Client, lone)
	ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(lone))
	for _, node := range w.nodes {
		for range w.rng.Intn(5) {
			w.newPod(node)
		}
	}
	for range w.rng.Intn(3) {
		w.newPDB()
	}
	return w
}

func (w *snapshotWorld) pick(n int) int { return w.rng.Intn(n) }

func (w *snapshotWorld) newPod(node *corev1.Node) {
	GinkgoHelper()
	app := []string{"a", "b", "c"}[w.pick(3)]
	om := metav1.ObjectMeta{Labels: map[string]string{"app": app}, Annotations: map[string]string{}}
	switch w.pick(5) {
	case 0, 1:
		om.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs-" + app, UID: types.UID("rs-" + app), Controller: lo.ToPtr(true)}}
	case 2:
		om.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: "ds", UID: "ds", Controller: lo.ToPtr(true)}}
	case 3:
		om.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "ss-" + app, UID: types.UID("ss-" + app), Controller: lo.ToPtr(true)}}
	}
	if w.pick(3) == 0 {
		om.Annotations[v1.DisruptionCostAnnotationKey] = strconv.Itoa(w.pick(2000000000) - 1000000000)
	} else if w.pick(4) == 0 {
		om.Annotations[corev1.PodDeletionCost] = strconv.Itoa(w.pick(1000))
	}
	if w.pick(6) == 0 {
		om.Annotations[v1.DoNotDisruptAnnotationKey] = []string{"true", "5m", "not-a-duration"}[w.pick(3)]
	}
	pod := test.Pod(test.PodOptions{ObjectMeta: om, NodeName: node.Name, Phase: corev1.PodRunning,
		ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}},
		Conditions:           []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}})
	pod.Status.StartTime = &metav1.Time{Time: env.Clock.Now()}
	ExpectApplied(ctx, env.Client, pod)
	ExpectReconcileSucceeded(ctx, w.podCtrl, client.ObjectKeyFromObject(pod))
	w.pods[client.ObjectKeyFromObject(pod)] = true
}

func (w *snapshotWorld) newPDB() {
	GinkgoHelper()
	opts := test.PDBOptions{Labels: map[string]string{"app": []string{"a", "b", "c"}[w.pick(3)]},
		Status: &policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: int32(w.pick(2))}}
	if w.pick(2) == 0 {
		opts.MaxUnavailable = lo.ToPtr(intstr.FromInt32(int32(w.pick(2))))
	} else {
		opts.MinAvailable = lo.ToPtr(intstr.FromString([]string{"50%", "100%"}[w.pick(2)]))
	}
	if w.pick(2) == 0 {
		opts.UnhealthyPodEvictionPolicy = lo.ToPtr(policyv1.AlwaysAllow)
	}
	p := test.PodDisruptionBudget(opts)
	ExpectApplied(ctx, env.Client, p)
	w.pdbs[client.ObjectKeyFromObject(p)] = true
}

func randomKey(w *snapshotWorld, keys map[types.NamespacedName]bool) (types.NamespacedName, bool) {
	ks := lo.Keys(keys)
	if len(ks) == 0 {
		return types.NamespacedName{}, false
	}
	sort.Slice(ks, func(i, j int) bool { return ks[i].String() < ks[j].String() })
	return ks[w.pick(len(ks))], true
}

func (w *snapshotWorld) randomPod() (*corev1.Pod, bool) {
	key, ok := randomKey(w, w.pods)
	if !ok {
		return nil, false
	}
	pod := &corev1.Pod{}
	return pod, env.Client.Get(ctx, key, pod) == nil
}

func (w *snapshotWorld) randomNode() *corev1.Node {
	return ExpectExists(ctx, env.Client, w.nodes[w.pick(len(w.nodes))])
}

func (w *snapshotWorld) randomNodeClaim() *v1.NodeClaim {
	return ExpectExists(ctx, env.Client, w.nodeClaims[w.pick(len(w.nodeClaims))])
}

func (w *snapshotWorld) randomProviderID() string {
	return w.nodeClaims[w.pick(len(w.nodeClaims))].Status.ProviderID
}

func (w *snapshotWorld) updatePod(pod *corev1.Pod, status bool) {
	GinkgoHelper()
	if status {
		Expect(env.Client.Status().Update(ctx, pod)).To(Succeed())
	} else {
		Expect(env.Client.Update(ctx, pod)).To(Succeed())
	}
	ExpectReconcileSucceeded(ctx, w.podCtrl, client.ObjectKeyFromObject(pod))
}

// randomOp applies one random change to the cluster and returns its name.
//
//nolint:gocyclo
func (w *snapshotWorld) randomOp() string {
	GinkgoHelper()
	ops := []string{
		"pod-bind", "pod-delete", "pod-terminal", "pod-dnd-add", "pod-dnd-remove", "pod-ready-flip",
		"node-dnd-toggle", "node-do-not-repair-toggle", "node-badnode-toggle", "node-initialized-toggle",
		"nc-drifted-flip", "nc-consolidatable-flip", "nc-rebooting-flip",
		"mark-for-deletion", "unmark-for-deletion", "queue-add", "queue-remove", "nominate", "buffer-pods",
		"pdb-create", "pdb-status", "pdb-delete", "np-budget", "np-consolidate-after", "np-delete-or-recreate",
		"it-remove-or-restore", "np-instance-type-error", "clock-advance", "clock-advance",
	}
	op := ops[w.pick(len(ops))]
	w.opCounts[op]++
	switch op {
	case "pod-bind":
		w.newPod(w.randomNode())
	case "pod-delete":
		if pod, ok := w.randomPod(); ok {
			ExpectDeleted(ctx, env.Client, pod)
			ExpectReconcileSucceeded(ctx, w.podCtrl, client.ObjectKeyFromObject(pod))
			delete(w.pods, client.ObjectKeyFromObject(pod))
		}
	case "pod-terminal":
		if pod, ok := w.randomPod(); ok {
			pod.Status.Phase = lo.Ternary(w.pick(2) == 0, corev1.PodSucceeded, corev1.PodFailed)
			w.updatePod(pod, true)
		}
	case "pod-dnd-add":
		if pod, ok := w.randomPod(); ok {
			pod.Annotations = lo.Assign(pod.Annotations, map[string]string{v1.DoNotDisruptAnnotationKey: []string{"true", "5m", "1h", "not-a-duration"}[w.pick(4)]})
			w.updatePod(pod, false)
		}
	case "pod-dnd-remove":
		if pod, ok := w.randomPod(); ok {
			delete(pod.Annotations, v1.DoNotDisruptAnnotationKey)
			w.updatePod(pod, false)
		}
	case "pod-ready-flip":
		if pod, ok := w.randomPod(); ok {
			for i := range pod.Status.Conditions {
				if pod.Status.Conditions[i].Type == corev1.PodReady {
					pod.Status.Conditions[i].Status = lo.Ternary(pod.Status.Conditions[i].Status == corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionTrue)
				}
			}
			w.updatePod(pod, true)
		}
	case "node-dnd-toggle", "node-do-not-repair-toggle":
		key := lo.Ternary(op == "node-dnd-toggle", v1.DoNotDisruptAnnotationKey, v1.DoNotRepairAnnotationKey)
		n := w.randomNode()
		if n.Annotations[key] == "true" {
			delete(n.Annotations, key)
		} else {
			n.Annotations = lo.Assign(n.Annotations, map[string]string{key: "true"})
		}
		ExpectApplied(ctx, env.Client, n)
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(n))
	case "node-badnode-toggle":
		n := w.randomNode()
		if _, found := lo.Find(n.Status.Conditions, func(c corev1.NodeCondition) bool { return c.Type == "BadNode" }); found {
			n.Status.Conditions = lo.Reject(n.Status.Conditions, func(c corev1.NodeCondition, _ int) bool { return c.Type == "BadNode" })
		} else {
			n.Status.Conditions = append(n.Status.Conditions, corev1.NodeCondition{Type: "BadNode", Status: corev1.ConditionFalse,
				LastTransitionTime: metav1.Time{Time: env.Clock.Now().Add(-time.Duration(w.pick(2)) * time.Hour)}})
		}
		ExpectApplied(ctx, env.Client, n)
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(n))
	case "node-initialized-toggle":
		n := w.randomNode()
		if n.Labels[v1.NodeInitializedLabelKey] == "true" {
			delete(n.Labels, v1.NodeInitializedLabelKey)
		} else {
			n.Labels[v1.NodeInitializedLabelKey] = "true"
		}
		ExpectApplied(ctx, env.Client, n)
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(n))
	case "nc-drifted-flip", "nc-consolidatable-flip", "nc-rebooting-flip":
		cond := map[string]string{"nc-drifted-flip": v1.ConditionTypeDrifted, "nc-consolidatable-flip": v1.ConditionTypeConsolidatable, "nc-rebooting-flip": v1.ConditionTypeRebooting}[op]
		nc := w.randomNodeClaim()
		switch {
		case !nc.StatusConditions().Get(cond).IsTrue():
			nc.StatusConditions().SetTrue(cond)
		case w.pick(2) == 0:
			_ = nc.StatusConditions().Clear(cond)
		default:
			nc.StatusConditions().SetFalse(cond, "SnapshotDiff", "SnapshotDiff")
		}
		ExpectApplied(ctx, env.Client, nc)
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nc))
	case "mark-for-deletion":
		cluster.MarkForDeletion(w.randomProviderID())
	case "unmark-for-deletion":
		cluster.UnmarkForDeletion(w.randomProviderID())
	case "queue-add":
		queue.Lock()
		queue.ProviderIDToCommand[w.randomProviderID()] = &disruption.Command{}
		queue.Unlock()
	case "queue-remove":
		queue.Lock()
		ids := lo.Keys(queue.ProviderIDToCommand)
		sort.Strings(ids)
		if len(ids) > 0 {
			delete(queue.ProviderIDToCommand, ids[w.pick(len(ids))])
		}
		queue.Unlock()
	case "nominate":
		cluster.NominateNodeForPod(ctx, w.randomProviderID())
	case "buffer-pods":
		counts := map[string]int{}
		for range w.pick(3) {
			counts[w.randomProviderID()] = 1 + w.pick(2)
		}
		cluster.UpdateBufferPodCounts(counts)
	case "pdb-create":
		w.newPDB()
	case "pdb-status":
		if key, ok := randomKey(w, w.pdbs); ok {
			p := &policyv1.PodDisruptionBudget{}
			Expect(env.Client.Get(ctx, key, p)).To(Succeed())
			p.Status.DisruptionsAllowed = lo.Ternary(p.Status.DisruptionsAllowed == 0, int32(1), int32(0))
			Expect(env.Client.Status().Update(ctx, p)).To(Succeed())
		}
	case "pdb-delete":
		if key, ok := randomKey(w, w.pdbs); ok {
			ExpectDeleted(ctx, env.Client, &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}})
			delete(w.pdbs, key)
		}
	case "np-budget", "np-consolidate-after":
		np := w.nodePools[w.pick(len(w.nodePools))]
		if w.deletedNPs[np.Name] {
			break
		}
		np = ExpectExists(ctx, env.Client, np)
		if op == "np-budget" {
			np.Spec.Disruption.Budgets = []v1.Budget{{Nodes: []string{"0", "1", "50%", "100%"}[w.pick(4)]}}
		} else if np.Spec.Replicas == nil {
			np.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration([]string{"Never", "0s", "30s"}[w.pick(3)])
		}
		ExpectApplied(ctx, env.Client, np)
	case "np-delete-or-recreate":
		np := w.nodePools[w.pick(len(w.nodePools))]
		if w.deletedNPs[np.Name] {
			re := np.DeepCopy()
			re.ResourceVersion, re.UID, re.CreationTimestamp = "", "", metav1.Time{}
			ExpectApplied(ctx, env.Client, re)
		} else {
			ExpectDeleted(ctx, env.Client, np)
		}
		w.deletedNPs[np.Name] = !w.deletedNPs[np.Name]
	case "it-remove-or-restore":
		if w.removedIT != nil {
			cloudProvider.InstanceTypes = append(cloudProvider.InstanceTypes, w.removedIT)
			w.removedIT = nil
		} else {
			i := w.pick(len(cloudProvider.InstanceTypes))
			w.removedIT = cloudProvider.InstanceTypes[i]
			cloudProvider.InstanceTypes = append(append([]*cloudprovider.InstanceType(nil), cloudProvider.InstanceTypes[:i]...), cloudProvider.InstanceTypes[i+1:]...)
		}
	case "np-instance-type-error":
		np := w.nodePools[w.pick(len(w.nodePools))]
		if _, ok := cloudProvider.ErrorsForNodePool[np.Name]; ok {
			delete(cloudProvider.ErrorsForNodePool, np.Name)
		} else {
			cloudProvider.ErrorsForNodePool[np.Name] = fmt.Errorf("snapshot diff: instance types unavailable")
		}
	case "clock-advance":
		// past the nomination window, do-not-disrupt=5m, the repair toleration (30m) and lifetime decay
		env.Clock.Step([]time.Duration{time.Second, 25 * time.Second, 6 * time.Minute, 31 * time.Minute}[w.pick(4)])
	}
	return op
}

// sortedNodes is DeepCopyNodes in providerID order. The snapshot and the oracle both take their nodes from it, so
// candidate order is comparable (DeepCopyNodes itself returns map order).
func sortedNodes() state.StateNodes {
	nodes := cluster.DeepCopyNodes()
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ProviderID() < nodes[j].ProviderID() })
	return nodes
}

type methodResult struct {
	fields     []disruption.CandidateFields
	stateNodes []*state.StateNode
	events     []string
	totals     map[string]disruption.NodePoolTotals
}

func result(cs []*disruption.Candidate, evs []string, totals map[string]disruption.NodePoolTotals) methodResult {
	return methodResult{
		fields:     lo.Map(cs, func(c *disruption.Candidate, _ int) disruption.CandidateFields { return c.Fields() }),
		stateNodes: lo.Map(cs, func(c *disruption.Candidate, _ int) *state.StateNode { return c.StateNode }),
		events:     evs,
		totals:     totals,
	}
}

func expectSameResult(where string, want, got methodResult) {
	GinkgoHelper()
	Expect(got.fields).To(Equal(want.fields), "%s: candidates (ids, order, fields)", where)
	for i := range want.stateNodes {
		Expect(reflect.DeepEqual(got.stateNodes[i], want.stateNodes[i])).To(BeTrue(), "%s: StateNode of %s", where, want.fields[i].ProviderID)
	}
	Expect(got.events).To(Equal(want.events), "%s: events", where)
	if got.totals != nil {
		Expect(got.totals).To(Equal(want.totals), "%s: NodePoolTotals", where)
	}
}

// legacy is one GetCandidatesWithTotals for m as it was before CandidateSnapshot, with its events.
func (w *snapshotWorld) legacy(m disruption.Method) methodResult {
	GinkgoHelper()
	cs, totals, err := disruption.LegacyGetCandidatesWithTotals(ctx, sortedNodes, env.Client, w.rec, env.Clock, cloudProvider, m.ShouldDisrupt, m.Class(), queue, clusterCost)
	Expect(err).ToNot(HaveOccurred())
	return result(cs, w.rec.take(), totals)
}

func (w *snapshotWorld) build() (*disruption.CandidateSnapshot, error) {
	return disruption.NewCandidateSnapshotFromNodes(ctx, sortedNodes, env.Client, w.rec, env.Clock, cloudProvider, queue)
}

func methodName(m disruption.Method) string {
	return strings.TrimPrefix(fmt.Sprintf("%T", m), "*disruption.")
}

func (w *snapshotWorld) check(op string) {
	GinkgoHelper()
	w.checks++
	w.rec.take()

	// 1. Controller.disrupt's rule: methods share a snapshot until one has candidates. A method with candidates gets
	// to modify the snapshot's StateNodes and pods too (as ComputeCommands may), since the next method rebuilds.
	var shared *disruption.CandidateSnapshot
	for _, m := range w.methods {
		where := fmt.Sprintf("op=%s controller-rule method=%s", op, methodName(m))
		want := w.legacy(m)
		if shared == nil {
			w.builds++
		}
		w.methodRuns++
		candidates, all, s, err := disruption.MethodCandidates(ctx, m, &shared, w.build)
		Expect(err).ToNot(HaveOccurred())
		evs := w.rec.take()
		var totals map[string]disruption.NodePoolTotals
		if len(candidates) > 0 {
			w.nonEmpty[methodName(m)]++
			Expect(shared).To(BeNil(), "%s: a method with candidates must not leave its snapshot to the next method", where)
			totals = s.ComputeNodePoolTotals(ctx, all, clusterCost)
		}
		expectSameResult(where, want, result(candidates, evs, totals))
		for _, c := range candidates {
			c.ScribbleShared()
		}
		for _, c := range all {
			c.Scribble()
		}
	}

	// 2. Every method from one snapshot, each overwriting all of its Candidates afterwards.
	s, err := w.build()
	Expect(err).ToNot(HaveOccurred())
	for _, m := range w.methods {
		where := fmt.Sprintf("op=%s one-snapshot method=%s", op, methodName(m))
		want := w.legacy(m)
		all := s.Candidates(ctx, m.Class())
		candidates := lo.Filter(all, func(c *disruption.Candidate, _ int) bool { return m.ShouldDisrupt(ctx, c) })
		got := result(candidates, w.rec.take(), s.ComputeNodePoolTotals(ctx, all, clusterCost))
		expectSameResult(where, want, got)
		for _, c := range all {
			c.Scribble()
		}
	}

	// 3. Rejection reasons, per node and class, against NewCandidate as it was.
	nodePoolMap, itMap, err := disruption.BuildNodePoolMap(ctx, env.Client, cloudProvider)
	Expect(err).ToNot(HaveOccurred())
	limits, err := pdb.NewLimits(ctx, env.Client)
	Expect(err).ToNot(HaveOccurred())
	s, err = w.build()
	Expect(err).ToNot(HaveOccurred())
	for _, class := range []string{disruption.RepairDisruptionClass, disruption.GracefulDisruptionClass, disruption.EventualDisruptionClass} {
		for _, n := range sortedNodes() {
			_, wantErr := disruption.LegacyNewCandidate(ctx, env.Client, w.rec, env.Clock, n, limits, nodePoolMap, itMap, queue, class)
			wantEvs := w.rec.take()
			gotErr, found := s.Explain(ctx, n.ProviderID(), class)
			Expect(found).To(BeTrue())
			Expect(fmt.Sprint(gotErr)).To(Equal(fmt.Sprint(wantErr)), "op=%s class=%s node=%s: rejection reason", op, class, n.ProviderID())
			Expect(w.rec.take()).To(Equal(wantEvs), "op=%s class=%s node=%s: events", op, class, n.ProviderID())
		}
	}
}
