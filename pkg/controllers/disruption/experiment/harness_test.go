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

// Package experiment is a research harness (not shipped) that drives the REAL
// Karpenter scheduler as a consolidation oracle on an in-memory fake client, so
// we can race candidate-set generators (baseline binary search vs graph-based)
// head-to-head. This file establishes the reusable substrate only.
package experiment

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes/scheme"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/dynamicresources/deviceallocation"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/operator/logging"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	kscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	"sigs.k8s.io/karpenter/pkg/state/virtualpods"
	"sigs.k8s.io/karpenter/pkg/test"

	// Side-effect imports register karpenter v1 (NodePool/NodeClaim) and the test
	// NodeClass into scheme.Scheme via their init() functions.
	_ "sigs.k8s.io/karpenter/pkg/apis/v1"
	_ "sigs.k8s.io/karpenter/pkg/test/v1alpha1"
)

// ---------------------------------------------------------------------------
// Harness substrate
// ---------------------------------------------------------------------------

type harness struct {
	ctx           context.Context
	client        client.Client
	clk           *clocktesting.FakeClock
	cloudProvider *fake.CloudProvider
	cluster       *state.Cluster
	prov          *provisioning.Provisioner
	recorder      *test.EventRecorder
	queue         *disruption.Queue
	nodeState     *informer.NodeController
	ncState       *informer.NodeClaimController

	nodePool *v1.NodePool
	nodes    []*corev1.Node
	claims   []*v1.NodeClaim
}

func newFakeClient() client.Client {
	return fakecr.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).
		WithIndex(&corev1.Node{}, "spec.providerID", func(o client.Object) []string {
			return []string{o.(*corev1.Node).Spec.ProviderID}
		}).
		WithIndex(&v1.NodeClaim{}, "status.providerID", func(o client.Object) []string {
			return []string{o.(*v1.NodeClaim).Status.ProviderID}
		}).
		WithIndex(&v1.NodeClaim{}, "spec.nodeClassRef.group", func(o client.Object) []string {
			return []string{o.(*v1.NodeClaim).Spec.NodeClassRef.Group}
		}).
		WithIndex(&v1.NodeClaim{}, "spec.nodeClassRef.kind", func(o client.Object) []string {
			return []string{o.(*v1.NodeClaim).Spec.NodeClassRef.Kind}
		}).
		WithIndex(&v1.NodeClaim{}, "spec.nodeClassRef.name", func(o client.Object) []string {
			return []string{o.(*v1.NodeClaim).Spec.NodeClassRef.Name}
		}).
		WithIndex(&v1.NodePool{}, "spec.template.spec.nodeClassRef.group", func(o client.Object) []string {
			return []string{o.(*v1.NodePool).Spec.Template.Spec.NodeClassRef.Group}
		}).
		WithIndex(&v1.NodePool{}, "spec.template.spec.nodeClassRef.kind", func(o client.Object) []string {
			return []string{o.(*v1.NodePool).Spec.Template.Spec.NodeClassRef.Kind}
		}).
		WithIndex(&v1.NodePool{}, "spec.template.spec.nodeClassRef.name", func(o client.Object) []string {
			return []string{o.(*v1.NodePool).Spec.Template.Spec.NodeClassRef.Name}
		}).
		WithIndex(&storagev1.VolumeAttachment{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*storagev1.VolumeAttachment).Spec.NodeName}
		}).
		Build()
}

func newHarness(t *testing.T) *harness {
	log.SetLogger(logging.NopLogger)
	ctx := options.ToContext(injection.WithControllerName(context.Background(), "experiment"), test.Options())
	clk := clocktesting.NewFakeClock(time.Now())
	c := newFakeClient()
	cp := fake.NewCloudProvider()
	cp.InstanceTypes = syntheticInstanceTypes()
	clusterCost := cost.NewClusterCost(ctx, cp, c)
	cl := state.NewCluster(clk, c, cp)
	nodeState := informer.NewNodeController(c, cl)
	ncState := informer.NewNodeClaimController(c, cp, cl, clusterCost)
	recorder := test.NewEventRecorder()
	draController := deviceallocation.NewController(c)
	prov := provisioning.NewProvisioner(c, recorder, cp, cl, clk, draController, virtualpods.NewVirtualPodCache(c))
	queue := disruption.NewQueue(c, recorder, cl, clk, prov)
	return &harness{
		ctx: ctx, client: c, clk: clk, cloudProvider: cp, cluster: cl, prov: prov,
		recorder: recorder, queue: queue, nodeState: nodeState, ncState: ncState,
	}
}

// ---------------------------------------------------------------------------
// Priced instance types: small/medium/large across 3 zones, on-demand + spot
// ---------------------------------------------------------------------------

var zones = []string{"test-zone-1", "test-zone-2", "test-zone-3"}

func offerings(base float64) cloudprovider.Offerings {
	var offs cloudprovider.Offerings
	for i, z := range zones {
		for _, ct := range []string{v1.CapacityTypeOnDemand, v1.CapacityTypeSpot} {
			p := base + 0.01*float64(i)
			if ct == v1.CapacityTypeSpot {
				p *= 0.30
			}
			offs = append(offs, &cloudprovider.Offering{
				Available: true,
				Price:     p,
				Requirements: kscheduling.NewLabelRequirements(map[string]string{
					v1.CapacityTypeLabelKey:  ct,
					corev1.LabelTopologyZone: z,
				}),
			})
		}
	}
	return offs
}

func syntheticInstanceTypes() []*cloudprovider.InstanceType {
	mk := func(name string, cpu, memGi int, base float64) *cloudprovider.InstanceType {
		vals := lo.Map(offerings(base), func(o *cloudprovider.Offering, _ int) cloudprovider.Offering { return *o })
		return fake.NewInstanceType(name,
			fake.WithResources(corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(fmt.Sprintf("%d", cpu)),
				corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", memGi)),
				corev1.ResourcePods:   resource.MustParse("110"),
			}),
			fake.WithOfferings(vals...),
		)
	}
	return []*cloudprovider.InstanceType{
		mk("small", 2, 4, 0.10),
		mk("medium", 8, 16, 0.40),
		mk("large", 32, 64, 1.60),
	}
}

func instanceType(name string) *cloudprovider.InstanceType {
	it, _ := lo.Find(syntheticInstanceTypes(), func(i *cloudprovider.InstanceType) bool { return i.Name == name })
	return it
}

// ---------------------------------------------------------------------------
// Synthetic cluster generation
// ---------------------------------------------------------------------------

type clusterParams struct {
	numNodes     int
	instanceType string // instance type every node runs (e.g. "large")
	podsPerNode  int    // reschedulable pods per node
	podCPU       string // cpu request per pod
}

func (h *harness) genCluster(t *testing.T, p clusterParams) {
	h.nodePool = test.NodePool(v1.NodePool{
		Spec: v1.NodePoolSpec{
			Template: v1.NodeClaimTemplate{
				Spec: v1.NodeClaimTemplateSpec{
					Requirements: []v1.NodeSelectorRequirementWithMinValues{
						{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn,
							Values: []string{"small", "medium", "large"}},
						{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn,
							Values: []string{v1.CapacityTypeOnDemand}},
					},
				},
			},
			Disruption: v1.Disruption{
				ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
				ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
			},
			Limits: v1.Limits{corev1.ResourceCPU: resource.MustParse("100000")},
		},
	})
	h.mustApply(t, h.nodePool)

	it := instanceType(p.instanceType)
	rs := test.ReplicaSet()
	h.mustApply(t, rs)

	for i := 0; i < p.numNodes; i++ {
		zone := zones[i%len(zones)]
		nc, node := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            h.nodePool.Name,
					corev1.LabelInstanceTypeStable: it.Name,
					corev1.LabelTopologyZone:       zone,
					v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
				},
			},
			Spec: v1.NodeClaimSpec{
				NodeClassRef: h.nodePool.Spec.Template.Spec.NodeClassRef,
			},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
				Allocatable: corev1.ResourceList{
					corev1.ResourceCPU:    it.Capacity[corev1.ResourceCPU],
					corev1.ResourceMemory: it.Capacity[corev1.ResourceMemory],
					corev1.ResourcePods:   resource.MustParse("110"),
				},
			},
		})
		// Make it a consolidation candidate.
		nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)

		h.mustApply(t, nc, node)

		// Bind reschedulable pods to the node.
		for j := 0; j < p.podsPerNode; j++ {
			pod := test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					UID: uuid.NewUUID(),
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID,
						Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true),
					}},
				},
				ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(p.podCPU)},
				},
			})
			pod.Spec.NodeName = node.Name
			h.mustApply(t, pod)
		}
		h.nodes = append(h.nodes, node)
		h.claims = append(h.claims, nc)
	}

	// Initialize + register nodes/claims, then push into cluster state via the informer controllers.
	h.makeInitializedAndStateUpdated(t)
}

// makeInitializedAndStateUpdated mirrors ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated
// without Ginkgo/Gomega.
func (h *harness) makeInitializedAndStateUpdated(t *testing.T) {
	for _, nc := range h.claims {
		cur := &v1.NodeClaim{}
		h.mustGet(t, client.ObjectKeyFromObject(nc), cur)
		cur.StatusConditions().SetTrue(v1.ConditionTypeLaunched)
		cur.StatusConditions().SetTrue(v1.ConditionTypeRegistered)
		cur.StatusConditions().SetTrue(v1.ConditionTypeInitialized)
		cur.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		h.mustApply(t, cur)
	}
	for _, node := range h.nodes {
		cur := &corev1.Node{}
		h.mustGet(t, client.ObjectKeyFromObject(node), cur)
		cur.Status.Phase = corev1.NodeRunning
		cur.Status.Conditions = []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			LastHeartbeatTime: metav1.NewTime(h.clk.Now()), LastTransitionTime: metav1.NewTime(h.clk.Now()),
			Reason: "KubeletReady",
		}}
		cur.Spec.Taints = lo.Reject(cur.Spec.Taints, func(taint corev1.Taint, _ int) bool {
			return taint.MatchTaint(&v1.UnregisteredNoExecuteTaint)
		})
		if cur.Labels == nil {
			cur.Labels = map[string]string{}
		}
		cur.Labels[v1.NodeRegisteredLabelKey] = "true"
		cur.Labels[v1.NodeInitializedLabelKey] = "true"
		h.mustApply(t, cur)
	}
	for _, node := range h.nodes {
		if _, err := h.nodeState.Reconcile(h.ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(node)}); err != nil {
			t.Fatalf("node state reconcile: %v", err)
		}
	}
	for _, nc := range h.claims {
		if _, err := h.ncState.Reconcile(h.ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(nc)}); err != nil {
			t.Fatalf("nodeclaim state reconcile: %v", err)
		}
	}
	// Register every bound pod in cluster state so inter-pod anti-affinity is
	// actually enforced by SimulateScheduling. Reconciling the node/nodeclaim
	// controllers alone does NOT populate the cluster's antiAffinityPods index
	// (state/cluster.go:946) — without this, anti-affine pods are wrongly allowed
	// to co-locate. Centralized here so every scenario gets correct anti-affinity.
	h.registerPods(t)
}

// registerPods calls cluster.UpdatePod for every pod currently in the fake client,
// populating the cluster's pod/anti-affinity indexes used by the scheduler.
func (h *harness) registerPods(t *testing.T) {
	t.Helper()
	podList := &corev1.PodList{}
	if err := h.client.List(h.ctx, podList); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	for i := range podList.Items {
		if err := h.cluster.UpdatePod(h.ctx, &podList.Items[i]); err != nil {
			t.Fatalf("cluster.UpdatePod: %v", err)
		}
	}
}

func (h *harness) mustApply(t *testing.T, objs ...client.Object) {
	t.Helper()
	for _, o := range objs {
		existing := o.DeepCopyObject().(client.Object)
		err := h.client.Get(h.ctx, client.ObjectKeyFromObject(o), existing)
		if err != nil {
			if cerr := h.client.Create(h.ctx, o); cerr != nil {
				t.Fatalf("create %T: %v", o, cerr)
			}
		} else {
			o.SetResourceVersion(existing.GetResourceVersion())
			if uerr := h.client.Update(h.ctx, o); uerr != nil {
				t.Fatalf("update %T: %v", o, uerr)
			}
		}
	}
}

func (h *harness) mustGet(t *testing.T, key client.ObjectKey, o client.Object) {
	t.Helper()
	if err := h.client.Get(h.ctx, key, o); err != nil {
		t.Fatalf("get %T %s: %v", o, key, err)
	}
}

// ---------------------------------------------------------------------------
// Oracle: run the REAL SimulateScheduling on a candidate subset
// ---------------------------------------------------------------------------

type oracleResult struct {
	decision string  // "delete" | "replace" | "no-op"
	savings  float64 // $/hr saved
	calls    int     // SimulateScheduling calls consumed (always 1 here)
}

// evalSet runs the real disruption.SimulateScheduling for the given candidate subset
// and interprets results exactly like computeConsolidation.
func (h *harness) evalSet(t *testing.T, candidates []*disruption.Candidate) oracleResult {
	t.Helper()
	res := oracleResult{decision: "no-op", calls: 1}
	if len(candidates) == 0 {
		return res
	}
	results, err := disruption.SimulateScheduling(h.ctx, h.client, h.cluster, h.prov, h.clk, h.recorder,
		[]pscheduling.Options{pscheduling.IsConsolidationSimulation}, candidates...)
	if err != nil {
		t.Fatalf("SimulateScheduling: %v", err)
	}
	if !results.AllNonPendingPodsScheduled() {
		return res // infeasible
	}
	candidatePrice := lo.SumBy(candidates, func(c *disruption.Candidate) float64 { return c.Price })
	switch len(results.NewNodeClaims) {
	case 0:
		res.decision = "delete"
		res.savings = candidatePrice
	case 1:
		replPrice := cheapestOfferingPrice(results.NewNodeClaims[0])
		if replPrice < candidatePrice {
			res.decision = "replace"
			res.savings = candidatePrice - replPrice
		}
	default:
		// m->n is rejected by consolidation; treat as no-op.
	}
	return res
}

func cheapestOfferingPrice(nc *pscheduling.NodeClaim) float64 {
	best := math.MaxFloat64
	for _, it := range nc.InstanceTypeOptions {
		// Respect the nodeclaim's requirements (e.g. capacity-type on-demand, zone) so
		// we don't underprice a replacement with a spot/zone offering the pool disallows.
		if o := it.Offerings.Available().Compatible(nc.Requirements).Cheapest(); o != nil && o.Price < best {
			best = o.Price
		}
	}
	if best == math.MaxFloat64 {
		return 0
	}
	return best
}

// ---------------------------------------------------------------------------
// Candidate construction helper (mirrors GetCandidates without the ShouldDisrupt filter,
// so subsets can be chosen freely by generators)
// ---------------------------------------------------------------------------

func (h *harness) allCandidates(t *testing.T) []*disruption.Candidate {
	t.Helper()
	mnc := h.multiNode()
	candidates, err := disruption.GetCandidates(h.ctx, h.cluster, h.client, h.recorder, h.clk, h.cloudProvider,
		mnc.ShouldDisrupt, mnc.Class(), h.queue)
	if err != nil {
		t.Fatalf("GetCandidates: %v", err)
	}
	return candidates
}

func (h *harness) multiNode() *disruption.MultiNodeConsolidation {
	c := disruption.MakeConsolidation(h.clk, h.cluster, h.client, h.prov, h.cloudProvider, h.recorder, h.queue)
	return disruption.NewMultiNodeConsolidation(c, disruption.WithValidator(nopValidator{}))
}

// nopValidator returns the command unchanged so the baseline doesn't sleep the 15s
// validation delay during the experiment.
type nopValidator struct{}

func (nopValidator) Validate(_ context.Context, cmd disruption.Command, _ time.Duration) (disruption.Command, error) {
	return cmd, nil
}

// baselineRun runs the real MultiNodeConsolidation.ComputeCommands and returns its command(s).
func (h *harness) baselineRun(t *testing.T) []disruption.Command {
	t.Helper()
	mnc := h.multiNode()
	budgets, err := disruption.BuildDisruptionBudgetMapping(h.ctx, h.cluster, h.clk, h.client, h.cloudProvider, h.recorder, mnc.Reason())
	if err != nil {
		t.Fatalf("BuildDisruptionBudgetMapping: %v", err)
	}
	candidates, err := disruption.GetCandidates(h.ctx, h.cluster, h.client, h.recorder, h.clk, h.cloudProvider,
		mnc.ShouldDisrupt, mnc.Class(), h.queue)
	if err != nil {
		t.Fatalf("GetCandidates: %v", err)
	}
	cmds, err := mnc.ComputeCommands(h.ctx, budgets, candidates...)
	if err != nil {
		t.Fatalf("ComputeCommands: %v", err)
	}
	return cmds
}

func commandSavings(cmd disruption.Command) float64 {
	del := lo.SumBy(cmd.Candidates, func(c *disruption.Candidate) float64 { return c.Price })
	var add float64
	for _, nc := range cmd.Results.NewNodeClaims {
		add += cheapestOfferingPrice(nc)
	}
	return del - add
}

// ---------------------------------------------------------------------------
// Smoke test
// ---------------------------------------------------------------------------

func TestHarnessSmoke(t *testing.T) {
	h := newHarness(t)
	// 6 "large" nodes, each ~25% CPU utilized (2 pods * 4 CPU = 8 of 32) -> ripe for consolidation.
	h.genCluster(t, clusterParams{numNodes: 6, instanceType: "large", podsPerNode: 2, podCPU: "4"})

	stateNodes := h.cluster.DeepCopyNodes()
	t.Logf("cluster state nodes: %d", len(stateNodes))
	if len(stateNodes) != 6 {
		t.Fatalf("expected 6 state nodes, got %d", len(stateNodes))
	}

	candidates := h.allCandidates(t)
	t.Logf("multi-node candidates: %d", len(candidates))
	if len(candidates) == 0 {
		t.Fatalf("expected >0 candidates")
	}

	// Baseline.
	cmds := h.baselineRun(t)
	t.Logf("baseline returned %d command(s)", len(cmds))
	for i, cmd := range cmds {
		t.Logf("  cmd[%d]: decision=%s candidates=%d replacements=%d savings=%.4f/hr",
			i, cmd.Decision(), len(cmd.Candidates), len(cmd.Replacements), commandSavings(cmd))
	}

	// evalSet on hand-picked subsets.
	for _, n := range []int{2, 3, len(candidates)} {
		if n > len(candidates) {
			continue
		}
		r := h.evalSet(t, candidates[:n])
		t.Logf("evalSet(first %d): decision=%s savings=%.4f/hr (calls=%d)", n, r.decision, r.savings, r.calls)
	}
}
