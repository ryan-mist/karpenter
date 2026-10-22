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

package pipeline_test

import (
	"context"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	coreapis "sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/controllers/dynamicresources/deviceallocation"
	"sigs.k8s.io/karpenter/pkg/controllers/node/health"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	"sigs.k8s.io/karpenter/pkg/state/prediction"
	"sigs.k8s.io/karpenter/pkg/state/virtualpods"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var ctx context.Context
var env *test.Environment
var cloudProvider *fake.CloudProvider
var clusterCost *cost.ClusterCost
var cluster *state.Cluster
var prov *provisioning.Provisioner
var queue *disruption.Queue
var recorder *test.EventRecorder
var nodeStateController *informer.NodeController
var nodeClaimStateController *informer.NodeClaimController
var podStateController *informer.PodController

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Disruption Pipeline")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(test.WithCRDs(coreapis.CRDs...), test.WithCRDs(v1alpha1.CRDs...))
	ctx = options.ToContext(ctx, test.Options())
	cloudProvider = fake.NewCloudProvider()
	clusterCost = cost.NewClusterCost(ctx, cloudProvider, env.Client)
	recorder = test.NewEventRecorder()
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = BeforeEach(func() {
	ctx = options.ToContext(ctx, test.Options())
	cloudProvider.Reset()
	cloudProvider.InstanceTypes = fake.InstanceTypesAssorted()
	clusterCost.Reset()
	recorder.Reset()
	env.Clock.SetTime(time.Now())
	// A repair policy matcher, as the operator builds one when node repair is enabled, so specs can use the repair method.
	repairPolicyMatcher := lo.Must(health.NewRepairPolicyMatcher(
		options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodeRepair: lo.ToPtr(true)}})), cloudProvider))
	cluster = state.NewCluster(env.Clock, env.Client, cloudProvider, state.WithRepairPolicyMatcher(repairPolicyMatcher))
	nodeStateController = informer.NewNodeController(env.Client, cluster)
	nodeClaimStateController = informer.NewNodeClaimController(env.Client, cloudProvider, cluster, clusterCost)
	podStateController = informer.NewPodController(env.Client, cluster)
	prov = provisioning.NewProvisioner(env.Client, recorder, cloudProvider, cluster, env.Clock, deviceallocation.NewController(env.Client),
		virtualpods.NewVirtualPodCache(env.Client), prediction.NewStore(env.Client))
	queue = disruption.NewQueue(env.Client, recorder, cluster, env.Clock, prov)
})

var _ = AfterEach(func() {
	ExpectCleanedUp(ctx, env.Client)
})

// world is a NodePool with managed, initialized nodes in cluster state.
type world struct {
	nodePool   *v1.NodePool
	nodeClaims []*v1.NodeClaim
	nodes      []*corev1.Node
}

// newWorld creates n nodes in one NodePool (100% budget, consolidate after 0s). Nodes listed in withPods get one
// reschedulable pod each.
func newWorld(n int, withPods ...int) *world {
	GinkgoHelper()
	w := &world{nodePool: test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
		ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
		ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
		Budgets:             []v1.Budget{{Nodes: "100%"}},
	}}})}
	it := fake.InstanceTypesAssorted()[0]
	cloudProvider.InstanceTypes = append(cloudProvider.InstanceTypes, it)
	of := it.Offerings[0]
	w.nodeClaims, w.nodes = test.NodeClaimsAndNodes(n, v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			v1.NodePoolLabelKey:            w.nodePool.Name,
			corev1.LabelInstanceTypeStable: it.Name,
			v1.CapacityTypeLabelKey:        of.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
			corev1.LabelTopologyZone:       of.Requirements.Get(corev1.LabelTopologyZone).Any(),
		}},
		Status: v1.NodeClaimStatus{Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("32"), corev1.ResourcePods: resource.MustParse("100")}},
	})
	for _, nc := range w.nodeClaims {
		nc.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
		nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
	}
	ExpectApplied(ctx, env.Client, w.nodePool)
	for i := range w.nodeClaims {
		ExpectApplied(ctx, env.Client, w.nodeClaims[i], w.nodes[i])
	}
	ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, w.nodes, w.nodeClaims)
	for _, i := range withPods {
		pod := test.Pod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs", UID: "rs", Controller: lo.ToPtr(true)}}},
			NodeName:   w.nodes[i].Name,
			Phase:      corev1.PodRunning,
		})
		ExpectApplied(ctx, env.Client, pod)
		ExpectReconcileSucceeded(ctx, podStateController, client.ObjectKeyFromObject(pod))
	}
	return w
}

func (w *world) providerIDs() []string {
	return lo.Map(w.nodeClaims, func(nc *v1.NodeClaim, _ int) string { return nc.Status.ProviderID })
}

// stubPlanner plans one delete-only plan per candidate it is given, or what plan returns.
type stubPlanner struct {
	name   string
	class  string
	reason v1.DisruptionReason
	shape  types.Shape
	filter func(*disruption.Candidate) bool
	plan   func(pipeline.PlanInput) (pipeline.PlanOutput, error)

	mu    sync.Mutex
	calls []pipeline.PlanInput
}

func newStubPlanner(name string) *stubPlanner {
	return &stubPlanner{name: name, class: disruption.GracefulDisruptionClass, reason: v1.DisruptionReasonDrifted, shape: types.ShapeReplacing}
}

func (p *stubPlanner) Name() string                { return p.name }
func (p *stubPlanner) Reason() v1.DisruptionReason { return p.reason }
func (p *stubPlanner) Class() string               { return p.class }
func (p *stubPlanner) Shape() types.Shape          { return p.shape }
func (p *stubPlanner) Filter(_ context.Context, c *disruption.Candidate, _ time.Time) (bool, *types.Verdict) {
	if p.filter != nil && !p.filter(c) {
		return false, &types.Verdict{Planner: p.name, ProviderID: c.ProviderID(), Reason: types.ReasonDoNotDisrupt}
	}
	return true, nil
}
func (p *stubPlanner) Plan(_ context.Context, in pipeline.PlanInput) (pipeline.PlanOutput, error) {
	p.mu.Lock()
	p.calls = append(p.calls, in)
	p.mu.Unlock()
	if p.plan != nil {
		return p.plan(in)
	}
	return pipeline.PlanOutput{Plans: lo.Map(in.Candidates, func(c *disruption.Candidate, _ int) *pipeline.Plan {
		return deletePlan(c)
	})}, nil
}

func (p *stubPlanner) planned() [][]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return lo.Map(p.calls, func(in pipeline.PlanInput, _ int) []string { return providerIDsOf(in.Candidates) })
}

func deletePlan(cs ...*disruption.Candidate) *pipeline.Plan {
	return &pipeline.Plan{Candidates: cs, Action: types.ActionDelete,
		Footprint: types.Footprint{Removed: sets.New(providerIDsOf(cs)...)}}
}

func providerIDsOf(cs []*disruption.Candidate) []string {
	return lo.Map(cs, func(c *disruption.Candidate, _ int) string { return c.ProviderID() })
}

// fakeExecutor records submissions and reserves their candidates in its view. Resolve ends a submission.
type fakeExecutor struct {
	mu        sync.Mutex
	submitted []*pipeline.Plan
	channels  map[*pipeline.Plan]chan pipeline.Resolution
	reserved  sets.Set[string]
	steps     []pipeline.CommitStepResult
	closed    map[chan pipeline.Resolution]bool
}

func newFakeExecutor() *fakeExecutor {
	return &fakeExecutor{channels: map[*pipeline.Plan]chan pipeline.Resolution{}, reserved: sets.New[string](), closed: map[chan pipeline.Resolution]bool{}}
}

func (e *fakeExecutor) Submit(_ context.Context, plans ...*pipeline.Plan) <-chan pipeline.Resolution {
	e.mu.Lock()
	defer e.mu.Unlock()
	ch := make(chan pipeline.Resolution, len(plans))
	for _, p := range plans {
		e.submitted = append(e.submitted, p)
		e.channels[p] = ch
		e.reserved.Insert(p.Footprint.Removed.UnsortedList()...)
	}
	return ch
}

// Step returns the scripted results in order, then empty results.
func (e *fakeExecutor) Step(context.Context) pipeline.CommitStepResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.steps) == 0 {
		return pipeline.CommitStepResult{}
	}
	r := e.steps[0]
	e.steps = e.steps[1:]
	return r
}

// resolveAll resolves every unresolved submitted plan as kind and closes the channels.
func (e *fakeExecutor) resolveAll(kind pipeline.ResolutionKind) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, p := range e.submitted {
		if ch := e.channels[p]; !e.closed[ch] {
			ch <- pipeline.Resolution{PlanID: p.ID, Kind: kind}
			e.reserved.Delete(p.Footprint.Removed.UnsortedList()...)
		}
	}
	for _, p := range e.submitted {
		if ch := e.channels[p]; !e.closed[ch] {
			close(ch)
			e.closed[ch] = true
		}
	}
}

func (e *fakeExecutor) submittedIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return lo.FlatMap(e.submitted, func(p *pipeline.Plan, _ int) []string { return providerIDsOf(p.Candidates) })
}

// view is the fake executor's ReservationView.
func (e *fakeExecutor) view() pipeline.ReservationView { return (*fakeView)(e) }

type fakeView fakeExecutor

func (v *fakeView) Version() uint64 { return 0 }
func (v *fakeView) IsReserved(id string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.reserved.Has(id)
}
func (v *fakeView) Unstarted() sets.Set[string] {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.reserved.Clone()
}
func (v *fakeView) Footprints() []types.Footprint          { return nil }
func (v *fakeView) InFlightDestinations() sets.Set[string] { return sets.New[string]() }
func (v *fakeView) StaticHeld(string) int64                { return 0 }
func (v *fakeView) ReservationsHeld(string) int            { return 0 }

// fakeBudgets has no budget for the reasons in zero.
type fakeBudgets struct{ zero sets.Set[v1.DisruptionReason] }

func (b fakeBudgets) Remaining(context.Context, string, v1.DisruptionReason) int { return 0 }
func (b fakeBudgets) AnyRemaining(_ context.Context, r v1.DisruptionReason) bool {
	return !b.zero.Has(r)
}
