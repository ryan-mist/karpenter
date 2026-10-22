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

// The shadow mode must not change what the disruption controller does (RFC D.3: "never writes"). These specs run the
// disruption controller on identical, deterministic state twice without the shadow (a determinism control) and once
// with it, and compare after every pass: the queued commands, every event, the methods' state
// (SingleNodeConsolidation.PreviouslyUnseenNodePools, consolidation dedup), cluster state per node, pending pods'
// scheduling records, and every API write; at the end, every karpenter_* series except the shadow's own (and those
// attributed to the controller disruption.shadow), and the disruption controller's log lines. The shadow's planners
// are wrapped to force mismatches (so rechecks run) or races, and its clock jumps to make its consolidation cores
// time out. Adapted from the Stack D re-review probe (docs/review/repro/stack-d-rereview-shadow-probe_test.go.txt).

import (
	"context"
	"fmt"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	dto "github.com/prometheus/client_model/go"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	"sigs.k8s.io/karpenter/pkg/controllers/dynamicresources/deviceallocation"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/state/prediction"
	"sigs.k8s.io/karpenter/pkg/state/virtualpods"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// ---------- recording client ----------

type recClient struct {
	client.Client
	mu  *sync.Mutex
	log *[]string
}

var genSuffix = regexp.MustCompile(`-[a-z0-9]{5}$`)

func (c recClient) rec(verb string, o client.Object) {
	c.mu.Lock()
	defer c.mu.Unlock()
	*c.log = append(*c.log, fmt.Sprintf("%s %T %s", verb, o, genSuffix.ReplaceAllString(o.GetName(), "-X")))
}
func (c recClient) Create(ctx context.Context, o client.Object, opts ...client.CreateOption) error {
	c.rec("create", o)
	return c.Client.Create(ctx, o, opts...)
}
func (c recClient) Update(ctx context.Context, o client.Object, opts ...client.UpdateOption) error {
	c.rec("update", o)
	return c.Client.Update(ctx, o, opts...)
}
func (c recClient) Patch(ctx context.Context, o client.Object, p client.Patch, opts ...client.PatchOption) error {
	c.rec("patch", o)
	return c.Client.Patch(ctx, o, p, opts...)
}
func (c recClient) Delete(ctx context.Context, o client.Object, opts ...client.DeleteOption) error {
	c.rec("delete", o)
	return c.Client.Delete(ctx, o, opts...)
}
func (c recClient) DeleteAllOf(ctx context.Context, o client.Object, opts ...client.DeleteAllOfOption) error {
	c.rec("deleteall", o)
	return c.Client.DeleteAllOf(ctx, o, opts...)
}
func (c recClient) Status() client.SubResourceWriter { return recSub{c.Client.Status(), c, "status"} }
func (c recClient) SubResource(s string) client.SubResourceClient {
	return recSubClient{c.Client.SubResource(s), c, s}
}

type recSub struct {
	client.SubResourceWriter
	c   recClient
	sub string
}

func (s recSub) Create(ctx context.Context, o client.Object, sub client.Object, opts ...client.SubResourceCreateOption) error {
	s.c.rec(s.sub+"-create", o)
	return s.SubResourceWriter.Create(ctx, o, sub, opts...)
}
func (s recSub) Update(ctx context.Context, o client.Object, opts ...client.SubResourceUpdateOption) error {
	s.c.rec(s.sub+"-update", o)
	return s.SubResourceWriter.Update(ctx, o, opts...)
}
func (s recSub) Patch(ctx context.Context, o client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
	s.c.rec(s.sub+"-patch", o)
	return s.SubResourceWriter.Patch(ctx, o, p, opts...)
}

type recSubClient struct {
	client.SubResourceClient
	c   recClient
	sub string
}

func (s recSubClient) Create(ctx context.Context, o client.Object, sub client.Object, opts ...client.SubResourceCreateOption) error {
	s.c.rec(s.sub+"-create", o)
	return s.SubResourceClient.Create(ctx, o, sub, opts...)
}
func (s recSubClient) Update(ctx context.Context, o client.Object, opts ...client.SubResourceUpdateOption) error {
	s.c.rec(s.sub+"-update", o)
	return s.SubResourceClient.Update(ctx, o, opts...)
}
func (s recSubClient) Patch(ctx context.Context, o client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
	s.c.rec(s.sub+"-patch", o)
	return s.SubResourceClient.Patch(ctx, o, p, opts...)
}

// ---------- clocks / planners ----------

// jumpClock advances by step on every Now, so the shadow's own single-node consolidation times out.
type jumpClock struct {
	*clocktesting.FakeClock
	step time.Duration
}

func (c *jumpClock) Now() time.Time                  { c.FakeClock.Step(c.step); return c.FakeClock.Now() }
func (c *jumpClock) Since(t time.Time) time.Duration { return c.Now().Sub(t) }

// slowPlanner: the first call plans nothing (forces a mismatch); calls in [slowFrom, slowTo] sleep d (honouring ctx).
type slowPlanner struct {
	pipeline.Planner
	calls            atomic.Int64
	empty            int64
	slowFrom, slowTo int64
	d                time.Duration
	blockOnCtx       bool
}

func (p *slowPlanner) Plan(ctx context.Context, in pipeline.PlanInput) (pipeline.PlanOutput, error) {
	n := p.calls.Add(1)
	if n >= p.slowFrom && n <= p.slowTo {
		if p.blockOnCtx {
			<-ctx.Done()
			return pipeline.PlanOutput{}, ctx.Err()
		}
		select {
		case <-time.After(p.d):
		case <-ctx.Done():
		}
	}
	if n <= p.empty {
		return pipeline.PlanOutput{}, nil
	}
	return p.Planner.Plan(ctx, in)
}

// ---------- environment ----------

var probeBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// probeClock is the probe's own fake clock. env.Clock is shared with other specs, whose leftover runners may wait on it;
// reconcile would then step it for them too, by an amount that depends on load, and consolidation's 5 minute state
// reset would make the disruption controller's passes differ between runs.
var probeClock *clocktesting.FakeClock

type probeEnv struct {
	writes    []string
	swrites   []string
	mu        sync.Mutex
	rc, src   recClient
	logs      []string
	logsMu    sync.Mutex
	lctx      context.Context
	methods   []disruption.Method
	legacy    *disruption.Controller
	shadow    *pipeline.Shadow
	pending   []*corev1.Pod
	nodePool  *v1.NodePool
	manyDrift bool
}

func newProbeEnv() *probeEnv {
	pe := &probeEnv{}
	pe.rc = recClient{Client: env.Client, mu: &pe.mu, log: &pe.writes}
	pe.src = recClient{Client: env.Client, mu: &pe.mu, log: &pe.swrites}
	ExpectCleanedUp(ctx, env.Client)
	probeClock = clocktesting.NewFakeClock(probeBase)
	recorder.Reset()
	clusterCost.Reset()
	cloudProvider.Reset()
	cloudProvider.InstanceTypes = fake.InstanceTypesAssorted()
	cluster = state.NewCluster(probeClock, env.Client, cloudProvider)
	nodeStateController = informer.NewNodeController(env.Client, cluster)
	nodeClaimStateController = informer.NewNodeClaimController(env.Client, cloudProvider, cluster, clusterCost)
	podStateController = informer.NewPodController(env.Client, cluster)
	prov = provisioning.NewProvisioner(pe.rc, recorder, cloudProvider, cluster, probeClock, deviceallocation.NewController(env.Client),
		virtualpods.NewVirtualPodCache(env.Client), prediction.NewStore(env.Client))
	queue = disruption.NewQueue(pe.rc, recorder, cluster, probeClock, prov)
	pe.lctx = log.IntoContext(ctx, funcr.New(func(prefix, args string) {
		pe.logsMu.Lock()
		defer pe.logsMu.Unlock()
		pe.logs = append(pe.logs, prefix+" "+args)
	}, funcr.Options{Verbosity: 4}))
	return pe
}

// build: 6 nodes of the first assorted type; n0 empty, n1 drifted+pod, n2,n3 small pods (multi), n4 a 2cpu pod
// (single replace), n5 a 20cpu pod; plus pending pods (one with preferred anti-affinity, one with a missing PVC).
func (pe *probeEnv) build(drift bool) {
	np := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "np-probe"}, Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
		ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
		ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
		Budgets:             []v1.Budget{{Nodes: "100%"}},
	}}})
	pe.nodePool = np
	ExpectApplied(ctx, env.Client, np)
	its := fake.InstanceTypesAssorted()
	it := its[len(its)-1]
	of := it.Offerings[0]
	var ncs []*v1.NodeClaim
	var nodes []*corev1.Node
	for i := 0; i < 6; i++ {
		nc, n := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("probe-%d", i), Labels: map[string]string{
				v1.NodePoolLabelKey:            np.Name,
				corev1.LabelInstanceTypeStable: it.Name,
				v1.CapacityTypeLabelKey:        of.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
				corev1.LabelTopologyZone:       of.Requirements.Get(corev1.LabelTopologyZone).Any(),
			}},
			Status: v1.NodeClaimStatus{ProviderID: fmt.Sprintf("fake://probe-%d", i),
				Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("32"), corev1.ResourcePods: resource.MustParse("100")}},
		})
		n.Name = nc.Name
		nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		if (drift && i == 1) || (pe.manyDrift && i >= 1 && i <= 3) {
			nc.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
		}
		ExpectApplied(ctx, env.Client, nc, n)
		ncs = append(ncs, nc)
		nodes = append(nodes, n)
	}
	ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, probeClock, nodeStateController, nodeClaimStateController, nodes, ncs)
	for i := 1; i < 6; i++ {
		for j := 0; j < i; j++ {
			pod := test.Pod(test.PodOptions{
				ObjectMeta:           metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d-%d", i, j), OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs", UID: "rs", Controller: lo.ToPtr(true)}}},
				NodeName:             nodes[i].Name,
				Phase:                corev1.PodRunning,
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(fmt.Sprintf("%dm", 200*i))}},
			})
			ExpectApplied(ctx, env.Client, pod)
			ExpectReconcileSucceeded(ctx, podStateController, client.ObjectKeyFromObject(pod))
		}
	}
	aa := test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Name: "pending-aa", Labels: map[string]string{"app": "aa"}},
		PodAntiPreferences: []corev1.WeightedPodAffinityTerm{{Weight: 1, PodAffinityTerm: corev1.PodAffinityTerm{
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "aa"}}, TopologyKey: corev1.LabelHostname}}},
		ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}}})
	pvc := test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Name: "pending-pvc"}, PersistentVolumeClaims: []string{"missing-pvc"}})
	ExpectApplied(ctx, env.Client, aa, pvc)
	pe.pending = []*corev1.Pod{aa, pvc}
}

type probeCfg struct {
	shadow      bool
	only        func(disruption.Method) bool
	shadowClock clock.Clock
	wrap        func(s *pipeline.Shadow)
	passes      int
	drift       bool
}

func (pe *probeEnv) setup(cfg probeCfg) {
	pe.build(cfg.drift)
	pe.methods = disruption.NewMethods(ctx, probeClock, cluster, pe.rc, prov, cloudProvider, recorder, queue)
	for _, m := range pe.methods {
		if s, ok := m.(*disruption.SingleNodeConsolidation); ok {
			s.PreviouslyUnseenNodePools = sets.New("unseen")
		}
	}
	if cfg.only != nil {
		pe.methods = lo.Filter(pe.methods, func(m disruption.Method, _ int) bool { return cfg.only(m) })
	}
	opts := []option.Function[disruption.ControllerOptions]{disruption.WithMethods(pe.methods...)}
	if cfg.shadow {
		clk := cfg.shadowClock
		if clk == nil {
			clk = probeClock
		}
		sctx := log.IntoContext(ctx, logr.Discard())
		pe.shadow = pipeline.NewShadow(sctx, clk, pe.src, prov, cloudProvider, cluster, queue, clusterCost, 1)
		pe.shadow.SetShadowLogger(funcr.New(func(prefix, args string) { fmt.Fprintln(GinkgoWriter, "SHADOW", args) }, funcr.Options{}))
		if cfg.wrap != nil {
			cfg.wrap(pe.shadow)
		}
		opts = append(opts, disruption.WithComputeObserver(pe.shadow))
	}
	pe.legacy = disruption.NewController(ctx, probeClock, pe.rc, prov, cloudProvider, recorder, cluster, queue, clusterCost, opts...)
}

// reconcile runs one Reconcile, stepping the fake clock through validation waits.
func (pe *probeEnv) reconcile(c context.Context) time.Duration {
	done := make(chan struct{})
	go func() {
		defer GinkgoRecover()
		defer close(done)
		ExpectSingletonReconciled(c, pe.legacy)
	}()
	start := time.Now()
	for {
		select {
		case <-done:
			return time.Since(start)
		case <-time.After(2 * time.Millisecond):
			if probeClock.HasWaiters() {
				probeClock.Step(disruption.CommandValidationDelay + time.Second)
			}
		}
	}
}

func (pe *probeEnv) observe() []string {
	var out []string
	cmds := lo.Map(queue.GetCommands(), func(c *disruption.Command, _ int) string {
		cs := lo.Map(c.Candidates, func(c *disruption.Candidate, _ int) string { return c.Name() })
		sort.Strings(cs)
		reps := lo.Map(c.Replacements, func(r *disruption.Replacement, _ int) string {
			its := lo.Map(r.InstanceTypeOptions, func(it *cloudprovider.InstanceType, _ int) string { return it.Name })
			sort.Strings(its)
			return strings.Join(its, ",")
		})
		sort.Strings(reps)
		return fmt.Sprintf("%s %v %v", c.Decision(), cs, reps)
	})
	sort.Strings(cmds)
	out = append(out, fmt.Sprintf("commands=%v", cmds))
	evs := lo.Map(recorder.Events(), func(e events.Event, _ int) string {
		return fmt.Sprintf("%s/%s/%s/%s", e.Type, e.Reason, fmt.Sprintf("%v", lo.Must(apimeta.Accessor(e.InvolvedObject)).GetName()), e.Message)
	})
	sort.Strings(evs)
	out = append(out, fmt.Sprintf("events=%v", evs))
	for _, m := range pe.methods {
		s := fmt.Sprintf("%T", m)
		if sc, ok := m.(*disruption.SingleNodeConsolidation); ok {
			s += fmt.Sprintf(" unseen=%v", sets.List(sc.PreviouslyUnseenNodePools))
		}
		if d, ok := m.(interface{ IsConsolidated() bool }); ok {
			s += fmt.Sprintf(" consolidated=%v", d.IsConsolidated())
		}
		out = append(out, s)
	}
	var nodeLines []string
	for _, n := range cluster.DeepCopyNodes() {
		nodeLines = append(nodeLines, fmt.Sprintf("node %s deleting=%v nominated=%v", genSuffix.ReplaceAllString(n.Name(), "-X"), n.MarkedForDeletion(), n.Nominated(probeClock)))
	}
	sort.Strings(nodeLines)
	out = append(out, nodeLines...)
	for _, p := range pe.pending {
		k := client.ObjectKeyFromObject(p)
		out = append(out, fmt.Sprintf("pod %s decided=%v ack=%v ok=%v", p.Name, cluster.PodSchedulingDecisionTime(k).Sub(probeBase), cluster.PodAckTime(k).Sub(probeBase), cluster.PodSchedulingSuccessTime(k).Sub(probeBase)))
	}
	pe.mu.Lock()
	ws := append([]string{}, pe.writes...)
	sort.Strings(ws)
	out = append(out, fmt.Sprintf("writes=%v", ws))
	pe.mu.Unlock()
	return out
}

// metricValues: every karpenter_* series except the shadow's; counters and gauges by value; histograms by count, and
// by sum unless they time something.
func metricValues() map[string]float64 {
	out := map[string]float64{}
	for _, mf := range lo.Must(crmetrics.Registry.Gather()) {
		name := mf.GetName()
		// Timestamps record the fake clock, which the stepping helper moves by a load-dependent amount.
		if !strings.HasPrefix(name, "karpenter_") || strings.Contains(name, "pipeline_shadow") || strings.Contains(name, "timestamp") {
			continue
		}
		for _, m := range mf.Metric {
			if lo.SomeBy(m.GetLabel(), func(l *dto.LabelPair) bool { return l.GetValue() == "disruption.shadow" }) {
				continue // the shadow's own work, attributed to it
			}
			k := name + fmt.Sprint(m.GetLabel())
			switch {
			case m.Counter != nil:
				out[k] = m.GetCounter().GetValue()
			case m.Gauge != nil:
				out[k+" (gauge)"] = m.GetGauge().GetValue()
			case m.Histogram != nil:
				out[k+" count"] = float64(m.GetHistogram().GetSampleCount())
				if !strings.Contains(name, "seconds") && !strings.Contains(name, "duration") {
					out[k+" sum"] = m.GetHistogram().GetSampleSum()
				}
			}
		}
	}
	return out
}

func metricDelta(before, after map[string]float64) map[string]float64 {
	out := map[string]float64{}
	for k, v := range after {
		if strings.HasSuffix(k, "(gauge)") {
			out[k] = v
			continue
		}
		if d := v - before[k]; d != 0 {
			out[k] = d
		}
	}
	return out
}

type probeResult struct {
	passes  [][]string
	metrics map[string]float64
	logs    []string
	swrites []string
	shadow  *pipeline.Shadow
}

var logMsg = regexp.MustCompile(`"level"=\d+ "msg"="[^"]*"`)
var logNoise = regexp.MustCompile(`"(duration|took|elapsed|[a-zA-Z_]*[Dd]uration)":"?[^,}]*"?|"[a-z]+-[a-z0-9]{5}"|-[a-z0-9]{5}"|"ID":"[^"]*"|"command-id":"[^"]*"`)

func runProbe(cfg probeCfg) probeResult {
	pe := newProbeEnv()
	pe.setup(cfg)
	before := metricValues()
	res := probeResult{shadow: pe.shadow}
	for range lo.Ternary(cfg.passes == 0, 4, cfg.passes) {
		pe.reconcile(pe.lctx)
		if pe.shadow != nil {
			pe.shadow.WaitIdle()
		}
		res.passes = append(res.passes, pe.observe())
		// bring new NodeClaims into cluster state so the next pass is synced
		ncs := &v1.NodeClaimList{}
		Expect(env.Client.List(ctx, ncs)).To(Succeed())
		for i := range ncs.Items {
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(&ncs.Items[i]))
		}
	}
	res.metrics = metricDelta(before, metricValues())
	pe.logsMu.Lock()
	res.logs = lo.Map(lo.Filter(pe.logs, func(l string, _ int) bool {
		return !strings.Contains(l, "Body") && !strings.Contains(l, "HTTP") && !strings.Contains(l, "\"level\"=5") && !strings.Contains(l, "\"level\"=6")
	}), func(l string, _ int) string { return logMsg.FindString(l) })
	sort.Strings(res.logs)
	_ = lo.Map(pe.logs, func(l string, _ int) string { return logNoise.ReplaceAllString(l, "") })
	pe.logsMu.Unlock()
	pe.mu.Lock()
	res.swrites = append([]string{}, pe.swrites...)
	pe.mu.Unlock()
	return res
}

func diffProbe(a, b probeResult) []string {
	var d []string
	for i := range max(len(a.passes), len(b.passes)) {
		var pa, pb []string
		if i < len(a.passes) {
			pa = a.passes[i]
		}
		if i < len(b.passes) {
			pb = b.passes[i]
		}
		if fmt.Sprint(pa) != fmt.Sprint(pb) {
			d = append(d, fmt.Sprintf("pass %d:\n  A=%v\n  B=%v", i, pa, pb))
		}
	}
	for k := range lo.Assign(a.metrics, b.metrics) {
		if a.metrics[k] != b.metrics[k] {
			d = append(d, fmt.Sprintf("metric %s: A=%v B=%v", k, a.metrics[k], b.metrics[k]))
		}
	}
	al, bl := strings.Join(a.logs, "\n"), strings.Join(b.logs, "\n")
	if al != bl {
		onlyB := lo.Without(b.logs, a.logs...)
		onlyA := lo.Without(a.logs, b.logs...)
		d = append(d, fmt.Sprintf("logs differ: %d vs %d lines; only A=%v\n only B=%v", len(a.logs), len(b.logs), onlyA, onlyB))
	}
	sort.Strings(d)
	return d
}

var _ = Describe("Shadow differential", func() {
	isSingle := func(m disruption.Method) bool { _, ok := m.(*disruption.SingleNodeConsolidation); return ok }
	isMulti := func(m disruption.Method) bool { _, ok := m.(*disruption.MultiNodeConsolidation); return ok }
	isEmpty := func(m disruption.Method) bool { _, ok := m.(*disruption.Emptiness); return ok }
	isDrift := func(m disruption.Method) bool { _, ok := m.(*disruption.Drift); return ok }
	forceAll := func(n int) func(s *pipeline.Shadow) {
		return func(s *pipeline.Shadow) {
			for _, name := range []string{pipeline.RepairPlanner, pipeline.EmptinessPlanner, pipeline.StaticDriftPlanner, pipeline.DriftPlanner,
				pipeline.MultiNodePlanner, pipeline.SingleNodePlanner} {
				if p := s.ShadowPlanner(name); p != nil {
					s.SetShadowPlanner(name, &emptyFirst{Planner: p, n: n})
				}
			}
		}
	}
	type tc struct {
		only  func(disruption.Method) bool
		wrap  func(*pipeline.Shadow)
		clk   func() clock.Clock
		drift bool
	}
	DescribeTable("doesn't change what the disruption controller does",
		func(c tc) {
			mk := func(sh bool) probeCfg {
				cfg := probeCfg{shadow: sh, only: c.only, wrap: c.wrap, drift: c.drift}
				if c.clk != nil {
					cfg.shadowClock = c.clk()
				}
				return cfg
			}
			a := runProbe(mk(false))
			a2 := runProbe(mk(false))
			for _, l := range diffProbe(a, a2) {
				fmt.Fprintln(GinkgoWriter, "CONTROLDIFF", l)
			}
			Expect(len(diffProbe(a, a2))).To(BeZero(), "control: legacy alone is not deterministic")
			b := runProbe(mk(true))
			mm := differentialCounts("karpenter_voluntary_disruption_pipeline_shadow_mismatches_total")
			cmp := differentialCounts("karpenter_voluntary_disruption_pipeline_shadow_comparisons_total")
			fmt.Fprintf(GinkgoWriter, "comparisons=%v mismatches=%v\n", cmp, mm)
			d := diffProbe(a, b)
			for _, l := range d {
				fmt.Fprintln(GinkgoWriter, "DIFF", l)
			}
			Expect(b.swrites).To(BeEmpty())
			Expect(len(d)).To(BeZero())
		},
		Entry("all methods, forced mismatches", tc{wrap: forceAll(1 << 30), drift: true}),
		Entry("all methods, race (first sample only)", tc{wrap: forceAll(1), drift: true}),
		Entry("all methods, no forcing", tc{drift: true}),
		Entry("single only, forced", tc{only: isSingle, wrap: forceAll(1 << 30)}),
		Entry("single only, race", tc{only: isSingle, wrap: forceAll(1)}),
		Entry("single only, forced, shadow times out", tc{only: isSingle, wrap: forceAll(1 << 30), clk: func() clock.Clock {
			return &jumpClock{FakeClock: clocktesting.NewFakeClock(probeBase), step: 5 * time.Minute}
		}}),
		Entry("multi only, forced", tc{only: isMulti, wrap: forceAll(1 << 30)}),
		Entry("multi only, forced, shadow times out", tc{only: isMulti, wrap: forceAll(1 << 30), clk: func() clock.Clock {
			return &jumpClock{FakeClock: clocktesting.NewFakeClock(probeBase), step: 20 * time.Second}
		}}),
		Entry("emptiness only, forced", tc{only: isEmpty, wrap: forceAll(1 << 30)}),
		Entry("drift only, forced", tc{only: isDrift, wrap: forceAll(1 << 30), drift: true}),
	)
})

func differentialCounts(name string) map[string]float64 {
	out := map[string]float64{}
	for _, mf := range lo.Must(crmetrics.Registry.Gather()) {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.Metric {
			out[fmt.Sprint(m.GetLabel())] = m.GetCounter().GetValue()
		}
	}
	return out
}

func shadowGoroutines() int {
	buf := make([]byte, 1<<22)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "pipeline.(*Shadow).run(")
}

// Off the disruption controller's path (RFC D.3, re-review R-3): a slow shadow planner, in the comparison or in the
// recheck, doesn't delay the disruption controller's pass, and no computation is sampled while a sample runs.
var _ = Describe("Shadow latency", func() {
	driftOnly := func(m disruption.Method) bool { _, ok := m.(*disruption.Drift); return ok }
	BeforeEach(func() {
		pipeline.ShadowMismatchesTotal.Reset()
		pipeline.ShadowComparisonsTotal.Reset()
	})
	DescribeTable("a slow shadow planner doesn't delay the disruption controller",
		func(slowFrom, slowTo int64) {
			pe := newProbeEnv()
			pe.manyDrift = true
			var sp *slowPlanner
			pe.setup(probeCfg{shadow: true, only: driftOnly, drift: true, wrap: func(s *pipeline.Shadow) {
				sp = &slowPlanner{Planner: s.ShadowPlanner(pipeline.DriftPlanner), empty: 1, slowFrom: slowFrom, slowTo: slowTo, d: 3 * time.Second}
				s.SetShadowPlanner(pipeline.DriftPlanner, sp)
			}})
			took := pe.reconcile(pe.lctx)
			Expect(took).To(BeNumerically("<", time.Second))
			Expect(queue.GetCommands()).To(HaveLen(1))
			Expect(shadowGoroutines()).To(Equal(1))
			// A second pass while the sample runs isn't sampled.
			before := differentialCounts("karpenter_voluntary_disruption_pipeline_shadow_comparisons_total")
			ncs := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, ncs)).To(Succeed())
			for i := range ncs.Items {
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(&ncs.Items[i]))
			}
			pe.reconcile(pe.lctx)
			Expect(differentialCounts("karpenter_voluntary_disruption_pipeline_shadow_comparisons_total")).To(Equal(before))
			pe.shadow.WaitIdle()
			Expect(shadowGoroutines()).To(BeZero())
		},
		Entry("in the comparison", int64(1), int64(1)),
		Entry("in the recheck", int64(2), int64(2)),
	)
	It("stops when its context is cancelled, recording nothing", func() {
		pe := newProbeEnv()
		// Drifted nodes enough that the recheck always reaches the (blocking) planner, whatever started meanwhile.
		pe.manyDrift = true
		pe.setup(probeCfg{shadow: true, only: driftOnly, drift: true, wrap: func(s *pipeline.Shadow) {
			s.SetShadowPlanner(pipeline.DriftPlanner, &slowPlanner{Planner: s.ShadowPlanner(pipeline.DriftPlanner), empty: 1, slowFrom: 2, slowTo: 100, blockOnCtx: true})
		}})
		cctx, cancel := context.WithCancel(pe.lctx)
		pe.reconcile(cctx)
		Expect(shadowGoroutines()).To(Equal(1))
		cancel()
		done := make(chan struct{})
		go func() { pe.shadow.WaitIdle(); close(done) }()
		Eventually(done, 5*time.Second).Should(BeClosed())
		Expect(shadowGoroutines()).To(BeZero())
		Expect(differentialCounts("karpenter_voluntary_disruption_pipeline_shadow_mismatches_total")).To(BeEmpty())
	})
})
