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

package pdb_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	karpenterv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
)

// stringRecorder records events as strings so two recorders' sequences can be compared.
type stringRecorder struct{ events []string }

func (r *stringRecorder) Publish(evts ...events.Event) {
	for _, e := range evts {
		r.events = append(r.events, fmt.Sprintf("%s/%s/%s/%s/%v", e.Type, e.Reason, e.Message,
			client.ObjectKeyFromObject(e.InvolvedObject.(client.Object)), e.DedupeValues))
	}
}

// sortedClient lists PDBs sorted by namespace and name. The test environment's List order isn't stable, and the order
// of the keys CanEvictPods returns for a pod with several PDBs follows the List order, so the differential test gives
// every side the same order.
type sortedClient struct{ client.Client }

func (c sortedClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := c.Client.List(ctx, list, opts...); err != nil {
		return err
	}
	if l, ok := list.(*policyv1.PodDisruptionBudgetList); ok {
		sort.Slice(l.Items, func(i, j int) bool {
			return client.ObjectKeyFromObject(&l.Items[i]).String() < client.ObjectKeyFromObject(&l.Items[j]).String()
		})
	}
	return nil
}

var _ = Describe("Index", func() {
	It("reuses a PDB's parsed form only while its UID and resourceVersion are unchanged", func() {
		p := test.PodDisruptionBudget(test.PDBOptions{Labels: podLabels, MaxUnavailable: lo.ToPtr(intstr.FromInt32(1))})
		other := test.PodDisruptionBudget(test.PDBOptions{Labels: map[string]string{"other": "value"}})
		ExpectApplied(ctx, env.Client, p, other)
		idx := pdb.NewIndex()
		ictx := pdb.WithIndex(ctx, idx)

		_, err := pdb.NewLimits(ictx, env.Client)
		Expect(err).ToNot(HaveOccurred())
		first := pdb.IndexEntries(idx)
		Expect(first).To(HaveLen(2))
		_, err = pdb.NewLimits(ictx, env.Client)
		Expect(err).ToNot(HaveOccurred())
		second := pdb.IndexEntries(idx)
		Expect(second[string(p.UID)]).To(BeIdenticalTo(first[string(p.UID)]))
		Expect(second[string(other.UID)]).To(BeIdenticalTo(first[string(other.UID)]))

		// A status change bumps the resourceVersion, so the PDB is parsed again; the other PDB is reused.
		p.Status.DisruptionsAllowed = 1
		Expect(env.Client.Status().Update(ctx, p)).To(Succeed())
		_, err = pdb.NewLimits(ictx, env.Client)
		Expect(err).ToNot(HaveOccurred())
		third := pdb.IndexEntries(idx)
		Expect(third[string(p.UID)]).ToNot(BeIdenticalTo(second[string(p.UID)]))
		Expect(third[string(other.UID)]).To(BeIdenticalTo(second[string(other.UID)]))

		// A deleted PDB is dropped from the Index.
		ExpectDeleted(ctx, env.Client, other)
		_, err = pdb.NewLimits(ictx, env.Client)
		Expect(err).ToNot(HaveOccurred())
		Expect(pdb.IndexEntries(idx)).To(HaveLen(1))
	})
	It("doesn't double count a PDB whose In requirement repeats a value", func() {
		p := test.PodDisruptionBudget(test.PDBOptions{MaxUnavailable: lo.ToPtr(intstr.FromInt32(1)), Status: &policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 1}})
		p.Spec.Selector = &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "app", Operator: metav1.LabelSelectorOpIn, Values: []string{"a", "a", "b"}}}}
		ExpectApplied(ctx, env.Client, p)
		pod := test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "a"}}})
		limits, err := pdb.NewLimits(pdb.WithIndex(ctx, pdb.NewIndex()), env.Client)
		Expect(err).ToNot(HaveOccurred())
		keys, ok := limits.CanEvictPods([]*v1.Pod{pod}, env.Clock, nil)
		Expect(ok).To(BeTrue())
		Expect(keys).To(BeEmpty())
	})
	// Differential test: with an Index that lives across PDB creates, deletes, recreates, spec, status and metadata
	// changes and clock steps, NewLimits gives the same CanEvictPods and IsCurrentlyReschedulable results and events as
	// NewLimits before the Index (LegacyNewLimits) and as NewLimits without an Index, for random PDBs and pods.
	// KARPENTER_PDB_DIFF_SEEDS sets the number of seeds (default 6).
	It("gives the same results and events as NewLimits without an Index", func() {
		seeds := 6
		if raw := os.Getenv("KARPENTER_PDB_DIFF_SEEDS"); raw != "" {
			var err error
			seeds, err = strconv.Atoi(raw)
			Expect(err).ToNot(HaveOccurred())
		}
		for seed := 1; seed <= seeds; seed++ {
			runPDBDifferential(int64(seed))
		}
	})
})

// nolint:gocyclo
func runPDBDifferential(seed int64) {
	r := rand.New(rand.NewSource(seed))
	clk := clock.NewFakeClock(time.Now())
	namespaces := lo.Times(3, func(_ int) string {
		ns := test.Namespace()
		ExpectApplied(ctx, env.Client, ns)
		return ns.Name
	})
	keys, values := []string{"app", "tier", "team"}, []string{"a", "b", "c"}
	randomSelector := func() *metav1.LabelSelector {
		switch r.Intn(9) {
		case 0:
			return &metav1.LabelSelector{}
		case 1:
			return &metav1.LabelSelector{MatchLabels: map[string]string{keys[r.Intn(3)]: values[r.Intn(3)], keys[r.Intn(3)]: values[r.Intn(3)]}}
		case 2:
			return &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: keys[r.Intn(3)], Operator: metav1.LabelSelectorOpIn,
				Values: []string{values[r.Intn(3)], values[r.Intn(3)]}}}}
		case 3:
			return &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: keys[r.Intn(3)], Operator: metav1.LabelSelectorOpNotIn, Values: []string{values[r.Intn(3)]}}}}
		case 4:
			return &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: keys[r.Intn(3)], Operator: metav1.LabelSelectorOpExists}}}
		case 5:
			return &metav1.LabelSelector{MatchLabels: map[string]string{keys[r.Intn(3)]: values[r.Intn(3)]},
				MatchExpressions: []metav1.LabelSelectorRequirement{{Key: keys[r.Intn(3)], Operator: metav1.LabelSelectorOpDoesNotExist}}}
		default:
			return &metav1.LabelSelector{MatchLabels: map[string]string{keys[r.Intn(3)]: values[r.Intn(3)]}}
		}
	}
	randomSpec := func(p *policyv1.PodDisruptionBudget) {
		p.Spec.Selector = randomSelector()
		p.Spec.MinAvailable, p.Spec.MaxUnavailable = nil, nil
		budgets := []intstr.IntOrString{intstr.FromInt32(0), intstr.FromInt32(1), intstr.FromString("0%"), intstr.FromString("50%"), intstr.FromString("100%")}
		if r.Intn(2) == 0 {
			p.Spec.MinAvailable = lo.ToPtr(budgets[r.Intn(len(budgets))])
		} else {
			p.Spec.MaxUnavailable = lo.ToPtr(budgets[r.Intn(len(budgets))])
		}
		p.Spec.UnhealthyPodEvictionPolicy = []*policyv1.UnhealthyPodEvictionPolicyType{nil, lo.ToPtr(policyv1.AlwaysAllow), lo.ToPtr(policyv1.IfHealthyBudget)}[r.Intn(3)]
	}
	newPDB := func() *policyv1.PodDisruptionBudget {
		p := test.PodDisruptionBudget(test.PDBOptions{ObjectMeta: metav1.ObjectMeta{Namespace: namespaces[r.Intn(3)]},
			Status: &policyv1.PodDisruptionBudgetStatus{ObservedGeneration: 1, DisruptionsAllowed: int32(r.Intn(3))}})
		randomSpec(p)
		return p
	}
	randomLabels := func() map[string]string {
		l := map[string]string{}
		for _, k := range keys {
			if r.Intn(3) > 0 {
				l[k] = values[r.Intn(3)]
			}
		}
		return l
	}
	pods := lo.Times(60, func(i int) *v1.Pod {
		p := test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Namespace: namespaces[r.Intn(3)], Labels: randomLabels(), UID: types.UID(fmt.Sprintf("pod-%d", i))}})
		p.Status.StartTime = &metav1.Time{Time: clk.Now()}
		switch r.Intn(10) {
		case 0:
			p.Annotations = map[string]string{karpenterv1.DoNotDisruptAnnotationKey: "true"}
		case 1:
			p.Annotations = map[string]string{karpenterv1.DoNotDisruptAnnotationKey: "10m"}
		case 2:
			p.Annotations = map[string]string{karpenterv1.DoNotDisruptAnnotationKey: "not-a-duration"}
		case 3:
			p.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionFalse}}
		case 4:
			p.DeletionTimestamp = &metav1.Time{Time: clk.Now()}
		case 5:
			p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: "n", UID: "n"}}
		case 6:
			p.Spec.Tolerations = []v1.Toleration{{Operator: v1.TolerationOpExists}}
		case 7:
			p.Status.Phase = v1.PodSucceeded
		}
		return p
	})
	pdbs := lo.Times(8+r.Intn(8), func(_ int) *policyv1.PodDisruptionBudget { return newPDB() })
	for _, p := range pdbs {
		ExpectApplied(ctx, env.Client, p)
	}

	kubeClient := sortedClient{Client: env.Client}
	idx := pdb.NewIndex()
	ictx := pdb.WithIndex(ctx, idx)
	for step := 0; step < 40; step++ {
		op := []string{"none", "create", "delete", "recreate", "spec", "status", "metadata", "clock"}[r.Intn(8)]
		i := r.Intn(len(pdbs))
		switch op {
		case "create":
			pdbs = append(pdbs, newPDB())
			ExpectApplied(ctx, env.Client, pdbs[len(pdbs)-1])
		case "delete":
			if len(pdbs) > 1 {
				ExpectDeleted(ctx, env.Client, pdbs[i])
				pdbs = append(pdbs[:i], pdbs[i+1:]...)
			}
		case "recreate":
			// Same name, new UID, possibly a different spec.
			ExpectDeleted(ctx, env.Client, pdbs[i])
			p := test.PodDisruptionBudget(test.PDBOptions{ObjectMeta: metav1.ObjectMeta{Name: pdbs[i].Name, Namespace: pdbs[i].Namespace},
				Status: &policyv1.PodDisruptionBudgetStatus{ObservedGeneration: 1, DisruptionsAllowed: int32(r.Intn(3))}})
			p.Spec = *pdbs[i].Spec.DeepCopy()
			if r.Intn(2) == 0 {
				randomSpec(p)
			}
			pdbs[i] = p
			ExpectApplied(ctx, env.Client, p)
		case "spec":
			randomSpec(pdbs[i])
			ExpectApplied(ctx, env.Client, pdbs[i])
		case "status":
			pdbs[i].Status.DisruptionsAllowed = int32(r.Intn(3))
			ExpectApplied(ctx, env.Client, pdbs[i])
		case "metadata":
			pdbs[i].Labels = map[string]string{"step": strconv.Itoa(step)}
			ExpectApplied(ctx, env.Client, pdbs[i])
		case "clock":
			clk.Step(time.Duration(r.Intn(15)) * time.Minute)
		}

		legacy, err := pdb.LegacyNewLimits(ctx, kubeClient)
		Expect(err).ToNot(HaveOccurred())
		withIndex, err := pdb.NewLimits(ictx, kubeClient)
		Expect(err).ToNot(HaveOccurred())
		withoutIndex, err := pdb.NewLimits(ctx, kubeClient)
		Expect(err).ToNot(HaveOccurred())

		desc := fmt.Sprintf("seed=%d step=%d op=%s", seed, step, op)
		for _, p := range pods {
			lr, ir, nr := &stringRecorder{}, &stringRecorder{}, &stringRecorder{}
			want := legacy.LegacyIsCurrentlyReschedulable(p, clk, lr)
			Expect(withIndex.IsCurrentlyReschedulable(p, clk, ir)).To(Equal(want), "%s IsCurrentlyReschedulable(%s) with Index", desc, p.UID)
			Expect(withoutIndex.IsCurrentlyReschedulable(p, clk, nr)).To(Equal(want), "%s IsCurrentlyReschedulable(%s) without Index", desc, p.UID)
			Expect(ir.events).To(Equal(lr.events), "%s IsCurrentlyReschedulable(%s) events with Index", desc, p.UID)
			Expect(nr.events).To(Equal(lr.events), "%s IsCurrentlyReschedulable(%s) events without Index", desc, p.UID)
		}
		// CanEvictPods for each pod alone and for random groups (a node's pods).
		groups := lo.Map(pods, func(p *v1.Pod, _ int) []*v1.Pod { return []*v1.Pod{p} })
		for g := 0; g < 20; g++ {
			groups = append(groups, lo.Samples(pods, 1+r.Intn(8)))
		}
		for _, group := range groups {
			lr, ir, nr := &stringRecorder{}, &stringRecorder{}, &stringRecorder{}
			wantKeys, want := legacy.LegacyCanEvictPods(group, clk, lr)
			gotKeys, got := withIndex.CanEvictPods(group, clk, ir)
			Expect(got).To(Equal(want), "%s CanEvictPods with Index", desc)
			Expect(gotKeys).To(Equal(wantKeys), "%s CanEvictPods keys with Index", desc)
			Expect(ir.events).To(Equal(lr.events), "%s CanEvictPods events with Index", desc)
			gotKeys, got = withoutIndex.CanEvictPods(group, clk, nr)
			Expect(got).To(Equal(want), "%s CanEvictPods without Index", desc)
			Expect(gotKeys).To(Equal(wantKeys), "%s CanEvictPods keys without Index", desc)
			Expect(nr.events).To(Equal(lr.events), "%s CanEvictPods events without Index", desc)
		}
	}
	for _, p := range pdbs {
		ExpectDeleted(ctx, env.Client, p)
	}
}
