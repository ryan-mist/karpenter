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

package scheduling

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// The index is a pure pre-filter, so every lookup must return exactly the groups found by scanning all of them.
var _ = Describe("Topology Group Index", func() {
	var ctx context.Context
	BeforeEach(func() {
		ctx = context.Background()
	})

	for seed := int64(0); seed < 25; seed++ {
		It(fmt.Sprintf("should match a linear scan of all topology groups (seed=%d)", seed), func() {
			r := rand.New(rand.NewSource(seed))
			g := &topologyIndexGen{r: r}
			t := newTopologyForIndexTest()
			ref := newTopologyForIndexTest()

			// pods that already run in the cluster and own inverse anti-affinity groups
			for i := 0; i < 20; i++ {
				p := g.pod(fmt.Sprintf("existing-%d", i))
				if p.Spec.Affinity == nil || p.Spec.Affinity.PodAntiAffinity == nil {
					continue
				}
				nodeLabels := map[string]string{corev1.LabelTopologyZone: g.zone(), corev1.LabelHostname: g.hostname()}
				Expect(t.updateInverseAntiAffinity(ctx, p, nodeLabels)).To(Succeed())
				Expect(ref.updateInverseAntiAffinity(ctx, p, nodeLabels)).To(Succeed())
			}
			// pods that we are scheduling, some of which are updated again as if they had been relaxed
			var pods []*corev1.Pod
			for i := 0; i < 60; i++ {
				p := g.pod(fmt.Sprintf("pending-%d", i))
				pods = append(pods, p)
				Expect(t.Update(ctx, p)).To(Succeed())
				Expect(ref.Update(ctx, p)).To(Succeed())
			}
			for i := 0; i < 80; i++ {
				p := pods[r.Intn(len(pods))]
				g.relax(p)
				Expect(t.Update(ctx, p)).To(Succeed())
				Expect(ref.Update(ctx, p)).To(Succeed())
			}

			probes := append([]*corev1.Pod{}, pods...)
			for i := 0; i < 60; i++ {
				probes = append(probes, g.pod(fmt.Sprintf("probe-%d", i)))
			}
			for _, p := range probes {
				expectOwnerIndexConsistent(t, p.UID)
				requirements := g.nodeRequirements()
				Expect(groupHashes(t.getMatchingTopologies(p, nil, requirements))).To(Equal(groupHashes(linearMatchingTopologies(t, p, nil, requirements))))
			}

			// Record must have the same effect on every group as recording with a linear scan
			for i := 0; i < 200; i++ {
				p := probes[r.Intn(len(probes))]
				requirements := g.nodeRequirements()
				t.Record(p, nil, requirements)
				linearRecord(ref, p, nil, requirements)
			}
			Expect(groupCounts(t.topologyGroups)).To(Equal(groupCounts(ref.topologyGroups)))
			Expect(groupCounts(t.inverseTopologyGroups)).To(Equal(groupCounts(ref.inverseTopologyGroups)))
		})
	}
	It("should support concurrent reads", func() {
		g := &topologyIndexGen{r: rand.New(rand.NewSource(42))}
		t := newTopologyForIndexTest()
		var pods []*corev1.Pod
		for i := 0; i < 50; i++ {
			p := g.pod(fmt.Sprintf("pending-%d", i))
			pods = append(pods, p)
			Expect(t.Update(ctx, p)).To(Succeed())
		}
		requirements := g.nodeRequirements()
		var wg sync.WaitGroup
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				for _, p := range pods {
					_, _ = t.AddRequirements(p, nil, scheduling.NewRequirements(), requirements)
				}
			}()
		}
		wg.Wait()
	})
})

func newTopologyForIndexTest() *legacyTopology {
	return &legacyTopology{
		kubeClient:            fakecr.NewFakeClient(),
		preferencePolicy:      PreferencePolicyRespect,
		domainGroups:          map[string]TopologyDomainGroup{},
		topologyGroups:        map[uint64]*legacyTopologyGroup{},
		inverseTopologyGroups: map[uint64]*legacyTopologyGroup{},
		topologyIndex:         newTopologyGroupIndex[*legacyTopologyGroup](),
		inverseTopologyIndex:  newTopologyGroupIndex[*legacyTopologyGroup](),
		excludedPods:          sets.New[string](),
	}
}

func expectOwnerIndexConsistent(t *legacyTopology, uid types.UID) {
	GinkgoHelper()
	owned := func(groups map[uint64]*legacyTopologyGroup) []uint64 {
		return groupHashes(lo.Filter(lo.Values(groups), func(tg *legacyTopologyGroup, _ int) bool { return tg.IsOwnedBy(uid) }))
	}
	Expect(groupHashes(t.topologyIndex.ownedBy(uid))).To(Equal(owned(t.topologyGroups)))
	Expect(groupHashes(t.inverseTopologyIndex.ownedBy(uid))).To(Equal(owned(t.inverseTopologyGroups)))
	// owners are unique
	Expect(lo.Uniq(t.topologyIndex.ownedBy(uid))).To(HaveLen(len(t.topologyIndex.ownedBy(uid))))
	Expect(lo.Uniq(t.inverseTopologyIndex.ownedBy(uid))).To(HaveLen(len(t.inverseTopologyIndex.ownedBy(uid))))
}

// linearMatchingTopologies is getMatchingTopologies implemented as a scan of every group.
func linearMatchingTopologies(t *legacyTopology, p *corev1.Pod, taints []corev1.Taint, requirements scheduling.Requirements) []*legacyTopologyGroup {
	var matchingTopologies []*legacyTopologyGroup
	for _, tg := range t.topologyGroups {
		if tg.IsOwnedBy(p.UID) {
			matchingTopologies = append(matchingTopologies, tg)
		}
	}
	for _, tg := range t.inverseTopologyGroups {
		if tg.Counts(p, taints, requirements) {
			matchingTopologies = append(matchingTopologies, tg)
		}
	}
	return matchingTopologies
}

// linearRecord is legacyTopology.Record implemented as a scan of every group.
func linearRecord(t *legacyTopology, p *corev1.Pod, taints []corev1.Taint, requirements scheduling.Requirements) {
	for _, tg := range t.topologyGroups {
		if tg.Counts(p, taints, requirements) {
			domains := requirements.Get(tg.Key)
			if tg.Type == TopologyTypePodAntiAffinity {
				tg.Record(domains.Values()...)
			} else if domains.Len() == 1 {
				tg.Record(domains.Values()[0])
			}
		}
	}
	for _, tg := range t.inverseTopologyGroups {
		if tg.IsOwnedBy(p.UID) {
			tg.Record(requirements.Get(tg.Key).Values()...)
		}
	}
}

func groupHashes(groups []*legacyTopologyGroup) []uint64 {
	hashes := lo.Map(groups, func(tg *legacyTopologyGroup, _ int) uint64 { return tg.Hash() })
	// sort so that comparisons are independent of iteration order, but keep duplicates so they are detected
	slices.Sort(hashes)
	return hashes
}

func groupCounts(groups map[uint64]*legacyTopologyGroup) map[uint64]map[string]int32 {
	return lo.MapValues(groups, func(tg *legacyTopologyGroup, _ uint64) map[string]int32 { return tg.domains })
}

type topologyIndexGen struct {
	r *rand.Rand
}

var indexTestLabelKeys = []string{"app", "tier", "team", "track"}

func (g *topologyIndexGen) value() string    { return fmt.Sprintf("v%d", g.r.Intn(3)) }
func (g *topologyIndexGen) zone() string     { return fmt.Sprintf("zone-%d", g.r.Intn(3)) }
func (g *topologyIndexGen) hostname() string { return fmt.Sprintf("host-%d", g.r.Intn(5)) }
func (g *topologyIndexGen) key() string      { return indexTestLabelKeys[g.r.Intn(len(indexTestLabelKeys))] }
func (g *topologyIndexGen) topologyKey() string {
	return lo.Ternary(g.r.Intn(2) == 0, corev1.LabelTopologyZone, corev1.LabelHostname)
}

func (g *topologyIndexGen) namespace() string { return fmt.Sprintf("ns-%d", g.r.Intn(2)) }

func (g *topologyIndexGen) labels() map[string]string {
	labels := map[string]string{}
	for _, k := range indexTestLabelKeys {
		if g.r.Intn(3) != 0 {
			labels[k] = g.value()
		}
	}
	return labels
}

func (g *topologyIndexGen) selector() *metav1.LabelSelector {
	if g.r.Intn(12) == 0 {
		return nil
	}
	s := &metav1.LabelSelector{}
	if n := g.r.Intn(3); n > 0 {
		s.MatchLabels = map[string]string{}
		for i := 0; i < n; i++ {
			s.MatchLabels[g.key()] = g.value()
		}
	}
	for i := g.r.Intn(3); i > 0; i-- {
		e := metav1.LabelSelectorRequirement{Key: g.key()}
		switch g.r.Intn(4) {
		case 0:
			e.Operator = metav1.LabelSelectorOpIn
			e.Values = []string{g.value(), g.value()}
		case 1:
			e.Operator = metav1.LabelSelectorOpNotIn
			e.Values = []string{g.value()}
		case 2:
			e.Operator = metav1.LabelSelectorOpExists
		case 3:
			e.Operator = metav1.LabelSelectorOpDoesNotExist
		}
		s.MatchExpressions = append(s.MatchExpressions, e)
	}
	return s
}

func (g *topologyIndexGen) term() corev1.PodAffinityTerm {
	term := corev1.PodAffinityTerm{TopologyKey: g.topologyKey(), LabelSelector: g.selector()}
	if g.r.Intn(4) == 0 {
		term.Namespaces = []string{g.namespace(), g.namespace()}
	}
	return term
}

func (g *topologyIndexGen) pod(name string) *corev1.Pod {
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: g.namespace(), UID: types.UID(name), Labels: g.labels()}}
	for i := g.r.Intn(3); i > 0; i-- {
		p.Spec.TopologySpreadConstraints = append(p.Spec.TopologySpreadConstraints, corev1.TopologySpreadConstraint{
			MaxSkew:           int32(1 + g.r.Intn(2)),
			TopologyKey:       g.topologyKey(),
			WhenUnsatisfiable: lo.Ternary(g.r.Intn(2) == 0, corev1.DoNotSchedule, corev1.ScheduleAnyway),
			LabelSelector:     g.selector(),
		})
	}
	if g.r.Intn(2) == 0 {
		return p
	}
	p.Spec.Affinity = &corev1.Affinity{}
	if g.r.Intn(2) == 0 {
		p.Spec.Affinity.PodAffinity = &corev1.PodAffinity{}
		for i := g.r.Intn(2); i > 0; i-- {
			p.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(p.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution, g.term())
		}
		for i := g.r.Intn(3); i > 0; i-- {
			p.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(p.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution, corev1.WeightedPodAffinityTerm{Weight: 1, PodAffinityTerm: g.term()})
		}
	}
	if g.r.Intn(2) == 0 {
		p.Spec.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{}
		for i := g.r.Intn(3); i > 0; i-- {
			p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, g.term())
		}
		for i := g.r.Intn(3); i > 0; i-- {
			p.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(p.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, corev1.WeightedPodAffinityTerm{Weight: 1, PodAffinityTerm: g.term()})
		}
	}
	return p
}

// relax drops a preference or a topology spread constraint from the pod, approximating preference relaxation.
func (g *topologyIndexGen) relax(p *corev1.Pod) {
	switch g.r.Intn(4) {
	case 0:
		if n := len(p.Spec.TopologySpreadConstraints); n > 0 {
			p.Spec.TopologySpreadConstraints = p.Spec.TopologySpreadConstraints[:n-1]
		}
	case 1:
		if p.Spec.Affinity != nil && p.Spec.Affinity.PodAffinity != nil {
			if n := len(p.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution); n > 0 {
				p.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution = p.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution[:n-1]
			}
		}
	case 2:
		if p.Spec.Affinity != nil && p.Spec.Affinity.PodAntiAffinity != nil {
			if n := len(p.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution); n > 0 {
				p.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = p.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[:n-1]
			}
		}
	case 3:
		// re-Update an unchanged pod, as the scheduler does with the original pod after a failed attempt
	}
}

func (g *topologyIndexGen) nodeRequirements() scheduling.Requirements {
	if g.r.Intn(2) == 0 {
		return scheduling.NewRequirements(
			scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, g.zone()),
			scheduling.NewRequirement(corev1.LabelHostname, corev1.NodeSelectorOpIn, g.hostname()),
		)
	}
	// a NodeClaim whose zone hasn't collapsed to a single value yet
	return scheduling.NewRequirements(scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, g.zone(), g.zone()))
}
