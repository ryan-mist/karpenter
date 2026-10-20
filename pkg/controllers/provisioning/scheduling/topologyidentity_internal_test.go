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
	"fmt"
	"math"
	"math/rand"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/scheduling"
)

var _ = Describe("TopologyGroupIdentity", func() {
	It("should hash like the legacy topology group", func() {
		for seed := range int64(3000) {
			g := &identityGen{topologyIndexGen{r: rand.New(rand.NewSource(seed))}}
			args := g.args()
			id := args.identity()
			legacy := args.legacy()
			Expect(id.Hash()).To(Equal(legacy.Hash()), "seed %d", seed)

			// the derived parts match the legacy group's once it's populated
			legacy.populate(args.pod, TopologyDomainGroup{})
			Expect(id.Selector().String()).To(Equal(legacy.selector.String()), "seed %d", seed)
			for range 5 {
				p := g.pod("other")
				Expect(id.selects(p)).To(Equal(legacy.selects(p)), "seed %d", seed)
			}
			Expect(id.Namespaces).To(Equal(sets.List(legacy.namespaces)))
			Expect(id.NodeFilter.Hash()).To(Equal(legacy.nodeFilter.Hash()))
		}
	})
	It("should hash the same after reordering selector expressions and values", func() {
		for seed := range int64(500) {
			g := &identityGen{topologyIndexGen{r: rand.New(rand.NewSource(seed))}}
			args := g.args()
			if args.selector == nil {
				continue
			}
			shuffled := args
			shuffled.selector = args.selector.DeepCopy()
			g.r.Shuffle(len(shuffled.selector.MatchExpressions), func(i, j int) {
				shuffled.selector.MatchExpressions[i], shuffled.selector.MatchExpressions[j] = shuffled.selector.MatchExpressions[j], shuffled.selector.MatchExpressions[i]
			})
			for i := range shuffled.selector.MatchExpressions {
				values := shuffled.selector.MatchExpressions[i].Values
				g.r.Shuffle(len(values), func(a, b int) { values[a], values[b] = values[b], values[a] })
			}
			if len(shuffled.selector.MatchExpressions) > 0 {
				shuffled.selector.MatchExpressions = append(shuffled.selector.MatchExpressions, shuffled.selector.MatchExpressions[0])
			}
			Expect(shuffled.identity().Hash()).To(Equal(args.identity().Hash()), "seed %d", seed)
			Expect(shuffled.legacy().Hash()).To(Equal(args.legacy().Hash()), "seed %d", seed)
			Expect(shuffled.identity().CountKey().Equal(args.identity().CountKey())).To(BeTrue(), "seed %d", seed)
		}
	})
	It("should share a CountKey between groups that only differ in how they use the counts", func() {
		for seed := range int64(500) {
			g := &identityGen{topologyIndexGen{r: rand.New(rand.NewSource(seed))}}
			args := g.args()
			args.topologyType = TopologyTypeSpread
			other := args
			other.topologyType = lo.Ternary(g.r.Intn(2) == 0, TopologyTypePodAffinity, TopologyTypePodAntiAffinity)
			other.maxSkew = math.MaxInt32
			other.minDomains = nil
			// a pod affinity group's seed tolerations come from the pod
			other.pod = args.pod.DeepCopy()
			other.pod.Spec.Tolerations = []corev1.Toleration{{Key: "other", Operator: corev1.TolerationOpExists}}
			a, b := args.identity(), other.identity()
			// affinity groups count on every node, so they only share counts with spread groups that do too
			matchesAll := newTopologyNodePredicate(a.NodeFilter) == nil
			Expect(a.CountKey().Equal(b.CountKey())).To(Equal(matchesAll), "seed %d", seed)
			Expect(b.CountKey().Equal(a.CountKey())).To(Equal(matchesAll), "seed %d", seed)
			if matchesAll {
				Expect(a.CountKey().Hash()).To(Equal(b.CountKey().Hash()))
			}
		}
	})
	It("should give the count key the counting selector and sorted namespaces", func() {
		args := identityArgs{
			topologyType: TopologyTypePodAffinity,
			key:          corev1.LabelTopologyZone,
			pod:          &corev1.Pod{},
			namespaces:   sets.New("b", "a"),
		}
		key := args.identity().CountKey()
		Expect(key.Namespaces).To(Equal([]string{"a", "b"}))
		Expect(key.TopologyKey).To(Equal(corev1.LabelTopologyZone))
		// nil selects every pod for counting, but no pod for recording
		Expect(key.ListSelector().Matches(labels.Set{"a": "b"})).To(BeTrue())
		Expect(args.identity().Selector().Matches(labels.Set{"a": "b"})).To(BeFalse())
		Expect(key.NodeFilter).To(BeNil())
	})
})

var _ = Describe("topologyNodePredicate", func() {
	It("should match like the node filter", func() {
		for seed := range int64(2000) {
			g := &identityGen{topologyIndexGen{r: rand.New(rand.NewSource(seed))}}
			args := g.args()
			args.topologyType = TopologyTypeSpread
			filter := args.identity().NodeFilter
			predicate := newTopologyNodePredicate(filter)
			for range 10 {
				nodeLabels, taints := g.node()
				expected := filter.Matches(taints, scheduling.NewLabelRequirements(nodeLabels))
				if predicate == nil {
					Expect(expected).To(BeTrue(), "seed %d: a nil predicate must only replace filters that match every node", seed)
					continue
				}
				Expect(predicate.Matches(nodeLabels, taints)).To(Equal(expected), "seed %d", seed)
				for _, key := range predicate.Keys() {
					Expect(lo.SomeBy(filter.Requirements, func(r scheduling.Requirements) bool { return r.Has(key) })).To(BeTrue())
				}
			}
		}
	})
	It("should only be equal to predicates that match the same nodes", func() {
		var predicates []*topologyNodePredicate
		for seed := range int64(1000) {
			g := &identityGen{topologyIndexGen{r: rand.New(rand.NewSource(seed))}}
			args := g.args()
			args.topologyType = TopologyTypeSpread
			if p := newTopologyNodePredicate(args.identity().NodeFilter); p != nil {
				predicates = append(predicates, p.(*topologyNodePredicate))
			}
		}
		g := &identityGen{topologyIndexGen{r: rand.New(rand.NewSource(0))}}
		nodes := lo.Times(200, func(int) lo.Tuple2[map[string]string, []corev1.Taint] { return lo.T2(g.node()) })
		byCanonical := lo.GroupBy(predicates, func(p *topologyNodePredicate) string { return p.canonical })
		Expect(len(byCanonical)).To(BeNumerically(">", 20))
		for _, equal := range byCanonical {
			for _, p := range equal[1:] {
				Expect(p.Equal(equal[0])).To(BeTrue())
				Expect(p.Hash()).To(Equal(equal[0].Hash()))
				for _, n := range nodes {
					Expect(p.Matches(n.A, n.B)).To(Equal(equal[0].Matches(n.A, n.B)))
				}
			}
		}
	})
})

// identityArgs are the arguments of newTopologyGroupIdentity and newLegacyTopologyGroupIdentity
type identityArgs struct {
	topologyType   TopologyType
	key            string
	pod            *corev1.Pod
	namespaces     sets.Set[string]
	selector       *metav1.LabelSelector
	maxSkew        int32
	minDomains     *int32
	taintPolicy    *corev1.NodeInclusionPolicy
	affinityPolicy *corev1.NodeInclusionPolicy
}

func (a identityArgs) identity() *TopologyGroupIdentity {
	return newTopologyGroupIdentity(a.topologyType, a.key, a.pod, a.namespaces, a.selector, a.maxSkew, a.minDomains, a.taintPolicy, a.affinityPolicy)
}

func (a identityArgs) legacy() *legacyTopologyGroup {
	return newLegacyTopologyGroupIdentity(a.topologyType, a.key, a.pod, a.namespaces, a.selector, a.maxSkew, a.minDomains, a.taintPolicy, a.affinityPolicy)
}

// identityGen extends the topology index generator with the parts of a pod that make up a node filter
type identityGen struct {
	topologyIndexGen
}

var identityTestNodeKeys = []string{corev1.LabelTopologyZone, corev1.LabelInstanceTypeStable, "pool", "team"}

func (g *identityGen) policy() *corev1.NodeInclusionPolicy {
	switch g.r.Intn(3) {
	case 0:
		return nil
	case 1:
		return lo.ToPtr(corev1.NodeInclusionPolicyHonor)
	}
	return lo.ToPtr(corev1.NodeInclusionPolicyIgnore)
}

func (g *identityGen) nodeValue() string { return fmt.Sprintf("n%d", g.r.Intn(3)) }

func (g *identityGen) taintKey() string { return fmt.Sprintf("taint-%d", g.r.Intn(3)) }

func (g *identityGen) args() identityArgs {
	p := g.pod("creator")
	if g.r.Intn(2) == 0 {
		p.Spec.NodeSelector = map[string]string{}
		for range 1 + g.r.Intn(2) {
			p.Spec.NodeSelector[identityTestNodeKeys[g.r.Intn(len(identityTestNodeKeys))]] = g.nodeValue()
		}
	}
	if g.r.Intn(2) == 0 {
		if p.Spec.Affinity == nil {
			p.Spec.Affinity = &corev1.Affinity{}
		}
		p.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{}}
		for range g.r.Intn(3) {
			term := corev1.NodeSelectorTerm{}
			for range g.r.Intn(3) {
				r := corev1.NodeSelectorRequirement{Key: identityTestNodeKeys[g.r.Intn(len(identityTestNodeKeys))]}
				switch g.r.Intn(4) {
				case 0:
					r.Operator, r.Values = corev1.NodeSelectorOpIn, []string{g.nodeValue(), g.nodeValue()}
				case 1:
					r.Operator, r.Values = corev1.NodeSelectorOpNotIn, []string{g.nodeValue()}
				case 2:
					r.Operator = corev1.NodeSelectorOpExists
				default:
					r.Operator = corev1.NodeSelectorOpDoesNotExist
				}
				term.MatchExpressions = append(term.MatchExpressions, r)
			}
			p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = append(p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms, term)
		}
	}
	for range g.r.Intn(3) {
		t := corev1.Toleration{Key: g.taintKey(), Operator: corev1.TolerationOpExists}
		if g.r.Intn(2) == 0 {
			t.Effect = corev1.TaintEffectNoSchedule
		}
		p.Spec.Tolerations = append(p.Spec.Tolerations, t)
	}
	args := identityArgs{
		topologyType:   TopologyType(g.r.Intn(3)),
		key:            g.topologyKey(),
		pod:            p,
		namespaces:     sets.New(g.namespace()),
		selector:       g.selector(),
		maxSkew:        int32(1 + g.r.Intn(3)),
		taintPolicy:    g.policy(),
		affinityPolicy: g.policy(),
	}
	if g.r.Intn(3) == 0 {
		args.namespaces.Insert(g.namespace(), g.namespace())
	}
	if g.r.Intn(3) == 0 {
		args.minDomains = lo.ToPtr(int32(1 + g.r.Intn(3)))
	}
	return args
}

func (g *identityGen) node() (map[string]string, []corev1.Taint) {
	nodeLabels := map[string]string{}
	for _, key := range identityTestNodeKeys {
		if g.r.Intn(4) != 0 {
			nodeLabels[key] = g.nodeValue()
		}
	}
	var taints []corev1.Taint
	for range g.r.Intn(3) {
		taints = append(taints, corev1.Taint{Key: g.taintKey(), Effect: lo.Ternary(g.r.Intn(3) == 0, corev1.TaintEffectPreferNoSchedule, corev1.TaintEffectNoSchedule)})
	}
	return nodeLabels, taints
}
