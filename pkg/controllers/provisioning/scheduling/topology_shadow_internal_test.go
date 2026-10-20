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
	"testing"

	"github.com/awslabs/operatorpkg/option"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
)

var _ = Describe("Topology Shadow", func() {
	for _, policy := range []PreferencePolicy{PreferencePolicyRespect, PreferencePolicyIgnore} {
		It(fmt.Sprintf("should make the same decisions as the legacy topology (policy=%d)", policy), func() {
			ctx := karpopts.ToContext(context.Background(), test.Options())
			stats := &shadowStats{}
			for seed := range int64(300) {
				g := &identityGen{topologyIndexGen{r: rand.New(rand.NewSource(seed))}}
				mismatches := g.runShadow(ctx, g.fixture(ctx), policy, uint64(seed), stats)
				Expect(mismatches).To(BeEmpty(), "seed %d", seed)
			}
			// the fixtures exercise every kind of decision
			Expect(stats.groups).To(BeNumerically(">", 1000))
			Expect(stats.ordered).To(BeNumerically(">", 100))
			Expect(stats.failed).To(BeNumerically(">", 100))
			Expect(stats.scheduled).To(BeNumerically(">", 1000))
		})
	}
})

var _ = Describe("Topology Shadow Reporter", func() {
	It("should select the reporter from the environment", func() {
		ctx := context.Background()
		GinkgoT().Setenv(TopologyShadowEnv, "")
		Expect(shadowReporter(ctx)).To(BeNil())
		GinkgoT().Setenv(TopologyShadowEnv, "1")
		Expect(func() { shadowReporter(ctx)("Update", "Update: difference") }).To(PanicWith(ContainSubstring("Update: difference")))
		GinkgoT().Setenv(TopologyShadowEnv, "log")
		report := shadowReporter(ctx)
		before := shadowMismatches("Record")
		for range maxLoggedShadowMismatches + 2 {
			report("Record", "Record: difference")
		}
		Expect(shadowMismatches("Record") - before).To(BeNumerically("==", maxLoggedShadowMismatches+2))
	})
})

// shadowMismatches returns the value of the shadow mismatch counter for the call
func shadowMismatches(call string) float64 {
	families, err := crmetrics.Registry.Gather()
	Expect(err).ToNot(HaveOccurred())
	for _, family := range families {
		if family.GetName() != "karpenter_scheduler_topology_shadow_mismatches_total" {
			continue
		}
		for _, m := range family.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == callLabel && label.GetValue() == call {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// FuzzTopologyShadow checks that the overlay and legacy topologies make the same decisions for random clusters and
// workloads.
func FuzzTopologyShadow(f *testing.F) {
	for seed := range int64(20) {
		f.Add(seed, true)
	}
	f.Fuzz(func(t *testing.T, seed int64, respect bool) {
		RegisterTestingT(t)
		ctx := karpopts.ToContext(context.Background(), test.Options())
		g := &identityGen{topologyIndexGen{r: rand.New(rand.NewSource(seed))}}
		if mismatches := g.runShadow(ctx, g.fixture(ctx), lo.Ternary(respect, PreferencePolicyRespect, PreferencePolicyIgnore), uint64(seed), &shadowStats{}); len(mismatches) > 0 {
			t.Fatalf("seed %d: %v", seed, mismatches)
		}
	})
}

type shadowStats struct {
	groups, ordered, failed, scheduled int
}

type shadowCandidate struct {
	taints       []corev1.Taint
	requirements scheduling.Requirements
	// hostname is set for a new NodeClaim, which registers it when a pod schedules to it
	hostname string
	options  []option.Function[scheduling.CompatibilityOptions]
}

// runShadow simulates scheduling the fixture's pods with a shadow topology, the way the scheduler uses a Topology: it
// registers the existing nodes' hostnames, tries every candidate node from 8 goroutines, records the pod on the first
// candidate that it fits, registers a new NodeClaim's hostname, and relaxes and re-Updates pods that don't fit. It
// returns the differences that the shadow topology found.
func (g *identityGen) runShadow(ctx context.Context, f *topologyFixture, policy PreferencePolicy, seed uint64, stats *shadowStats) []string {
	var mu sync.Mutex
	var mismatches []string
	report := func(_, m string) {
		mu.Lock()
		defer mu.Unlock()
		mismatches = append(mismatches, m)
	}
	s, err := newShadowTopology(ctx, f.client, f.cluster, f.stateNodes, f.nodePools, f.instanceTypes, f.pods, &options{preferencePolicy: policy, topologySeed: &seed}, report)
	if err != nil {
		return mismatches
	}
	stats.groups += len(s.overlay.groups) + len(s.overlay.inverseGroups)

	var candidates []shadowCandidate
	for _, n := range f.stateNodes {
		if n.Node == nil {
			continue
		}
		s.Register(corev1.LabelHostname, n.HostName())
		candidates = append(candidates, shadowCandidate{taints: n.Taints(), requirements: scheduling.NewLabelRequirements(n.Labels())})
	}
	zones := []string{"zone-0", "zone-1", "zone-2"}
	newNodeClaim := func(i int) shadowCandidate {
		np := f.nodePools[g.r.Intn(len(f.nodePools))]
		hostname := fmt.Sprintf("hostname-placeholder-%d", i)
		c := shadowCandidate{
			taints:   np.Spec.Template.Spec.Taints,
			hostname: hostname,
			requirements: scheduling.NewRequirements(
				scheduling.NewRequirement(corev1.LabelHostname, corev1.NodeSelectorOpIn, hostname),
				scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, lo.Samples(zones, 1+g.r.Intn(len(zones)))...),
			),
			options: []option.Function[scheduling.CompatibilityOptions]{scheduling.AllowUndefinedWellKnownLabels},
		}
		return c
	}
	for i := range 3 {
		candidates = append(candidates, newNodeClaim(i))
	}

	for i, p := range f.pods {
		for attempt := range 3 {
			podRequirements := scheduling.NewStrictPodRequirements(p)
			s.GetTopologyZoneConstraints(p, scheduling.NewPodRequirements(p))
			// every candidate is tried concurrently, as in the scheduler's parallel rounds
			results := make([]scheduling.Requirements, len(candidates))
			var wg sync.WaitGroup
			for w := range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for c := w; c < len(candidates); c += 8 {
						if r, err := s.AddRequirements(p, candidates[c].taints, podRequirements, candidates[c].requirements, candidates[c].options...); err == nil {
							results[c] = r
						}
					}
				}()
			}
			wg.Wait()
			if index := slices.IndexFunc(results, func(r scheduling.Requirements) bool { return r != nil }); index >= 0 {
				c := candidates[index]
				if c.hostname != "" {
					s.Register(corev1.LabelHostname, c.hostname)
					// a new NodeClaim takes the place of the one the pod scheduled to
					candidates = append(candidates, newNodeClaim(len(candidates)+i*10+attempt))
				}
				s.Record(p, c.taints, results[index], c.options...)
				// the candidate's requirements are tightened, as for ExistingNode and NodeClaim
				candidates[index] = shadowCandidate{taints: c.taints, requirements: results[index], options: c.options}
				stats.scheduled++
				break
			}
			stats.failed++
			g.relax(p)
			// relaxation can also add a toleration, which changes the identity of the pod's groups that honor taints, so
			// new groups are created after pods were recorded and hostnames registered
			if g.r.Intn(3) == 0 {
				p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{Key: g.taintKey(), Operator: corev1.TolerationOpExists})
			}
			_ = s.Update(ctx, p)
		}
	}
	stats.ordered += int(s.ordered.Load())
	return mismatches
}
