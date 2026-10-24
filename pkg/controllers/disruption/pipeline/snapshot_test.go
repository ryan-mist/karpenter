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
	"testing"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func TestCatalogVersionChangesWithEveryOfferingInput(t *testing.T) {
	assorted := fake.InstanceTypesAssorted()[:20]
	// Assorted instance types have one offering each; give the first one per zone and capacity type, as real ones have.
	first := assorted[0].Offerings[0]
	for _, zone := range []string{"test-zone-1", "test-zone-2", "test-zone-3"} {
		for _, ct := range []string{v1.CapacityTypeSpot, v1.CapacityTypeOnDemand} {
			assorted[0].Offerings = append(assorted[0].Offerings, &cloudprovider.Offering{Price: first.Price, Available: true,
				Requirements: scheduling.NewLabelRequirements(map[string]string{corev1.LabelTopologyZone: zone, v1.CapacityTypeLabelKey: ct})})
		}
	}
	its := lo.SliceToMap(assorted, func(it *cloudprovider.InstanceType) (string, *cloudprovider.InstanceType) { return it.Name, it })
	base := pipeline.CatalogVersion(its)
	if again := pipeline.CatalogVersion(its); again != base {
		t.Fatalf("same catalog: got %s, want %s", again, base)
	}
	// The order of the offerings doesn't matter: providers build them from sets.
	for _, it := range its {
		it.Offerings = lo.Reverse(append(cloudprovider.Offerings{}, it.Offerings...))
	}
	if again := pipeline.CatalogVersion(its); again != base {
		t.Fatalf("same offerings in another order: got %s, want %s", again, base)
	}
	o := assorted[0].Offerings[0]
	for name, mutate := range map[string]func(){
		"price":                func() { o.Price += 0.001 },
		"availability":         func() { o.Available = !o.Available },
		"reservation capacity": func() { o.ReservationCapacity++ },
	} {
		saved := *o
		mutate()
		if got := pipeline.CatalogVersion(its); got == base {
			t.Errorf("%s change: version unchanged", name)
		}
		*o = saved
	}
	if got := pipeline.CatalogVersion(its); got != base {
		t.Fatalf("restored catalog: got %s, want %s", got, base)
	}
	if got := pipeline.CatalogVersion(nil); got == base {
		t.Errorf("unavailable catalog: got the version of a listed one")
	}
	delete(its, lo.Keys(its)[0])
	if got := pipeline.CatalogVersion(its); got == base {
		t.Errorf("removed instance type: version unchanged")
	}
}

type scopePlanner struct {
	name, class string
	totals      bool
}

func (p scopePlanner) Name() string                { return p.name }
func (p scopePlanner) Reason() v1.DisruptionReason { return v1.DisruptionReasonDrifted }
func (p scopePlanner) Class() string               { return p.class }
func (p scopePlanner) Shape() types.Shape          { return types.ShapeReplacing }
func (p scopePlanner) NeedsTotals() bool           { return p.totals }
func (p scopePlanner) Filter(context.Context, *disruption.Candidate, time.Time) (bool, *types.Verdict) {
	return true, nil
}
func (p scopePlanner) Plan(context.Context, pipeline.PlanInput) (pipeline.PlanOutput, error) {
	return pipeline.PlanOutput{}, nil
}

func TestScopeForCollectsClassesAndTotals(t *testing.T) {
	s := pipeline.ScopeFor(types.LaneBackground,
		scopePlanner{name: "a", class: disruption.GracefulDisruptionClass, totals: true},
		scopePlanner{name: "b", class: disruption.GracefulDisruptionClass, totals: true},
		scopePlanner{name: "c", class: disruption.EventualDisruptionClass},
	)
	if s.Lane != types.LaneBackground {
		t.Errorf("lane: got %s", s.Lane)
	}
	if want := []string{disruption.GracefulDisruptionClass, disruption.EventualDisruptionClass}; !lo.ElementsMatch(s.Classes, want) || len(s.Classes) != 2 {
		t.Errorf("classes: got %v, want %v", s.Classes, want)
	}
	if want := []string{disruption.GracefulDisruptionClass}; len(s.NeedTotals) != 1 || s.NeedTotals[0] != want[0] {
		t.Errorf("totals: got %v, want %v", s.NeedTotals, want)
	}
}
