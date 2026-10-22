//go:build test_performance

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
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/utils/pdb"
)

// listClient lists a fixed set of PDBs, deep copying them as the informer cache does.
type listClient struct {
	client.Client
	pdbs []policyv1.PodDisruptionBudget
}

func (c listClient) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	l := list.(*policyv1.PodDisruptionBudgetList)
	l.Items = make([]policyv1.PodDisruptionBudget, len(c.pdbs))
	for i := range c.pdbs {
		c.pdbs[i].DeepCopyInto(&l.Items[i])
	}
	return nil
}

// benchPDBs returns n PDBs across 10 namespaces, each selecting one app, and 2000 pods spread over the same apps,
// like one disruption simulation's candidate pods.
func benchPDBs(n int) (listClient, []*v1.Pod) {
	c := listClient{}
	for i := 0; i < n; i++ {
		c.pdbs = append(c.pdbs, policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pdb-%d", i), Namespace: fmt.Sprintf("ns-%d", i%10),
				UID: types.UID(fmt.Sprintf("pdb-%d", i)), ResourceVersion: "1"},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MaxUnavailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
				Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": fmt.Sprintf("app-%d", i)}},
			},
			Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 1},
		})
	}
	var pods []*v1.Pod
	for i := 0; i < 2000; i++ {
		app := i % (n + n/4 + 1) // some pods have no PDB
		pods = append(pods, &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d", i), Namespace: fmt.Sprintf("ns-%d", app%10),
			Labels: map[string]string{"app": fmt.Sprintf("app-%d", app), "pod-template-hash": "abc", "tier": "web", "team": "t"}}})
	}
	return c, pods
}

// BenchmarkLimits is one SimulateScheduling's PDB work: NewLimits and IsCurrentlyReschedulable for every candidate
// pod. The ctx outlives the iterations, as one Reconcile's does.
func BenchmarkLimits(b *testing.B) {
	for _, n := range []int{20, 200} {
		b.Run(fmt.Sprintf("pdbs=%d", n), func(b *testing.B) {
			c, pods := benchPDBs(n)
			ctx := pdb.WithIndex(context.Background(), pdb.NewIndex())
			clk := clock.NewFakeClock(time.Now())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				limits, err := pdb.NewLimits(ctx, c)
				if err != nil {
					b.Fatal(err)
				}
				for _, p := range pods {
					limits.IsCurrentlyReschedulable(p, clk, nil)
				}
			}
		})
	}
}

// BenchmarkNewLimits is NewLimits alone, as in the candidate build and per deleting node.
func BenchmarkNewLimits(b *testing.B) {
	for _, n := range []int{20, 200} {
		b.Run(fmt.Sprintf("pdbs=%d", n), func(b *testing.B) {
			c, _ := benchPDBs(n)
			ctx := pdb.WithIndex(context.Background(), pdb.NewIndex())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := pdb.NewLimits(ctx, c); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
