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

package topoindex

import (
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// inverseModel is cluster state as ForPodsWithAntiAffinity reads it
type inverseModel struct {
	antiAffinityPods map[types.NamespacedName]*corev1.Pod
	bindings         map[types.NamespacedName]string
	stateNodes       map[string]*corev1.Node
}

type termCounts struct {
	owners map[types.UID]int32
	counts map[string]int32
}

// reference groups the pods ForPodsWithAntiAffinity would yield by (namespace, term), as InverseTerm does
func (m *inverseModel) reference() map[string]*termCounts {
	out := map[string]*termCounts{}
	for key, pod := range m.antiAffinityPods {
		nodeName, ok := m.bindings[key]
		if !ok {
			continue
		}
		node, ok := m.stateNodes[nodeName]
		if !ok {
			continue
		}
		for _, term := range pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			encoded, _ := json.Marshal(term)
			k := pod.Namespace + "/" + string(encoded)
			tc, ok := out[k]
			if !ok {
				tc = &termCounts{owners: map[types.UID]int32{}, counts: map[string]int32{}}
				out[k] = tc
			}
			tc.owners[pod.UID]++
			if domain, ok := node.Labels[term.TopologyKey]; ok {
				tc.counts[domain]++
			}
		}
	}
	return out
}

func indexInverse(t *testing.T, s *Snapshot) map[string]*termCounts {
	t.Helper()
	out := map[string]*termCounts{}
	s.InverseTerms(func(term *InverseTerm) bool {
		encoded, _ := json.Marshal(term.Term)
		k := term.Namespace + "/" + string(encoded)
		if _, ok := out[k]; ok {
			t.Fatalf("term %s published twice", k)
		}
		tc := &termCounts{owners: map[types.UID]int32{}, counts: maps.Clone(term.Counts)}
		recount := map[string]int32{}
		for _, o := range term.Owners {
			tc.owners[o.UID] += o.Terms
			if o.HasDomain {
				recount[o.Domain] += o.Terms
			}
		}
		if !maps.Equal(recount, term.Counts) {
			t.Fatalf("term %s owners %v don't add up to counts %v", k, term.Owners, term.Counts)
		}
		out[k] = tc
		return true
	})
	return out
}

func antiAffinityPod(r *rand.Rand, name string, uid int) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: fmt.Sprintf("ns%d", r.IntN(2)), UID: types.UID(fmt.Sprintf("%s-%d", name, uid))}}
	pod.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{}}
	for range 1 + r.IntN(2) {
		pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
			corev1.PodAffinityTerm{
				TopologyKey:   []string{corev1.LabelHostname, corev1.LabelTopologyZone}[r.IntN(2)],
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": fmt.Sprint(r.IntN(2))}},
			})
	}
	return pod
}

func TestInverseTermsMatchReference(t *testing.T) {
	for seed := range uint64(100) {
		r := rand.New(rand.NewPCG(seed, seed^0x1eaf)) //nolint:gosec // deterministic test input
		ix := New()
		m := &inverseModel{antiAffinityPods: map[types.NamespacedName]*corev1.Pod{}, bindings: map[types.NamespacedName]string{}, stateNodes: map[string]*corev1.Node{}}
		stateNode := func(name string) *corev1.Node { return m.stateNodes[name] }
		for step := range 300 {
			key := types.NamespacedName{Namespace: "x", Name: fmt.Sprintf("pod-%d", r.IntN(12))}
			nodeName := fmt.Sprintf("node-%d", r.IntN(4))
			switch op := r.IntN(10); {
			case op < 3:
				// UpdatePod of a pod with anti-affinity; sometimes a new pod with the same name
				pod, ok := m.antiAffinityPods[key]
				if !ok || r.IntN(4) == 0 {
					pod = antiAffinityPod(r, key.Name, step)
				}
				m.antiAffinityPods[key] = pod
				name, bound := m.bindings[key]
				ix.SetAntiAffinityPod(key, pod, name, bound, stateNode(name))
			case op < 4:
				delete(m.antiAffinityPods, key)
				ix.SetAntiAffinityPod(key, nil, "", false, nil)
			case op < 6:
				m.bindings[key] = nodeName
				ix.SetBinding(key, nodeName, true, stateNode(nodeName))
			case op < 7:
				delete(m.bindings, key)
				ix.SetBinding(key, "", false, nil)
			case op < 9:
				node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName, Labels: map[string]string{}}}
				if r.IntN(3) != 0 {
					node.Labels[corev1.LabelTopologyZone] = fmt.Sprintf("zone-%d", r.IntN(2))
				}
				if r.IntN(3) != 0 {
					node.Labels[corev1.LabelHostname] = nodeName
				}
				m.stateNodes[nodeName] = node
				ix.SetStateNode(nodeName, node)
			default:
				delete(m.stateNodes, nodeName)
				ix.SetStateNode(nodeName, nil)
			}
			want := m.reference()
			got := indexInverse(t, ix.Snapshot())
			if len(got) != len(want) {
				t.Fatalf("seed %d step %d: %d terms, want %d", seed, step, len(got), len(want))
			}
			for k, w := range want {
				g, ok := got[k]
				if !ok || !maps.Equal(g.owners, w.owners) || !maps.Equal(g.counts, w.counts) {
					t.Fatalf("seed %d step %d: term %s = %+v, want %+v", seed, step, k, g, w)
				}
			}
		}
		if len(ix.invOwners) != len(m.antiAffinityPods) {
			t.Fatalf("seed %d: %d owners, want %d", seed, len(ix.invOwners), len(m.antiAffinityPods))
		}
		referenced := map[string]bool{}
		for _, o := range ix.invOwners {
			for term := range o.terms {
				referenced[term.key] = true
			}
		}
		if keys := slices.Sorted(maps.Keys(ix.invTerms)); len(keys) != len(referenced) {
			t.Fatalf("seed %d: %d terms kept, %d referenced", seed, len(keys), len(referenced))
		}
	}
}

func TestInverseNoopsDontBumpGeneration(t *testing.T) {
	ix := New()
	r := rand.New(rand.NewPCG(1, 1)) //nolint:gosec // deterministic test input
	key := types.NamespacedName{Namespace: "x", Name: "p"}
	pod := antiAffinityPod(r, "p", 0)
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n", Labels: map[string]string{corev1.LabelHostname: "n"}}}
	ix.SetAntiAffinityPod(key, pod, "n", true, node)
	gen := ix.Generation()
	ix.SetAntiAffinityPod(key, pod, "n", true, node)
	ix.SetBinding(key, "n", true, node)
	ix.SetStateNode("n", node.DeepCopy())
	ix.SetBinding(types.NamespacedName{Name: "other"}, "n", true, node)
	ix.SetStateNode("unrelated", node)
	if ix.Generation() != gen {
		t.Fatalf("no-op inverse updates bumped the generation %d -> %d", gen, ix.Generation())
	}
}
