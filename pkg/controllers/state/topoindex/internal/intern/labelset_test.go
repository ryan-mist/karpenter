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

package intern

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"testing"
)

func TestAcquireSharesEqualLabelSets(t *testing.T) {
	table := NewTable[int]()
	a, created := table.Acquire("ns", map[string]string{"app": "a", "tier": "web"})
	if !created {
		t.Fatal("first Acquire should create the label set")
	}
	b, created := table.Acquire("ns", map[string]string{"tier": "web", "app": "a"})
	if created || a != b {
		t.Fatal("equal labels in the same namespace should share a label set")
	}
	c, _ := table.Acquire("other", map[string]string{"app": "a", "tier": "web"})
	if c == a {
		t.Fatal("label sets in different namespaces must not be shared")
	}
	d, _ := table.Acquire("ns", map[string]string{"app": "a"})
	if d == a {
		t.Fatal("different labels must not share a label set")
	}
	// the encoding must not let a key/value boundary move
	e, _ := table.Acquire("ns", map[string]string{"ab": "c"})
	f, _ := table.Acquire("ns", map[string]string{"a": "bc"})
	if e == f {
		t.Fatal("label sets with different key/value splits must not be shared")
	}
	if table.Len() != 5 {
		t.Fatalf("Len() = %d, want 5", table.Len())
	}
}

func TestAcquireCopiesLabels(t *testing.T) {
	table := NewTable[int]()
	l := map[string]string{"app": "a"}
	s, _ := table.Acquire("ns", l)
	l["app"] = "b"
	if s.Labels["app"] != "a" {
		t.Fatal("interned labels changed with the caller's map")
	}
	if _, ok := table.Lookup("ns", map[string]string{"app": "a"}); !ok {
		t.Fatal("Lookup didn't find the interned labels")
	}
	empty, _ := table.Acquire("ns", nil)
	if empty.Labels == nil || len(empty.Labels) != 0 {
		t.Fatal("nil labels should intern as an empty set")
	}
	if again, _ := table.Acquire("ns", map[string]string{}); again != empty {
		t.Fatal("nil and empty labels should share a label set")
	}
}

func TestReleaseRemovesLastReference(t *testing.T) {
	table := NewTable[int]()
	s, _ := table.Acquire("ns", map[string]string{"app": "a"})
	table.Acquire("ns", map[string]string{"app": "a"})
	if table.Release(s) {
		t.Fatal("Release reported the last reference while one remained")
	}
	if !table.Release(s) {
		t.Fatal("Release didn't report the last reference")
	}
	if _, ok := table.Lookup("ns", map[string]string{"app": "a"}); ok {
		t.Fatal("released label set is still in the table")
	}
	n, created := table.Acquire("ns", map[string]string{"app": "a"})
	if !created || n == s {
		t.Fatal("Acquire after the last Release should create a new label set")
	}
}

// TestTableMatchesReference checks refcounts and sharing against a map keyed by fmt's (sorted) map formatting.
func TestTableMatchesReference(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 7)) //nolint:gosec // deterministic test input
	table := NewTable[int]()
	type held struct {
		set *LabelSet[int]
		key string
	}
	refs := map[string]int{}
	var all []held
	for range 5000 {
		if len(all) > 0 && r.IntN(2) == 0 {
			i := r.IntN(len(all))
			h := all[i]
			all = append(all[:i], all[i+1:]...)
			refs[h.key]--
			if last := table.Release(h.set); last != (refs[h.key] == 0) {
				t.Fatalf("Release(%s) last = %t, want %t", h.key, last, refs[h.key] == 0)
			}
			if refs[h.key] == 0 {
				delete(refs, h.key)
			}
			continue
		}
		ns := fmt.Sprintf("ns%d", r.IntN(3))
		l := map[string]string{}
		for range r.IntN(4) {
			l[fmt.Sprintf("k%d", r.IntN(3))] = fmt.Sprintf("v%d", r.IntN(3))
		}
		s, _ := table.Acquire(ns, l)
		if s.Namespace != ns || !maps.Equal(s.Labels, l) {
			t.Fatalf("Acquire(%s, %v) returned %s/%v", ns, l, s.Namespace, s.Labels)
		}
		key := fmt.Sprint(ns, l)
		refs[key]++
		if s.Refs() != refs[key] {
			t.Fatalf("Refs() = %d, want %d", s.Refs(), refs[key])
		}
		all = append(all, held{set: s, key: key})
	}
	if table.Len() != len(refs) {
		t.Fatalf("Len() = %d, want %d", table.Len(), len(refs))
	}
}

func BenchmarkAcquireExisting(b *testing.B) {
	table := NewTable[int]()
	l := map[string]string{"app": "web", "pod-template-hash": "5d8f7c9b6", "team": "a", "tier": "frontend", "version": "v1"}
	table.Acquire("ns", l)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		s, _ := table.Acquire("ns", l)
		table.Release(s)
	}
}
