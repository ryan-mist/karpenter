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

package cow

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"sync"
	"testing"
)

type frozen struct {
	view *View[string, int]
	want map[string]int
}

func viewContents(v *View[string, int]) map[string]int {
	got := map[string]int{}
	v.All(func(k string, val int) bool {
		got[k] = val
		return true
	})
	return got
}

func checkView(t *testing.T, f frozen) {
	t.Helper()
	if got := viewContents(f.view); !maps.Equal(got, f.want) {
		t.Fatalf("view changed after freeze: got %v, want %v", got, f.want)
	}
	if f.view.Len() != len(f.want) {
		t.Fatalf("view Len() = %d, want %d", f.view.Len(), len(f.want))
	}
	for k, want := range f.want {
		if got, ok := f.view.Get(k); !ok || got != want {
			t.Fatalf("view Get(%q) = %d, %t, want %d", k, got, ok, want)
		}
	}
}

// runOps applies a random sequence of writes and freezes to a Map and a reference map, and checks after every step
// that the Map matches the reference and that every view still matches the reference at the time it was frozen.
func runOps(t *testing.T, r *rand.Rand, shards, keys, steps int) {
	t.Helper()
	m := NewMap[string, int](shards)
	ref := map[string]int{}
	var views []frozen
	for step := range steps {
		k := fmt.Sprintf("k%d", r.IntN(keys))
		switch op := r.IntN(10); {
		case op < 5:
			m.Set(k, step)
			ref[k] = step
		case op < 8:
			m.Delete(k)
			delete(ref, k)
		default:
			views = append(views, frozen{view: m.Freeze(), want: maps.Clone(ref)})
			if len(views) > 8 {
				views = views[1:]
			}
		}
		if m.Len() != len(ref) {
			t.Fatalf("step %d: Len() = %d, want %d", step, m.Len(), len(ref))
		}
		got, ok := m.Get(k)
		if want, wantOK := ref[k]; ok != wantOK || got != want {
			t.Fatalf("step %d: Get(%q) = %d, %t, want %d, %t", step, k, got, ok, want, wantOK)
		}
		for _, f := range views {
			checkView(t, f)
		}
	}
	checkView(t, frozen{view: m.Freeze(), want: ref})
}

func TestMapMatchesReference(t *testing.T) {
	for seed := range uint64(50) {
		r := rand.New(rand.NewPCG(seed, seed)) //nolint:gosec // deterministic test input
		runOps(t, r, 1+r.IntN(16), 1+r.IntN(64), 400)
	}
}

func TestFreezeSharesUnchangedShards(t *testing.T) {
	m := NewMap[string, int](4)
	for i := range 100 {
		m.Set(fmt.Sprintf("k%d", i), i)
	}
	v1 := m.Freeze()
	if v2 := m.Freeze(); v2 != v1 {
		t.Fatal("freezing without a write should return the same view")
	}
	m.Set("k1", 1) // same value: still a write
	v2 := m.Freeze()
	if v2 == v1 {
		t.Fatal("freezing after a write should return a new view")
	}
	m.Set("k0", -1)
	v3 := m.Freeze()
	changed := 0
	for i := range v2.shards {
		if v2.shards[i] != v3.shards[i] {
			changed++
		}
	}
	if changed != 1 {
		t.Fatalf("expected exactly one shard to be copied by one write, got %d", changed)
	}
	if got, _ := v2.Get("k0"); got != 0 {
		t.Fatalf("old view saw a later write: k0 = %d", got)
	}
	// deleting a missing key must not copy a shard
	m.Delete("missing")
	v4 := m.Freeze()
	for i := range v3.shards {
		if v3.shards[i] != v4.shards[i] {
			t.Fatalf("shard %d was copied by a delete of a missing key", i)
		}
	}
}

// TestViewsUnderConcurrentWrites runs readers over views while the writer keeps writing and freezing. Run with -race.
func TestViewsUnderConcurrentWrites(t *testing.T) {
	m := NewMap[string, int](8)
	var mu sync.Mutex
	latest := frozen{view: m.Freeze(), want: map[string]int{}}
	ref := map[string]int{}
	var wg sync.WaitGroup
	done := make(chan struct{})
	for range 2 {
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				mu.Lock()
				f := latest
				mu.Unlock()
				if got := viewContents(f.view); !maps.Equal(got, f.want) {
					t.Errorf("concurrent view mismatch: got %d entries, want %d", len(got), len(f.want))
					return
				}
			}
		})
	}
	r := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // deterministic test input
	for step := range 5000 {
		k := fmt.Sprintf("k%d", r.IntN(200))
		if r.IntN(3) == 0 {
			m.Delete(k)
			delete(ref, k)
		} else {
			m.Set(k, step)
			ref[k] = step
		}
		if step%10 == 0 {
			f := frozen{view: m.Freeze(), want: maps.Clone(ref)}
			mu.Lock()
			latest = f
			mu.Unlock()
		}
	}
	close(done)
	wg.Wait()
}

func FuzzMap(f *testing.F) {
	f.Add(uint64(1), uint8(4), uint8(16))
	f.Fuzz(func(t *testing.T, seed uint64, shards uint8, keys uint8) {
		r := rand.New(rand.NewPCG(seed, ^seed)) //nolint:gosec // deterministic test input
		runOps(t, r, int(shards%32)+1, int(keys)+1, 300)
	})
}

func BenchmarkMapSetFrozen(b *testing.B) {
	for _, n := range []int{3_000, 150_000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			m := NewMap[string, int](1024)
			keys := make([]string, n)
			for i := range keys {
				keys[i] = fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
				m.Set(keys[i], i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				// one publish per 16 writes, as with lazy publishing at about 167 events/s
				if i%16 == 0 {
					m.Freeze()
				}
				m.Set(keys[i%n], i)
			}
		})
	}
}
