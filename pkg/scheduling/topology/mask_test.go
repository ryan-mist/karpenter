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

package topology_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/scheduling/topology"
)

var _ = Describe("DomainDict", func() {
	It("should intern domains to stable, dense IDs", func() {
		d := topology.Dict("test.example.com/dict-stable")
		Expect(topology.Dict("test.example.com/dict-stable")).To(BeIdenticalTo(d))
		a, b := d.Intern("a"), d.Intern("b")
		old := d.View()
		Expect(d.InternAll("c", "a", "c", "d")).To(Equal([]topology.DomainID{b + 1, a, b + 1, b + 2}))
		Expect(d.Intern("a")).To(Equal(a))
		v := d.View()
		Expect(v.Len()).To(Equal(old.Len() + 2))
		for _, name := range []string{"a", "b", "c", "d"} {
			id, ok := v.Lookup(name)
			Expect(ok).To(BeTrue())
			Expect(v.Name(id)).To(Equal(name))
		}
		// an older view is unchanged
		_, ok := old.Lookup("c")
		Expect(ok).To(BeFalse())
		Expect(old.Len()).To(Equal(2))
	})
	It("should be safe for concurrent use", func() {
		d := topology.Dict("test.example.com/dict-concurrent")
		var wg sync.WaitGroup
		results := make([]map[string]topology.DomainID, 8)
		for w := range 8 {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				results[w] = map[string]topology.DomainID{}
				r := rand.New(rand.NewSource(int64(w)))
				for range 2000 {
					name := fmt.Sprintf("d%d", r.Intn(300))
					results[w][name] = d.Intern(name)
					v := d.View()
					id, ok := v.Lookup(name)
					Expect(ok).To(BeTrue())
					Expect(v.Name(id)).To(Equal(name))
				}
			}()
		}
		wg.Wait()
		v := d.View()
		Expect(v.Len()).To(Equal(300))
		for _, r := range results {
			for name, id := range r {
				Expect(v.Name(id)).To(Equal(name))
			}
		}
	})
	It("should count hostname and large keys sparsely", func() {
		Expect(topology.Dense(corev1.LabelHostname)).To(BeFalse())
		Expect(topology.Dense(corev1.LabelTopologyZone)).To(BeTrue())
		d := topology.Dict("test.example.com/dict-large")
		d.InternAll(lo.Times(topology.MaxDenseDomains, func(i int) string { return fmt.Sprint(i) })...)
		Expect(topology.Dense(d.Key())).To(BeTrue())
		d.Intern("one-more")
		Expect(topology.Dense(d.Key())).To(BeFalse())
	})
})

var _ = Describe("Mask", func() {
	ids := func(m *topology.Mask) []int {
		result := []int{}
		m.ForEach(func(id topology.DomainID) bool { result = append(result, int(id)); return true })
		return result
	}
	It("should behave like a set", func() {
		for seed := range int64(300) {
			r := rand.New(rand.NewSource(seed))
			universe := []int{8, 64, 65, 130, 300}[r.Intn(5)]
			random := func() (topology.Mask, sets.Set[int]) {
				var m topology.Mask
				s := sets.New[int]()
				for range r.Intn(universe) {
					id := r.Intn(universe)
					m.Set(topology.DomainID(id))
					s.Insert(id)
					if r.Intn(4) == 0 {
						id = r.Intn(universe)
						m.Clear(topology.DomainID(id))
						s.Delete(id)
					}
				}
				return m, s
			}
			a, as := random()
			b, bs := random()
			Expect(ids(&a)).To(Equal(sets.List(as)), "seed %d", seed)
			Expect(a.Count()).To(Equal(as.Len()))
			Expect(a.Any()).To(Equal(as.Len() > 0))
			for id := range universe + 70 {
				Expect(a.Has(topology.DomainID(id))).To(Equal(as.Has(id)))
			}
			and := a.And(&b)
			Expect(ids(&and)).To(Equal(sets.List(as.Intersection(bs))), "seed %d", seed)
			Expect(a.Intersects(&b)).To(Equal(as.Intersection(bs).Len() > 0))
			andNot := a.AndNot(&b)
			Expect(ids(&andNot)).To(Equal(sets.List(as.Difference(bs))), "seed %d", seed)
			or := a.Clone()
			or.Or(&b)
			Expect(ids(&or)).To(Equal(sets.List(as.Union(bs))), "seed %d", seed)
			Expect(ids(&a)).To(Equal(sets.List(as)), "Or must not change its argument or the clone's source")
			Expect(or.Equal(&a)).To(Equal(as.Union(bs).Equal(as)))
			Expect(a.Equal(&a)).To(BeTrue())

			// rotated iteration visits ids >= start ascending, then ids < start ascending
			start := r.Intn(universe + 70)
			var rotated []int
			a.ForEachFrom(topology.DomainID(start), func(id topology.DomainID) bool { rotated = append(rotated, int(id)); return true })
			expected := append(lo.Filter(sets.List(as), func(id int, _ int) bool { return id >= start }), lo.Filter(sets.List(as), func(id int, _ int) bool { return id < start })...)
			Expect(append([]int{}, rotated...)).To(Equal(expected), "seed %d start %d", seed, start)
			// and stops when asked
			var first []int
			a.ForEachFrom(topology.DomainID(start), func(id topology.DomainID) bool { first = append(first, int(id)); return false })
			Expect(append([]int{}, first...)).To(Equal(lo.Subset(expected, 0, 1)))
		}
	})
	It("should not allocate for small masks", func() {
		var a, b topology.Mask
		a.Set(3)
		b.Set(5)
		Expect(testing.AllocsPerRun(100, func() {
			c := a.And(&b)
			c.Or(&a)
			_ = c.AndNot(&b)
		})).To(BeZero())
	})
})
