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

package topology

import (
	"math/bits"
	"slices"
)

// Mask is a set of DomainIDs. The first 64 IDs are stored inline, so masks over the domains of zones, capacity types
// and similar keys don't allocate. The zero value is the empty set. Masks share their storage when copied by value,
// so mutate a Clone of a mask that may be shared.
type Mask struct {
	w0   uint64
	rest []uint64
}

func (m *Mask) word(i int) uint64 {
	if i == 0 {
		return m.w0
	}
	if i-1 < len(m.rest) {
		return m.rest[i-1]
	}
	return 0
}

func (m *Mask) words() int {
	return 1 + len(m.rest)
}

// Set adds the domain.
func (m *Mask) Set(id DomainID) {
	w := int(id >> 6)
	if w == 0 {
		m.w0 |= 1 << (id & 63)
		return
	}
	if w > len(m.rest) {
		m.rest = append(m.rest, make([]uint64, w-len(m.rest))...)
	}
	m.rest[w-1] |= 1 << (id & 63)
}

// Clear removes the domain.
func (m *Mask) Clear(id DomainID) {
	w := int(id >> 6)
	if w == 0 {
		m.w0 &^= 1 << (id & 63)
		return
	}
	if w <= len(m.rest) {
		m.rest[w-1] &^= 1 << (id & 63)
	}
}

// Has reports whether the mask has the domain.
func (m *Mask) Has(id DomainID) bool {
	return m.word(int(id>>6))&(1<<(id&63)) != 0
}

// Any reports whether the mask is non-empty.
func (m *Mask) Any() bool {
	if m.w0 != 0 {
		return true
	}
	for _, w := range m.rest {
		if w != 0 {
			return true
		}
	}
	return false
}

// Count returns the number of domains in the mask.
func (m *Mask) Count() int {
	n := bits.OnesCount64(m.w0)
	for _, w := range m.rest {
		n += bits.OnesCount64(w)
	}
	return n
}

// Clone returns a copy of the mask that doesn't share storage with it.
func (m Mask) Clone() Mask {
	return Mask{w0: m.w0, rest: slices.Clone(m.rest)}
}

// Equal reports whether the masks have the same domains.
func (m *Mask) Equal(o *Mask) bool {
	for i := range max(m.words(), o.words()) {
		if m.word(i) != o.word(i) {
			return false
		}
	}
	return true
}

// Or adds the domains of o to m.
func (m *Mask) Or(o *Mask) {
	m.w0 |= o.w0
	if len(o.rest) > len(m.rest) {
		m.rest = append(m.rest, make([]uint64, len(o.rest)-len(m.rest))...)
	}
	for i, w := range o.rest {
		m.rest[i] |= w
	}
}

// And returns m ∩ o.
func (m *Mask) And(o *Mask) Mask {
	r := Mask{w0: m.w0 & o.w0}
	if n := min(len(m.rest), len(o.rest)); n > 0 {
		r.rest = make([]uint64, n)
		for i := range n {
			r.rest[i] = m.rest[i] & o.rest[i]
		}
	}
	return r
}

// AndNot returns m \ o.
func (m *Mask) AndNot(o *Mask) Mask {
	r := Mask{w0: m.w0 &^ o.w0}
	if len(m.rest) > 0 {
		r.rest = make([]uint64, len(m.rest))
		for i, w := range m.rest {
			r.rest[i] = w &^ o.word(i+1)
		}
	}
	return r
}

// Intersects reports whether m ∩ o is non-empty.
func (m *Mask) Intersects(o *Mask) bool {
	if m.w0&o.w0 != 0 {
		return true
	}
	for i := range min(len(m.rest), len(o.rest)) {
		if m.rest[i]&o.rest[i] != 0 {
			return true
		}
	}
	return false
}

// ForEach calls f for each domain in ascending order until f returns false.
func (m *Mask) ForEach(f func(DomainID) bool) {
	m.ForEachFrom(0, f)
}

// ForEachFrom calls f for each domain, starting at start and wrapping around: the domains >= start in ascending order,
// then the domains < start in ascending order, until f returns false.
func (m *Mask) ForEachFrom(start DomainID, f func(DomainID) bool) {
	n := m.words()
	sw := int(start >> 6)
	if sw >= n {
		sw, start = 0, 0
	}
	// the first word only from start, then every word after it, then the words before it, then the first word below start
	for i := 0; i <= n; i++ {
		wi := (sw + i) % n
		w := m.word(wi)
		switch {
		case i == 0:
			w &= ^uint64(0) << (start & 63)
		case i == n:
			w &^= ^uint64(0) << (start & 63)
		}
		for w != 0 {
			t := bits.TrailingZeros64(w)
			if !f(DomainID(wi<<6 | t)) {
				return
			}
			w &= w - 1
		}
	}
}
