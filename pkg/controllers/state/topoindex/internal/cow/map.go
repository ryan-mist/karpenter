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

// Package cow provides a copy-on-write map whose contents can be frozen into immutable views in O(shards), so that a
// single writer can keep updating it while readers hold views without locks.
package cow

import (
	"hash/maphash"
	"maps"
)

// Map is a map from K to V with a single writer. Freeze returns an immutable View of the current contents. The map is
// split into shards: Freeze tags every shard as shared, and the first write to a shared shard copies it, so a View
// only shares the shards that haven't changed since it was taken. A write costs O(1) when its shard isn't shared and
// O(len(shard)) when it is; a Freeze costs O(shards).
//
// Map isn't safe for concurrent use. Views are immutable and safe for concurrent use.
type Map[K ~string, V any] struct {
	seed   maphash.Seed
	shards []*shard[K, V]
	// gen is the generation of shards that this Map owns exclusively. Shards with an older generation may be shared with
	// a View and are copied before they're written.
	gen uint64
	len int
}

type shard[K ~string, V any] struct {
	gen uint64
	m   map[K]V
}

// NewMap returns an empty map with n shards. n is rounded up to a power of two.
func NewMap[K ~string, V any](n int) *Map[K, V] {
	size := 1
	for size < n {
		size <<= 1
	}
	return &Map[K, V]{seed: maphash.MakeSeed(), shards: make([]*shard[K, V], size), gen: 1}
}

func (m *Map[K, V]) index(k K) int {
	return int(maphash.String(m.seed, string(k)) & uint64(len(m.shards)-1)) //nolint:gosec // the shard count is a power of two that fits in an int
}

// Get returns the value for k.
func (m *Map[K, V]) Get(k K) (V, bool) {
	s := m.shards[m.index(k)]
	if s == nil {
		var zero V
		return zero, false
	}
	v, ok := s.m[k]
	return v, ok
}

// Len returns the number of entries.
func (m *Map[K, V]) Len() int {
	return m.len
}

// writable returns the shard at i, copying it first if it may be shared with a View.
func (m *Map[K, V]) writable(i int) *shard[K, V] {
	s := m.shards[i]
	switch {
	case s == nil:
		s = &shard[K, V]{gen: m.gen, m: map[K]V{}}
		m.shards[i] = s
	case s.gen != m.gen:
		s = &shard[K, V]{gen: m.gen, m: maps.Clone(s.m)}
		m.shards[i] = s
	}
	return s
}

// Set stores v for k.
func (m *Map[K, V]) Set(k K, v V) {
	s := m.writable(m.index(k))
	if _, ok := s.m[k]; !ok {
		m.len++
	}
	s.m[k] = v
}

// Delete removes k. Deleting a key that isn't present doesn't copy its shard.
func (m *Map[K, V]) Delete(k K) {
	i := m.index(k)
	if s := m.shards[i]; s == nil {
		return
	} else if _, ok := s.m[k]; !ok {
		return
	}
	delete(m.writable(i).m, k)
	m.len--
}

// Freeze returns an immutable view of the current contents. Later writes to the Map don't affect it.
func (m *Map[K, V]) Freeze() *View[K, V] {
	v := &View[K, V]{seed: m.seed, shards: make([]*shard[K, V], len(m.shards)), len: m.len}
	copy(v.shards, m.shards)
	m.gen++
	return v
}

// View is an immutable view of a Map, returned by Map.Freeze.
type View[K ~string, V any] struct {
	seed   maphash.Seed
	shards []*shard[K, V]
	len    int
}

// Get returns the value for k.
func (v *View[K, V]) Get(k K) (V, bool) {
	s := v.shards[int(maphash.String(v.seed, string(k))&uint64(len(v.shards)-1))] //nolint:gosec // see Map.index
	if s == nil {
		var zero V
		return zero, false
	}
	val, ok := s.m[k]
	return val, ok
}

// Len returns the number of entries.
func (v *View[K, V]) Len() int {
	return v.len
}

// All calls fn for every entry, in no particular order, until fn returns false.
func (v *View[K, V]) All(fn func(K, V) bool) {
	for _, s := range v.shards {
		if s == nil {
			continue
		}
		for k, val := range s.m {
			if !fn(k, val) {
				return
			}
		}
	}
}
