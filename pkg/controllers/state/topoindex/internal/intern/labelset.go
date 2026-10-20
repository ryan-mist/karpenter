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

// Package intern interns (namespace, labels) pairs so that pods with the same labels share one immutable label set.
package intern

import (
	"maps"
	"slices"

	"k8s.io/apimachinery/pkg/labels"
)

// LabelSet is an interned (namespace, labels) pair. Namespace and Labels are immutable once interned, so a LabelSet
// can be shared with readers. Data is owned by the Table's user and is only accessed by the writer.
type LabelSet[T any] struct {
	Namespace string
	Labels    labels.Set
	Data      T

	key  string
	refs int
}

// Refs returns the number of references held on the label set.
func (s *LabelSet[T]) Refs() int {
	return s.refs
}

// Table is a reference-counted table of label sets, keyed by the canonical encoding of (namespace, labels). Lookups
// of an existing label set don't allocate. Table isn't safe for concurrent use.
type Table[T any] struct {
	sets map[string]*LabelSet[T]
	buf  []byte
	keys []string
}

func NewTable[T any]() *Table[T] {
	return &Table[T]{sets: map[string]*LabelSet[T]{}}
}

// encode returns the canonical encoding "namespace\x00k\x01v\x00...", with keys sorted. Label keys and values, and
// namespace names, can't contain these bytes, so the encoding is unambiguous.
func (t *Table[T]) encode(namespace string, l map[string]string) []byte {
	t.keys = t.keys[:0]
	for k := range l {
		t.keys = append(t.keys, k)
	}
	slices.Sort(t.keys)
	b := append(t.buf[:0], namespace...)
	b = append(b, 0)
	for _, k := range t.keys {
		b = append(b, k...)
		b = append(b, 1)
		b = append(b, l[k]...)
		b = append(b, 0)
	}
	t.buf = b
	return b
}

// Lookup returns the label set for (namespace, l) without taking a reference.
func (t *Table[T]) Lookup(namespace string, l map[string]string) (*LabelSet[T], bool) {
	s, ok := t.sets[string(t.encode(namespace, l))]
	return s, ok
}

// Acquire returns the label set for (namespace, l) and takes a reference on it. created is true if the label set
// didn't exist. The labels are copied, so l may change afterwards.
func (t *Table[T]) Acquire(namespace string, l map[string]string) (s *LabelSet[T], created bool) {
	b := t.encode(namespace, l)
	if s, ok := t.sets[string(b)]; ok {
		s.refs++
		return s, false
	}
	s = &LabelSet[T]{Namespace: namespace, Labels: labels.Set(maps.Clone(l)), key: string(b), refs: 1}
	if s.Labels == nil {
		s.Labels = labels.Set{}
	}
	t.sets[s.key] = s
	return s, true
}

// Release drops a reference on s. It returns true if that was the last reference, in which case s is removed from the
// table. A later Acquire of the same labels returns a new LabelSet.
func (t *Table[T]) Release(s *LabelSet[T]) bool {
	s.refs--
	if s.refs > 0 {
		return false
	}
	delete(t.sets, s.key)
	return true
}

// Len returns the number of live label sets.
func (t *Table[T]) Len() int {
	return len(t.sets)
}

// Bytes estimates the memory retained by the label sets' keys and values (not counting map and struct overhead).
func (t *Table[T]) Bytes() int {
	n := 0
	for key := range t.sets {
		// the canonical key, plus the label strings, which are copied into the set's own map
		n += 2 * len(key)
	}
	return n
}
