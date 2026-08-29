/*
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
	"math/rand"
	"sort"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

// newTestExistingNode builds a managed ExistingNode whose Name() resolves to name and whose
// Initialized() resolves to initialized. Managed nodes are only Initialized when the
// initialized label is present, and Name() returns Node.Name once the node is Registered.
func newTestExistingNode(name string, initialized bool) *ExistingNode {
	labels := map[string]string{v1.NodeRegisteredLabelKey: "true"}
	if initialized {
		labels[v1.NodeInitializedLabelKey] = "true"
	}
	return &ExistingNode{
		StateNode: &state.StateNode{
			NodeClaim: &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}},
			Node:      &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}},
		},
	}
}

// oldSortComparator is the pre-optimization comparator, kept here as the behavioral oracle.
func oldSortComparator(nodes []*ExistingNode) func(i, j int) bool {
	return func(i, j int) bool {
		if nodes[i].Initialized() && !nodes[j].Initialized() {
			return true
		}
		if !nodes[i].Initialized() && nodes[j].Initialized() {
			return false
		}
		return nodes[i].Name() < nodes[j].Name()
	}
}

func TestSortExistingNodes(t *testing.T) {
	// Build a mix of initialized/uninitialized nodes across a range of names.
	var input []*ExistingNode
	for _, name := range []string{"b", "a", "d", "c", "e", "f", "g", "h"} {
		input = append(input, newTestExistingNode(name, len(name)%2 == 0 || name == "a"))
	}

	// Shuffle to exercise arbitrary input orderings.
	rng := rand.New(rand.NewSource(1))
	rng.Shuffle(len(input), func(i, j int) { input[i], input[j] = input[j], input[i] })

	// Reference result from the original comparator on a copy of the shuffled input.
	want := make([]*ExistingNode, len(input))
	copy(want, input)
	sort.SliceStable(want, oldSortComparator(want))

	s := &Scheduler{existingNodes: input}
	s.sortExistingNodes()
	got := s.existingNodes

	if len(got) != len(want) {
		t.Fatalf("length mismatch: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ordering mismatch at %d: got %q want %q", i, got[i].Name(), want[i].Name())
		}
	}

	// Explicit invariants: initialized nodes come first; within a status, names ascend.
	sawUninitialized := false
	for i, n := range got {
		if !n.Initialized() {
			sawUninitialized = true
		} else if sawUninitialized {
			t.Fatalf("initialized node %q appeared after an uninitialized node", n.Name())
		}
		if i > 0 && got[i-1].Initialized() == n.Initialized() && got[i-1].Name() > n.Name() {
			t.Fatalf("names not ascending within status: %q before %q", got[i-1].Name(), n.Name())
		}
	}
}

// TestSortExistingNodesStable verifies that equal keys (same initialized status and same name)
// preserve their input order, matching sort.SliceStable semantics.
func TestSortExistingNodesStable(t *testing.T) {
	// Two distinct node objects sharing the same name and status must keep input order.
	first := newTestExistingNode("dup", true)
	second := newTestExistingNode("dup", true)
	other := newTestExistingNode("zzz", true)

	s := &Scheduler{existingNodes: []*ExistingNode{other, first, second}}
	s.sortExistingNodes()
	got := s.existingNodes

	if got[0] != first || got[1] != second || got[2] != other {
		t.Fatalf("stability not preserved: got order [%p %p %p], want [first=%p second=%p other=%p]",
			got[0], got[1], got[2], first, second, other)
	}
}
