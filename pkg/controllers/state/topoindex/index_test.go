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
	"fmt"
	"maps"
	"math/rand/v2"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
)

func testPod(uid, namespace, nodeName string, l map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Name: uid, Namespace: namespace, Labels: l, ResourceVersion: "1"},
		Spec:       corev1.PodSpec{NodeName: nodeName},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func testNode(name string, l map[string]string, taints ...corev1.Taint) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: l, ResourceVersion: "1"},
		Spec:       corev1.NodeSpec{Taints: taints},
	}
}

// next returns a copy of obj with a new resource version, as an update from the API server would have
func next[T interface {
	DeepCopy() T
	GetResourceVersion() string
	SetResourceVersion(string)
}](obj T) T {
	out := obj.DeepCopy()
	var rv int
	fmt.Sscan(obj.GetResourceVersion(), &rv) //nolint:errcheck
	out.SetResourceVersion(fmt.Sprint(rv + 1))
	return out
}

func expectChange(t *testing.T, ix *Index, want bool, changed bool, what string) {
	t.Helper()
	if changed != want {
		t.Fatalf("%s: changed = %t, want %t", what, changed, want)
	}
}

func TestPodDiffing(t *testing.T) {
	ix := New()
	pod := testPod("a", "ns", "n1", map[string]string{"app": "a"})
	expectChange(t, ix, true, ix.ObservePod(pod), "new pod")
	gen := ix.Generation()
	expectChange(t, ix, false, ix.ObservePod(pod), "same resource version")

	status := next(pod)
	status.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	expectChange(t, ix, false, ix.ObservePod(status), "status-only update")
	annotations := next(status)
	annotations.Annotations = map[string]string{"x": "y"}
	expectChange(t, ix, false, ix.ObservePod(annotations), "annotation-only update")
	if ix.Generation() != gen {
		t.Fatalf("generation bumped by no-op updates: %d -> %d", gen, ix.Generation())
	}

	relabeled := next(annotations)
	relabeled.Labels = map[string]string{"app": "b"}
	expectChange(t, ix, true, ix.ObservePod(relabeled), "label change")
	terminating := next(relabeled)
	terminating.DeletionTimestamp = &metav1.Time{}
	expectChange(t, ix, true, ix.ObservePod(terminating), "terminating")
	if f, _ := ix.podFacts.Get("a"); f.Countable {
		t.Fatal("terminating pod is countable")
	}
	terminal := next(terminating)
	terminal.DeletionTimestamp = nil
	terminal.Status.Phase = corev1.PodSucceeded
	// terminating and terminal pods are both not countable, so nothing topology reads changed
	expectChange(t, ix, false, ix.ObservePod(terminal), "terminating to terminal")
	moved := next(terminal)
	moved.Status.Phase = corev1.PodRunning
	moved.Spec.NodeName = "n2"
	expectChange(t, ix, true, ix.ObservePod(moved), "node change")
	if f, _ := ix.podFacts.Get("a"); !f.Countable || f.NodeName != "n2" || f.Labels["app"] != "b" {
		t.Fatalf("unexpected fact %+v", f)
	}
	expectChange(t, ix, true, ix.DeletePod("a"), "delete")
	expectChange(t, ix, false, ix.DeletePod("a"), "second delete")
	if ix.sets.Len() != 0 || len(ix.nodes) != 0 || ix.podFacts.Len() != 0 {
		t.Fatalf("delete left state behind: %d sets, %d nodes, %d facts", ix.sets.Len(), len(ix.nodes), ix.podFacts.Len())
	}
}

func TestNodeDiffing(t *testing.T) {
	ix := New()
	taint := corev1.Taint{Key: "k", Value: "v", Effect: corev1.TaintEffectNoSchedule}
	node := testNode("n1", map[string]string{"zone": "a"}, taint)
	expectChange(t, ix, true, ix.ObserveNode(node), "new node")
	expectChange(t, ix, false, ix.ObserveNode(node), "same resource version")
	heartbeat := next(node)
	heartbeat.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	heartbeat.Spec.Taints[0].TimeAdded = &metav1.Time{}
	expectChange(t, ix, false, ix.ObserveNode(heartbeat), "status and taint time update")
	relabeled := next(heartbeat)
	relabeled.Labels = map[string]string{"zone": "b"}
	expectChange(t, ix, true, ix.ObserveNode(relabeled), "label change")
	retainted := next(relabeled)
	retainted.Spec.Taints[0].Effect = corev1.TaintEffectNoExecute
	expectChange(t, ix, true, ix.ObserveNode(retainted), "taint change")
	if n, _ := ix.nodeFacts.Get("n1"); n.Labels["zone"] != "b" || n.Taints[0].Effect != corev1.TaintEffectNoExecute {
		t.Fatalf("unexpected node %+v", n)
	}
	expectChange(t, ix, true, ix.DeleteNode("n1"), "delete")
	expectChange(t, ix, false, ix.DeleteNode("n1"), "second delete")
	if len(ix.nodes) != 0 {
		t.Fatal("deleted node without pods wasn't dropped")
	}
}

func TestNodeRecordOutlivesNodeWhilePodsAreBound(t *testing.T) {
	ix := New()
	ix.ObservePod(testPod("a", "ns", "n1", nil))
	if n := ix.nodes["n1"]; n == nil || n.node != nil || len(n.pods) != 1 {
		t.Fatal("a pod bound to an unknown node should create a placeholder node record")
	}
	ix.ObserveNode(testNode("n1", nil))
	ix.DeleteNode("n1")
	if n := ix.nodes["n1"]; n == nil || len(n.pods) != 1 {
		t.Fatal("deleting a node must keep the pods bound to it")
	}
	ix.DeletePod("a")
	if _, ok := ix.nodes["n1"]; ok {
		t.Fatal("placeholder node record wasn't dropped with its last pod")
	}
}

func TestNamespaceDiffing(t *testing.T) {
	ix := New()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns", Labels: map[string]string{"team": "a"}}}
	expectChange(t, ix, true, ix.ObserveNamespace(ns), "new namespace")
	expectChange(t, ix, false, ix.ObserveNamespace(ns.DeepCopy()), "same labels")
	ns.Labels["team"] = "b"
	expectChange(t, ix, true, ix.ObserveNamespace(ns), "label change")
	expectChange(t, ix, true, ix.DeleteNamespace("ns"), "delete")
	expectChange(t, ix, false, ix.DeleteNamespace("ns"), "second delete")
}

func TestHandlersSynced(t *testing.T) {
	ix := New()
	if ix.HandlersSynced() {
		t.Fatal("index without handlers reported synced")
	}
	synced := false
	ix.SetHandlersSynced(func() bool { return true }, func() bool { return synced })
	if ix.HandlersSynced() {
		t.Fatal("index reported synced before every handler synced")
	}
	synced = true
	if !ix.HandlersSynced() {
		t.Fatal("index didn't report synced")
	}
}

// checkFacts checks the index against the objects it was fed, including its postings.
func checkFacts(t *testing.T, ix *Index, pods map[types.UID]*corev1.Pod, nodes map[string]*corev1.Node) {
	t.Helper()
	if ix.podFacts.Len() != len(pods) || len(ix.pods) != len(pods) {
		t.Fatalf("index has %d facts / %d records, want %d", ix.podFacts.Len(), len(ix.pods), len(pods))
	}
	setPods := map[*labelSet]int{}
	nodePods := map[string]int{}
	for uid, pod := range pods {
		f, ok := ix.podFacts.Get(uid)
		countable := podutils.IsScheduled(pod) && !podutils.IsTerminal(pod) && !podutils.IsTerminating(pod)
		if !ok || f.Namespace != pod.Namespace || !maps.Equal(f.Labels, pod.Labels) || f.NodeName != pod.Spec.NodeName || f.Countable != countable {
			t.Fatalf("fact for %s = %+v, want pod %s/%v on %q countable=%t", uid, f, pod.Namespace, pod.Labels, pod.Spec.NodeName, countable)
		}
		rec := ix.pods[uid]
		if countable {
			if rec.set.Data.pods[rec.setPos] != rec || ix.nodes[rec.nodeName].pods[rec.nodePos] != rec {
				t.Fatalf("postings of %s are inconsistent", uid)
			}
			setPods[rec.set]++
			nodePods[rec.nodeName]++
		}
		if _, ok := ix.nsSets[rec.set.Namespace][rec.set]; !ok {
			t.Fatalf("label set of %s isn't in its namespace's sets", uid)
		}
	}
	for _, sets := range ix.nsSets {
		for set := range sets {
			if len(set.Data.pods) != setPods[set] {
				t.Fatalf("label set %v has %d countable pods, want %d", set.Labels, len(set.Data.pods), setPods[set])
			}
		}
	}
	for name, n := range ix.nodes {
		if len(n.pods) != nodePods[name] {
			t.Fatalf("node %s has %d countable pods, want %d", name, len(n.pods), nodePods[name])
		}
		if want, ok := nodes[name]; ok != (n.node != nil) || ok && !maps.Equal(n.node.Labels, want.Labels) {
			t.Fatalf("node %s = %+v, want %v", name, n.node, want)
		}
		if n.node == nil && len(n.pods) == 0 {
			t.Fatalf("node record %s should have been dropped", name)
		}
	}
	if ix.nodeFacts.Len() != len(nodes) {
		t.Fatalf("index has %d nodes, want %d", ix.nodeFacts.Len(), len(nodes))
	}
}

func TestFactsMatchReference(t *testing.T) {
	for seed := range uint64(30) {
		r := rand.New(rand.NewPCG(seed, seed)) //nolint:gosec // deterministic test input
		ix := New()
		pods := map[types.UID]*corev1.Pod{}
		nodes := map[string]*corev1.Node{}
		for step := range 400 {
			uid := types.UID(fmt.Sprintf("p%d", r.IntN(40)))
			nodeName := fmt.Sprintf("n%d", r.IntN(6))
			switch op := r.IntN(10); {
			case op < 4:
				pod := testPod(string(uid), fmt.Sprintf("ns%d", r.IntN(2)), "", map[string]string{"app": fmt.Sprint(r.IntN(3))})
				if old, ok := pods[uid]; ok {
					pod = next(old)
				}
				switch r.IntN(4) {
				case 0:
					pod.Spec.NodeName = nodeName
				case 1:
					pod.Labels = map[string]string{"app": fmt.Sprint(r.IntN(3)), "tier": fmt.Sprint(r.IntN(2))}
				case 2:
					pod.DeletionTimestamp = &metav1.Time{}
				default:
					pod.Status.Phase = corev1.PodFailed
				}
				ix.ObservePod(pod)
				pods[uid] = pod
			case op < 6:
				ix.DeletePod(uid)
				delete(pods, uid)
			case op < 8:
				node := testNode(nodeName, map[string]string{"zone": fmt.Sprint(r.IntN(3))})
				if old, ok := nodes[nodeName]; ok {
					node = next(old)
					node.Labels = map[string]string{"zone": fmt.Sprint(r.IntN(3))}
				}
				ix.ObserveNode(node)
				nodes[nodeName] = node
			default:
				ix.DeleteNode(nodeName)
				delete(nodes, nodeName)
			}
			if step%20 == 0 {
				checkFacts(t, ix, pods, nodes)
			}
		}
		checkFacts(t, ix, pods, nodes)
	}
}
