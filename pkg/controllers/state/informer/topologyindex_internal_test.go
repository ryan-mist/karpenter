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

package informer

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	toolscache "k8s.io/client-go/tools/cache"

	"sigs.k8s.io/karpenter/pkg/controllers/state/topoindex"
)

func TestTopologyIndexPodHandler(t *testing.T) {
	ix := topoindex.New()
	h := podHandler(ix)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", UID: "1", ResourceVersion: "1"}, Spec: corev1.PodSpec{NodeName: "n"}}
	h.OnAdd(pod, true)
	gen := ix.Generation()
	if gen == 0 {
		t.Fatal("add didn't reach the index")
	}
	// a pod recreated with the same name is a different pod
	recreated := pod.DeepCopy()
	recreated.UID = "2"
	h.OnUpdate(pod, recreated)
	if ix.DeletePod("1") {
		t.Fatal("update to a new UID didn't delete the old pod")
	}
	h.OnDelete(toolscache.DeletedFinalStateUnknown{Key: "ns/p", Obj: recreated})
	if ix.DeletePod("2") {
		t.Fatal("tombstone delete didn't delete the pod")
	}
}

func TestTopologyIndexNodeAndNamespaceHandlers(t *testing.T) {
	ix := topoindex.New()
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n", ResourceVersion: "1"}}
	nodes := nodeHandler(ix)
	nodes.OnAdd(node, false)
	nodes.OnDelete(toolscache.DeletedFinalStateUnknown{Key: "n", Obj: node})
	if ix.DeleteNode("n") {
		t.Fatal("tombstone delete didn't delete the node")
	}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns", Labels: map[string]string{"a": "b"}}}
	namespaces := namespaceHandler(ix)
	namespaces.OnAdd(namespace, false)
	updated := namespace.DeepCopy()
	updated.Labels["a"] = "c"
	gen := ix.Generation()
	namespaces.OnUpdate(namespace, updated)
	if ix.Generation() == gen {
		t.Fatal("namespace relabel didn't reach the index")
	}
	namespaces.OnDelete(updated)
	if ix.DeleteNamespace("ns") {
		t.Fatal("delete didn't delete the namespace")
	}
}
