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
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"sigs.k8s.io/karpenter/pkg/controllers/state/topoindex"
)

// TopologyIndexController feeds the topology index from informer event handlers on Pods, Nodes and Namespaces. Unlike
// the state reconcilers, the handlers see every change as soon as the informer cache applies it, so the index matches
// what a List or Get from the cache would return. It doesn't reconcile anything itself.
type TopologyIndexController struct {
	index *topoindex.Index
}

func NewTopologyIndexController(index *topoindex.Index) *TopologyIndexController {
	return &TopologyIndexController{index: index}
}

func (c *TopologyIndexController) Name() string {
	return "state.topologyindex"
}

func (c *TopologyIndexController) Register(ctx context.Context, m manager.Manager) error {
	handlers := []struct {
		obj     client.Object
		handler toolscache.ResourceEventHandler
	}{
		{obj: &corev1.Pod{}, handler: podHandler(c.index)},
		{obj: &corev1.Node{}, handler: nodeHandler(c.index)},
		{obj: &corev1.Namespace{}, handler: namespaceHandler(c.index)},
	}
	var synced []func() bool
	for _, h := range handlers {
		informer, err := m.GetCache().GetInformer(ctx, h.obj)
		if err != nil {
			return fmt.Errorf("getting informer for %T, %w", h.obj, err)
		}
		registration, err := informer.AddEventHandler(h.handler)
		if err != nil {
			return fmt.Errorf("adding topology index event handler for %T, %w", h.obj, err)
		}
		synced = append(synced, registration.HasSynced)
	}
	c.index.SetHandlersSynced(synced...)
	return nil
}

// tombstone returns the object of a delete event, which may be wrapped in a DeletedFinalStateUnknown if the watch
// missed the delete.
func tombstone[T any](obj any) (T, bool) {
	if d, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
		obj = d.Obj
	}
	o, ok := obj.(T)
	return o, ok
}

func podHandler(index *topoindex.Index) toolscache.ResourceEventHandler {
	return toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if pod, ok := obj.(*corev1.Pod); ok {
				index.ObservePod(pod)
			}
		},
		UpdateFunc: func(oldObj, newObj any) {
			pod, ok := newObj.(*corev1.Pod)
			if !ok {
				return
			}
			// a relist can replace a pod with another pod of the same name
			if old, ok := oldObj.(*corev1.Pod); ok && old.UID != pod.UID {
				index.DeletePod(old.UID)
			}
			index.ObservePod(pod)
		},
		DeleteFunc: func(obj any) {
			if pod, ok := tombstone[*corev1.Pod](obj); ok {
				index.DeletePod(pod.UID)
			}
		},
	}
}

func nodeHandler(index *topoindex.Index) toolscache.ResourceEventHandler {
	return toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if node, ok := obj.(*corev1.Node); ok {
				index.ObserveNode(node)
			}
		},
		UpdateFunc: func(_, newObj any) {
			if node, ok := newObj.(*corev1.Node); ok {
				index.ObserveNode(node)
			}
		},
		DeleteFunc: func(obj any) {
			if node, ok := tombstone[*corev1.Node](obj); ok {
				index.DeleteNode(node.Name)
			}
		},
	}
}

func namespaceHandler(index *topoindex.Index) toolscache.ResourceEventHandler {
	return toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if namespace, ok := obj.(*corev1.Namespace); ok {
				index.ObserveNamespace(namespace)
			}
		},
		UpdateFunc: func(_, newObj any) {
			if namespace, ok := newObj.(*corev1.Namespace); ok {
				index.ObserveNamespace(namespace)
			}
		},
		DeleteFunc: func(obj any) {
			if namespace, ok := tombstone[*corev1.Namespace](obj); ok {
				index.DeleteNamespace(namespace.Name)
			}
		},
	}
}
