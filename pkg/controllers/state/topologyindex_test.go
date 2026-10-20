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

package state_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/controllers/state/topoindex"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

type inverseTermCounts struct {
	Owners map[types.UID]int32
	Counts map[string]int32
}

// forPodsWithAntiAffinityTerms groups what ForPodsWithAntiAffinity yields by (namespace, term), the way the topology
// index publishes it
func forPodsWithAntiAffinityTerms() map[string]inverseTermCounts {
	out := map[string]inverseTermCounts{}
	cluster.ForPodsWithAntiAffinity(func(p *corev1.Pod, n *corev1.Node) bool {
		for _, term := range p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			encoded, err := json.Marshal(term)
			Expect(err).ToNot(HaveOccurred())
			key := p.Namespace + "/" + string(encoded)
			tc, ok := out[key]
			if !ok {
				tc = inverseTermCounts{Owners: map[types.UID]int32{}, Counts: map[string]int32{}}
				out[key] = tc
			}
			tc.Owners[p.UID]++
			if domain, ok := n.Labels[term.TopologyKey]; ok {
				tc.Counts[domain]++
			}
		}
		return true
	})
	return out
}

func indexInverseTerms(index *topoindex.Index) map[string]inverseTermCounts {
	out := map[string]inverseTermCounts{}
	index.Snapshot().InverseTerms(func(t *topoindex.InverseTerm) bool {
		encoded, err := json.Marshal(t.Term)
		Expect(err).ToNot(HaveOccurred())
		tc := inverseTermCounts{Owners: map[types.UID]int32{}, Counts: t.Counts}
		for _, o := range t.Owners {
			tc.Owners[o.UID] += o.Terms
		}
		out[t.Namespace+"/"+string(encoded)] = tc
		return true
	})
	return out
}

var _ = Describe("Topology Index", func() {
	var index *topoindex.Index
	BeforeEach(func() {
		index = topoindex.New()
		cluster.SetTopologyIndex(index)
	})
	AfterEach(func() {
		cluster.SetTopologyIndex(nil)
	})

	It("should be fed by informer event handlers", func() {
		mgr, err := manager.New(env.Config, manager.Options{Metrics: server.Options{BindAddress: "0"}, HealthProbeBindAddress: "0"})
		Expect(err).ToNot(HaveOccurred())
		fed := topoindex.New()
		Expect(informer.NewTopologyIndexController(fed).Register(ctx, mgr)).To(Succeed())
		mgrCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			defer GinkgoRecover()
			Expect(mgr.Start(mgrCtx)).To(Succeed())
		}()
		Eventually(fed.Synced).Should(BeTrue())

		node := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{corev1.LabelTopologyZone: "zone-a"}}})
		pod := test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "fed"}}, NodeName: node.Name})
		ExpectApplied(ctx, env.Client, node, pod)
		spec := topoindex.GroupSpec{TopologyKey: corev1.LabelTopologyZone, Namespaces: []string{pod.Namespace}, Selector: labels.SelectorFromSet(labels.Set{"app": "fed"})}
		counts := func() map[string]int32 {
			_, c := fed.Register(spec)
			return c.Counts()
		}
		Eventually(counts).Should(Equal(map[string]int32{"zone-a": 1}))

		stored := &corev1.Node{}
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(node), stored)).To(Succeed())
		stored.Labels[corev1.LabelTopologyZone] = "zone-b"
		Expect(env.Client.Update(ctx, stored)).To(Succeed())
		Eventually(counts).Should(Equal(map[string]int32{"zone-b": 1}))

		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
		pod.Finalizers = nil
		Expect(env.Client.Update(ctx, pod)).To(Succeed())
		Expect(env.Client.Delete(ctx, pod)).To(Succeed())
		Eventually(counts).Should(BeEmpty())
	})

	It("should hydrate from the API server", func() {
		node := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{corev1.LabelTopologyZone: "zone-a"}}})
		pods := []*corev1.Pod{
			test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "a"}}, NodeName: node.Name}),
			test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "a"}}, NodeName: node.Name}),
			test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "b"}}, NodeName: node.Name}),
			test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "a"}}}),
		}
		ExpectApplied(ctx, env.Client, node, pods[0], pods[1], pods[2], pods[3])
		Expect(index.Synced()).To(BeFalse())
		ExpectTopologyStateHydrated(ctx, env.Client, cluster)
		Expect(index.Synced()).To(BeTrue())
		_, counts := index.Register(topoindex.GroupSpec{
			TopologyKey: corev1.LabelTopologyZone,
			Namespaces:  []string{pods[0].Namespace},
			Selector:    labels.SelectorFromSet(labels.Set{"app": "a"}),
		})
		Expect(counts.Counts()).To(Equal(map[string]int32{"zone-a": 2}))
		_, ok := index.Snapshot().Namespace(pods[0].Namespace)
		Expect(ok).To(BeTrue())

		// a later hydration drops what the API server no longer has
		ExpectDeleted(ctx, env.Client, pods[0])
		ExpectTopologyStateHydrated(ctx, env.Client, cluster)
		counts, _ = index.Snapshot().Counts(counts.Key())
		Expect(counts.Counts()).To(Equal(map[string]int32{"zone-a": 1}))
	})

	// The inverse anti-affinity view is fed by cluster state's own writes, so after any sequence of cluster state
	// updates it must hold exactly what ForPodsWithAntiAffinity yields.
	It("should mirror ForPodsWithAntiAffinity under random cluster churn", func() {
		nonEmpty := 0
		for seed := range int64(3) {
			r := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test input
			var nodes []*corev1.Node
			for i := range 4 {
				labels := map[string]string{corev1.LabelTopologyZone: fmt.Sprintf("zone-%d", i%2)}
				if i != 3 {
					// node 3 is unmanaged, has no hostname label and gets its name as its provider ID
					labels[v1.NodePoolLabelKey] = nodePool.Name
					labels[corev1.LabelInstanceTypeStable] = cloudProvider.InstanceTypes[0].Name
					labels[corev1.LabelHostname] = fmt.Sprintf("host-%d-%d", seed, i)
				}
				node := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node-%d-%d", seed, i), Labels: labels}})
				if i != 3 {
					node.Spec.ProviderID = test.RandomProviderID()
				}
				nodes = append(nodes, node)
			}
			pods := map[string]*corev1.Pod{}
			for step := range 120 {
				name := fmt.Sprintf("pod-%d-%d", seed, r.Intn(10))
				node := nodes[r.Intn(len(nodes))]
				pod, exists := pods[name]
				if exists {
					if err := env.Client.Get(ctx, client.ObjectKeyFromObject(pod), pod); errors.IsNotFound(err) {
						delete(pods, name)
						exists = false
					}
				}
				deleting := exists && pod.DeletionTimestamp != nil
				switch op := r.Intn(12); {
				case op < 3 && !exists:
					opts := test.PodOptions{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: map[string]string{"app": fmt.Sprint(r.Intn(2))}}}
					if r.Intn(4) != 0 {
						for range 1 + r.Intn(2) {
							opts.PodAntiRequirements = append(opts.PodAntiRequirements, corev1.PodAffinityTerm{
								TopologyKey:   []string{corev1.LabelHostname, corev1.LabelTopologyZone}[r.Intn(2)],
								LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": fmt.Sprint(r.Intn(2))}},
							})
						}
					}
					if r.Intn(3) == 0 {
						opts.Finalizers = []string{"test/finalizer"}
					}
					pod = test.UnschedulablePod(opts)
					ExpectApplied(ctx, env.Client, pod)
					pods[name] = pod
					ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod))
				case op < 5 && exists && !deleting && pod.Spec.NodeName == "":
					if err := env.Client.Get(ctx, client.ObjectKeyFromObject(node), &corev1.Node{}); err == nil {
						ExpectManualBinding(ctx, env.Client, pod, node)
						Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
					}
					_, _ = podController.Reconcile(ctx, reconcileRequest(pod))
				case op < 6 && exists:
					pod.Status.Phase = corev1.PodSucceeded
					Expect(env.Client.Status().Update(ctx, pod)).To(Succeed())
					_, _ = podController.Reconcile(ctx, reconcileRequest(pod))
				case op < 7 && exists:
					// ExpectApplied adds a finalizer, so a delete leaves the pod terminating unless it's removed
					if r.Intn(2) == 0 {
						pod.Finalizers = nil
						Expect(env.Client.Update(ctx, pod)).To(Succeed())
					}
					Expect(client.IgnoreNotFound(env.Client.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace}}))).To(Succeed())
					_, _ = podController.Reconcile(ctx, reconcileRequest(pod))
				case op < 10:
					stored := &corev1.Node{}
					if err := env.Client.Get(ctx, client.ObjectKeyFromObject(node), stored); errors.IsNotFound(err) {
						node.ResourceVersion, node.UID = "", ""
						ExpectApplied(ctx, env.Client, node)
					} else {
						stored.Labels[corev1.LabelTopologyZone] = fmt.Sprintf("zone-%d", r.Intn(3))
						ExpectApplied(ctx, env.Client, stored)
					}
					// sometimes leave the Node out of cluster state for a while, so pods bind to it before cluster state
					// has it and get their binding when it's reconciled
					if r.Intn(3) != 0 {
						_, _ = nodeController.Reconcile(ctx, reconcileRequest(node))
					}
				default:
					stored := &corev1.Node{}
					if err := env.Client.Get(ctx, client.ObjectKeyFromObject(node), stored); err == nil {
						stored.Finalizers = nil
						Expect(env.Client.Update(ctx, stored)).To(Succeed())
					}
					Expect(client.IgnoreNotFound(env.Client.Delete(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: node.Name}}))).To(Succeed())
					_, _ = nodeController.Reconcile(ctx, reconcileRequest(node))
				}
				want := forPodsWithAntiAffinityTerms()
				Expect(indexInverseTerms(index)).To(Equal(want), "seed %d step %d", seed, step)
				if len(want) > 0 {
					nonEmpty++
				}
			}
			ExpectCleanedUp(ctx, env.Client)
			cluster.Reset()
		}
		// the churn must actually reach states with anti-affinity pods bound to nodes in cluster state
		Expect(nonEmpty).To(BeNumerically(">", 100))
	})
})

func reconcileRequest(obj client.Object) reconcile.Request {
	return reconcile.Request{NamespacedName: client.ObjectKeyFromObject(obj)}
}
