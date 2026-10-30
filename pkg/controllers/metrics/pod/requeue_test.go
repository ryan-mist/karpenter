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

package pod_test

import (
	"bytes"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/common/expfmt"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/metrics/pod"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// gatherPodMetrics returns the text exposition of every karpenter_pods_* family
func gatherPodMetrics() string {
	GinkgoHelper()
	families, err := crmetrics.Registry.Gather()
	Expect(err).ToNot(HaveOccurred())
	buf := &bytes.Buffer{}
	for _, mf := range families {
		if strings.HasPrefix(mf.GetName(), "karpenter_pods_") {
			_, err := expfmt.MetricFamilyToText(buf, mf)
			Expect(err).ToNot(HaveOccurred())
		}
	}
	return buf.String()
}

func nodeLabels(zone, instanceType string) map[string]string {
	return map[string]string{
		corev1.LabelTopologyZone:       zone,
		corev1.LabelArchStable:         "amd64",
		v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
		corev1.LabelInstanceTypeStable: instanceType,
		v1.NodePoolLabelKey:            "default",
	}
}

var _ = Describe("Requeue", func() {
	scheduled := corev1.PodCondition{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}
	ready := corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}

	DescribeTable("should only requeue pods with time-based metrics",
		func(seenPending bool, opts test.PodOptions, expected bool) {
			p := test.Pod(opts)
			if seenPending {
				pending := p.DeepCopy()
				pending.Status.Phase = corev1.PodPending
				pending.Status.Conditions = nil
				ExpectApplied(ctx, env.Client, pending)
				ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(p))
				p.ResourceVersion = pending.ResourceVersion
			}
			ExpectApplied(ctx, env.Client, p)
			result := ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(p))
			Expect(result.RequeueAfter).To(Equal(lo.Ternary(expected, 30*time.Second, 10*time.Minute)))
		},
		Entry("pending and unbound", false, test.PodOptions{Phase: corev1.PodPending}, true),
		Entry("pending and bound", false, test.PodOptions{Phase: corev1.PodPending, NodeName: "node", Conditions: []corev1.PodCondition{scheduled}}, true),
		Entry("without a phase and unbound", false, test.PodOptions{}, true),
		Entry("without a phase and bound", false, test.PodOptions{NodeName: "node"}, false),
		Entry("running and not ready after pending", true, test.PodOptions{Phase: corev1.PodRunning, NodeName: "node", Conditions: []corev1.PodCondition{scheduled}}, true),
		Entry("running and ready after pending", true, test.PodOptions{Phase: corev1.PodRunning, NodeName: "node", Conditions: []corev1.PodCondition{scheduled, ready}}, false),
		Entry("running and not ready when first seen running", false, test.PodOptions{Phase: corev1.PodRunning, NodeName: "node", Conditions: []corev1.PodCondition{scheduled}}, false),
		Entry("succeeded after pending", true, test.PodOptions{Phase: corev1.PodSucceeded, NodeName: "node", Conditions: []corev1.PodCondition{scheduled}}, false),
		Entry("failed and unbound after pending", true, test.PodOptions{Phase: corev1.PodFailed}, false),
	)
	It("should only pass node events that change the labels the pod metrics read", func() {
		node := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Labels: nodeLabels("test-zone-1", "small")}})
		updated := node.DeepCopy()
		updated.Annotations = map[string]string{"foo": "bar"}
		updated.Labels["foo"] = "bar"
		updated.Status.Conditions = nil
		Expect(pod.NodeLabelsChanged.Update(event.UpdateEvent{ObjectOld: node, ObjectNew: updated})).To(BeFalse())
		for k := range nodeLabels("", "") {
			updated := node.DeepCopy()
			updated.Labels[k] = "changed"
			Expect(pod.NodeLabelsChanged.Update(event.UpdateEvent{ObjectOld: node, ObjectNew: updated})).To(BeTrue())
			delete(updated.Labels, k)
			Expect(pod.NodeLabelsChanged.Update(event.UpdateEvent{ObjectOld: node, ObjectNew: updated})).To(BeTrue())
		}
		Expect(pod.NodeLabelsChanged.Create(event.CreateEvent{Object: node})).To(BeTrue())
		Expect(pod.NodeLabelsChanged.Delete(event.DeleteEvent{Object: node})).To(BeTrue())
	})
	It("should map a node to the pods bound to it", func() {
		node := test.Node()
		bound := []*corev1.Pod{test.Pod(test.PodOptions{NodeName: node.Name}), test.Pod(test.PodOptions{NodeName: node.Name})}
		ExpectApplied(ctx, env.Client, node, bound[0], bound[1], test.Pod(), test.Pod(test.PodOptions{NodeName: "other"}))
		Expect(podController.PodsForNode(ctx, node)).To(ConsistOf(
			lo.Map(bound, func(p *corev1.Pod, _ int) any { return HaveField("NamespacedName", client.ObjectKeyFromObject(p)) })...,
		))
	})
	// The reconciles that are no longer requeued must not change the emitted metrics. Each step below reconciles what
	// the controller still reconciles (requeued pods, and pods enqueued by node events), gathers the metrics, then
	// reconciles every other pod, as the 30s requeue did before, and expects the gathered metrics to be identical.
	It("should emit the same metrics as reconciling every pod", func() {
		n1 := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Labels: nodeLabels("test-zone-1", "small")}})
		n2 := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Labels: nodeLabels("test-zone-2", "large")}})
		n3 := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Labels: nodeLabels("test-zone-3", "medium")}})
		ExpectApplied(ctx, env.Client, n1, n2)
		owner := []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs", UID: "uid"}}
		onNode := func(n *corev1.Node, phase corev1.PodPhase, conds ...corev1.PodCondition) *corev1.Pod {
			return test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{OwnerReferences: owner}, NodeName: n.Name, Phase: phase, Conditions: conds})
		}
		pendingAcked := test.Pod(test.PodOptions{Phase: corev1.PodPending})
		pendingSchedulable := test.Pod(test.PodOptions{Phase: corev1.PodPending})
		pendingBound := onNode(n1, corev1.PodPending, scheduled)
		runningNotReady := onNode(n1, corev1.PodRunning, scheduled)
		runningReady := onNode(n1, corev1.PodRunning, scheduled, ready)
		succeeded := onNode(n1, corev1.PodSucceeded, scheduled)
		failedUnbound := test.Pod(test.PodOptions{Phase: corev1.PodFailed})
		noPhase := test.Pod()
		runningOnN2 := onNode(n2, corev1.PodRunning, scheduled, ready)
		runningOnN3 := onNode(n3, corev1.PodRunning, scheduled, ready)
		// These go through Pending first so that their unstarted and unbound metrics are tracked
		startedLater := []*corev1.Pod{runningNotReady, runningReady, succeeded, failedUnbound}
		pods := []*corev1.Pod{pendingAcked, pendingSchedulable, pendingBound, runningNotReady, runningReady, succeeded, failedUnbound, noPhase, runningOnN2, runningOnN3}

		cluster.AckPods(pendingAcked, noPhase)
		cluster.MarkPodSchedulingDecisions(ctx, map[*corev1.Pod]error{}, map[string][]*corev1.Pod{"n1": {pendingSchedulable, runningNotReady}}, map[string][]*corev1.Pod{"nc1": {pendingSchedulable, runningNotReady}})
		for _, p := range startedLater {
			pending := p.DeepCopy()
			pending.Status.Phase = corev1.PodPending
			pending.Status.Conditions = nil
			ExpectApplied(ctx, env.Client, pending)
			ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(p))
			p.ResourceVersion = pending.ResourceVersion
		}
		for _, p := range pods {
			ExpectApplied(ctx, env.Client, p)
		}

		requeued := sets.New[types.NamespacedName]()
		reconcilePods := func(keys ...types.NamespacedName) {
			for _, key := range keys {
				if ExpectReconcileSucceeded(ctx, podController, key).RequeueAfter == 30*time.Second {
					requeued.Insert(key)
				} else {
					requeued.Delete(key)
				}
			}
		}
		all := lo.Map(pods, func(p *corev1.Pod, _ int) types.NamespacedName { return client.ObjectKeyFromObject(p) })
		expectSameAsReconcilingAll := func(enqueued ...types.NamespacedName) {
			reconcilePods(requeued.Union(sets.New(enqueued...)).UnsortedList()...)
			got := gatherPodMetrics()
			reconcilePods(sets.New(all...).Difference(requeued).UnsortedList()...)
			Expect(gatherPodMetrics()).To(Equal(got))
		}
		reconcilePods(all...)
		Expect(requeued.UnsortedList()).To(ConsistOf(lo.Map([]*corev1.Pod{pendingAcked, pendingSchedulable, pendingBound, runningNotReady, noPhase}, func(p *corev1.Pod, _ int) types.NamespacedName {
			return client.ObjectKeyFromObject(p)
		})))
		before := gatherPodMetrics()

		By("letting time pass")
		time.Sleep(10 * time.Millisecond)
		expectSameAsReconcilingAll()
		Expect(gatherPodMetrics()).ToNot(Equal(before))

		By("changing scheduling state in cluster state")
		cluster.AckPods(lo.Map(pods, func(p *corev1.Pod, _ int) *corev1.Pod { return p })...)
		cluster.MarkPodSchedulingDecisions(ctx, map[*corev1.Pod]error{}, map[string][]*corev1.Pod{"n1": {runningReady, succeeded, runningOnN2}}, map[string][]*corev1.Pod{"nc1": {runningReady, succeeded, runningOnN2}})
		expectSameAsReconcilingAll()

		By("changing node labels, deleting a node and creating a node")
		var enqueued []types.NamespacedName
		stored := n1.DeepCopy()
		n1.Labels[corev1.LabelTopologyZone] = "test-zone-4"
		n1.Labels[v1.NodePoolLabelKey] = "other"
		ExpectApplied(ctx, env.Client, n1)
		Expect(pod.NodeLabelsChanged.Update(event.UpdateEvent{ObjectOld: stored, ObjectNew: n1})).To(BeTrue())
		ExpectDeleted(ctx, env.Client, n2)
		ExpectApplied(ctx, env.Client, n3)
		for _, n := range []*corev1.Node{n1, n2, n3} {
			enqueued = append(enqueued, lo.Map(podController.PodsForNode(ctx, n), func(r reconcile.Request, _ int) types.NamespacedName { return r.NamespacedName })...)
		}
		expectSameAsReconcilingAll(enqueued...)
		_, found := FindMetricWithLabelValues("karpenter_pods_state", map[string]string{"name": runningReady.Name, "zone": "test-zone-4", "nodepool": "other"})
		Expect(found).To(BeTrue())
		_, found = FindMetricWithLabelValues("karpenter_pods_state", map[string]string{"name": runningOnN2.Name, "zone": "", "managed": "false"})
		Expect(found).To(BeTrue())
		_, found = FindMetricWithLabelValues("karpenter_pods_state", map[string]string{"name": runningOnN3.Name, "zone": "test-zone-3", "managed": "true"})
		Expect(found).To(BeTrue())
	})
})
