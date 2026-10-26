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

package disruption_test

import (
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// Decision #16: a command whose pods were simulated onto launched but uninitialized nodes waits for them.
var _ = Describe("Queue dependencies", func() {
	var dependencyClaim *v1.NodeClaim
	var dependencyNode *corev1.Node
	var stateNode *disruption.Candidate

	BeforeEach(func() {
		nodePool = test.NodePool()
		labels := map[string]string{
			v1.NodePoolLabelKey:            nodePool.Name,
			corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			v1.CapacityTypeLabelKey:        cloudProvider.InstanceTypes[0].Offerings.Cheapest().Requirements.Get(v1.CapacityTypeLabelKey).Any(),
			corev1.LabelTopologyZone:       cloudProvider.InstanceTypes[0].Offerings.Cheapest().Requirements.Get(corev1.LabelTopologyZone).Any(),
		}
		nodeClaim1, node1 = test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Status: v1.NodeClaimStatus{ProviderID: test.RandomProviderID()}})
		dependencyClaim, dependencyNode = test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Status: v1.NodeClaimStatus{ProviderID: test.RandomProviderID()}})
		node1.Spec.Taints = append(node1.Spec.Taints, v1.DisruptedNoScheduleTaint)
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim1, node1, dependencyClaim, dependencyNode)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node1}, []*v1.NodeClaim{nodeClaim1})
		// The dependency is launched and registered, but not initialized.
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(dependencyClaim))
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(dependencyNode))
		stateNode = &disruption.Candidate{StateNode: ExpectStateNodeExistsForNodeClaim(cluster, nodeClaim1), NodePool: nodePool}
		disruption.QueueDependencyFailuresTotal.Reset()
	})
	command := func(replacements ...*disruption.Replacement) *disruption.Command {
		return &disruption.Command{
			Method:            disruption.NewDrift(env.Client, cluster, prov, recorder, env.Clock),
			CreationTimestamp: env.Clock.Now(),
			ID:                uuid.New(),
			Candidates:        []*disruption.Candidate{stateNode},
			Replacements:      replacements,
			Dependencies:      []string{dependencyClaim.Status.ProviderID},
		}
	}
	expectRolledBack := func() {
		Expect(queue.HasAny(nodeClaim1.Status.ProviderID)).To(BeFalse())
		node := ExpectNodeExists(ctx, env.Client, node1.Name)
		Expect(node.Spec.Taints).ToNot(ContainElement(v1.DisruptedNoScheduleTaint))
		ExpectExists(ctx, env.Client, nodeClaim1)
		// Not ExpectStateNodeExistsForNodeClaim: the deleted dependency's state node has no NodeClaim left.
		marked, found := false, false
		for n := range cluster.Nodes() {
			if n.ProviderID() == nodeClaim1.Status.ProviderID {
				marked, found = n.MarkedForDeletion(), true
			}
		}
		Expect(found).To(BeTrue())
		Expect(marked).To(BeFalse())
		Expect(ExpectExists(ctx, env.Client, nodeClaim1).StatusConditions().Get(v1.ConditionTypeDisruptionReason)).To(BeNil())
	}

	It("waits for an uninitialized dependency, then deletes the candidate", func() {
		cmd := command()
		Expect(queue.StartCommand(ctx, cmd)).To(Succeed())
		ExpectObjectReconciled(ctx, env.Client, queue, nodeClaim1)
		Expect(queue.HasAny(nodeClaim1.Status.ProviderID)).To(BeTrue())
		ExpectExists(ctx, env.Client, nodeClaim1)
		ExpectMetricGaugeValue(disruption.QueueCommandsWaitingOnDependencies, 1, nil)
		Expect(queue.InFlightDestinations().Has(dependencyClaim.Status.ProviderID)).To(BeTrue())

		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{dependencyNode}, []*v1.NodeClaim{dependencyClaim})
		ExpectObjectReconciled(ctx, env.Client, queue, nodeClaim1)
		Expect(cmd.Succeeded).To(BeTrue())
		Expect(queue.HasAny(nodeClaim1.Status.ProviderID)).To(BeFalse())
		ExpectMetricGaugeValue(disruption.QueueCommandsWaitingOnDependencies, 0, nil)
		ExpectNodeClaimsCascadeDeletion(ctx, env.Client, nodeClaim1)
		ExpectNotFound(ctx, env.Client, nodeClaim1, node1)
		ExpectExists(ctx, env.Client, dependencyClaim)
	})
	It("waits for its replacements and its dependencies", func() {
		nct := scheduling.NewNodeClaimTemplate(nodePool)
		nct.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
		cmd := command(&disruption.Replacement{NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct}})
		Expect(queue.StartCommand(ctx, cmd)).To(Succeed())
		replacement := ExpectExists(ctx, env.Client, &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: cmd.Replacements[0].Name}})
		replacement, replacementNode := ExpectNodeClaimDeployedAndStateUpdated(ctx, env.Client, cluster, cloudProvider, replacement)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{replacementNode}, []*v1.NodeClaim{replacement})

		// The replacement is initialized, the dependency isn't: the candidate stays.
		ExpectObjectReconciled(ctx, env.Client, queue, nodeClaim1)
		Expect(cmd.Replacements[0].Initialized).To(BeTrue())
		Expect(queue.HasAny(nodeClaim1.Status.ProviderID)).To(BeTrue())
		ExpectExists(ctx, env.Client, nodeClaim1)

		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{dependencyNode}, []*v1.NodeClaim{dependencyClaim})
		ExpectObjectReconciled(ctx, env.Client, queue, nodeClaim1)
		Expect(cmd.Succeeded).To(BeTrue())
		ExpectNodeClaimsCascadeDeletion(ctx, env.Client, nodeClaim1)
		ExpectNotFound(ctx, env.Client, nodeClaim1, node1)
	})
	It("rolls the command back when a dependency is deleted", func() {
		cmd := command()
		Expect(queue.StartCommand(ctx, cmd)).To(Succeed())
		ExpectObjectReconciled(ctx, env.Client, queue, nodeClaim1)
		Expect(queue.HasAny(nodeClaim1.Status.ProviderID)).To(BeTrue())

		ExpectDeleted(ctx, env.Client, dependencyClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(dependencyClaim))
		ExpectObjectReconciled(ctx, env.Client, queue, nodeClaim1)
		Expect(cmd.Succeeded).To(BeFalse())
		expectRolledBack()
		ExpectMetricCounterValue(disruption.QueueDependencyFailuresTotal, 1, map[string]string{"cause": disruption.DependencyFailureCauseDeleted.Name})
		ExpectMetricGaugeValue(disruption.QueueCommandsWaitingOnDependencies, 0, nil)
	})
	It("rolls the command back when a dependency is deleted while its replacement is initializing", func() {
		nct := scheduling.NewNodeClaimTemplate(nodePool)
		nct.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
		cmd := command(&disruption.Replacement{NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct}})
		Expect(queue.StartCommand(ctx, cmd)).To(Succeed())
		ExpectDeleted(ctx, env.Client, dependencyClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(dependencyClaim))
		ExpectObjectReconciled(ctx, env.Client, queue, nodeClaim1)
		Expect(cmd.Replacements[0].Initialized).To(BeFalse())
		expectRolledBack()
		ExpectMetricCounterValue(disruption.QueueDependencyFailuresTotal, 1, map[string]string{"cause": disruption.DependencyFailureCauseDeleted.Name})
	})
	It("rolls the command back at once when a dependency is being deleted (N-4)", func() {
		// A finalizer holds the dependency's NodeClaim: it is being deleted, but still exists.
		dependencyClaim.Finalizers = append(dependencyClaim.Finalizers, "karpenter.sh/test-finalizer")
		ExpectApplied(ctx, env.Client, dependencyClaim)
		DeferCleanup(func() {
			stored := &v1.NodeClaim{}
			if env.Client.Get(ctx, client.ObjectKeyFromObject(dependencyClaim), stored) == nil {
				stored.Finalizers = nil
				Expect(client.IgnoreNotFound(env.Client.Update(ctx, stored))).To(Succeed())
			}
		})
		cmd := command()
		Expect(queue.StartCommand(ctx, cmd)).To(Succeed())
		ExpectObjectReconciled(ctx, env.Client, queue, nodeClaim1)
		Expect(queue.HasAny(nodeClaim1.Status.ProviderID)).To(BeTrue())

		Expect(env.Client.Delete(ctx, dependencyClaim)).To(Succeed())
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(dependencyClaim))
		Expect(ExpectExists(ctx, env.Client, dependencyClaim).DeletionTimestamp.IsZero()).To(BeFalse())
		ExpectObjectReconciled(ctx, env.Client, queue, nodeClaim1)
		Expect(cmd.Succeeded).To(BeFalse())
		expectRolledBack()
		ExpectMetricCounterValue(disruption.QueueDependencyFailuresTotal, 1, map[string]string{"cause": disruption.DependencyFailureCauseDeleted.Name})
	})
	It("doesn't count a deleted replacement as a dependency failure while a dependency waits (N-1)", func() {
		nct := scheduling.NewNodeClaimTemplate(nodePool)
		nct.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
		cmd := command(&disruption.Replacement{NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct}})
		Expect(queue.StartCommand(ctx, cmd)).To(Succeed())
		ExpectObjectReconciled(ctx, env.Client, queue, nodeClaim1)
		ExpectMetricGaugeValue(disruption.QueueCommandsWaitingOnDependencies, 1, nil)

		replacement := ExpectExists(ctx, env.Client, &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: cmd.Replacements[0].Name}})
		ExpectDeleted(ctx, env.Client, replacement)
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(replacement))
		ExpectObjectReconciled(ctx, env.Client, queue, nodeClaim1)
		Expect(cmd.Succeeded).To(BeFalse())
		Expect(queue.HasAny(nodeClaim1.Status.ProviderID)).To(BeFalse())
		_, found := FindMetricWithLabelValues("karpenter_voluntary_disruption_queue_dependency_failures_total", map[string]string{})
		Expect(found).To(BeFalse())
		ExpectMetricGaugeValue(disruption.QueueCommandsWaitingOnDependencies, 0, nil)

		// Past the timeout too: the replacement's failure is still not a dependency's.
		cmd = command(&disruption.Replacement{NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct}})
		Expect(queue.StartCommand(ctx, cmd)).To(Succeed())
		ExpectObjectReconciled(ctx, env.Client, queue, nodeClaim1)
		replacement = ExpectExists(ctx, env.Client, &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: cmd.Replacements[0].Name}})
		ExpectDeleted(ctx, env.Client, replacement)
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(replacement))
		env.Clock.Step(11 * time.Minute)
		ExpectObjectReconciled(ctx, env.Client, queue, nodeClaim1)
		Expect(queue.HasAny(nodeClaim1.Status.ProviderID)).To(BeFalse())
		_, found = FindMetricWithLabelValues("karpenter_voluntary_disruption_queue_dependency_failures_total", map[string]string{})
		Expect(found).To(BeFalse())
	})
	It("rolls the command back when a dependency doesn't initialize before the timeout", func() {
		cmd := command()
		Expect(queue.StartCommand(ctx, cmd)).To(Succeed())
		ExpectObjectReconciled(ctx, env.Client, queue, nodeClaim1)
		Expect(queue.HasAny(nodeClaim1.Status.ProviderID)).To(BeTrue())

		env.Clock.Step(11 * time.Minute)
		ExpectObjectReconciled(ctx, env.Client, queue, nodeClaim1)
		Expect(cmd.Succeeded).To(BeFalse())
		expectRolledBack()
		ExpectMetricCounterValue(disruption.QueueDependencyFailuresTotal, 1, map[string]string{"cause": disruption.DependencyFailureCauseTimeout.Name})
	})
	It("doesn't count a legacy command's failure as a dependency failure", func() {
		nct := scheduling.NewNodeClaimTemplate(nodePool)
		nct.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
		cmd := command(&disruption.Replacement{NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct}})
		cmd.Dependencies = nil
		disruption.QueueCommandsWaitingOnDependencies.Reset()
		Expect(queue.StartCommand(ctx, cmd)).To(Succeed())
		ExpectObjectReconciled(ctx, env.Client, queue, nodeClaim1)
		// A queue that never had a command with dependencies doesn't export the gauge.
		_, found := FindMetricWithLabelValues("karpenter_voluntary_disruption_queue_commands_waiting_on_dependencies", map[string]string{})
		Expect(found).To(BeFalse())
		env.Clock.Step(11 * time.Minute)
		ExpectObjectReconciled(ctx, env.Client, queue, nodeClaim1)
		expectRolledBack()
		_, found = FindMetricWithLabelValues("karpenter_voluntary_disruption_queue_dependency_failures_total", map[string]string{})
		Expect(found).To(BeFalse())
	})
})
