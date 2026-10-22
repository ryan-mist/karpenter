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

package disruption

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
)

// This file holds what the disruption pipeline (package pipeline, behind the DisruptionPipeline feature gate) reads
// from this package. Nothing here is used by the disruption controller.

// ReschedulablePods are the candidate's pods that would be rescheduled if it were disrupted. They are the informer
// cache's pods and must not be modified.
func (c *Candidate) ReschedulablePods() []*corev1.Pod {
	return c.reschedulablePods
}

// NewCandidateSnapshotFromNodes is NewCandidateSnapshot with the state-node deep copy supplied by the caller.
func NewCandidateSnapshotFromNodes(ctx context.Context, deepCopyNodes func() state.StateNodes, kubeClient client.Client, recorder events.Recorder,
	clk clock.Clock, cloudProvider cloudprovider.CloudProvider, queue *Queue,
) (*CandidateSnapshot, error) {
	return newCandidateSnapshot(ctx, deepCopyNodes, kubeClient, recorder, clk, cloudProvider, queue)
}

// Now is the snapshot's fixed now, at which every candidate is evaluated.
func (s *CandidateSnapshot) Now() time.Time {
	return s.clk.Now()
}

// Nodes is the snapshot's deep copy of the state nodes. It must not be modified.
func (s *CandidateSnapshot) Nodes() state.StateNodes {
	return s.nodes
}

// NodePools are the managed NodePools by name, read when the snapshot was built. They must not be modified.
func (s *CandidateSnapshot) NodePools() map[string]*v1.NodePool {
	return s.nodePoolMap
}

// InstanceTypes are each NodePool's instance types by name. A NodePool whose instance types couldn't be listed is
// missing. They must not be modified.
func (s *CandidateSnapshot) InstanceTypes() map[string]map[string]*cloudprovider.InstanceType {
	return s.nodePoolToInstanceTypesMap
}

// PDBs are the PodDisruptionBudgets read when the snapshot was built.
func (s *CandidateSnapshot) PDBs() pdb.Limits {
	return s.pdbs
}

// Explain returns why the snapshot node with providerID is not a candidate for disruptionClass (nil if it is one), and
// whether the snapshot has the node. It publishes the events NewCandidate would.
func (s *CandidateSnapshot) Explain(ctx context.Context, providerID string, disruptionClass string) (error, bool) {
	for i, n := range s.nodes {
		if n.ProviderID() == providerID {
			_, err := s.candidate(ctx, i, disruptionClass)
			return err, true
		}
	}
	return nil, false
}

// InFlightDestinations are the existing nodes (providerIDs) that the queue's commands are moving pods onto.
func (q *Queue) InFlightDestinations() sets.Set[string] {
	q.RLock()
	defer q.RUnlock()
	out := sets.New[string]()
	for _, cmd := range q.ProviderIDToCommand {
		for _, n := range cmd.Results.ExistingNodes {
			if len(n.Pods) > 0 {
				out.Insert(n.ProviderID())
			}
		}
	}
	return out
}
