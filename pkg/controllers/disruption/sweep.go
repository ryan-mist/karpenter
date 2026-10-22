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
	"fmt"
	"sync"

	"github.com/awslabs/operatorpkg/serrors"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
)

// MarkSweeper removes the karpenter.sh/disruption:NoSchedule taint and the DisruptionReason condition that a
// disruption action left on nodes it no longer owns, e.g. because Karpenter restarted or a command failed to start
// after it marked its candidates.
//
// A node is read from the API server only if cluster state shows a mark to remove, or if this process may have marked
// it without cluster state having observed that yet (see Recheck). For every other node the Get would find nothing to
// write unless the API server is ahead of cluster state. Cluster state is built from the same informer cache that
// kubeClient reads from, so the only such case is a write by another actor that cluster state hasn't observed yet,
// which the Get could equally have missed had the write landed a moment later. That mark is removed by the first sweep
// after cluster state observes it.
type MarkSweeper struct {
	kubeClient client.Client
	cluster    *state.Cluster
	clock      clock.Clock

	mu      sync.Mutex
	recheck sets.Set[string]
}

func NewMarkSweeper(kubeClient client.Client, cluster *state.Cluster, clk clock.Clock) *MarkSweeper {
	return &MarkSweeper{kubeClient: kubeClient, cluster: cluster, clock: clk, recheck: sets.New[string]()}
}

// Recheck makes sweeps read the marks of these nodes from the API server whatever cluster state shows, until a sweep
// has done so for a node that keep rejects, or the node has left cluster state. Call it with the candidates of every
// command before starting it: StartCommand marks candidates through the API server, and cluster state only sees the
// marks once the informer delivers them, so marks left by a command that fails after marking would otherwise be swept
// only once cluster state catches up.
func (s *MarkSweeper) Recheck(providerIDs ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recheck.Insert(providerIDs...)
}

// Sweep removes the marks from every node in cluster state that keep rejects. keep is evaluated on deep copies,
// outside the cluster state lock. Today it keeps nodes that are in the queue, MarkedForDeletion or rebooting.
// Sweep must not run concurrently with StartCommand, which marks nodes before the queue owns them.
func (s *MarkSweeper) Sweep(ctx context.Context, keep func(*state.StateNode) bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	type marked struct {
		node             *state.StateNode
		taint, condition bool
		recheck          bool
	}
	var candidates []marked
	inState := sets.New[string]()
	for n := range s.cluster.Nodes() {
		recheck := s.recheck.Has(n.ProviderID())
		if recheck {
			inState.Insert(n.ProviderID())
		}
		m := marked{recheck: recheck,
			taint:     recheck || n.NoScheduleTaintRemovalNeeded(),
			condition: recheck || n.NodeClaimConditionClearNeeded(v1.ConditionTypeDisruptionReason),
		}
		if m.taint || m.condition {
			m.node = n.DeepCopy()
			candidates = append(candidates, m)
		}
	}
	// keep runs outside the cluster state lock: it may take other locks (the queue's).
	var taintNodes, conditionNodes []*state.StateNode
	var swept []string
	for _, m := range candidates {
		if keep(m.node) {
			continue
		}
		if m.recheck {
			swept = append(swept, m.node.ProviderID())
		}
		if m.taint {
			taintNodes = append(taintNodes, m.node)
		}
		if m.condition {
			conditionNodes = append(conditionNodes, m.node)
		}
	}
	s.recheck = s.recheck.Intersection(inState)
	if err := state.RequireNoScheduleTaint(ctx, s.kubeClient, false, taintNodes...); err != nil {
		return serrors.Wrap(fmt.Errorf("removing taint from nodes, %w", err), "taint", pretty.Taint(v1.DisruptedNoScheduleTaint))
	}
	if err := state.ClearNodeClaimsCondition(ctx, s.kubeClient, s.clock, v1.ConditionTypeDisruptionReason, conditionNodes...); err != nil {
		return serrors.Wrap(fmt.Errorf("removing condition from nodeclaims, %w", err), "condition", v1.ConditionTypeDisruptionReason)
	}
	s.recheck.Delete(swept...)
	return nil
}

// keepDisruptionMarks returns true for nodes whose disruption marks are owned by an in-flight action: queued
// commands, nodes MarkedForDeletion, and reboots (which bypass the queue but keep their DisruptionReason during drain).
func (c *Controller) keepDisruptionMarks(n *state.StateNode) bool {
	return c.queue.HasAny(n.ProviderID()) || n.MarkedForDeletion() || n.RebootInProgress()
}

func candidateProviderIDs(cmds []Command) []string {
	return lo.FlatMap(cmds, func(cmd Command, _ int) []string {
		return lo.Map(cmd.Candidates, func(c *Candidate, _ int) string { return c.ProviderID() })
	})
}
