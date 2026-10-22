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

package pipeline

import (
	"context"

	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
)

// laneRunner runs one lane's rounds in its own goroutine, paced by the lane's trigger. It runs only on the leader.
type laneRunner struct {
	lane    Lane
	trigger Trigger
	clock   clock.Clock
}

func (r *laneRunner) NeedLeaderElection() bool { return true }

func (r *laneRunner) Start(ctx context.Context) error {
	ctx = injection.WithControllerName(ctx, "disruption.pipeline")
	var woke types.WakeSet
	for {
		last := r.lane.Step(ctx)
		if last.Err != nil && ctx.Err() == nil {
			log.FromContext(ctx).Error(last.Err, "disruption pipeline round failed", "lane", last.Lane)
		}
		woke = 0
		for {
			wait := r.trigger.Pace(last, woke) - r.clock.Since(last.EndedAt)
			if wait <= 0 {
				break
			}
			select {
			case <-ctx.Done():
				return nil
			case <-r.clock.After(wait):
			case w := <-r.trigger.Wake():
				woke |= w
				continue
			}
			break
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

// Readier is implemented by an Executor that can signal that it has work (a plan was submitted).
type Readier interface {
	Ready() <-chan struct{}
}

// commitRunner runs the commit loop in its own goroutine. It steps when a plan is submitted, when a waiting plan's
// ValidateAt comes, every second while plans wait for a gate, and every SweepTick for the sweep. When the lease is lost
// it drops the waiting plans. It runs only on the leader.
type commitRunner struct {
	executor Executor
	clock    clock.Clock
}

func (r *commitRunner) NeedLeaderElection() bool { return true }

func (r *commitRunner) Start(ctx context.Context) error {
	ctx = injection.WithControllerName(ctx, "disruption.pipeline")
	var ready <-chan struct{}
	if rd, ok := r.executor.(Readier); ok {
		ready = rd.Ready()
	}
	// However the loop ends (the lease is lost, possibly during a step), drop the waiting plans: their reservations
	// are in memory only. A step with a cancelled context drops instead of admitting.
	defer r.executor.Step(ctx)
	for {
		res := r.executor.Step(ctx)
		if ctx.Err() != nil {
			return nil
		}
		wait := SweepTick
		if res.Waiting > 0 {
			wait = gatePoll
			if !res.NextReady.IsZero() {
				wait = min(wait, max(res.NextReady.Sub(r.clock.Now()), 0))
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-r.clock.After(wait):
		case <-ready:
		}
	}
}
