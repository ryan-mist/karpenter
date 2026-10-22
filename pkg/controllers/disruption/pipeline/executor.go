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
	"time"

	"github.com/google/uuid"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
)

// Executor is the hand-off from the lanes to the single commit loop that admits and starts plans.
type Executor interface {
	// Submit reserves the plans in the Ledger before it returns, so every lane plans around them from then on, and
	// queues them for admission. The channel receives exactly one Resolution per plan and is then closed.
	Submit(ctx context.Context, plans ...*Plan) <-chan Resolution
	// Step runs one commit step: it admits the plans that are ready and starts the admitted ones.
	Step(ctx context.Context) CommitStepResult
}

type ResolutionKind int

const (
	// Started: the plan's command started.
	Started ResolutionKind = iota
	// Rejected: the plan was rejected at Reserve or admission.
	Rejected
	// Dropped: the plan waited too long, or the pipeline stopped.
	Dropped
)

// Resolution is how a submitted plan ended.
type Resolution struct {
	PlanID uuid.UUID
	Kind   ResolutionKind
	// Reason is why the plan was rejected or dropped; empty when it started.
	Reason types.RejectReason
	// Replanned reports that the plan started as part of a sibling re-plan.
	Replanned bool
	// AtSubmit reports that the plan was rejected at Submit, before it was reserved.
	AtSubmit bool
}

// CommitStepResult is what one commit step did.
type CommitStepResult struct {
	Admitted, Rejected, Waiting int
	// NextReady is the earliest future ValidateAt of a waiting plan; zero if every waiting plan waits for a gate.
	NextReady time.Time
	Swept     bool
}
