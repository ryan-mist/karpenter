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
	"strings"
	"sync/atomic"
	"time"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/samber/lo"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

// evaluationStats accumulates what a method's ComputeCommands does deep in its call tree (scheduling simulations,
// command validation) so the controller can emit it once per evaluation. It travels on the context so the Method
// interface doesn't change; outside an evaluation there are no stats and recording is a no-op. Fields are atomic
// because a method may simulate from more than one goroutine.
type evaluationStats struct {
	simulations atomic.Int64
	validations atomic.Int64
	validating  atomic.Int64 // nanoseconds spent in Validator.Validate
}

type evaluationStatsKey struct{}

func withEvaluationStats(ctx context.Context, s *evaluationStats) context.Context {
	return context.WithValue(ctx, evaluationStatsKey{}, s)
}

func evaluationStatsFromContext(ctx context.Context) *evaluationStats {
	s, _ := ctx.Value(evaluationStatsKey{}).(*evaluationStats)
	return s
}

// recordSimulation counts one scheduling simulation against the current evaluation.
func recordSimulation(ctx context.Context) {
	if s := evaluationStatsFromContext(ctx); s != nil {
		s.simulations.Add(1)
	}
}

// validate runs the validator and attributes its time to the current evaluation's validate phase.
func validate(ctx context.Context, v Validator, cmd Command, validationPeriod time.Duration) (Command, error) {
	if s := evaluationStatsFromContext(ctx); s != nil {
		start := time.Now()
		defer func() {
			s.validations.Add(1)
			s.validating.Add(int64(time.Since(start)))
		}()
	}
	return v.Validate(ctx, cmd, validationPeriod)
}

// methodLabels are the labels that identify a disruption method on the voluntary-disruption metrics.
func methodLabels(m Method) map[string]string {
	return map[string]string{
		metrics.ReasonLabel:    strings.ToLower(string(m.Reason())),
		ConsolidationTypeLabel: m.ConsolidationType(),
	}
}

func observePhase(phase opmetrics.Value, labels map[string]string, d time.Duration) {
	PhaseDurationSeconds.Observe(d.Seconds(), lo.Assign(labels, map[string]string{phaseLabel: phase.Name}))
}
