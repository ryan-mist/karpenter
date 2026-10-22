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
	"context"
	"fmt"
	"os"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/singleton"
	. "github.com/onsi/ginkgo/v2"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
)

// pipelineTwin runs this suite as a twin of itself with the DisruptionPipeline gate on: every spec that reconciles the
// suite's disruptionController reconciles the pipeline controller (pipeline.NewController with its default planners and
// lanes, i.e. the gate-on configuration) through its synchronous runner instead of disruption.NewController. Unset,
// the suite runs exactly as on mainline. Test-only, like the other KARPENTER_TEST_* switches.
//
//	KARPENTER_TEST_DISRUPTION_PIPELINE=1 go test ./pkg/controllers/disruption/
//
// Specs that construct their own disruption.NewController (continue_test.go, the scale benchmark, validation_test.go's
// registration specs, ...) or call methods, cores and the Queue directly test the gate-off path and run unchanged in
// both modes. Specs where the pipeline intentionally differs call pipelineDifference with the decision, or branch on
// pipelineTwin to check the pipeline's outcome.
var pipelineTwin = os.Getenv("KARPENTER_TEST_DISRUPTION_PIPELINE") == "1"

// newSuiteDisruptionController is the controller the suite's specs reconcile: the gate-off controller with no-op
// validators, or in the twin run the pipeline controller. The pipeline's planners always re-validate with the methods'
// own validators after the 15s validation period, so the twin steps the fake clock while the commit loop waits for it.
func newSuiteDisruptionController() singleton.Reconciler {
	if pipelineTwin {
		return &syncPipeline{Controller: newPipelineController(), autoStep: true}
	}
	return disruption.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost, disruption.WithMethods(NewMethodsWithNopValidator()...))
}

// newRealValidatorDisruptionController is the controller for specs that step the clock through the validation period
// themselves. The pipeline always validates for real, so the twin doesn't step the clock for them.
func newRealValidatorDisruptionController() singleton.Reconciler {
	if pipelineTwin {
		return &syncPipeline{Controller: newPipelineController()}
	}
	return disruption.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost, disruption.WithMethods(NewMethodsWithRealValidator()...))
}

func newPipelineController() *pipeline.Controller {
	return pipeline.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost)
}

// syncPipeline is the pipeline's synchronous runner as the suite reconciles it. With autoStep, while Reconcile blocks on
// the fake clock (the commit loop waiting for a plan's ValidateAt), it steps the clock by the validation period, as
// the gate-off suite's no-op validators skip that wait.
type syncPipeline struct {
	*pipeline.Controller
	autoStep bool
}

func (s *syncPipeline) Reconcile(ctx context.Context) (reconciler.Result, error) {
	if !s.autoStep {
		return s.Controller.Reconcile(ctx)
	}
	type out struct {
		r   reconciler.Result
		err error
	}
	done := make(chan out, 1)
	go func() {
		defer GinkgoRecover()
		r, err := s.Controller.Reconcile(ctx)
		done <- out{r, err}
	}()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case o := <-done:
			return o.r, o.err
		case <-tick.C:
			if env.Clock.HasWaiters() {
				env.Clock.Step(disruption.CommandValidationDelay)
			}
		}
	}
}

// autoStepping is the twin's pipeline controller, stepping the clock through the validation period.
func autoStepping(r singleton.Reconciler) singleton.Reconciler {
	return &syncPipeline{Controller: r.(*syncPipeline).Controller, autoStep: true}
}

// pipelineDifference skips the spec in the twin run: the pipeline intentionally behaves differently here, per the
// cited design decision (docs/design: BRIEF decision or reconciliation row).
func pipelineDifference(decision, why string) {
	GinkgoHelper()
	if pipelineTwin {
		Skip(fmt.Sprintf("DisruptionPipeline: intended difference (%s): %s", decision, why))
	}
}
