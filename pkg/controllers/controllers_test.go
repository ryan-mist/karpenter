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

package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/samber/lo"
	clock "k8s.io/utils/clock/testing"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline"
	"sigs.k8s.io/karpenter/pkg/test"
)

func TestDisruptionControllerFollowsTheDisruptionPipelineGate(t *testing.T) {
	for _, tc := range []struct {
		gate bool
		want string
	}{
		{gate: false, want: "disruption"},
		{gate: true, want: "disruption.pipeline"},
	} {
		ctx := test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{DisruptionPipeline: lo.ToPtr(tc.gate)}}).ToContext(context.Background())
		c := newDisruptionController(ctx, clock.NewFakeClock(time.Now()), nil, nil, nil, nil, nil, &disruption.Queue{}, nil)
		if got := c.(interface{ Name() string }).Name(); got != tc.want {
			t.Errorf("DisruptionPipeline=%t: got controller %q, want %q", tc.gate, got, tc.want)
		}
		if _, isPipeline := c.(*pipeline.Controller); isPipeline != tc.gate {
			t.Errorf("DisruptionPipeline=%t: got pipeline controller %t", tc.gate, isPipeline)
		}
	}
}

func TestDisruptionControllerRunsTheShadowWithTheGateOff(t *testing.T) {
	t.Setenv(pipeline.ShadowEnv, "0.5")
	ctx := test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{DisruptionPipeline: lo.ToPtr(false)}}).ToContext(context.Background())
	c := newDisruptionController(ctx, clock.NewFakeClock(time.Now()), nil, nil, nil, nil, nil, &disruption.Queue{}, nil)
	if got := c.(interface{ Name() string }).Name(); got != "disruption" {
		t.Errorf("shadow mode: got controller %q, want the disruption controller", got)
	}
}
