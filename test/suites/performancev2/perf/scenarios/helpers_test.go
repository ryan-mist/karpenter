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

package scenarios

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/kwok/apis/v1alpha1"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/test/suites/performancev2/env"
	"sigs.k8s.io/karpenter/test/suites/performancev2/perf"
)

// The core provider's NodeClass is unstructured, so a type assertion alone silently turned the KWOK-only parts off.
func TestIsKWOK(t *testing.T) {
	for name, tc := range map[string]struct {
		run  *perf.Run
		want bool
	}{
		"unstructured KWOKNodeClass": {&perf.Run{NodeClass: env.KWOKNodeClass()}, true},
		"typed KWOKNodeClass":        {&perf.Run{NodeClass: &v1alpha1.KWOKNodeClass{}}, true},
		"another NodeClass":          {&perf.Run{NodeClass: test.NodeClass()}, false},
	} {
		if got := isKWOK(tc.run); got != tc.want {
			t.Errorf("%s: isKWOK = %v, want %v", name, got, tc.want)
		}
	}
}

func TestFrac(t *testing.T) {
	for _, tc := range []struct{ n, num, den, want int }{{1000, 35, 1000, 35}, {100, 35, 1000, 3}, {10, 1, 11, 1}, {0, 1, 2, 1}} {
		if got := frac(tc.n, tc.num, tc.den); got != tc.want {
			t.Errorf("frac(%d, %d, %d) = %d, want %d", tc.n, tc.num, tc.den, got, tc.want)
		}
	}
}

func TestObjects(t *testing.T) {
	if got := objects([]*corev1.Pod{{}, {}}); len(got) != 2 {
		t.Errorf("objects returned %d objects, want 2", len(got))
	}
}
