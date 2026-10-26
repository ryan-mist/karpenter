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
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/samber/lo"
	"k8s.io/utils/clock"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

// Internal constants of drift fairness. None of them is a user setting.
const (
	// minPoolBackoff and maxPoolBackoff bound how long a NodePool is skipped after one of its commands failed; the
	// backoff doubles with each consecutive failure.
	minPoolBackoff = time.Minute
	maxPoolBackoff = 10 * time.Minute
	// backoffJitter is the fraction a backoff is randomly lengthened or shortened by.
	backoffJitter = 0.1
)

// poolFairness orders a planner's candidates round-robin across NodePools, least recently served first, and skips a
// NodePool for a while after one of its commands failed, so a NodePool whose replacements can't launch (an ICE'd
// instance type) doesn't hold up the others (k#3080, k#2927). A NodePool is served when one of its plans starts.
type poolFairness struct {
	clock  clock.Clock
	jitter func() float64

	mu      sync.Mutex
	served  map[string]time.Time
	backoff map[string]poolBackoff
}

type poolBackoff struct {
	until    time.Time
	failures int
}

func newPoolFairness(clk clock.Clock) *poolFairness {
	return &poolFairness{clock: clk, jitter: rand.Float64, served: map[string]time.Time{}, backoff: map[string]poolBackoff{}}
}

// eligible drops the candidates of NodePools that are backing off.
func (f *poolFairness) eligible(cs []*disruption.Candidate) []*disruption.Candidate {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.clock.Now()
	return lo.Filter(cs, func(c *disruption.Candidate, _ int) bool { return !f.backoff[c.NodePool.Name].until.After(now) })
}

// order orders each NodePool's candidates with within, then interleaves the NodePools, least recently served first.
func (f *poolFairness) order(within func([]*disruption.Candidate)) func([]*disruption.Candidate) {
	return func(cs []*disruption.Candidate) {
		within(cs)
		byPool := lo.GroupBy(cs, func(c *disruption.Candidate) string { return c.NodePool.Name })
		pools := lo.Keys(byPool)
		f.mu.Lock()
		sort.Slice(pools, func(i, j int) bool {
			si, sj := f.served[pools[i]], f.served[pools[j]]
			if !si.Equal(sj) {
				return si.Before(sj)
			}
			return pools[i] < pools[j]
		})
		f.mu.Unlock()
		out := cs[:0:0]
		for len(out) < len(cs) {
			for _, np := range pools {
				if len(byPool[np]) > 0 {
					out = append(out, byPool[np][0])
					byPool[np] = byPool[np][1:]
				}
			}
		}
		copy(cs, out)
	}
}

// started records that a plan of these NodePools started.
func (f *poolFairness) started(pools ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.clock.Now()
	for _, np := range pools {
		f.served[np] = now
	}
}

// finished records how a command of these NodePools ended: a failure backs them off, a success resets their backoff.
func (f *poolFairness) finished(succeeded bool, pools ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, np := range pools {
		if succeeded {
			delete(f.backoff, np)
			continue
		}
		b := f.backoff[np]
		b.failures++
		d := min(maxPoolBackoff, minPoolBackoff<<min(b.failures-1, 10))
		d = time.Duration(float64(d) * (1 + backoffJitter*(2*f.jitter()-1)))
		b.until = f.clock.Now().Add(d)
		f.backoff[np] = b
	}
}

func nodePoolsOf(cs []*disruption.Candidate) []string {
	return lo.Uniq(lo.Map(cs, func(c *disruption.Candidate, _ int) string { return c.NodePool.Name }))
}

// stateDedup skips a consolidation method while cluster state hasn't changed since it last found nothing to do, as
// the disruption controller's consolidation state does (state.Cluster.ConsolidationState, which also changes at least
// every 5 minutes).
type stateDedup struct {
	cluster *state.Cluster

	mu sync.Mutex
	at time.Time
}

func (d *stateDedup) IsConsolidated() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return !d.at.IsZero() && d.at.Equal(d.cluster.ConsolidationState())
}

func (d *stateDedup) MarkConsolidated() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.at = d.cluster.ConsolidationState()
}
