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
	"sync"
	"time"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
)

// WakeHub delivers semantic wake-ups to lanes: a budget slot freed (a reservation released or a command completed),
// cluster state synced, one of the lane's plans resolved. It never delivers raw informer events. Each lane's channel
// coalesces: a lane that hasn't read its wake-ups yet gets them merged into one.
type WakeHub struct {
	mu    sync.Mutex
	lanes map[types.LaneID]*wakeChan
}

type wakeChan struct {
	mu sync.Mutex
	ch chan types.WakeSet
}

func NewWakeHub() *WakeHub {
	return &WakeHub{lanes: map[types.LaneID]*wakeChan{}}
}

// Subscribe returns the lane's wake channel.
func (h *WakeHub) Subscribe(lane types.LaneID) <-chan types.WakeSet {
	return h.lane(lane).ch
}

func (h *WakeHub) lane(lane types.LaneID) *wakeChan {
	h.mu.Lock()
	defer h.mu.Unlock()
	w, ok := h.lanes[lane]
	if !ok {
		w = &wakeChan{ch: make(chan types.WakeSet, 1)}
		h.lanes[lane] = w
	}
	return w
}

// Publish wakes one lane, or every lane if lane is "".
func (h *WakeHub) Publish(lane types.LaneID, w types.WakeSet) {
	h.mu.Lock()
	targets := make([]*wakeChan, 0, len(h.lanes))
	for id, c := range h.lanes {
		if lane == "" || id == lane {
			targets = append(targets, c)
		}
	}
	h.mu.Unlock()
	for _, c := range targets {
		c.publish(w)
	}
}

func (c *wakeChan) publish(w types.WakeSet) {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case old := <-c.ch:
		w |= old
	default:
	}
	c.ch <- w
}

// wakeObserver publishes the wake-ups the pipeline itself sees: a plan's resolution wakes its lane, and cluster state
// becoming synced (seen by the commit loop) wakes every lane.
type wakeObserver struct {
	NopObserver
	hub    *WakeHub
	laneOf map[string]types.LaneID

	mu     sync.Mutex
	synced bool
}

func newWakeObserver(hub *WakeHub, lanes ...LaneSpec) *wakeObserver {
	o := &wakeObserver{hub: hub, laneOf: map[string]types.LaneID{}, synced: true}
	for _, l := range lanes {
		for _, p := range l.Planners {
			o.laneOf[p.Name()] = l.ID
		}
	}
	return o
}

func (o *wakeObserver) Resolved(p *Plan, r Resolution, _ time.Duration) {
	// A plan rejected at Submit held nothing, and its lane sees the rejection at its next round. Waking on it would
	// re-plan the same rejection right away, in a loop.
	if r.AtSubmit {
		return
	}
	if lane, ok := o.laneOf[p.Planner]; ok {
		o.hub.Publish(lane, types.WakeResolution)
	}
	// A reserved plan that was rejected or dropped frees its slot for every lane.
	if r.Kind != Started {
		o.hub.Publish("", types.WakeLedgerRelease)
	}
}

func (o *wakeObserver) CommitStepFinished(_ CommitStepResult, synced bool, _ time.Duration) {
	o.mu.Lock()
	edge := synced && !o.synced
	o.synced = synced
	o.mu.Unlock()
	if edge {
		o.hub.Publish("", types.WakeSynced)
	}
}

// wakeTrigger is a PollTrigger that a wake-up can cut short: the Fast lane runs right away when a slot freed, cluster
// state synced or one of its plans resolved; the Background lane runs after its duty cycle when a slot freed or one of
// its plans resolved. Without a wake-up it paces like the PollTrigger (there are no Drifted or Consolidatable wake-ups
// yet, so the idle pace stays the polling period).
type wakeTrigger struct {
	PollTrigger
	wake <-chan types.WakeSet
}

func (t wakeTrigger) Wake() <-chan types.WakeSet { return t.wake }

func (t wakeTrigger) Pace(last StepResult, woke types.WakeSet) time.Duration {
	poll := t.PollTrigger.Pace(last, 0)
	if t.Background {
		if woke&(types.WakeLedgerRelease|types.WakeResolution) != 0 {
			return min(poll, max(minPace, dutyFactor*last.Took))
		}
		return poll
	}
	if woke&(types.WakeLedgerRelease|types.WakeSynced|types.WakeResolution) != 0 {
		return 0
	}
	return poll
}
