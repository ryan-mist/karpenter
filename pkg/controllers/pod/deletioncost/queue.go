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

package deletioncost

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	utilscontroller "sigs.k8s.io/karpenter/pkg/utils/controller"
)

const (
	queueBaseDelay = 100 * time.Millisecond
	queueMaxDelay  = 10 * time.Second
	// Concurrency parity with the eviction queue.
	minReconciles = 100
	maxReconciles = 5000
)

type queueItem struct {
	rank  int
	clear bool
}

// Fire-and-forget, modeled after terminator.Queue.
type Queue struct {
	sync.Mutex

	source     chan event.TypedGenericEvent[*corev1.Pod]
	items      map[terminator.QueueKey]queueItem
	kubeClient client.Client
}

func NewQueue(kubeClient client.Client) *Queue {
	return &Queue{
		source:     make(chan event.TypedGenericEvent[*corev1.Pod], 10000),
		items:      map[terminator.QueueKey]queueItem{},
		kubeClient: kubeClient,
	}
}

func (q *Queue) Name() string {
	return "pod.deletioncost.queue"
}

func (q *Queue) Register(ctx context.Context, m manager.Manager) error {
	maxConcurrentReconciles := utilscontroller.LinearScaleReconciles(utilscontroller.CPUCount(ctx), minReconciles, maxReconciles)
	qps, bucketSize := utilscontroller.GetTypedBucketConfigs(100, minReconciles, maxConcurrentReconciles)
	logger := m.GetLogger().WithValues("controller", q.Name())
	// Requests are keyed by QueueKey (name and UID) so a reconcile can drop the exact entry whose pod is gone.
	return builder.TypedControllerManagedBy[terminator.QueueKey](m).
		Named(q.Name()).
		WatchesRawSource(source.TypedChannel(q.source, handler.TypedFuncs[*corev1.Pod, terminator.QueueKey]{
			GenericFunc: func(_ context.Context, e event.TypedGenericEvent[*corev1.Pod], queue workqueue.TypedRateLimitingInterface[terminator.QueueKey]) {
				queue.Add(terminator.NewQueueKey(e.Object))
			},
		})).
		WithOptions(controller.TypedOptions[terminator.QueueKey]{
			RateLimiter: workqueue.NewTypedMaxOfRateLimiter[terminator.QueueKey](
				workqueue.NewTypedItemExponentialFailureRateLimiter[terminator.QueueKey](queueBaseDelay, queueMaxDelay),
				&workqueue.TypedBucketRateLimiter[terminator.QueueKey]{Limiter: rate.NewLimiter(rate.Limit(qps), bucketSize)},
			),
			MaxConcurrentReconciles: maxConcurrentReconciles,
		}).
		WithLogConstructor(func(key *terminator.QueueKey) logr.Logger {
			if key == nil {
				return logger
			}
			return logger.WithValues("namespace", key.Namespace, "name", key.Name)
		}).
		Complete(reconcile.TypedFunc[terminator.QueueKey](q.reconcileKey))
}

// Add is last-writer-wins on the desired state, and pushes to the channel only
// on first insertion so repeated Adds for one pod do not fan out.
func (q *Queue) Add(pod *corev1.Pod, rank int, clear bool) {
	q.Lock()
	defer q.Unlock()

	qk := terminator.NewQueueKey(pod)
	_, enqueued := q.items[qk]
	q.items[qk] = queueItem{rank: rank, clear: clear}
	if !enqueued {
		q.source <- event.TypedGenericEvent[*corev1.Pod]{Object: pod}
	}
}

func (q *Queue) Has(pod *corev1.Pod) bool {
	q.Lock()
	defer q.Unlock()
	_, ok := q.items[terminator.NewQueueKey(pod)]
	return ok
}

func (q *Queue) complete(qk terminator.QueueKey) {
	q.Lock()
	defer q.Unlock()
	delete(q.items, qk)
}

// reconcileKey resolves a queue entry to the pod it was enqueued for. If that pod no longer exists, or its name now
// belongs to a different pod, nothing will enqueue the entry again, so it is dropped here. Only the exact key being
// reconciled is removed, so a same-name replacement that is itself queued is never affected.
func (q *Queue) reconcileKey(ctx context.Context, key terminator.QueueKey) (reconcile.Result, error) {
	pod := &corev1.Pod{}
	if err := q.kubeClient.Get(ctx, key.NamespacedName, pod); err != nil {
		if apierrors.IsNotFound(err) {
			q.skipNotFound(ctx, key)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	if pod.UID != key.UID {
		q.skipNotFound(ctx, key)
		return reconcile.Result{}, nil
	}
	return q.Reconcile(ctx, pod)
}

func (q *Queue) skipNotFound(ctx context.Context, qk terminator.QueueKey) {
	log.FromContext(ctx).V(1).WithValues("pod", klog.KRef(qk.Namespace, qk.Name)).Info("skipping pod annotation update, target not found")
	podAnnotationWritesTotal.Inc(map[string]string{resultLabel: ResultSkippedNotFound.Name})
	q.complete(qk)
}

func (q *Queue) Reconcile(ctx context.Context, pod *corev1.Pod) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, q.Name())

	qk := terminator.NewQueueKey(pod)
	q.Lock()
	item, ok := q.items[qk]
	q.Unlock()
	if !ok {
		// The enqueued pod was replaced at the same name before we got here.
		return reconcile.Result{}, nil
	}

	if q.matchesDesired(pod, item) {
		q.complete(qk)
		podAnnotationWritesTotal.Inc(map[string]string{resultLabel: ResultSkippedUnchanged.Name})
		return reconcile.Result{}, nil
	}

	var err error
	if item.clear {
		err = clearAnnotation(ctx, q.kubeClient, pod)
	} else {
		err = patchAnnotation(ctx, q.kubeClient, pod, strconv.Itoa(item.rank))
	}
	if err == nil {
		podAnnotationWritesTotal.Inc(map[string]string{resultLabel: ResultUpdated.Name})
		q.complete(qk)
		return reconcile.Result{}, nil
	}
	if apierrors.IsNotFound(err) {
		q.skipNotFound(ctx, qk)
		return reconcile.Result{}, nil
	}
	if apierrors.IsConflict(err) {
		log.FromContext(ctx).V(1).WithValues("pod", klog.KObj(pod)).Info("skipping pod annotation update, write raced")
		podAnnotationWritesTotal.Inc(map[string]string{resultLabel: ResultSkippedConflict.Name})
		q.complete(qk)
		return reconcile.Result{}, nil
	}
	podAnnotationWritesTotal.Inc(map[string]string{resultLabel: ResultError.Name})
	return reconcile.Result{}, err
}

func (q *Queue) matchesDesired(pod *corev1.Pod, item queueItem) bool {
	return podHasDesiredAnnotation(pod, item.rank, item.clear)
}
