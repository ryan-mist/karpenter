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

// Package pipeline is the disruption controller behind the DisruptionPipeline feature gate (alpha, default off). It
// replaces the disruption package's "first method wins" loop with Observe → Plan → Admit & Execute: lanes of
// side-effect-free planners plan on one snapshot per round, and a single commit loop admits their plans against live
// state and starts them through the shared disruption.Queue.
//
// With the gate off this package is not constructed, and the disruption package's controller runs as on mainline.
// The package imports disruption; disruption never imports it.
package pipeline

import (
	"context"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/singleton"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/state/cost"
)

// pollingPeriod is how long the controller waits after a round that admitted nothing, as the disruption controller's
// own polling period.
const pollingPeriod = 10 * time.Second

// Controller is the pipeline disruption controller. It shares the disruption.Queue with the disruption package, so
// started commands are executed exactly as the disruption controller's are.
type Controller struct {
	clock         clock.Clock
	kubeClient    client.Client
	cluster       *state.Cluster
	provisioner   *provisioning.Provisioner
	cloudProvider cloudprovider.CloudProvider
	recorder      events.Recorder
	queue         *disruption.Queue
	clusterCost   *cost.ClusterCost
}

func NewController(_ context.Context, clk clock.Clock, kubeClient client.Client, provisioner *provisioning.Provisioner,
	cp cloudprovider.CloudProvider, recorder events.Recorder, cluster *state.Cluster, queue *disruption.Queue, clusterCost *cost.ClusterCost,
) *Controller {
	return &Controller{
		clock:         clk,
		kubeClient:    kubeClient,
		cluster:       cluster,
		provisioner:   provisioner,
		cloudProvider: cp,
		recorder:      recorder,
		queue:         queue,
		clusterCost:   clusterCost,
	}
}

func (c *Controller) Name() string {
	return "disruption.pipeline"
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("disruption.pipeline").
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}

// Reconcile runs one round. The pipeline has no lanes yet, so a round does nothing.
func (c *Controller) Reconcile(_ context.Context) (reconciler.Result, error) {
	return reconciler.Result{RequeueAfter: pollingPeriod}, nil
}
