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

package topoindex

import (
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

const (
	subsystem = "cluster_state"

	kindLabel   = "kind"
	resultLabel = "result"

	kindPod       = "pod"
	kindNode      = "node"
	kindNamespace = "namespace"

	resultNoop = "noop"
	resultDiff = "diff"
)

var (
	eventKind = opmetrics.Label{
		Name: kindLabel,
		Help: "The kind of object the informer event was for.",
		Values: []opmetrics.Value{
			{Name: kindPod, Help: "A Pod event."},
			{Name: kindNode, Help: "A Node event."},
			{Name: kindNamespace, Help: "A Namespace event."},
		},
	}
	eventResult = opmetrics.Label{
		Name: resultLabel,
		Help: "Whether the event changed a fact the topology index holds.",
		Values: []opmetrics.Value{
			{Name: resultNoop, Help: "Nothing topology reads changed, e.g. a resync or a status update."},
			{Name: resultDiff, Help: "A fact changed and the index generation was bumped."},
		},
	}

	EventsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: subsystem,
			Name:      "topology_index_events_total",
			Help:      "Number of informer events applied to the topology index. Labeled by object kind and whether the event changed a fact.",
		},
		[]opmetrics.Label{eventKind, eventResult},
		opmetrics.Alpha,
	)

	eventLabels = map[string][2]map[string]string{}
)

func init() {
	for _, kind := range []string{kindPod, kindNode, kindNamespace} {
		eventLabels[kind] = [2]map[string]string{
			{kindLabel: kind, resultLabel: resultNoop},
			{kindLabel: kind, resultLabel: resultDiff},
		}
	}
}

func eventsTotal(kind string, changed bool) {
	labels := eventLabels[kind][0]
	if changed {
		labels = eventLabels[kind][1]
	}
	EventsTotal.Inc(labels)
}
