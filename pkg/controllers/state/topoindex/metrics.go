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

	causeLabel         = "cause"
	mismatchCounts     = "counts"
	mismatchInverse    = "inverse"
	mismatchRace       = "race"
	schedulerSubsystem = "scheduler"

	resultHit        = "hit"
	resultRegistered = "registered"
	resultCollision  = "collision"

	stateLabel   = "state"
	stateCounted = "counted"
	stateIgnored = "ignored"
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

	podState = opmetrics.Label{
		Name: stateLabel,
		Help: "Whether topology counts the pod.",
		Values: []opmetrics.Value{
			{Name: stateCounted, Help: "The pod is scheduled, and not terminal or terminating."},
			{Name: stateIgnored, Help: "The pod is pending, terminal or terminating."},
		},
	}

	Pods = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: subsystem,
			Name:      "topology_index_pods",
			Help:      "Number of pods the topology index holds facts for. Labeled by whether topology counts them.",
		},
		[]opmetrics.Label{podState},
		opmetrics.Alpha,
	)
	LabelSets = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: subsystem,
			Name:      "topology_index_label_sets",
			Help:      "Number of distinct (namespace, labels) sets of the pods the topology index holds.",
		},
		[]opmetrics.Label{},
		opmetrics.Alpha,
	)
	RegisteredGroups = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: subsystem,
			Name:      "topology_index_registered_groups",
			Help:      "Number of groups whose counts the topology index maintains.",
		},
		[]opmetrics.Label{},
		opmetrics.Alpha,
	)
	Generation = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: subsystem,
			Name:      "topology_index_generation",
			Help:      "Generation of the last published topology index snapshot.",
		},
		[]opmetrics.Label{},
		opmetrics.Alpha,
	)
	GroupRegistrationsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: subsystem,
			Name:      "topology_index_group_registrations_total",
			Help:      "Number of groups registered in the topology index.",
		},
		[]opmetrics.Label{},
		opmetrics.Alpha,
	)
	GroupEvictionsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: subsystem,
			Name:      "topology_index_group_evictions_total",
			Help:      "Number of groups the topology index stopped maintaining because they weren't read for a while or the group limit was reached.",
		},
		[]opmetrics.Label{},
		opmetrics.Alpha,
	)
	RegistrationDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: subsystem,
			Name:      "topology_index_registration_duration_seconds",
			Help:      "Duration of the scan that computes a group's counts when it's first registered.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]opmetrics.Label{},
		opmetrics.Alpha,
	)

	mismatchCause = opmetrics.Label{
		Name: causeLabel,
		Help: "What differed between the topology index and listing pods.",
		Values: []opmetrics.Value{
			{Name: mismatchCounts, Help: "A topology group's counts of existing pods, net of the pods the simulation schedules."},
			{Name: mismatchInverse, Help: "The required anti-affinity terms of existing pods, their owners or their counts."},
			{Name: mismatchRace, Help: "Either of the above, but the index and listing agreed when compared again at about the same moment: the index trailed the informer cache. Doesn't indicate a bug."},
		},
	}
	DifferentialChecksTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "topology_index_differential_checks_total",
			Help:      "Number of scheduling simulations that compared the topology index with listing pods (--topology-index-differential-sample-rate).",
		},
		[]opmetrics.Label{},
		opmetrics.Alpha,
	)
	DifferentialMismatchesTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "topology_index_differential_mismatches_total",
			Help:      "Number of differences between the topology index and listing pods found in compared simulations. Labeled by what differed; cause=race differences went away when compared again, so only the other causes indicate a bug.",
		},
		[]opmetrics.Label{mismatchCause},
		opmetrics.Alpha,
	)
	mismatchLabels = map[string]map[string]string{
		mismatchCounts:  {causeLabel: mismatchCounts},
		mismatchInverse: {causeLabel: mismatchInverse},
		mismatchRace:    {causeLabel: mismatchRace},
	}
	lookupResult = opmetrics.Label{
		Name: resultLabel,
		Help: "How a scheduling simulation's topology group was answered.",
		Values: []opmetrics.Value{
			{Name: resultHit, Help: "The group was registered in the snapshot the simulation took."},
			{Name: resultRegistered, Help: "The group was registered on first use, scanning the pods once."},
			{Name: resultCollision, Help: "Another node filter with the same hash was registered, so the pods were listed instead."},
		},
	}
	LookupsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: subsystem,
			Name:      "topology_index_lookups_total",
			Help:      "Number of topology group counts that scheduling simulations read from the topology index. Labeled by how they were answered.",
		},
		[]opmetrics.Label{lookupResult},
		opmetrics.Alpha,
	)
	lookupLabels = map[string]map[string]string{
		resultHit:        {resultLabel: resultHit},
		resultRegistered: {resultLabel: resultRegistered},
		resultCollision:  {resultLabel: resultCollision},
	}

	eventLabels   = map[string][2]map[string]string{}
	countedLabels = map[string]string{stateLabel: stateCounted}
	ignoredLabels = map[string]string{stateLabel: stateIgnored}
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

func (ix *Index) recordMetricsLocked() {
	Pods.Set(float64(ix.countable), countedLabels)
	Pods.Set(float64(len(ix.pods)-ix.countable), ignoredLabels)
	LabelSets.Set(float64(ix.sets.Len()), nil)
	RegisteredGroups.Set(float64(len(ix.groups)), nil)
	Generation.Set(float64(ix.gen), nil)
}

func lookupsTotal(result string) {
	LookupsTotal.Inc(lookupLabels[result])
}
