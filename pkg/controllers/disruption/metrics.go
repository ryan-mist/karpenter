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
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

const (
	voluntaryDisruptionSubsystem = "voluntary_disruption"
	decisionLabel                = "decision"
	ConsolidationTypeLabel       = "consolidation_type"
	CandidatesIneligible         = "candidates_ineligible"
	policyLabel                  = "policy"
	terminateFirstReasonLabel    = "terminate_first_reason"
	phaseLabel                   = "phase"
)

// Phase names for the `phase` dimension of PhaseDurationSeconds.
const (
	candidatesPhase = "candidates"
	budgetsPhase    = "budgets"
	computePhase    = "compute"
	validatePhase   = "validate"
	startPhase      = "start"
)

var (
	MultiNodeConsolidationType = opmetrics.Value{
		Name: "multi",
		Help: "Consolidation that considers removing multiple nodes at once.",
	}
	SingleNodeConsolidationType = opmetrics.Value{
		Name: "single",
		Help: "Consolidation that considers removing a single node.",
	}
	EmptyConsolidationType = opmetrics.Value{
		Name: "empty",
		Help: "Consolidation that removes empty nodes.",
	}
)

var (
	CandidatesPhase = opmetrics.Value{
		Name: candidatesPhase,
		Help: "Building the method's disruption candidates from cluster state.",
	}
	BudgetsPhase = opmetrics.Value{
		Name: budgetsPhase,
		Help: "Building the per-NodePool disruption budget mapping for the method's reason.",
	}
	ComputePhase = opmetrics.Value{
		Name: computePhase,
		Help: "Computing the method's commands, including scheduling simulations and excluding validation.",
	}
	ValidatePhase = opmetrics.Value{
		Name: validatePhase,
		Help: "Validating a computed command: the validation delay plus the candidate and scheduling re-check.",
	}
	StartPhase = opmetrics.Value{
		Name: startPhase,
		Help: "Handing the method's commands to the orchestration queue.",
	}
)

var (
	// Phase is the `phase` dimension of PhaseDurationSeconds: the step of a single method evaluation.
	Phase = opmetrics.Label{
		Name:   phaseLabel,
		Help:   "The step of a disruption method evaluation.",
		Values: []opmetrics.Value{CandidatesPhase, BudgetsPhase, ComputePhase, ValidatePhase, StartPhase},
	}
	ConsolidationType = opmetrics.Label{
		Name:   ConsolidationTypeLabel,
		Help:   "The consolidation algorithm that produced the decision.",
		Values: []opmetrics.Value{MultiNodeConsolidationType, SingleNodeConsolidationType, EmptyConsolidationType},
	}
	// DecisionDim is the `decision` dimension for the voluntary-disruption decision
	// counters, whose value is the command's action.
	DecisionDim = opmetrics.Label{
		Name: decisionLabel,
		Help: "The disruption decision taken for the candidate(s).",
		Values: []opmetrics.Value{
			{
				Name: string(NoOpDecision),
				Help: "No disruption action was taken.",
			},
			{
				Name: string(ReplaceDecision),
				Help: "The candidate(s) were replaced with more efficient capacity.",
			},
			{
				Name: string(DeleteDecision),
				Help: "The candidate(s) were deleted without replacement.",
			},
			{
				Name: string(TerminateFirstDecision),
				Help: "The candidate(s) were deleted without staging a replacement first; reactive provisioning refills afterward.",
			},
			{
				Name: string(RebootDecision),
				Help: "The candidate(s) were rebooted in place.",
			},
		},
	}
	// ApprovalDim is the `decision` dimension for the balanced-consolidation move
	// metrics, which score each candidate move and record whether it was approved or
	// rejected — a disjoint value set from DecisionDim, so it is a separate Label.
	ApprovalDim = opmetrics.Label{
		Name: decisionLabel,
		Help: "Whether a scored balanced-consolidation move was approved or rejected.",
		Values: []opmetrics.Value{
			{
				Name: string(ApprovedDecision),
				Help: "The move's cost savings justified the pod disruption; it was approved.",
			},
			{
				Name: string(RejectedDecision),
				Help: "The move's cost savings did not justify the pod disruption; it was rejected.",
			},
		},
	}
	Policy = opmetrics.Label{
		Name: policyLabel,
		Help: "The NodePool consolidation policy in effect for the move.",
	}
	TerminateFirstReasonDim = opmetrics.Label{
		Name: terminateFirstReasonLabel,
		Help: "Why the terminate-first command couldn't stage its replacement before terminating the node.",
		Values: []opmetrics.Value{
			{
				Name: string(TerminateFirstNoReservedCapacity),
				Help: "The replacement has no reserved capacity to launch into except the slot the node holds in a full capacity reservation.",
			},
			{
				Name: string(TerminateFirstStaticAtLimit),
				Help: "The node's static NodePool is at its node limit, so the replacement can't launch alongside it.",
			},
		},
	}
)

func init() {
	// Initialize the consolidation_type series that can time out to 0. Only the
	// multi- and single-node algorithms run a bounded search that can hit a timeout;
	// empty-node consolidation does not, so it is not pre-initialized here.
	for _, ct := range []opmetrics.Value{MultiNodeConsolidationType, SingleNodeConsolidationType} {
		ConsolidationTimeoutsTotal.Add(0, map[string]string{ConsolidationTypeLabel: ct.Name})
	}
}

var (
	EvaluationDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decision_evaluation_duration_seconds",
			Help:      "Duration of the disruption decision evaluation process in seconds. Labeled by method and consolidation type.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]opmetrics.Label{metrics.DisruptionReason, ConsolidationType},
		opmetrics.Beta,
	)
	// PhaseDurationSeconds splits a method evaluation into its steps. The phases don't overlap: compute excludes the
	// time spent in validate, so per evaluation their sum is approximately decision_evaluation_duration_seconds. A phase
	// is only observed when the evaluation reaches it.
	PhaseDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decision_evaluation_phase_duration_seconds",
			Help:      "Duration of each phase of a disruption method evaluation in seconds. Compute excludes validation. Labeled by phase, method, and consolidation type.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]opmetrics.Label{Phase, metrics.DisruptionReason, ConsolidationType},
		opmetrics.Alpha,
	)
	SchedulingSimulationsPerEvaluation = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decision_evaluation_scheduling_simulations",
			Help:      "Number of scheduling simulations run by a single disruption method evaluation, including validation re-simulations. Labeled by method and consolidation type.",
			Buckets:   []float64{0, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
		},
		[]opmetrics.Label{metrics.DisruptionReason, ConsolidationType},
		opmetrics.Alpha,
	)
	LastEvaluationTimestampSeconds = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "last_evaluation_timestamp_seconds",
			Help:      "Unix time at which a disruption method was last evaluated. Labeled by method and consolidation type.",
		},
		[]opmetrics.Label{metrics.DisruptionReason, ConsolidationType},
		opmetrics.Alpha,
	)
	SkippedEvaluationsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "skipped_evaluations_total",
			Help:      "Number of times a disruption method was not evaluated because an earlier method in the same pass started commands. Labeled by method and consolidation type.",
		},
		[]opmetrics.Label{metrics.DisruptionReason, ConsolidationType},
		opmetrics.Alpha,
	)
	ClusterStateGatedPassesTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "cluster_state_gated_passes_total",
			Help:      "Number of disruption passes that evaluated no method because cluster state was not synced.",
		},
		[]opmetrics.Label{},
		opmetrics.Alpha,
	)
	ClusterStateGateDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "cluster_state_gate_duration_seconds",
			Help:      "Duration in seconds of each period in which the disruption loop waited for cluster state to sync, observed when the loop resumes.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]opmetrics.Label{},
		opmetrics.Alpha,
	)
	QueueCommands = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "queue_commands",
			Help:      "Number of disruption commands in flight in the orchestration queue.",
		},
		[]opmetrics.Label{},
		opmetrics.Alpha,
	)
	QueueCommandsWaitingOnDependencies = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "queue_commands_waiting_on_dependencies",
			Help:      "Number of disruption commands in the orchestration queue whose pods were simulated onto nodes that aren't initialized yet, and that are waiting for them before deleting their candidates.",
		},
		[]opmetrics.Label{},
		opmetrics.Alpha,
	)
	QueueDependencyFailuresTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "queue_dependency_failures_total",
			Help:      "Number of disruption commands that failed because a node their pods were simulated onto was deleted, or didn't initialize before the command timed out. Labeled by the decision, the disruption reason, the consolidation type and the cause.",
		},
		[]opmetrics.Label{DecisionDim, metrics.DisruptionReason, ConsolidationType, DependencyFailureCause},
		opmetrics.Alpha,
	)
	DecisionsPerformedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decisions_total",
			Help:      "Number of disruption decisions performed. Labeled by disruption decision, reason, and consolidation type.",
		},
		[]opmetrics.Label{DecisionDim, metrics.DisruptionReason, ConsolidationType},
		opmetrics.GA,
	)
	NodepoolDecisionsPerformed = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decisions_by_nodepool_total",
			Help:      "Number of disruption decisions performed by nodepool. Labeled by nodepool name, disruption decision, reason, and consolidation type.",
		},
		[]opmetrics.Label{metrics.NodePool, DecisionDim, metrics.DisruptionReason, ConsolidationType},
		opmetrics.Alpha,
	)
	TerminateFirstDecisionsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "terminate_first_decisions_total",
			Help:      "Number of terminate-first disruption decisions performed. Labeled by nodepool name, reason, and why the replacement couldn't be staged first.",
		},
		[]opmetrics.Label{metrics.NodePool, metrics.DisruptionReason, TerminateFirstReasonDim},
		opmetrics.Alpha,
	)
	EligibleNodes = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "eligible_nodes",
			Help:      "Number of nodes eligible for disruption by Karpenter. Labeled by disruption reason.",
		},
		[]opmetrics.Label{metrics.DisruptionReason},
		opmetrics.Beta,
	)
	ConsolidationTimeoutsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_timeouts_total",
			Help:      "Number of times the Consolidation algorithm has reached a timeout. Labeled by consolidation type.",
		},
		[]opmetrics.Label{ConsolidationType},
		opmetrics.Beta,
	)
	FailedValidationsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "failed_validations_total",
			Help:      "Number of candidates that were selected for disruption but failed validation. Labeled by consolidation type.",
		},
		[]opmetrics.Label{ConsolidationType},
		opmetrics.Alpha,
	)
	NodePoolAllowedDisruptions = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "allowed_disruptions",
			Help:      "The number of nodes for a given NodePool that can be concurrently disrupting at a point in time. Labeled by NodePool. Note that allowed disruptions can change very rapidly, as new nodes may be created and others may be deleted at any point.",
		},
		[]opmetrics.Label{metrics.NodePool, metrics.DisruptionReason},
		opmetrics.GA,
	)
	NodePoolNodesConsumingBudgets = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "nodes_consuming_budgets",
			Help:      "The number of nodes consuming the budget of a nodepool at a point in time. Labeled by NodePool.",
		},
		[]opmetrics.Label{metrics.NodePool, metrics.DisruptionReason},
		opmetrics.Alpha,
	)
	DisruptionQueueFailuresTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "queue_failures_total",
			Help:      "The number of times that an enqueued disruption decision failed. Labeled by disruption method.",
		},
		[]opmetrics.Label{DecisionDim, metrics.DisruptionReason, ConsolidationType},
		opmetrics.Beta,
	)
	ConsolidationScoreHistogram = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Name:      "consolidation_score",
			Help:      "Score of balanced consolidation moves. Labeled by decision, NodePool, and policy.",
			Buckets:   []float64{0.1, 0.25, 0.33, 0.5, 1.0, 2.0, 5.0, 10.0},
		},
		[]opmetrics.Label{ApprovalDim, metrics.NodePool, Policy},
		opmetrics.Alpha,
	)
	ConsolidationMovesTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "consolidation_moves_total",
			Help:      "Number of balanced consolidation moves. Labeled by decision, NodePool, and policy.",
		},
		[]opmetrics.Label{ApprovalDim, metrics.NodePool, Policy},
		opmetrics.Alpha,
	)
	// NodeClaimsUnhealthyDisruptedTotal preserves the per-condition/per-image breakdown the retired node.health
	// controller emitted, which the reason-labeled karpenter_nodeclaims_disrupted_total loses. Labeled by the repair
	// condition, the owning NodePool, the capacity type, and the image ID.
	NodeClaimsUnhealthyDisruptedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeClaimSubsystem,
			Name:      "unhealthy_disrupted_total",
			Help:      "Number of unhealthy nodeclaims disrupted in total by node repair. Labeled by the condition the node was disrupted on, the owning nodepool, the capacity type, the image ID, and the termination mode.",
		},
		[]opmetrics.Label{RepairCondition, metrics.NodePool, metrics.CapacityType, ImageID, metrics.TerminationMode},
		opmetrics.Alpha,
	)
)

const (
	conditionLabel = "condition"
	imageIDLabel   = "image_id"
	causeLabel     = "cause"
)

var (
	RepairCondition = opmetrics.Label{
		Name: conditionLabel,
		Help: "The node status condition type that triggered node repair disruption.",
	}
	ImageID = opmetrics.Label{
		Name: imageIDLabel,
		Help: "The image ID of the node that was disrupted.",
	}
	DependencyFailureCauseDeleted = opmetrics.Value{
		Name: "deleted",
		Help: "The dependency's NodeClaim was deleted (launch failure, registration or initialization TTL) or is being deleted.",
	}
	DependencyFailureCauseTimeout = opmetrics.Value{
		Name: "timeout",
		Help: "The dependency didn't initialize before the command timed out.",
	}
	DependencyFailureCause = opmetrics.Label{
		Name:   causeLabel,
		Help:   "Why a command's dependency failed.",
		Values: []opmetrics.Value{DependencyFailureCauseDeleted, DependencyFailureCauseTimeout},
	}
)
