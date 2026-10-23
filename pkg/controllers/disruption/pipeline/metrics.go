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
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/pipeline/types"
	"sigs.k8s.io/karpenter/pkg/metrics"
)

// The pipeline's metrics are emitted only with the DisruptionPipeline feature gate on, except the shadow metrics,
// which only the shadow mode emits (with the gate off).
const (
	subsystem    = "voluntary_disruption"
	laneLabel    = "lane"
	phaseLabel   = "phase"
	resultLabel  = "result"
	gateLabel    = "gate"
	plannerLabel = "planner"
	causeLabel   = "cause"
	levelLabel   = "level"
	kindLabel    = "kind"
	stageLabel   = "stage"
	dropLabel    = "drop_reason"
)

// Planning stages of a round's candidates, admission results of a batch, and joint simulation results and stages.
// They exist only as metric values.
var (
	stageEligible = opmetrics.Value{Name: "eligible", Help: "Candidates the planner was given."}
	stagePlanned  = opmetrics.Value{Name: "planned", Help: "Candidates in the round's plans."}
	stageDeferred = opmetrics.Value{Name: "deferred", Help: "Candidates the planner deferred to a later round (a joint simulation's later chunks)."}
	batchStarted  = opmetrics.Value{Name: "started", Help: "At least one plan of the batch starts."}
	batchRejected = opmetrics.Value{Name: "rejected", Help: "Every plan of the batch was rejected."}
	batchWaited   = opmetrics.Value{Name: "waited", Help: "The batch waits for a later commit step."}
	batchDropped  = opmetrics.Value{Name: "dropped", Help: "The batch waited too long, or the pipeline stopped."}
	jointAccepted = opmetrics.Value{Name: "accepted", Help: "A batch of candidates was accepted."}
	jointNone     = opmetrics.Value{Name: "none", Help: "No batch worked out; the planner planned one candidate at a time."}
	jointTimedOut = opmetrics.Value{Name: "timed_out", Help: "A simulation ran past its deadline."}
	jointFirst    = opmetrics.Value{Name: "first", Help: "Candidates of the first simulation."}
)

// Shadow comparison levels, mismatch kinds and causes.
const (
	shadowSnapshot  = "snapshot"
	shadowReference = "reference"
	kindCandidates  = "candidates"
	kindBudget      = "budget"
	kindCommands    = "commands"
	causeRace       = "race"
	causeLogic      = "logic"
)

// Round phases.
const (
	phaseSnapshot = "snapshot"
	phasePlan     = "plan"
	phaseAdmit    = "admit"
)

// Planner names of the default planners, in the disruption controller's method order. They are metric label values.
const (
	RepairPlanner      = "repair"
	EmptinessPlanner   = "emptiness"
	StaticDriftPlanner = "static_drift"
	DriftPlanner       = "drift"
	MultiNodePlanner   = "multi"
	SingleNodePlanner  = "single"
)

// Round and plan results.
const (
	roundPlanned = "planned"
	roundNoop    = "noop"
	roundGated   = "gated"
	planAdmitted = "admitted"
	planRejected = "rejected"
	planDropped  = "dropped"
)

var (
	LaneDim = opmetrics.Label{
		Name: laneLabel,
		Help: "The disruption pipeline lane.",
		Values: []opmetrics.Value{
			{Name: string(types.LaneFast), Help: "Repair, emptiness, static drift and drift."},
			{Name: string(types.LaneBackground), Help: "Multi- and single-node consolidation."},
		},
	}
	RoundPhase = opmetrics.Label{
		Name: phaseLabel,
		Help: "The phase of a disruption pipeline round.",
		Values: []opmetrics.Value{
			{Name: phaseSnapshot, Help: "Building the round's snapshot of cluster state."},
			{Name: phasePlan, Help: "Running the lane's planners and submitting their plans."},
			{Name: phaseAdmit, Help: "Admitting the lane's plans in the commit loop."},
		},
	}
	RoundResult = opmetrics.Label{
		Name: resultLabel,
		Help: "What a disruption pipeline round did.",
		Values: []opmetrics.Value{
			{Name: roundPlanned, Help: "A planner submitted plans."},
			{Name: roundNoop, Help: "No planner submitted a plan."},
			{Name: roundGated, Help: "The lane didn't plan."},
		},
	}
	GateDim = opmetrics.Label{
		Name: gateLabel,
		Help: "Why a disruption pipeline lane or planner didn't plan.",
		Values: []opmetrics.Value{
			{Name: string(types.GateSynced), Help: "Cluster state isn't synced."},
			{Name: string(types.GateBudgetZero), Help: "No NodePool has budget left for the lane's reasons."},
			{Name: string(types.GateBreaker), Help: "The repair breaker tripped."},
			{Name: string(types.GateUnresolved), Help: "The planner's last plans are unresolved."},
			{Name: string(types.GatePaced), Help: "The lane is paced, or the planner backs off because none of its last plans started."},
			{Name: string(types.GateHydrating), Help: "The prediction store is hydrating."},
		},
	}
	PlannerDim = opmetrics.Label{
		Name: plannerLabel,
		Help: "The disruption pipeline planner, or admitter for admission re-simulations.",
		Values: []opmetrics.Value{
			{Name: RepairPlanner, Help: "Node repair."},
			{Name: EmptinessPlanner, Help: "Empty-node consolidation."},
			{Name: StaticDriftPlanner, Help: "Drift of static NodePools."},
			{Name: DriftPlanner, Help: "Drift."},
			{Name: MultiNodePlanner, Help: "Multi-node consolidation."},
			{Name: SingleNodePlanner, Help: "Single-node consolidation."},
		},
	}
	PlanResult = opmetrics.Label{
		Name: resultLabel,
		Help: "How a disruption pipeline plan ended.",
		Values: []opmetrics.Value{
			{Name: planAdmitted, Help: "The plan started."},
			{Name: planRejected, Help: "The plan was rejected at reservation, admission or start."},
			{Name: planDropped, Help: "The plan waited too long, or the pipeline stopped."},
		},
	}
	RejectReasonDim = opmetrics.Label{
		Name: metrics.ReasonLabel,
		Help: "Why a disruption pipeline plan was rejected or dropped; empty if it started.",
		Values: append([]opmetrics.Value{{Name: "", Help: "Not rejected: the plan started."}}, lo.Map([]types.RejectReason{
			types.ReasonConflict, types.ReasonBudget, types.ReasonNodePoolLimit, types.ReasonStaticNodeLimit, types.ReasonCapacityReservation,
			types.ReasonCandidateInvalid, types.ReasonCandidateBlocked, types.ReasonCandidateNominated, types.ReasonCandidateIneligible,
			types.ReasonPodsChanged, types.ReasonResimUnschedulable, types.ReasonResimChanged, types.ReasonSiblingRejected, types.ReasonDeferred,
			types.ReasonStartFailed, types.ReasonBackpressure, types.ReasonDeadline, types.ReasonCanceled, types.ReasonError,
		}, func(r types.RejectReason, _ int) opmetrics.Value {
			return opmetrics.Value{Name: string(r), Help: "Rejected or dropped: " + string(r) + "."}
		})...),
	}
	ResimCauseDim = opmetrics.Label{
		Name: causeLabel,
		Help: "Why admission re-simulated a plan.",
		Values: []opmetrics.Value{
			{Name: string(types.ResimAlways), Help: "Consolidation always re-simulates."},
			{Name: string(types.ResimCoupled), Help: "A related plan started after the plan's simulation."},
			{Name: string(types.ResimStale), Help: "The plan is older than the stale limit."},
			{Name: string(types.ResimCatalog), Help: "Instance types or offerings changed."},
			{Name: string(types.ResimPodsChanged), Help: "A candidate's pods changed."},
			{Name: string(types.ResimSibling), Help: "A plan from the same joint simulation was rejected."},
			{Name: string(types.ResimPending), Help: "A pending pod the plan sized for was placed."},
		},
	}
)

var (
	RoundDurationSeconds = opmetrics.NewPrometheusHistogram(crmetrics.Registry, prometheus.HistogramOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_round_duration_seconds",
		Help:    "Duration of a phase of a disruption pipeline lane round in seconds. Labeled by lane and phase.",
		Buckets: metrics.DurationBuckets(),
	}, []opmetrics.Label{LaneDim, RoundPhase}, opmetrics.Alpha)
	RoundsTotal = opmetrics.NewPrometheusCounter(crmetrics.Registry, prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_rounds_total",
		Help: "Number of disruption pipeline lane rounds. Labeled by lane and result.",
	}, []opmetrics.Label{LaneDim, RoundResult}, opmetrics.Alpha)
	GatedSecondsTotal = opmetrics.NewPrometheusCounter(crmetrics.Registry, prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_gated_seconds_total",
		Help: "Seconds a disruption pipeline lane, or one of its planners, didn't plan, by why. Labeled by lane and gate.",
	}, []opmetrics.Label{LaneDim, GateDim}, opmetrics.Alpha)
	PlannerDurationSeconds = opmetrics.NewPrometheusHistogram(crmetrics.Registry, prometheus.HistogramOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_planner_duration_seconds",
		Help:    "Duration of a disruption pipeline planner's round (candidates and Plan) in seconds. Labeled by planner.",
		Buckets: metrics.DurationBuckets(),
	}, []opmetrics.Label{PlannerDim}, opmetrics.Alpha)
	AdmissionDurationSeconds = opmetrics.NewPrometheusHistogram(crmetrics.Registry, prometheus.HistogramOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_admission_duration_seconds",
		Help:    "Duration of admitting one disruption pipeline plan or batch in seconds. Labeled by planner.",
		Buckets: metrics.DurationBuckets(),
	}, []opmetrics.Label{PlannerDim}, opmetrics.Alpha)
	PlanWaitSeconds = opmetrics.NewPrometheusHistogram(crmetrics.Registry, prometheus.HistogramOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_plan_wait_seconds",
		Help:    "Seconds from a disruption pipeline plan's submission to its start. Labeled by lane and planner.",
		Buckets: metrics.DurationBuckets(),
	}, []opmetrics.Label{LaneDim, PlannerDim}, opmetrics.Alpha)
	AdmissionResimsTotal = opmetrics.NewPrometheusCounter(crmetrics.Registry, prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_admission_resims_total",
		Help: "Number of admission re-simulations of disruption pipeline plans, by cause. Labeled by planner and cause.",
	}, []opmetrics.Label{PlannerDim, ResimCauseDim}, opmetrics.Alpha)
	PlansTotal = opmetrics.NewPrometheusCounter(crmetrics.Registry, prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_plans_total",
		Help: "Number of disruption pipeline plans by outcome. Labeled by planner, result and reject reason.",
	}, []opmetrics.Label{PlannerDim, PlanResult, RejectReasonDim}, opmetrics.Alpha)
	UnresolvedPlans = opmetrics.NewPrometheusGauge(crmetrics.Registry, prometheus.GaugeOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_unresolved_plans",
		Help: "Number of submitted disruption pipeline plans that haven't started or been rejected. Labeled by lane.",
	}, []opmetrics.Label{LaneDim}, opmetrics.Alpha)
	LedgerReservedNodes = opmetrics.NewPrometheusGauge(crmetrics.Registry, prometheus.GaugeOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_ledger_reserved_nodes",
		Help: "Number of nodes reserved by disruption pipeline plans that haven't started; they count against the budget. Labeled by nodepool.",
	}, []opmetrics.Label{metrics.NodePool}, opmetrics.Alpha)
	LastAdmittedTimestampSeconds = opmetrics.NewPrometheusGauge(crmetrics.Registry, prometheus.GaugeOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_last_admitted_timestamp_seconds",
		Help: "Unix time at which a disruption pipeline planner's plan last started. Labeled by planner.",
	}, []opmetrics.Label{PlannerDim}, opmetrics.Alpha)
	TaintSweepDurationSeconds = opmetrics.NewPrometheusHistogram(crmetrics.Registry, prometheus.HistogramOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_taint_sweep_duration_seconds",
		Help:    "Duration of the disruption pipeline's sweep of leftover disruption taints and conditions in seconds.",
		Buckets: metrics.DurationBuckets(),
	}, []opmetrics.Label{}, opmetrics.Alpha)
)

var (
	ShadowLevelDim = opmetrics.Label{
		Name: levelLabel,
		Help: "What the disruption pipeline's shadow mode compared.",
		Values: []opmetrics.Value{
			{Name: shadowSnapshot, Help: "Each planner's candidates and budgets against the disruption controller's."},
			{Name: shadowReference, Help: "The reference planner's plans against the disruption controller's commands."},
		},
	}
	ShadowKindDim = opmetrics.Label{
		Name: kindLabel,
		Help: "What differed in a shadow comparison.",
		Values: []opmetrics.Value{
			{Name: kindCandidates, Help: "The candidate sets."},
			{Name: kindBudget, Help: "A NodePool's remaining budget."},
			{Name: kindCommands, Help: "The commands, also up to equivalent nodes."},
		},
	}
	ShadowCauseDim = opmetrics.Label{
		Name: causeLabel,
		Help: "Why a shadow comparison differed, from a recheck on fresh state.",
		Values: []opmetrics.Value{
			{Name: causeRace, Help: "The recheck agreed: cluster state changed in between."},
			{Name: causeLogic, Help: "The recheck differed too, or none was possible."},
		},
	}
)

var (
	ShadowComparisonsTotal = opmetrics.NewPrometheusCounter(crmetrics.Registry, prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_shadow_comparisons_total",
		Help: "Number of the disruption pipeline's shadow comparisons. Labeled by level and planner.",
	}, []opmetrics.Label{ShadowLevelDim, PlannerDim}, opmetrics.Alpha)
	ShadowMismatchesTotal = opmetrics.NewPrometheusCounter(crmetrics.Registry, prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_shadow_mismatches_total",
		Help: "Number of the disruption pipeline's shadow comparisons that differed. Labeled by level, planner, kind and cause.",
	}, []opmetrics.Label{ShadowLevelDim, PlannerDim, ShadowKindDim, ShadowCauseDim}, opmetrics.Alpha)
	ShadowAgreementRatio = opmetrics.NewPrometheusGauge(crmetrics.Registry, prometheus.GaugeOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_shadow_agreement_ratio",
		Help: "Share of the disruption pipeline's reference-level shadow comparisons that agreed, since start. Labeled by planner.",
	}, []opmetrics.Label{PlannerDim}, opmetrics.Alpha)
)

var (
	PlanStageDim = opmetrics.Label{
		Name:   stageLabel,
		Help:   "How far a planner's candidates got in a round.",
		Values: []opmetrics.Value{stageEligible, stagePlanned, stageDeferred},
	}
	BatchResultDim = opmetrics.Label{
		Name:   resultLabel,
		Help:   "What admission did with a batch of plans.",
		Values: []opmetrics.Value{batchStarted, batchRejected, batchWaited, batchDropped},
	}
)

var (
	PlannedCandidates = opmetrics.NewPrometheusHistogram(crmetrics.Registry, prometheus.HistogramOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_planned_candidates",
		Help:    "Candidates of a disruption pipeline planner's round, by how far they got. Labeled by planner and stage.",
		Buckets: []float64{0, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000},
	}, []opmetrics.Label{PlannerDim, PlanStageDim}, opmetrics.Alpha)
	PlansPerRound = opmetrics.NewPrometheusHistogram(crmetrics.Registry, prometheus.HistogramOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_plans_per_round",
		Help:    "Plans a disruption pipeline planner submitted in a round that planned. Labeled by planner.",
		Buckets: []float64{1, 2, 3, 5, 10, 20, 50, 100},
	}, []opmetrics.Label{PlannerDim}, opmetrics.Alpha)
	AdmittedBatchesTotal = opmetrics.NewPrometheusCounter(crmetrics.Registry, prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_admitted_batches_total",
		Help: "Number of admission decisions on a disruption pipeline batch (a plan alone counts as a batch of one). Labeled by planner and result.",
	}, []opmetrics.Label{PlannerDim, BatchResultDim}, opmetrics.Alpha)
)

var (
	JointResultDim = opmetrics.Label{
		Name:   resultLabel,
		Help:   "How a planner's joint simulation ended.",
		Values: []opmetrics.Value{jointAccepted, jointNone, jointTimedOut},
	}
	JointStageDim = opmetrics.Label{
		Name: stageLabel,
		Help: "Candidates of a joint simulation, by stage.",
		Values: []opmetrics.Value{
			jointFirst,
			{Name: jointAccepted.Name, Help: "Candidates the batch accepted."},
			{Name: stageDeferred.Name, Help: "Accepted candidates left to a later round."},
		},
	}
	JointDropDim = opmetrics.Label{
		Name: dropLabel,
		Help: "Why a joint simulation took a candidate out of its batch.",
		Values: []opmetrics.Value{
			{Name: string(disruption.DropPodsUnschedulable), Help: "One of the candidate's pods failed to schedule."},
			{Name: string(disruption.DropUninitializedDestination), Help: "Reserved, not produced since dependency tracking (decision #16): one of the candidate's pods was placed on a node that isn't initialized while uninitialized destinations were fenced."},
			{Name: string(disruption.DropHalved), Help: "The batch was halved after a pod of no candidate failed to schedule."},
		},
	}
)

var (
	JointSimulationsTotal = opmetrics.NewPrometheusCounter(crmetrics.Registry, prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_joint_simulations_total",
		Help: "Number of a disruption pipeline planner's joint simulations, by how they ended. Labeled by planner and result.",
	}, []opmetrics.Label{PlannerDim, JointResultDim}, opmetrics.Alpha)
	JointChunkedTotal = opmetrics.NewPrometheusCounter(crmetrics.Registry, prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_joint_simulations_chunked_total",
		Help: "Number of a disruption pipeline planner's accepted joint simulations that were planned again in chunks of ComponentCap. Labeled by planner.",
	}, []opmetrics.Label{PlannerDim}, opmetrics.Alpha)
	JointCandidates = opmetrics.NewPrometheusHistogram(crmetrics.Registry, prometheus.HistogramOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_joint_simulation_candidates",
		Help:    "Candidates of a disruption pipeline planner's joint simulation. Labeled by planner and stage.",
		Buckets: []float64{0, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000},
	}, []opmetrics.Label{PlannerDim, JointStageDim}, opmetrics.Alpha)
	JointDroppedCandidatesTotal = opmetrics.NewPrometheusCounter(crmetrics.Registry, prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_joint_simulation_dropped_candidates_total",
		Help: "Number of candidates a disruption pipeline planner's joint simulations took out of their batch. Labeled by planner and drop_reason.",
	}, []opmetrics.Label{PlannerDim, JointDropDim}, opmetrics.Alpha)
	JointSimulationDurationSeconds = opmetrics.NewPrometheusHistogram(crmetrics.Registry, prometheus.HistogramOpts{
		Namespace: metrics.Namespace, Subsystem: subsystem, Name: "pipeline_joint_simulation_duration_seconds",
		Help:    "Duration of the simulation that accepted a disruption pipeline planner's batch. Labeled by planner.",
		Buckets: metrics.DurationBuckets(),
	}, []opmetrics.Label{PlannerDim}, opmetrics.Alpha)
)
