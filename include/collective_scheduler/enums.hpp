// Collective Scheduler — semantic enum types.
//
// Collective Scheduler 1.0.0
// Copyright 2026 Summon Software Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <stdexcept>
#include <string_view>

namespace collective_scheduler {

// Production status: every data element that feeds a decision carries an
// explicit provenance tag. UNKNOWN evidence never silently becomes an
// authoritative positive.
enum class ProvenanceStatus : std::uint8_t {
  REAL,        // Directly observed on the live system.
  MEASURED,    // Measured from a running system (possibly a different phase).
  REPORTED,    // Reported by a trusted runtime boundary.
  DERIVED,     // Derived from other evidence.
  ESTIMATED,   // Modeled estimate.
  FORECAST,    // Projected future value, not guaranteed present.
  SYNTHETIC,   // Explicitly synthetic scenario (no physical hardware backing).
  UNKNOWN,     // No supporting evidence.
};

enum class CollectiveKind : std::uint8_t {
  ALL_REDUCE,
  ALL_GATHER,
  REDUCE_SCATTER,
  BROADCAST,
  REDUCE,
  GATHER,
  SCATTER,
  ALL_TO_ALL,
  BARRIER,
  CUSTOM,
  UNKNOWN,
};

enum class Readiness : std::uint8_t {
  READY,
  WAITING_DEPENDENCY,
  WAITING_PARTICIPANT,
  WAITING_COMMUNICATION_PLAN,
  WAITING_RESERVATION,
  WAITING_RESOURCE,
  WAITING_CAPACITY,
  BLOCKED_CONGESTION,
  BLOCKED_POLICY,
  REVALIDATION_REQUIRED,
  STALE,
  UNKNOWN,
};

enum class ConflictSeverity : std::uint8_t { NONE, LOW, MEDIUM, HIGH, FATAL };

enum class ConflictClassification : std::uint8_t {
  NO_CONFLICT,
  PARTICIPANT_CONFLICT,
  RESOURCE_CONFLICT,
  LINK_CONFLICT,
  BANDWIDTH_CONFLICT,
  RESERVATION_CONFLICT,
  DEPENDENCY_CONFLICT,
  POLICY_CONFLICT,
  UNKNOWN,
};

enum class OverlapMode : std::uint8_t {
  EXCLUSIVE,
  OVERLAP_ALLOWED,
  OVERLAP_LIMITED,
  SERIALIZE,
  UNKNOWN,
};

enum class LifecycleState : std::uint8_t {
  DECLARED,
  WAITING,
  READY,
  ELIGIBLE,
  QUEUED,
  GRANTED,
  DISPATCHING,
  ACTIVE,
  DRAINING,
  COMPLETED,
  CANCELLED,
  FAILED,
  REVALIDATION_REQUIRED,
  SUPERSEDED,
  STALE,
  RETIRED,
};

enum class QueueClass : std::uint8_t {
  LATENCY_CRITICAL,
  DEADLINE,
  STANDARD,
  THROUGHPUT,
  BACKGROUND,
  RECOVERY,
  CHECKPOINT,
  CONTROL,
};

enum class PriorityInversionAction : std::uint8_t {
  REQUEST_PREEMPTION,
  DRAIN_CURRENT,
  DEFER_NEW_CONFLICTING_WORK,
  WAIT,
  NO_ACTION,
};

enum class FairnessClass : std::uint8_t {
  DEFAULT,
  PROTECTED,
  BEST_EFFORT,
};

enum class DependencyKind : std::uint8_t {
  TENSOR_READY,
  PREDECESSOR_COMPLETED,
  EXECUTION_PHASE_COMPLETE,
  CHECKPOINT_READY,
  MODEL_STAGE_READY,
  TIMER,
  CUSTOM,
  UNKNOWN,
};

enum class ReservationStrength : std::uint8_t {
  HARD,
  SOFT,
  NONE,
};

enum class SchedulingFactorKind : std::uint8_t {
  EXTERNAL_PRIORITY,
  DEADLINE_SLACK,
  AGE,
  FAIRNESS_DEFICIT,
  STARVATION_RISK,
  EXPECTED_DURATION,
  CONGESTION_PENALTY,
  RESERVATION_FIT,
  COMMUNICATION_COST,
  RESOURCE_FOOTPRINT,
  PARTICIPANT_AVAILABILITY,
  OVERLAP_BENEFIT,
  FAILURE_RETRY_COST,
  RECOVERY_IMPORTANCE,
  POLICY_CLASS,
  POLICY_WEIGHT,
};

enum class RejectionReason : std::uint8_t {
  NONE,
  STALE_COLLECTIVE_GENERATION,
  STALE_PARTICIPANT_GENERATION,
  MISSING_PARTICIPANT,
  UNSATISFIED_DEPENDENCY,
  INVALID_COMMUNICATION_PLAN,
  INVALID_RESERVATION,
  INSUFFICIENT_CAPACITY,
  CAPABILITY_MISMATCH,
  UNHEALTHY,
  NO_EXECUTION_AUTHORITY,
  INVALID_SCHEDULING_WINDOW,
  EXCLUSION_POLICY,
  HARD_CONGESTION_CEILING,
  STALE_COORDINATOR_EPOCH,
  CANCELLED,
  SUPERSEDED,
  REVALIDATION_REQUIRED,
  STALE_GRANT,
  MALFORMED_REQUEST,
  RESOURCE_CONFLICT,
  UNKNOWN,
};

// A compact deterministic textual name for each enum. Used for explanations,
// diagnostics, protocol encoding, and deterministic serialization. No
// environment-specific text is emitted.
inline constexpr std::string_view name_of(ProvenanceStatus v) noexcept {
  switch (v) {
    case ProvenanceStatus::REAL: return "REAL";
    case ProvenanceStatus::MEASURED: return "MEASURED";
    case ProvenanceStatus::REPORTED: return "REPORTED";
    case ProvenanceStatus::DERIVED: return "DERIVED";
    case ProvenanceStatus::ESTIMATED: return "ESTIMATED";
    case ProvenanceStatus::FORECAST: return "FORECAST";
    case ProvenanceStatus::SYNTHETIC: return "SYNTHETIC";
    case ProvenanceStatus::UNKNOWN: return "UNKNOWN";
  }
  return "UNKNOWN";
}
inline constexpr std::string_view name_of(CollectiveKind v) noexcept {
  switch (v) {
    case CollectiveKind::ALL_REDUCE: return "ALL_REDUCE";
    case CollectiveKind::ALL_GATHER: return "ALL_GATHER";
    case CollectiveKind::REDUCE_SCATTER: return "REDUCE_SCATTER";
    case CollectiveKind::BROADCAST: return "BROADCAST";
    case CollectiveKind::REDUCE: return "REDUCE";
    case CollectiveKind::GATHER: return "GATHER";
    case CollectiveKind::SCATTER: return "SCATTER";
    case CollectiveKind::ALL_TO_ALL: return "ALL_TO_ALL";
    case CollectiveKind::BARRIER: return "BARRIER";
    case CollectiveKind::CUSTOM: return "CUSTOM";
    case CollectiveKind::UNKNOWN: return "UNKNOWN";
  }
  return "UNKNOWN";
}
inline constexpr std::string_view name_of(Readiness v) noexcept {
  switch (v) {
    case Readiness::READY: return "READY";
    case Readiness::WAITING_DEPENDENCY: return "WAITING_DEPENDENCY";
    case Readiness::WAITING_PARTICIPANT: return "WAITING_PARTICIPANT";
    case Readiness::WAITING_COMMUNICATION_PLAN: return "WAITING_COMMUNICATION_PLAN";
    case Readiness::WAITING_RESERVATION: return "WAITING_RESERVATION";
    case Readiness::WAITING_RESOURCE: return "WAITING_RESOURCE";
    case Readiness::WAITING_CAPACITY: return "WAITING_CAPACITY";
    case Readiness::BLOCKED_CONGESTION: return "BLOCKED_CONGESTION";
    case Readiness::BLOCKED_POLICY: return "BLOCKED_POLICY";
    case Readiness::REVALIDATION_REQUIRED: return "REVALIDATION_REQUIRED";
    case Readiness::STALE: return "STALE";
    case Readiness::UNKNOWN: return "UNKNOWN";
  }
  return "UNKNOWN";
}
inline constexpr std::string_view name_of(ConflictSeverity v) noexcept {
  switch (v) {
    case ConflictSeverity::NONE: return "NONE";
    case ConflictSeverity::LOW: return "LOW";
    case ConflictSeverity::MEDIUM: return "MEDIUM";
    case ConflictSeverity::HIGH: return "HIGH";
    case ConflictSeverity::FATAL: return "FATAL";
  }
  return "HIGH";
}
inline constexpr std::string_view name_of(ConflictClassification v) noexcept {
  switch (v) {
    case ConflictClassification::NO_CONFLICT: return "NO_CONFLICT";
    case ConflictClassification::PARTICIPANT_CONFLICT: return "PARTICIPANT_CONFLICT";
    case ConflictClassification::RESOURCE_CONFLICT: return "RESOURCE_CONFLICT";
    case ConflictClassification::LINK_CONFLICT: return "LINK_CONFLICT";
    case ConflictClassification::BANDWIDTH_CONFLICT: return "BANDWIDTH_CONFLICT";
    case ConflictClassification::RESERVATION_CONFLICT: return "RESERVATION_CONFLICT";
    case ConflictClassification::DEPENDENCY_CONFLICT: return "DEPENDENCY_CONFLICT";
    case ConflictClassification::POLICY_CONFLICT: return "POLICY_CONFLICT";
    case ConflictClassification::UNKNOWN: return "UNKNOWN";
  }
  return "UNKNOWN";
}
inline constexpr std::string_view name_of(OverlapMode v) noexcept {
  switch (v) {
    case OverlapMode::EXCLUSIVE: return "EXCLUSIVE";
    case OverlapMode::OVERLAP_ALLOWED: return "OVERLAP_ALLOWED";
    case OverlapMode::OVERLAP_LIMITED: return "OVERLAP_LIMITED";
    case OverlapMode::SERIALIZE: return "SERIALIZE";
    case OverlapMode::UNKNOWN: return "UNKNOWN";
  }
  return "UNKNOWN";
}
inline constexpr std::string_view name_of(LifecycleState v) noexcept {
  switch (v) {
    case LifecycleState::DECLARED: return "DECLARED";
    case LifecycleState::WAITING: return "WAITING";
    case LifecycleState::READY: return "READY";
    case LifecycleState::ELIGIBLE: return "ELIGIBLE";
    case LifecycleState::QUEUED: return "QUEUED";
    case LifecycleState::GRANTED: return "GRANTED";
    case LifecycleState::DISPATCHING: return "DISPATCHING";
    case LifecycleState::ACTIVE: return "ACTIVE";
    case LifecycleState::DRAINING: return "DRAINING";
    case LifecycleState::COMPLETED: return "COMPLETED";
    case LifecycleState::CANCELLED: return "CANCELLED";
    case LifecycleState::FAILED: return "FAILED";
    case LifecycleState::REVALIDATION_REQUIRED: return "REVALIDATION_REQUIRED";
    case LifecycleState::SUPERSEDED: return "SUPERSEDED";
    case LifecycleState::STALE: return "STALE";
    case LifecycleState::RETIRED: return "RETIRED";
  }
  return "RETIRED";
}
inline constexpr std::string_view name_of(QueueClass v) noexcept {
  switch (v) {
    case QueueClass::LATENCY_CRITICAL: return "LATENCY_CRITICAL";
    case QueueClass::DEADLINE: return "DEADLINE";
    case QueueClass::STANDARD: return "STANDARD";
    case QueueClass::THROUGHPUT: return "THROUGHPUT";
    case QueueClass::BACKGROUND: return "BACKGROUND";
    case QueueClass::RECOVERY: return "RECOVERY";
    case QueueClass::CHECKPOINT: return "CHECKPOINT";
    case QueueClass::CONTROL: return "CONTROL";
  }
  return "STANDARD";
}
inline constexpr std::string_view name_of(PriorityInversionAction v) noexcept {
  switch (v) {
    case PriorityInversionAction::REQUEST_PREEMPTION: return "REQUEST_PREEMPTION";
    case PriorityInversionAction::DRAIN_CURRENT: return "DRAIN_CURRENT";
    case PriorityInversionAction::DEFER_NEW_CONFLICTING_WORK: return "DEFER_NEW_CONFLICTING_WORK";
    case PriorityInversionAction::WAIT: return "WAIT";
    case PriorityInversionAction::NO_ACTION: return "NO_ACTION";
  }
  return "NO_ACTION";
}
inline constexpr std::string_view name_of(FairnessClass v) noexcept {
  switch (v) {
    case FairnessClass::DEFAULT: return "DEFAULT";
    case FairnessClass::PROTECTED: return "PROTECTED";
    case FairnessClass::BEST_EFFORT: return "BEST_EFFORT";
  }
  return "DEFAULT";
}
inline constexpr std::string_view name_of(DependencyKind v) noexcept {
  switch (v) {
    case DependencyKind::TENSOR_READY: return "TENSOR_READY";
    case DependencyKind::PREDECESSOR_COMPLETED: return "PREDECESSOR_COMPLETED";
    case DependencyKind::EXECUTION_PHASE_COMPLETE: return "EXECUTION_PHASE_COMPLETE";
    case DependencyKind::CHECKPOINT_READY: return "CHECKPOINT_READY";
    case DependencyKind::MODEL_STAGE_READY: return "MODEL_STAGE_READY";
    case DependencyKind::TIMER: return "TIMER";
    case DependencyKind::CUSTOM: return "CUSTOM";
    case DependencyKind::UNKNOWN: return "UNKNOWN";
  }
  return "UNKNOWN";
}
inline constexpr std::string_view name_of(ReservationStrength v) noexcept {
  switch (v) {
    case ReservationStrength::HARD: return "HARD";
    case ReservationStrength::SOFT: return "SOFT";
    case ReservationStrength::NONE: return "NONE";
  }
  return "NONE";
}
inline constexpr std::string_view name_of(SchedulingFactorKind v) noexcept {
  switch (v) {
    case SchedulingFactorKind::EXTERNAL_PRIORITY: return "EXTERNAL_PRIORITY";
    case SchedulingFactorKind::DEADLINE_SLACK: return "DEADLINE_SLACK";
    case SchedulingFactorKind::AGE: return "AGE";
    case SchedulingFactorKind::FAIRNESS_DEFICIT: return "FAIRNESS_DEFICIT";
    case SchedulingFactorKind::STARVATION_RISK: return "STARVATION_RISK";
    case SchedulingFactorKind::EXPECTED_DURATION: return "EXPECTED_DURATION";
    case SchedulingFactorKind::CONGESTION_PENALTY: return "CONGESTION_PENALTY";
    case SchedulingFactorKind::RESERVATION_FIT: return "RESERVATION_FIT";
    case SchedulingFactorKind::COMMUNICATION_COST: return "COMMUNICATION_COST";
    case SchedulingFactorKind::RESOURCE_FOOTPRINT: return "RESOURCE_FOOTPRINT";
    case SchedulingFactorKind::PARTICIPANT_AVAILABILITY: return "PARTICIPANT_AVAILABILITY";
    case SchedulingFactorKind::OVERLAP_BENEFIT: return "OVERLAP_BENEFIT";
    case SchedulingFactorKind::FAILURE_RETRY_COST: return "FAILURE_RETRY_COST";
    case SchedulingFactorKind::RECOVERY_IMPORTANCE: return "RECOVERY_IMPORTANCE";
    case SchedulingFactorKind::POLICY_CLASS: return "POLICY_CLASS";
    case SchedulingFactorKind::POLICY_WEIGHT: return "POLICY_WEIGHT";
  }
  return "POLICY_WEIGHT";
}
inline constexpr std::string_view name_of(RejectionReason v) noexcept {
  switch (v) {
    case RejectionReason::NONE: return "NONE";
    case RejectionReason::STALE_COLLECTIVE_GENERATION: return "STALE_COLLECTIVE_GENERATION";
    case RejectionReason::STALE_PARTICIPANT_GENERATION: return "STALE_PARTICIPANT_GENERATION";
    case RejectionReason::MISSING_PARTICIPANT: return "MISSING_PARTICIPANT";
    case RejectionReason::UNSATISFIED_DEPENDENCY: return "UNSATISFIED_DEPENDENCY";
    case RejectionReason::INVALID_COMMUNICATION_PLAN: return "INVALID_COMMUNICATION_PLAN";
    case RejectionReason::INVALID_RESERVATION: return "INVALID_RESERVATION";
    case RejectionReason::INSUFFICIENT_CAPACITY: return "INSUFFICIENT_CAPACITY";
    case RejectionReason::CAPABILITY_MISMATCH: return "CAPABILITY_MISMATCH";
    case RejectionReason::UNHEALTHY: return "UNHEALTHY";
    case RejectionReason::NO_EXECUTION_AUTHORITY: return "NO_EXECUTION_AUTHORITY";
    case RejectionReason::INVALID_SCHEDULING_WINDOW: return "INVALID_SCHEDULING_WINDOW";
    case RejectionReason::EXCLUSION_POLICY: return "EXCLUSION_POLICY";
    case RejectionReason::HARD_CONGESTION_CEILING: return "HARD_CONGESTION_CEILING";
    case RejectionReason::STALE_COORDINATOR_EPOCH: return "STALE_COORDINATOR_EPOCH";
    case RejectionReason::CANCELLED: return "CANCELLED";
    case RejectionReason::SUPERSEDED: return "SUPERSEDED";
    case RejectionReason::REVALIDATION_REQUIRED: return "REVALIDATION_REQUIRED";
    case RejectionReason::STALE_GRANT: return "STALE_GRANT";
    case RejectionReason::MALFORMED_REQUEST: return "MALFORMED_REQUEST";
    case RejectionReason::RESOURCE_CONFLICT: return "RESOURCE_CONFLICT";
    case RejectionReason::UNKNOWN: return "UNKNOWN";
  }
  return "UNKNOWN";
}

}  // namespace collective_scheduler
