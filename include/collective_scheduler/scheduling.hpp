// Collective Scheduler — scheduling decision and grant model.
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

#include <collective_scheduler/domains.hpp>
#include <collective_scheduler/enums.hpp>
#include <collective_scheduler/overlap.hpp>
#include <collective_scheduler/request.hpp>
#include <collective_scheduler/time.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace collective_scheduler {

// One named scheduling factor value with its provenance.
struct FactorValue {
  SchedulingFactorKind kind;
  double value = 0.0;
  ProvenanceStatus source = ProvenanceStatus::DERIVED;
};

// Evidence generations that were current at the moment a decision / grant was
// produced. A grant that does not carry the current generation for a required
// domain is stale and must be rejected.
struct EvidenceSnapshot {
  CoordinatorEpoch epoch;
  CollectiveGeneration collective_gen;
  CollectiveRequestGeneration request_gen;
  CommunicationPlanGeneration plan_gen;
  ParticipantGeneration participant_gen;                        // representative (max-present)
  DependencyGeneration dependency_gen;                          // representative
  ReservationGeneration reservation_gen;
  CongestionGeneration congestion_gen;
  CapacityGeneration capacity_gen;
  PolicyGeneration policy_gen;
  PriorityGeneration priority_gen;
  FairnessGeneration fairness_gen;
  TopologyGeneration topology_gen;
  CapabilityGeneration capability_gen;
  HealthGeneration health_gen;
};

// A bundle of revalidation requirements captured with a decision: revalidate
// before handoff and again before every completion acceptance.
struct RevalidationRequirements {
  bool require_fresh_participants = true;
  bool require_fresh_plan = true;
  bool require_fresh_reservation = true;
  bool require_fresh_dependencies = true;
  bool require_fresh_congestion = true;
  bool require_fresh_capacity = true;
  bool require_fresh_execution_authority = true;
  bool require_current_epoch = true;
};

// A priority-inversion event: a lower-priority ACTIVE collective is blocking a
// higher-priority eligible collective. The scheduler never preempts; it emits a
// typed action (DRAIN_CURRENT / DEFER_NEW_CONFLICTING_WORK / REQUEST_PREEMPTION).
struct PriorityInversionEvent {
  CollectiveRequestId holder;      // lower-priority ACTIVE collective
  CollectiveRequestId requester;   // higher-priority blocked collective
  PriorityInversionAction action = PriorityInversionAction::NO_ACTION;
  std::int32_t holder_priority = 0;
  std::int32_t requester_priority = 0;
  EvidenceSnapshot evidence;
};

// A scheduling decision is NOT execution. It records which collective(s) the
// scheduler selects now, their ordering, the overlap group, the grant
// generations, the ranking factors, and the typed explanation.
struct ScheduleDecision {
  CollectiveScheduleId id;
  CollectiveScheduleGeneration gen;
  std::vector<CollectiveRequestId> selected;      // deterministic order
  std::vector<OverlapGroup> overlap_groups;
  std::vector<CollectiveGrantId> grants;          // one per selected, parallel
  EvidenceSnapshot evidence;
  RevalidationRequirements revalidation;
  std::vector<FactorValue> ranking_factors;
  std::vector<CollectiveRequestId> fallback;      // next eligible order (deterministic)
  std::vector<PriorityInversionEvent> priority_inversions;
  std::string explanation;
};

// A bounded grant authorizing execution handoff for exactly one collective.
// A grant is explicit and generation-bound. The grant never silently patches an
// old grant with current evidence; a stale grant must reject.
struct CollectiveGrant {
  CollectiveGrantId id;
  CollectiveGrantGeneration gen;
  CollectiveRequestId request;
  CollectiveRequestGeneration request_gen;
  CollectiveId collective;
  CollectiveGeneration collective_gen;
  CollectiveScheduleId schedule;
  CollectiveScheduleGeneration schedule_gen;
  std::vector<ParticipantId> participants;
  std::vector<ParticipantGeneration> participant_gens;
  CommunicationPlanId plan;
  CommunicationPlanGeneration plan_gen;
  EvidenceSnapshot evidence;
  RevalidationRequirements revalidation;
  WorkloadId workload;
  ExecutionId execution_attempt;      // execution-attempt authority, if bound
  ExecutionGeneration execution_attempt_gen;
  PolicyGeneration policy_gen;
  bool active = false;                // has reached ACTIVE
  bool terminal = false;              // reached a terminal outcome

  bool operator==(const CollectiveGrant&) const = default;
};

// Outcome enum for grant revalidation / completion adjudication.
enum class GrantAdjudication : std::uint8_t {
  VALID,
  STALE_GRANT,
  STALE_EVIDENCE,
  CANCELLED,
  SUPERSEDED,
  NOT_ACTIVE,
  ALREADY_TERMINAL,
  EPOCH_MISMATCH,
  UNKNOWN,
};

}  // namespace collective_scheduler
