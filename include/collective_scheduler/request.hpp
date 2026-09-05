// Collective Scheduler — collective request model.
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
#include <collective_scheduler/participant.hpp>
#include <collective_scheduler/plan.hpp>
#include <collective_scheduler/time.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace collective_scheduler {

// A dependency edge required by a collective. Readiness for the edge is owned by
// the Dependency Fabric; the scheduler only consumes the current DependencyGeneration
// binding. A stale DependencyGeneration must not unblock current work.
struct DependencyRequirement {
  DependencyId id;
  DependencyGeneration gen;
  DependencyKind kind = DependencyKind::UNKNOWN;
  ProvenanceStatus provenance = ProvenanceStatus::UNKNOWN;
};

// Resource footprint claim expected by a collective. The scheduler uses this for
// conflict / overlap analysis; actual scarce-resource claims come from the
// Resource Broker.
struct ResourceClaim {
  ResourceId resource;
  ResourceGeneration gen;
  double units = 0.0;
  bool exclusive = true;
  ProvenanceStatus provenance = ProvenanceStatus::SYNTHETIC;
};

// A scheduling window: earliest start, latest start, absolute deadline, and the
// expected duration. All in the scheduler's logical time domain. Wall-clock
// absolute deadlines are flagged by their source semantics (see time.hpp).
struct SchedulingWindow {
  Nanoseconds earliest_start_ns = 0;
  Nanoseconds latest_start_ns = 0;   // 0 means unbounded
  Nanoseconds deadline_ns = 0;       // 0 means no deadline
  Nanoseconds expected_duration_ns = 0;
  Nanoseconds slack_ns = 0;
  ProvenanceStatus source = ProvenanceStatus::REPORTED;

  bool has_deadline() const noexcept { return deadline_ns != 0; }
};

// External priority. Higher value = higher priority. Bound to a
// PriorityGeneration so a stale priority can never rank a collective.
struct ExternalPriority {
  std::int32_t value = 0;
  PriorityGeneration gen;
  bool authoritative = false;
};

// All information required to schedule one collective. The scheduler validates
// this on submission and rejects malformed requests.
struct CollectiveRequest {
  CollectiveRequestId id;
  CollectiveRequestGeneration gen;

  CollectiveId collective;
  CollectiveGeneration collective_gen;

  CollectiveKind kind = CollectiveKind::UNKNOWN;

  // Participant set plus the generation each participant is expected to have.
  std::vector<ParticipantId> participants;   // sorted, unique
  std::vector<ParticipantGeneration> participant_gens;  // parallel to participants

  std::uint64_t payload_bytes = 0;
  std::vector<std::uint64_t> per_participant_payload;  // optional, parallel

  CommunicationPlanId plan_id;
  CommunicationPlanGeneration plan_gen;

  std::vector<DependencyRequirement> dependencies;
  std::vector<ResourceClaim> resource_claims;
  std::vector<std::string> exclusion_classes;   // sorted, unique

  ExternalPriority priority;
  SchedulingWindow window;

  OverlapMode overlap = OverlapMode::UNKNOWN;
  FairnessClass fairness_class = FairnessClass::DEFAULT;

  std::optional<ReservationGeneration> reservation_gen;

  WorkloadId workload;
  WorkloadGeneration workload_gen;

  QueueClass queue_class = QueueClass::STANDARD;

  std::uint32_t max_retries = 0;
  std::uint32_t retry_count = 0;

  PolicyGeneration policy_gen;
  ProvenanceStatus provenance = ProvenanceStatus::REPORTED;

  bool operator==(const CollectiveRequest&) const = default;
};

}  // namespace collective_scheduler
