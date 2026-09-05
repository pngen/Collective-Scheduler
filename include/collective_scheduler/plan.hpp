// Collective Scheduler — communication plan domain record.
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
#include <collective_scheduler/time.hpp>

#include <cstdint>
#include <vector>

namespace collective_scheduler {

// A communication plan describes HOW participants may exchange data. Collective
// Scheduler decides WHEN that plan may execute; it never recomputes the route.
//
// A stale CommunicationPlanGeneration must never authorize execution.
struct CommunicationPlan {
  CommunicationPlanId id;
  CommunicationPlanGeneration gen;
  CollectiveKind kind = CollectiveKind::UNKNOWN;
  TopologyGeneration topology_gen;
  std::vector<ParticipantId> participants;  // sorted, unique
  std::vector<FlowId> flows;                // sorted
  std::vector<ResourceId> resources;        // sorted, unique
  std::vector<LinkId> links;                // sorted, unique
  std::vector<PathId> paths;                // sorted, unique
  std::uint64_t byte_count = 0;             // total payload bytes
  double estimated_bandwidth = 0.0;         // bytes/second
  Nanoseconds estimated_duration_ns = 0;    // expected completion cost
  Nanoseconds communication_cost = 0;       // aggregate cost metric
  ProvenanceStatus provenance = ProvenanceStatus::SYNTHETIC;
  bool valid = false;

  bool operator==(const CommunicationPlan&) const = default;
};

}  // namespace collective_scheduler
