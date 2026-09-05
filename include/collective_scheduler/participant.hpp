// Collective Scheduler — participant domain record.
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

#include <array>
#include <optional>
#include <vector>

namespace collective_scheduler {

// A participant is a first-class actor in a collective. It binds an identity
// and a fresh generation to a concrete worker boot and the physical resources it
// uses. A participant process restart produces a fresh WorkerBootId and a fresh
// ParticipantGeneration; that is the authoritative liveness change the scheduler
// must honor.
//
// Participant liveness is NOT durable authority. A published readiness bool
// must be re-published after each boot.
struct Participant {
  ParticipantId id;
  ParticipantGeneration gen;
  WorkerId worker;
  WorkerBootId worker_boot;
  SourceId source;
  SourceBootId source_boot;

  DeviceId device;
  NodeId node;
  NicId nic;

  CapabilityGeneration capability_gen;
  HealthGeneration health_gen;
  TopologyGeneration topology_gen;

  // Non-authoritative diagnostic only (never persisted as authority).
  std::uint32_t process_id = 0;

  bool ready = false;
  ProvenanceStatus readiness_source = ProvenanceStatus::UNKNOWN;

  ProvenanceStatus provenance = ProvenanceStatus::UNKNOWN;

  bool operator==(const Participant&) const = default;
};

// A participant group is a stable named set of participant identities. Group
// generations let a group definition change without silently inheriting old
// members.
struct ParticipantGroup {
  ParticipantGroupId id;
  ParticipantGroupGeneration gen;
  std::vector<ParticipantId> members;  // sorted, unique, validated

  bool operator==(const ParticipantGroup&) const = default;
};

}  // namespace collective_scheduler
