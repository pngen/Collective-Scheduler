// Collective Scheduler — readiness model.
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

#include <vector>

namespace collective_scheduler {

// The outcome of a narrow readiness evaluation. Readiness consumes external
// evidence only; it never reimplements Dependency Fabric traversal. UNKNOWN is
// never treated as READY. A READY status plus hard eligibility is required
// before a collective may be ranked.
struct ReadinessResult {
  Readiness status = Readiness::UNKNOWN;
  std::vector<RejectionReason> reasons;       // populated when not READY
  std::vector<ParticipantId> missing_participants;

  bool is_ready() const noexcept { return status == Readiness::READY; }
  bool is_waiting() const noexcept {
    switch (status) {
      case Readiness::WAITING_DEPENDENCY:
      case Readiness::WAITING_PARTICIPANT:
      case Readiness::WAITING_COMMUNICATION_PLAN:
      case Readiness::WAITING_RESERVATION:
      case Readiness::WAITING_RESOURCE:
      case Readiness::WAITING_CAPACITY:
        return true;
      default:
        return false;
    }
  }
  bool is_blocked() const noexcept {
    switch (status) {
      case Readiness::BLOCKED_CONGESTION:
      case Readiness::BLOCKED_POLICY:
        return true;
      default:
        return false;
    }
  }
};

}  // namespace collective_scheduler
