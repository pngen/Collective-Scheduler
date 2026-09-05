// Collective Scheduler — reservation current-evidence record.
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

// A bandwidth / resource reservation. The scheduler never creates reservations;
// it consumes the current reservation evidence for scheduling and ordering.
// A stale ReservationGeneration must not protect or free bandwidth.
struct Reservation {
  ReservationGeneration gen;
  ReservationStrength strength = ReservationStrength::NONE;
  double reserved_bandwidth = 0.0;   // bytes/second reserved
  std::vector<ResourceId> resources;
  std::vector<LinkId> links;
  Nanoseconds window_start_ns = 0;
  Nanoseconds window_end_ns = 0;
  ProvenanceStatus provenance = ProvenanceStatus::REPORTED;
  bool valid = false;

  bool operator==(const Reservation&) const = default;
};

}  // namespace collective_scheduler
