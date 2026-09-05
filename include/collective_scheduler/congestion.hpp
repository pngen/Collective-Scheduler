// Collective Scheduler — congestion current-evidence record.
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

#include <cstdint>
#include <vector>

namespace collective_scheduler {

// Live congestion evidence published by the Congestion Fabric. The scheduler
// consumes it for hard-blocking, overlap limitation, and ordering; it never
// measures congestion itself.
//
// A stale CongestionGeneration must not influence current ordering as fresh
// evidence.
struct CongestionState {
  CongestionGeneration gen;
  double utilization = 0.0;       // [0,1]
  double residual_bandwidth = 0.0;  // bytes/second remaining
  bool is_congested = false;
  bool hard_ceiling = false;      // if true, hard policy ceiling applies
  double hard_ceiling_utilization = 1.0;
  bool backpressure_intent = false;
  std::vector<LinkId> bottlenecks;
  ProvenanceStatus provenance = ProvenanceStatus::REPORTED;

  bool operator==(const CongestionState&) const = default;
};

}  // namespace collective_scheduler
