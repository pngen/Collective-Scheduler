// Collective Scheduler — capacity current-evidence record.
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

namespace collective_scheduler {

// Current usable capacity published by the Capacity Fabric. FORECAST capacity
// is never treated as guaranteed present capacity.
struct CapacityState {
  CapacityGeneration gen;
  double usable_capacity = 0.0;   // bytes/second or units
  double headroom = 0.0;
  double confidence = 0.0;        // [0,1]
  ProvenanceStatus provenance = ProvenanceStatus::REPORTED;

  bool operator==(const CapacityState&) const = default;
};

}  // namespace collective_scheduler
