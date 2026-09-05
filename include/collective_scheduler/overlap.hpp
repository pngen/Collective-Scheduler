// Collective Scheduler — overlap model.
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

#include <algorithm>
#include <cstddef>
#include <string>
#include <vector>

namespace collective_scheduler {

// A bounded group of collective requests that the scheduler has determined may
// execute concurrently under explicit evidence. Never claim actual hardware
// concurrency unless execution evidence proves it.
struct OverlapGroup {
  CollectiveGroupId id;
  CollectiveGroupGeneration gen;
  std::vector<CollectiveRequestId> members;  // sorted, unique, non-empty
  OverlapMode mode = OverlapMode::UNKNOWN;
  std::size_t bound = 1;  // max concurrent members

  void normalize() {
    std::sort(members.begin(), members.end());
    members.erase(std::unique(members.begin(), members.end()), members.end());
  }
};

// The result of evaluating whether a candidate may overlap with an existing
// overlap set under current evidence.
struct OverlapDecision {
  bool allowed = false;
  OverlapMode mode = OverlapMode::UNKNOWN;
  std::size_t resulting_group_size = 0;
  std::string reason;
};

}  // namespace collective_scheduler
