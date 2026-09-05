// Collective Scheduler — dependency current-evidence record.
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

namespace collective_scheduler {

// Readiness for a dependency edge. The Dependency Fabric owns dependency
// traversal; the scheduler consumes only this narrow current binding. A stale
// DependencyGeneration must never unblock current collective work.
struct DependencyState {
  DependencyId id;
  DependencyKind kind = DependencyKind::CUSTOM;
  DependencyGeneration gen;
  bool satisfied = false;
  ProvenanceStatus source = ProvenanceStatus::UNKNOWN;

  bool operator==(const DependencyState&) const = default;
};

}  // namespace collective_scheduler
