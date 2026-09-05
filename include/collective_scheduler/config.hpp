// Collective Scheduler — resource bounds and policy configuration.
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

#include <cstdint>

namespace collective_scheduler {

// Hard resource bounds. All externally supplied counts are checked against
// these before any allocation. These are substantive limits, not suggestions.
struct Bounds {
  std::size_t max_collectives = 100000;          // total tracked collective requests
  std::size_t max_participants = 100000;         // total registered participants
  std::size_t max_participants_per_collective = 4096;
  std::size_t max_ready_queue_size = 100000;
  std::size_t max_active_grants = 4096;
  std::size_t max_overlap_group = 4096;
  std::size_t max_overlap_group_size = 4096;
  std::size_t max_conflict_edges = 2000000;
  std::size_t max_policy_classes = 256;
  std::size_t max_history_records = 100000;
  std::size_t max_workers = 4096;
  std::size_t max_protocol_payload = 16 * 1024 * 1024;   // 16 MiB frame
  std::size_t max_pending_frames = 4096;
  std::size_t max_explanation_size = 4096;
  std::size_t max_paths_per_plan = 4096;
  std::size_t max_dependencies_per_request = 4096;
};

// Policy grades that govern scheduling behavior. Deterministic, explicit, and
// versioned by PolicyGeneration.
struct PolicyConfig {
  // Deterministic arbitration weights (must all be non-negative).
  double w_priority = 100.0;
  double w_deadline = 200.0;
  double w_age = 1.0;
  double w_fairness_deficit = 120.0;
  double w_starvation_risk = 150.0;
  double w_expected_duration = 0.5;   // negative weight: shorter first
  double w_congestion_penalty = 80.0;
  double w_reservation_fit = 90.0;
  double w_communication_cost = 0.5;
  double w_resource_footprint = 0.5;
  double w_participant_availability = 40.0;

  // Fairness / starvation prevention.
  std::uint64_t fairness_window_grants = 64;   // grants over which share is measured
  double protected_min_share = 0.05;           // minimum share for a protected class
  std::uint64_t max_bypass_count = 16;         // max consecutive bypasses before promotion
  double aging_growth = 0.05;                  // added fairness deficit share per bypass
  std::uint64_t minimum_service_interval = 1;  // minimum grants before repeat selector

  // Overlap.
  std::uint64_t max_overlap_group = 2;
  bool overlap_enabled = true;

  // Recovery.
  std::size_t max_epochs_retained = 16;

  // Bounds.
  Bounds bounds;
};

}  // namespace collective_scheduler
