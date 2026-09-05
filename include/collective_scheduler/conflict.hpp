// Collective Scheduler — conflict model.
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
#include <collective_scheduler/plan.hpp>
#include <collective_scheduler/request.hpp>

#include <string>
#include <vector>

namespace collective_scheduler {

// Converged resource/link/participant footprint of a collective, used for
// conflict detection. Participants come from the request; resources and links
// come from the request's resource claims and its current communication plan.
// Footprints are always normalized to sorted-unique.
struct Footprint {
  std::vector<ParticipantId> participants;
  std::vector<ResourceId> resources;
  std::vector<LinkId> links;
  std::vector<std::string> exclusion_classes;
  bool footprint_unknown = false;  // any overlapping dimension unknown

  void normalize();
  bool empty() const noexcept {
    return participants.empty() && resources.empty() && links.empty() &&
           exclusion_classes.empty();
  }
};

// The result of comparing two collectives' footprints.
struct ConflictResult {
  ConflictClassification classification = ConflictClassification::NO_CONFLICT;
  ConflictSeverity severity = ConflictSeverity::NONE;
  std::vector<std::string> reasons;

  bool has_conflict() const noexcept { return classification != ConflictClassification::NO_CONFLICT; }
  bool conflict_is_unknown() const noexcept { return classification == ConflictClassification::UNKNOWN; }
  // True when the two collectives must NOT safely run concurrently. UNKNOWN
  // conflict evidence always returns true (conservative).
  bool forbids_overlap() const noexcept {
    if (classification == ConflictClassification::NO_CONFLICT) return false;
    if (classification == ConflictClassification::UNKNOWN) return true;
    return severity >= ConflictSeverity::HIGH;
  }
};

// Compute the footprint of a request from its participant set and resource
// claims, merged with the resource/link/path footprint of its current plan.
Footprint make_footprint(const std::vector<ParticipantId>& participants,
                         const std::vector<ResourceClaim>& resource_claims,
                         const CommunicationPlan* plan);

// Classify the pairwise conflict between two footprints. Deterministic: given
// identical footprints, the classification is identical.
ConflictResult classify_conflict(const Footprint& a, const Footprint& b);

bool intersect_nonempty(const std::vector<ParticipantId>& a, const std::vector<ParticipantId>& b) noexcept;
bool intersect_nonempty(const std::vector<ResourceId>& a, const std::vector<ResourceId>& b) noexcept;
bool intersect_nonempty(const std::vector<LinkId>& a, const std::vector<LinkId>& b) noexcept;
bool intersect_nonempty(const std::vector<std::string>& a, const std::vector<std::string>& b) noexcept;

}  // namespace collective_scheduler
