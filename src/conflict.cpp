// Collective Scheduler — conflict detection (implementation).
//
// Collective Scheduler 1.0.0
// Copyright 2026 Summon Software Labs.

#include <collective_scheduler/conflict.hpp>
#include <collective_scheduler/deterministic.hpp>

namespace collective_scheduler {

namespace {
template <typename T>
bool set_intersect(const std::vector<T>& a, const std::vector<T>& b) noexcept {
  auto i = a.begin(); auto j = b.begin();
  while (i != a.end() && j != b.end()) {
    if (*i < *j) ++i;
    else if (*j < *i) ++j;
    else return true;
  }
  return false;
}
}  // namespace

void Footprint::normalize() {
  sort_unique(participants);
  sort_unique_ids(resources);
  sort_unique_links(links);
  sort_unique_strings(exclusion_classes);
}

bool intersect_nonempty(const std::vector<ParticipantId>& a, const std::vector<ParticipantId>& b) noexcept {
  return set_intersect(a, b);
}
bool intersect_nonempty(const std::vector<ResourceId>& a, const std::vector<ResourceId>& b) noexcept {
  return set_intersect(a, b);
}
bool intersect_nonempty(const std::vector<LinkId>& a, const std::vector<LinkId>& b) noexcept {
  return set_intersect(a, b);
}
bool intersect_nonempty(const std::vector<std::string>& a, const std::vector<std::string>& b) noexcept {
  return set_intersect(a, b);
}

Footprint make_footprint(const std::vector<ParticipantId>& participants,
                         const std::vector<ResourceClaim>& resource_claims,
                         const CommunicationPlan* plan) {
  Footprint fp;
  fp.participants = participants;
  for (const auto& c : resource_claims) fp.resources.push_back(c.resource);
  if (plan) {
    fp.resources.insert(fp.resources.end(), plan->resources.begin(), plan->resources.end());
    fp.links = plan->links;
    fp.footprint_unknown = !plan->valid;
  } else {
    fp.footprint_unknown = true;  // missing plan = unknown footprint
  }
  fp.normalize();
  return fp;
}

ConflictResult classify_conflict(const Footprint& a, const Footprint& b) {
  ConflictResult r;
  if (intersect_nonempty(a.participants, b.participants)) {
    r.classification = ConflictClassification::PARTICIPANT_CONFLICT;
    r.severity = ConflictSeverity::FATAL;
    r.reasons.push_back("participant overlap");
    return r;
  }
  if (intersect_nonempty(a.resources, b.resources)) {
    r.classification = ConflictClassification::RESOURCE_CONFLICT;
    r.severity = ConflictSeverity::HIGH;
    r.reasons.push_back("resource overlap");
    return r;
  }
  if (intersect_nonempty(a.links, b.links)) {
    r.classification = ConflictClassification::LINK_CONFLICT;
    r.severity = ConflictSeverity::HIGH;
    r.reasons.push_back("link overlap");
    return r;
  }
  if (intersect_nonempty(a.exclusion_classes, b.exclusion_classes)) {
    r.classification = ConflictClassification::POLICY_CONFLICT;
    r.severity = ConflictSeverity::HIGH;
    r.reasons.push_back("exclusion class overlap");
    return r;
  }
  if (a.footprint_unknown || b.footprint_unknown) {
    r.classification = ConflictClassification::UNKNOWN;
    r.severity = ConflictSeverity::HIGH;
    r.reasons.push_back("unknown footprint evidence");
    return r;
  }
  r.classification = ConflictClassification::NO_CONFLICT;
  r.severity = ConflictSeverity::NONE;
  return r;
}

}  // namespace collective_scheduler