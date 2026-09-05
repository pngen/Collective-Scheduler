// Collective Scheduler — temporal arbitration runtime (implementation).
//
// Collective Scheduler 1.0.0
// Copyright 2026 Summon Software Labs.

#include <collective_scheduler/scheduler.hpp>

#include <collective_scheduler/deterministic.hpp>
#include <collective_scheduler/error.hpp>
#include <collective_scheduler/persistence.hpp>

#include <algorithm>
#include <cmath>
#include <deque>
#include <map>
#include <optional>
#include <set>
#include <tuple>
#include <unordered_map>
#include <utility>

namespace collective_scheduler {

using L = LifecycleState;

// ---------------------------------------------------------------------------
// Scheduler internal state.
// ---------------------------------------------------------------------------
struct Scheduler::Record {
  CollectiveRequest request;
  Lifecycle lifecycle;
  Readiness last_readiness;
  std::uint64_t enqueue_seq = 0;
  std::uint64_t age_ticks = 0;
  FairnessAccount fairness;
  std::vector<RejectionReason> eligibility_reasons;
  bool hard_eligible = false;
  bool in_ready_queue = false;
  Footprint footprint;
  std::optional<CollectiveGrant> current_grant;
  CoordinatorEpoch grant_epoch;
};

struct Scheduler::GrantState {
  CollectiveGrant grant;
  bool handed_off = false;
  bool active = false;
  bool terminal = false;
  WorkerBootId completed_by;
  SourceBootId completed_by_source;
};

class Scheduler::Impl {
 public:
  mutable std::shared_mutex mu;
  CoordinatorEpoch epoch;
  LogicalTime now;
  PolicyConfig policy;
  std::shared_ptr<ResourceBrokerPort> broker;
  std::shared_ptr<CollectiveFabricPort> fabric;
  std::shared_ptr<PreemptionFabricPort> preemption;

  std::unordered_map<CollectiveRequestId, Record> records;
  std::unordered_map<ParticipantId, Participant> participants;
  std::unordered_map<CommunicationPlanId, CommunicationPlan> plans;
  std::unordered_map<DependencyId, DependencyState> deps;
  std::map<ReservationGeneration, Reservation> reservations;
  std::optional<CongestionState> congestion;
  std::optional<CapacityState> capacity;
  std::unordered_map<CollectiveGrantId, GrantState> grants;
  std::unordered_map<ParticipantId, std::set<CollectiveRequestId>> participant_requests;

  std::uint64_t next_seq = 0;
  CollectiveRequestId next_request_id{1};
  CollectiveScheduleId next_schedule_id{1};
  CollectiveGrantId next_grant_id{1};
  CollectiveGroupId next_group_id{1};
  CollectiveScheduleGeneration next_schedule_gen{1};
  CollectiveGrantGeneration next_grant_gen{1};
  CollectiveGroupGeneration next_group_gen{1};

  std::size_t active_grants = 0;
  std::size_t queued_count = 0;
  std::size_t ready_count = 0;
  std::size_t eligible_count = 0;
  std::size_t retired_count = 0;
  std::size_t schedule_counter = 0;

  CapacityGeneration capacity_gen;
  CongestionGeneration congestion_gen;
  ReservationGeneration reservation_gen;
  PolicyGeneration policy_gen;
  PriorityGeneration priority_gen{1};
  FairnessGeneration fairness_gen{1};
  TopologyGeneration topology_gen;
  CapabilityGeneration capability_gen;
  HealthGeneration health_gen;

  const CommunicationPlan* plan(const CommunicationPlanId& id) const {
    auto it = plans.find(id);
    return it == plans.end() ? nullptr : &it->second;
  }
  const Participant* participant(const ParticipantId& id) const {
    auto it = participants.find(id);
    return it == participants.end() ? nullptr : &it->second;
  }

  Readiness compute_readiness(Record& r) const;
  bool hard_eligible(const Record& r, std::vector<RejectionReason>& reasons) const;
  std::vector<double> ranking_key(const Record& r) const;
  EvidenceSnapshot make_snapshot() const;
  GrantAdjudication revalidate_grant(const CollectiveGrant& g);
  bool can_overlap(const Record& a, const Record& b) const;
  void collect_priority_inversions(ScheduleDecision& dec) const;
  ScheduleDecision schedule();
  std::size_t active_conflicts(const Record& r) const;
  CollectiveGrant create_grant(Record& r, const EvidenceSnapshot& snap, const ScheduleDecision& dec);
  void promote_to_granted(Record& r) const;
};

namespace {
// Saturating checked add for counts. Returns false on overflow.
bool checked_add(std::uint64_t a, std::uint64_t b, std::uint64_t& out) noexcept {
  if (a > (std::numeric_limits<std::uint64_t>::max)() - b) return false;
  out = a + b; return true;
}

bool finite_double(double v) noexcept { return std::isfinite(v); }

void validate_request(const CollectiveRequest& req, const PolicyConfig& p) {
  using namespace collective_scheduler;
  if (req.kind == CollectiveKind::UNKNOWN)
    throw_malformed("collective kind UNKNOWN has no supported execution semantics");
  if (req.participants.empty()) throw_malformed("empty participant set");
  if (req.participants.size() > p.bounds.max_participants_per_collective)
    throw_bounds("participants exceed per-collective bound");
  auto dup = req.participants;
  sort_unique(dup);
  if (dup.size() != req.participants.size()) throw_malformed("duplicate participants");
  if (req.participant_gens.size() != req.participants.size())
    throw_malformed("participant generation count mismatch");
  if (!req.per_participant_payload.empty() && req.per_participant_payload.size() != req.participants.size())
    throw_malformed("per-participant payload count mismatch");
  if (req.plan_id.null()) throw_malformed("missing communication plan id");
  if (req.plan_gen.null()) throw_malformed("missing communication plan generation");
  if (req.window.latest_start_ns != 0 && req.window.earliest_start_ns > req.window.latest_start_ns)
    throw_malformed("impossible scheduling window");
  if (req.window.deadline_ns != 0 && req.window.deadline_ns < req.window.earliest_start_ns)
    throw_malformed("impossible scheduling window: deadline before earliest start");
  if (req.window.deadline_ns != 0 && req.window.latest_start_ns != 0 &&
      req.window.latest_start_ns > req.window.deadline_ns)
    throw_malformed("impossible scheduling window: latest start after deadline");
  for (const auto& rc : req.resource_claims)
    if (!finite_double(rc.units)) throw_malformed("NaN/Inf resource claim units");
  if (req.resource_claims.size() > p.bounds.max_participants_per_collective)
    throw_bounds("resource claims exceed bound");
  if (req.dependencies.size() > p.bounds.max_dependencies_per_request)
    throw_bounds("dependencies exceed bound");
  auto ec = req.exclusion_classes;
  sort_unique_strings(ec);
  if (ec.size() != req.exclusion_classes.size())
    throw_malformed("duplicate exclusion class");
  // overflow guard on aggregate payload (no overflow allowed).
  std::uint64_t sum = 0;
  for (auto v : req.per_participant_payload) {
    std::uint64_t t;
    if (!checked_add(sum, v, t)) throw_overflow("per-participant payload sum overflow");
    sum = t;
  }
}
}  // namespace

// ---------------------------------------------------------------------------
// Constructor / destructor.
// ---------------------------------------------------------------------------
Scheduler::Scheduler(Options opts) : impl_(std::make_shared<Impl>()) {
  impl_->policy = opts.policy;
  impl_->broker = opts.broker ? opts.broker : std::make_shared<AlwaysGrantingResourceBroker>();
  impl_->fabric = opts.fabric ? opts.fabric : std::make_shared<RecordingCollectiveFabric>();
  impl_->preemption = opts.preemption;
  impl_->epoch = opts.start_epoch;
}
Scheduler::~Scheduler() = default;

namespace {
bool is_terminal_state(LifecycleState s) noexcept {
  return s == LifecycleState::COMPLETED || s == LifecycleState::CANCELLED ||
         s == LifecycleState::SUPERSEDED || s == LifecycleState::STALE ||
         s == LifecycleState::RETIRED;
}
constexpr RevalidationRequirements kDefaultRevalidation;
}  // namespace

// ---------------------------------------------------------------------------
// Impl algorithm methods.
// ---------------------------------------------------------------------------
Readiness Scheduler::Impl::compute_readiness(Record& r) const {
  const auto& req = r.request;
  bool any_missing = false;
  bool any_stale = false;
  for (std::size_t i = 0; i < req.participants.size(); ++i) {
    auto it = participants.find(req.participants[i]);
    if (it == participants.end()) { any_missing = true; continue; }
    if (it->second.gen.value != req.participant_gens[i].value) { any_stale = true; continue; }
    if (!it->second.ready || it->second.readiness_source == ProvenanceStatus::UNKNOWN) {
      any_missing = true;
    }
  }
  if (any_stale) return Readiness::STALE;
  if (any_missing) return Readiness::WAITING_PARTICIPANT;
  auto pl = plan(req.plan_id);
  if (!pl) return Readiness::WAITING_COMMUNICATION_PLAN;
  if (pl->gen.value != req.plan_gen.value) return Readiness::STALE;
  if (!pl->valid) return Readiness::WAITING_COMMUNICATION_PLAN;
  for (const auto& d : req.dependencies) {
    auto it = deps.find(d.id);
    if (it == deps.end()) return Readiness::WAITING_DEPENDENCY;
    if (it->second.gen.value != d.gen.value) return Readiness::STALE;
    if (!it->second.satisfied) return Readiness::WAITING_DEPENDENCY;
  }
  if (req.reservation_gen) {
    if (*req.reservation_gen != reservation_gen) return Readiness::STALE;
    auto it = reservations.find(*req.reservation_gen);
    if (it == reservations.end()) return Readiness::WAITING_RESERVATION;
    if (!it->second.valid) return Readiness::WAITING_RESERVATION;
  }
  if (congestion && congestion->hard_ceiling &&
      congestion->utilization >= congestion->hard_ceiling_utilization)
    return Readiness::BLOCKED_CONGESTION;
  if (capacity) {
    double demand = 0.0;
    for (const auto& rc : req.resource_claims) demand += rc.units;
    if (demand > capacity->usable_capacity) return Readiness::WAITING_CAPACITY;
  }
  return Readiness::READY;
}

bool Scheduler::Impl::hard_eligible(const Record& r, std::vector<RejectionReason>& reasons) const {
  reasons.clear();
  const auto& req = r.request;
  if (r.last_readiness != Readiness::READY) {
    switch (r.last_readiness) {
      case Readiness::STALE: reasons.push_back(RejectionReason::STALE_COLLECTIVE_GENERATION); break;
      case Readiness::BLOCKED_CONGESTION: reasons.push_back(RejectionReason::HARD_CONGESTION_CEILING); break;
      case Readiness::BLOCKED_POLICY: reasons.push_back(RejectionReason::EXCLUSION_POLICY); break;
      default: reasons.push_back(RejectionReason::UNKNOWN); break;
    }
    return false;
  }
  if (req.window.earliest_start_ns != 0 && now.value() < req.window.earliest_start_ns) {
    reasons.push_back(RejectionReason::INVALID_SCHEDULING_WINDOW); return false;
  }
  if (req.window.latest_start_ns != 0 && now.value() > req.window.latest_start_ns) {
    reasons.push_back(RejectionReason::INVALID_SCHEDULING_WINDOW); return false;
  }
  if (req.window.deadline_ns != 0 &&
      now.value() + req.window.expected_duration_ns > req.window.deadline_ns) {
    reasons.push_back(RejectionReason::INVALID_SCHEDULING_WINDOW); return false;
  }
  for (std::size_t i = 0; i < req.participants.size(); ++i) {
    auto it = participants.find(req.participants[i]);
    if (it == participants.end()) { reasons.push_back(RejectionReason::MISSING_PARTICIPANT); return false; }
    if (it->second.gen.value != req.participant_gens[i].value) {
      reasons.push_back(RejectionReason::STALE_PARTICIPANT_GENERATION); return false;
    }
    if (!it->second.ready || it->second.readiness_source == ProvenanceStatus::UNKNOWN) {
      reasons.push_back(RejectionReason::MISSING_PARTICIPANT); return false;
    }
  }
  auto pl = plan(req.plan_id);
  if (!pl || pl->gen.value != req.plan_gen.value || !pl->valid) {
    reasons.push_back(RejectionReason::INVALID_COMMUNICATION_PLAN); return false;
  }
  for (const auto& d : req.dependencies) {
    auto it = deps.find(d.id);
    if (it == deps.end() || it->second.gen.value != d.gen.value || !it->second.satisfied) {
      reasons.push_back(RejectionReason::UNSATISFIED_DEPENDENCY); return false;
    }
  }
  if (req.reservation_gen) {
    if (*req.reservation_gen != reservation_gen) { reasons.push_back(RejectionReason::INVALID_RESERVATION); return false; }
    auto it = reservations.find(*req.reservation_gen);
    if (it == reservations.end() || !it->second.valid) { reasons.push_back(RejectionReason::INVALID_RESERVATION); return false; }
  }
  if (congestion && congestion->hard_ceiling &&
      congestion->utilization >= congestion->hard_ceiling_utilization) {
    reasons.push_back(RejectionReason::HARD_CONGESTION_CEILING); return false;
  }
  if (req.workload_gen.null()) {
    reasons.push_back(RejectionReason::NO_EXECUTION_AUTHORITY); return false;
  }
  return true;
}

EvidenceSnapshot Scheduler::Impl::make_snapshot() const {
  EvidenceSnapshot s;
  s.epoch = epoch;
  s.policy_gen = policy_gen;
  s.priority_gen = priority_gen;
  s.fairness_gen = fairness_gen;
  s.congestion_gen = congestion_gen;
  s.capacity_gen = capacity_gen;
  s.reservation_gen = reservation_gen;
  s.topology_gen = topology_gen;
  s.capability_gen = capability_gen;
  s.health_gen = health_gen;
  return s;
}

std::vector<double> Scheduler::Impl::ranking_key(const Record& r) const {
  const auto& req = r.request;
  const PolicyConfig& p = policy;
  std::vector<double> k;
  k.reserve(11);
  // [0] deadline urgency (larger when deadline nearer).
  double deadline = 0.0;
  if (req.window.deadline_ns != 0 && now.value() < req.window.deadline_ns) {
    Nanoseconds rem = req.window.deadline_ns - now.value();
    double slack = static_cast<double>(rem) / 1e9;
    deadline = 1.0 / (1.0 + slack);
  }
  k.push_back(deadline * p.w_deadline);
  // [1] external priority.
  k.push_back(static_cast<double>(req.priority.value) / 100.0 * p.w_priority);
  // [2] starvation pressure, with an explicit guaranteed anti-starvation
  // promotion after max_bypass_count consecutive bypasses (a bounded,
  // deterministic starvation-prevention guarantee that never violates hard
  // safety — the collective must still be hard-eligible to be ranked at all).
  FairnessPolicy fp{0.05, p.aging_growth, p.max_bypass_count, p.fairness_window_grants, true};
  double starv = starvation_pressure(r.fairness, fp);
  if (r.fairness.consecutive_bypasses >= p.max_bypass_count) starv = 1000.0;
  k.push_back(starv * p.w_starvation_risk);
  // [3] fairness deficit.
  double dfc = r.fairness.deficit; if (dfc > 1.0) dfc = 1.0;
  k.push_back(dfc * p.w_fairness_deficit);
  // [4] age.
  k.push_back(static_cast<double>(r.age_ticks) / 1000.0 * p.w_age);
  // [5] expected duration (shorter better -> negative).
  k.push_back(-static_cast<double>(req.window.expected_duration_ns) / 1e9 * p.w_expected_duration);
  // [6] reservation fit.
  double resfit = 0.0;
  if (req.reservation_gen && reservations.count(*req.reservation_gen)) resfit = 1.0;
  k.push_back(resfit * p.w_reservation_fit);
  // [7] communication cost (lower better -> negative).
  k.push_back(-static_cast<double>(req.window.expected_duration_ns) / 1e9 * p.w_communication_cost);
  // [8] resource footprint (smaller better -> negative).
  k.push_back(-static_cast<double>(req.resource_claims.size()) * p.w_resource_footprint);
  // [9] congestion penalty (higher utilization penalizes).
  double cong = 0.0;
  if (congestion) cong = congestion->utilization;
  k.push_back(-cong * p.w_congestion_penalty);
  // [10] participant availability (fully available = 1).
  k.push_back(1.0 * p.w_participant_availability);
  return k;
}

GrantAdjudication Scheduler::Impl::revalidate_grant(const CollectiveGrant& g) {
  auto rit = records.find(g.request);
  if (rit == records.end()) return GrantAdjudication::STALE_GRANT;
  const Record& rec = rit->second;
  if (rec.request.gen.value != g.request_gen.value) return GrantAdjudication::SUPERSEDED;
  if (rec.request.collective_gen.value != g.collective_gen.value) return GrantAdjudication::STALE_GRANT;
  if (g.evidence.epoch.value != epoch.value) return GrantAdjudication::EPOCH_MISMATCH;
  if (rec.lifecycle.state() == LifecycleState::CANCELLED) return GrantAdjudication::CANCELLED;
  if (rec.lifecycle.state() == LifecycleState::SUPERSEDED) return GrantAdjudication::SUPERSEDED;
  if (rec.lifecycle.state() == LifecycleState::STALE) return GrantAdjudication::STALE_GRANT;
  if (g.revalidation.require_fresh_plan) {
    auto pl = plan(g.plan);
    if (!pl || pl->gen.value != g.plan_gen.value || !pl->valid) return GrantAdjudication::STALE_GRANT;
  }
  if (g.revalidation.require_fresh_participants) {
    if (g.participants.size() != rec.request.participants.size()) return GrantAdjudication::STALE_GRANT;
    for (std::size_t i = 0; i < g.participants.size(); ++i) {
      auto it = participants.find(g.participants[i]);
      if (it == participants.end()) return GrantAdjudication::STALE_GRANT;
      if (it->second.gen.value != g.participant_gens[i].value) return GrantAdjudication::STALE_GRANT;
      if (!it->second.ready || it->second.readiness_source == ProvenanceStatus::UNKNOWN)
        return GrantAdjudication::STALE_GRANT;
    }
  }
  if (g.revalidation.require_fresh_dependencies) {
    for (const auto& d : rec.request.dependencies) {
      auto it = deps.find(d.id);
      if (it == deps.end() || it->second.gen.value != d.gen.value) return GrantAdjudication::STALE_GRANT;
    }
  }
  if (g.revalidation.require_fresh_reservation && rec.request.reservation_gen) {
    if (*rec.request.reservation_gen != reservation_gen) return GrantAdjudication::STALE_GRANT;
  }
  if (g.revalidation.require_fresh_congestion && congestion &&
      congestion->gen.value != g.evidence.congestion_gen.value)
    return GrantAdjudication::STALE_GRANT;
  if (rec.lifecycle.state() == LifecycleState::COMPLETED) return GrantAdjudication::ALREADY_TERMINAL;
  return GrantAdjudication::VALID;
}

bool Scheduler::Impl::can_overlap(const Record& a, const Record& b) const {
  if (a.request.overlap == OverlapMode::UNKNOWN || b.request.overlap == OverlapMode::UNKNOWN) return false;
  if (a.request.overlap == OverlapMode::EXCLUSIVE || b.request.overlap == OverlapMode::EXCLUSIVE) return false;
  auto cr = classify_conflict(a.footprint, b.footprint);
  if (cr.forbids_overlap()) return false;
  if (a.request.reservation_gen && b.request.reservation_gen &&
      a.request.reservation_gen == b.request.reservation_gen) return false;
  if (congestion) {
    double bw = 0.0;
    auto pa = plan(a.request.plan_id);
    auto pb = plan(b.request.plan_id);
    if (pa) bw += pa->estimated_bandwidth;
    if (pb) bw += pb->estimated_bandwidth;
    if (bw > congestion->residual_bandwidth) return false;
    if (congestion->hard_ceiling && congestion->utilization >= congestion->hard_ceiling_utilization) return false;
  }
  return true;
}

std::size_t Scheduler::Impl::active_conflicts(const Record& r) const {
  std::size_t n = 0;
  for (const auto& [gid, gs] : grants) {
    (void)gid;
    if (!gs.active || gs.terminal) continue;
    auto it = records.find(gs.grant.request);
    if (it == records.end()) continue;
    auto cr = classify_conflict(r.footprint, it->second.footprint);
    if (cr.forbids_overlap()) ++n;
  }
  return n;
}

void Scheduler::Impl::promote_to_granted(Record& r) const {
  switch (r.lifecycle.state()) {
    case LifecycleState::READY: r.lifecycle.transition(LifecycleState::ELIGIBLE); [[fallthrough]];
    case LifecycleState::ELIGIBLE: r.lifecycle.transition(LifecycleState::QUEUED); [[fallthrough]];
    case LifecycleState::QUEUED: r.lifecycle.transition(LifecycleState::GRANTED); break;
    case LifecycleState::GRANTED: break;
    default: throw_transition("cannot promote to granted");
  }
  r.in_ready_queue = true;
}

CollectiveGrant Scheduler::Impl::create_grant(Record& r, const EvidenceSnapshot& snap, const ScheduleDecision& dec) {
  CollectiveGrant g;
  g.id = next_grant_id++;
  g.gen = next_grant_gen++;
  g.request = r.request.id;
  g.request_gen = r.request.gen;
  g.collective = r.request.collective;
  g.collective_gen = r.request.collective_gen;
  g.schedule = dec.id;
  g.schedule_gen = dec.gen;
  g.participants = r.request.participants;
  g.participant_gens = r.request.participant_gens;
  g.plan = r.request.plan_id;
  g.plan_gen = r.request.plan_gen;
  g.evidence = snap;
  g.evidence.request_gen = r.request.gen;
  g.evidence.plan_gen = r.request.plan_gen;
  g.evidence.collective_gen = r.request.collective_gen;
  if (!g.participant_gens.empty()) {
    ParticipantGeneration m = g.participant_gens.front();
    for (auto v : g.participant_gens) if (v.value > m.value) m = v;
    g.evidence.participant_gen = m;
  }
  DependencyGeneration dm = DependencyGeneration(0);
  for (const auto& d : r.request.dependencies) if (d.gen.value > dm.value) dm = d.gen;
  g.evidence.dependency_gen = dm;
  g.revalidation = kDefaultRevalidation;
  g.workload = r.request.workload;
  g.execution_attempt = ExecutionId(0);
  g.execution_attempt_gen = ExecutionGeneration(0);
  g.policy_gen = policy_gen;
  r.current_grant = g;
  grants.emplace(g.id, GrantState{g, false, false, false, WorkerBootId(0), SourceBootId(0)});
  return g;
}

void Scheduler::Impl::collect_priority_inversions(ScheduleDecision& dec) const {
  for (auto req_id : dec.fallback) {
    auto rit = records.find(req_id);
    if (rit == records.end()) continue;
    const Record& req_rec = rit->second;
    if (!req_rec.request.priority.authoritative) continue;
    for (const auto& [gid, gs] : grants) {
      (void)gid;
      if (!gs.active || gs.terminal) continue;
      auto hit = records.find(gs.grant.request);
      if (hit == records.end()) continue;
      const Record& holder = hit->second;
      if (holder.request.priority.value >= req_rec.request.priority.value) continue;
      auto cr = classify_conflict(req_rec.footprint, holder.footprint);
      if (!cr.forbids_overlap()) continue;
      PriorityInversionEvent ev;
      ev.holder = holder.request.id;
      ev.requester = req_rec.request.id;
      ev.holder_priority = holder.request.priority.value;
      ev.requester_priority = req_rec.request.priority.value;
      ev.evidence = make_snapshot();
      bool urgent = req_rec.request.window.deadline_ns != 0 &&
                    req_rec.request.window.deadline_ns <= now.value() + req_rec.request.window.expected_duration_ns * 2;
      ev.action = urgent ? PriorityInversionAction::REQUEST_PREEMPTION : PriorityInversionAction::DRAIN_CURRENT;
      dec.priority_inversions.push_back(ev);
    }
  }
}
ScheduleDecision Scheduler::Impl::schedule() {
  ScheduleDecision dec;
  dec.id = next_schedule_id++;
  dec.gen = next_schedule_gen++;
  dec.evidence = make_snapshot();
  dec.revalidation = kDefaultRevalidation;

  // Recompute readiness everywhere; collect candidates.
  std::vector<CollectiveRequestId> candidates;
  for (auto& [id, rec] : records) {
    (void)id;
    if (is_terminal_state(rec.lifecycle.state())) continue;
    rec.age_ticks++;
    rec.footprint = make_footprint(rec.request.participants, rec.request.resource_claims, plan(rec.request.plan_id));
    Readiness rd = compute_readiness(rec);
    rec.last_readiness = rd;
    rec.hard_eligible = false;
    rec.eligibility_reasons.clear();
    // Derive lifecycle from readiness (guarded, conservative).
    switch (rec.lifecycle.state()) {
      case LifecycleState::DECLARED:
        if (rd == Readiness::READY) rec.lifecycle.transition(LifecycleState::READY);
        else { rec.lifecycle.transition(LifecycleState::WAITING); rec.last_readiness = rd; }
        break;
      case LifecycleState::WAITING:
        if (rd == Readiness::READY) rec.lifecycle.transition(LifecycleState::READY);
        else if (rd == Readiness::STALE) { rec.lifecycle.transition(LifecycleState::REVALIDATION_REQUIRED); continue; }
        break;
      case LifecycleState::READY:
        if (rd != Readiness::READY && rd == Readiness::STALE) { rec.lifecycle.transition(LifecycleState::REVALIDATION_REQUIRED); continue; }
        break;
      case LifecycleState::ELIGIBLE:
      case LifecycleState::QUEUED:
        if (rd == Readiness::STALE) { rec.lifecycle.transition(LifecycleState::REVALIDATION_REQUIRED); continue; }
        if (rd == Readiness::WAITING_DEPENDENCY || rd == Readiness::WAITING_PARTICIPANT ||
            rd == Readiness::WAITING_COMMUNICATION_PLAN || rd == Readiness::WAITING_RESERVATION ||
            rd == Readiness::WAITING_RESOURCE || rd == Readiness::WAITING_CAPACITY) {
          rec.lifecycle.transition(LifecycleState::WAITING); continue;
        }
        break;
      default: break;
    }
    if (rec.lifecycle.state() == LifecycleState::READY) {
      std::vector<RejectionReason> reasons;
      if (hard_eligible(rec, reasons)) { rec.hard_eligible = true; rec.eligibility_reasons.clear(); candidates.push_back(rec.request.id); }
      else { rec.eligibility_reasons = reasons; }
    } else if (rec.lifecycle.state() == LifecycleState::ELIGIBLE || rec.lifecycle.state() == LifecycleState::QUEUED) {
      std::vector<RejectionReason> reasons;
      if (hard_eligible(rec, reasons)) { rec.hard_eligible = true; rec.eligibility_reasons.clear(); candidates.push_back(rec.request.id); }
      else { rec.eligibility_reasons = reasons; }
    }
  }

  // Refresh counts.
  ready_count = 0; queued_count = 0; eligible_count = 0;
  for (const auto& [id, rec] : records) {
    (void)id;
    if (rec.lifecycle.state() == LifecycleState::READY) ++ready_count;
    if (rec.lifecycle.state() == LifecycleState::QUEUED || rec.lifecycle.state() == LifecycleState::ELIGIBLE) ++queued_count;
    if (rec.hard_eligible) ++eligible_count;
  }

  // Deterministic rank.
  // Named ranking factors are combined into an explicit weighted scale (the
  // weights are named and configurable in PolicyConfig; they are NOT one opaque
  // scalar). The same factors are returned verbatim in the decision for
  // explanation and inspection. Summing lets fairness/starvation pressure trade
  // off against priority/deadline instead of a single factor dominating.
  struct Ranked { CollectiveRequestId id; std::vector<double> key; double score = 0.0; std::uint64_t seq; };
  std::vector<Ranked> rankedList;
  for (auto id : candidates) {
    auto key = ranking_key(records[id]);
    double s = 0.0;
    for (auto v : key) s += v;
    rankedList.push_back({id, std::move(key), s, records[id].enqueue_seq});
  }
  std::stable_sort(rankedList.begin(), rankedList.end(), [this](const Ranked& a, const Ranked& b) {
    if (a.score != b.score) return a.score > b.score;
    int c = cmp_rank(a.key, b.key);
    if (c != 0) return c > 0;
    // Content-based tie-break on the collective identity so that submission order
    // cannot change the schedule; queue age / sequence are strictly later keys.
    auto ca = records.at(a.id).request.collective.value;
    auto cb = records.at(b.id).request.collective.value;
    if (ca != cb) return ca < cb;
    if (a.seq != b.seq) return a.seq < b.seq;
    return a.id.value < b.id.value;
  });

  // Selection with bounded overlap.
  std::vector<CollectiveRequestId> selected;
  for (const auto& rk : rankedList) {
    Record& rec = records[rk.id];
    if (active_grants + selected.size() >= policy.bounds.max_active_grants) break;
    bool exclusive = (rec.request.overlap == OverlapMode::EXCLUSIVE);
    if (exclusive && !selected.empty()) continue;
    if (active_conflicts(rec) > 0) continue;
    bool ok = true;
    for (auto sel : selected) if (!can_overlap(rec, records[sel])) { ok = false; break; }
    if (!ok) continue;
    if (selected.size() >= policy.bounds.max_overlap_group) break;
    selected.push_back(rk.id);
    if (exclusive) break;
  }

  // Create grants.
  for (auto id : selected) {
    Record& rec = records[id];
    try { promote_to_granted(rec); } catch (const scheduler_error&) { continue; }
    CollectiveGrant g = create_grant(rec, dec.evidence, dec);
    dec.grants.push_back(g.id);
  }
  dec.selected = selected;

  // Overlap group.
  if (!selected.empty()) {
    OverlapGroup grp;
    grp.id = next_group_id++;
    grp.gen = next_group_gen++;
    grp.members = selected; grp.normalize();
    if (selected.size() == 1) { grp.mode = records[selected[0]].request.overlap; }
    else {
      grp.mode = OverlapMode::OVERLAP_ALLOWED;
      for (auto id : selected) if (records[id].request.overlap == OverlapMode::OVERLAP_LIMITED) grp.mode = OverlapMode::OVERLAP_LIMITED;
    }
    grp.bound = policy.bounds.max_overlap_group;
    dec.overlap_groups.push_back(grp);
  }

  // Fairness / starvation accounting.
  for (auto id : candidates) {
    Record& rec = records[id];
    bool sel = false;
    for (auto s : selected) if (s == id) { sel = true; break; }
    if (sel) {
      rec.fairness.observed_grants++;
      rec.fairness.observed_service += 1.0;
      rec.fairness.consecutive_bypasses = 0;
      rec.fairness.deficit = rec.fairness.deficit > policy.aging_growth ? rec.fairness.deficit - policy.aging_growth : 0.0;
      rec.fairness.starved = false;
    } else {
      rec.fairness.consecutive_bypasses++;
      rec.fairness.wait_ticks++;
      rec.fairness.deficit += policy.aging_growth;
      rec.fairness.starved = rec.fairness.consecutive_bypasses >= policy.max_bypass_count;
    }
  }

  // Fallback (next eligible in deterministic order, excluding selected).
  for (const auto& rk : rankedList) {
    bool sel = false; for (auto s : selected) if (s == rk.id) { sel = true; break; }
    if (!sel) dec.fallback.push_back(rk.id);
  }

  // Ranking factors (top candidate).
  if (!rankedList.empty()) {
    auto key = ranking_key(records[rankedList[0].id]);
    const SchedulingFactorKind kinds[] = { SchedulingFactorKind::DEADLINE_SLACK, SchedulingFactorKind::EXTERNAL_PRIORITY, SchedulingFactorKind::STARVATION_RISK, SchedulingFactorKind::FAIRNESS_DEFICIT, SchedulingFactorKind::AGE, SchedulingFactorKind::EXPECTED_DURATION, SchedulingFactorKind::RESERVATION_FIT, SchedulingFactorKind::COMMUNICATION_COST, SchedulingFactorKind::RESOURCE_FOOTPRINT, SchedulingFactorKind::CONGESTION_PENALTY, SchedulingFactorKind::PARTICIPANT_AVAILABILITY };
    for (std::size_t i = 0; i < key.size() && i < 11; ++i) dec.ranking_factors.push_back({kinds[i], key[i], ProvenanceStatus::DERIVED});
  }

  collect_priority_inversions(dec);
  dec.explanation = "collective " + std::to_string(dec.selected.size()) + " selected";
  return dec;
}

// ---------------------------------------------------------------------------
// Public API.
// ---------------------------------------------------------------------------
void Scheduler::publishParticipant(Participant p) {
  std::unique_lock l(impl_->mu);
  auto it = impl_->participants.find(p.id);
  if (it != impl_->participants.end()) {
    if (p.gen.value < it->second.gen.value) throw_transition("participant generation regress");
    bool boot_changed = (it->second.worker_boot.value != p.worker_boot.value);
    if (p.gen.value > it->second.gen.value || boot_changed) {
      auto pit = impl_->participant_requests.find(p.id);
      if (pit != impl_->participant_requests.end()) {
        for (auto rid : pit->second) {
          auto rit = impl_->records.find(rid); if (rit == impl_->records.end()) continue;
          auto& rec = rit->second;
          if (is_terminal_state(rec.lifecycle.state())) continue;
          bool affected = false;
          for (std::size_t i = 0; i < rec.request.participants.size(); ++i)
            if (rec.request.participants[i] == p.id && rec.request.participant_gens[i].value != p.gen.value) affected = true;
          if (!affected) continue;
          try {
            switch (rec.lifecycle.state()) {
              case LifecycleState::READY: case LifecycleState::ELIGIBLE: case LifecycleState::QUEUED:
              case LifecycleState::GRANTED: case LifecycleState::DISPATCHING: case LifecycleState::ACTIVE:
                rec.lifecycle.transition(LifecycleState::REVALIDATION_REQUIRED); break;
              default: break;
            }
          } catch (const scheduler_error&) {}
        }
      }
    }
  }
  impl_->participants[p.id] = std::move(p);
  impl_->participant_requests[p.id];
}
void Scheduler::publishPlan(CommunicationPlan plan) {
  std::unique_lock l(impl_->mu);
  auto it = impl_->plans.find(plan.id);
  if (it != impl_->plans.end() && plan.gen.value < it->second.gen.value) throw_transition("plan generation regress");
  impl_->plans[plan.id] = std::move(plan);
}
void Scheduler::publishDependency(DependencyState dep) {
  std::unique_lock l(impl_->mu);
  auto it = impl_->deps.find(dep.id);
  if (it != impl_->deps.end() && dep.gen.value < it->second.gen.value) throw_transition("dependency generation regress");
  impl_->deps[dep.id] = std::move(dep);
}
void Scheduler::publishReservation(Reservation res) {
  std::unique_lock l(impl_->mu);
  impl_->reservations[res.gen] = std::move(res);
  impl_->reservation_gen = res.gen;
}
void Scheduler::publishCongestion(CongestionState cong) {
  std::unique_lock l(impl_->mu);
  if (impl_->congestion && cong.gen.value < impl_->congestion->gen.value) throw_transition("congestion generation regress");
  impl_->congestion_gen = cong.gen;
  impl_->congestion = std::move(cong);
}
void Scheduler::publishCapacity(CapacityState cap) {
  std::unique_lock l(impl_->mu);
  if (impl_->capacity && cap.gen.value < impl_->capacity->gen.value) throw_transition("capacity generation regress");
  impl_->capacity_gen = cap.gen;
  impl_->capacity = std::move(cap);
}
void Scheduler::setPolicy(PolicyConfig cfg) {
  std::unique_lock l(impl_->mu);
  impl_->policy = cfg;
  impl_->policy_gen.value = impl_->policy_gen.value + 1;
}

CollectiveRequestId Scheduler::submit(CollectiveRequest req) {
  std::unique_lock l(impl_->mu);
  if (impl_->records.size() >= impl_->policy.bounds.max_collectives) throw_bounds("collective count bound");
  auto id = impl_->next_request_id++;
  req.id = id;
  req.gen = CollectiveRequestGeneration(1);
  validate_request(req, impl_->policy);
  Record rec;
  rec.request = std::move(req);
  rec.enqueue_seq = impl_->next_seq++;
  rec.lifecycle = Lifecycle(LifecycleState::DECLARED);
  rec.fairness.cls = rec.request.fairness_class;
  for (auto pid : rec.request.participants) impl_->participant_requests[pid].insert(id);
  impl_->records.emplace(id, std::move(rec));
  return id;
}

void Scheduler::supersede(CollectiveRequest req) {
  std::unique_lock l(impl_->mu);
  auto rit = impl_->records.find(req.id);
  if (rit == impl_->records.end()) throw_invalid("supersede for unknown request");
  if (req.gen.value <= rit->second.request.gen.value) throw_transition("supersede generation must advance");
  validate_request(req, impl_->policy);
  // Revoke any in-flight grant (external, after unlock).
  auto& old = rit->second;
  auto old_grant = old.current_grant;
  if (old.lifecycle.state() != LifecycleState::COMPLETED && old.lifecycle.state() != LifecycleState::CANCELLED) {
    try { old.lifecycle.transition(LifecycleState::SUPERSEDED); } catch (const scheduler_error&) { old.lifecycle = Lifecycle(LifecycleState::SUPERSEDED); }
  }
  if (old_grant) { auto gs = impl_->grants.find(old_grant->id); if (gs != impl_->grants.end()) { gs->second.terminal = true; gs->second.active = false; } if (impl_->active_grants > 0) --impl_->active_grants; }
  // Replace request in-place with new generation.
  Record nrec;
  nrec.request = std::move(req);
  nrec.enqueue_seq = old.enqueue_seq;
  nrec.lifecycle = Lifecycle(LifecycleState::WAITING);
  nrec.fairness = old.fairness;
  for (auto pid : nrec.request.participants) impl_->participant_requests[pid].insert(nrec.request.id);
  impl_->records[nrec.request.id] = std::move(nrec);
}

void Scheduler::cancel(CollectiveRequestId id) {
  std::shared_ptr<CollectiveFabricPort> fabric;
  std::optional<CollectiveGrant> revoke;
  WorkloadId release_wl; bool release=false;
  {
    std::unique_lock l(impl_->mu);
    auto rit = impl_->records.find(id); if (rit == impl_->records.end()) throw_invalid("cancel for unknown request");
    auto& rec = rit->second;
    if (is_terminal_state(rec.lifecycle.state())) return;  // exactly one terminal outcome
    if (rec.current_grant && impl_->grants.count(rec.current_grant->id)) {
      auto gs = impl_->grants.find(rec.current_grant->id);
      if (!gs->second.terminal) { revoke = gs->second.grant; gs->second.terminal = true; if (gs->second.active && impl_->active_grants > 0) --impl_->active_grants; gs->second.active = false; }
      fabric = impl_->fabric;
      release_wl = rec.request.workload; release = true;
    }
    try { rec.lifecycle.transition(LifecycleState::CANCELLED); } catch (const scheduler_error&) { rec.lifecycle = Lifecycle(LifecycleState::CANCELLED); }
  }
  if (release) { if (impl_->broker) impl_->broker->release(release_wl); }
  if (revoke && fabric) fabric->revoke(*revoke);
}

void Scheduler::advanceCoordinatorEpoch(CoordinatorEpoch epoch) {
  std::unique_lock l(impl_->mu);
  if (epoch.value <= impl_->epoch.value) throw_transition("coordinator epoch must advance");
  impl_->epoch = epoch;
}
void Scheduler::advanceTime(Nanoseconds d) { std::unique_lock l(impl_->mu); impl_->now.advance(d); }
CoordinatorEpoch Scheduler::coordinatorEpoch() const { std::shared_lock l(impl_->mu); return impl_->epoch; }
CapacityGeneration Scheduler::capacityGeneration() const { std::shared_lock l(impl_->mu); return impl_->capacity_gen; }
CongestionGeneration Scheduler::congestionGeneration() const { std::shared_lock l(impl_->mu); return impl_->congestion_gen; }
ReservationGeneration Scheduler::reservationGeneration() const { std::shared_lock l(impl_->mu); return impl_->reservation_gen; }
ScheduleDecision Scheduler::schedule() {
  ScheduleDecision dec;
  { std::unique_lock l(impl_->mu); dec = impl_->schedule(); }
  // Emit priority-inversion actions through the Preemption Fabric port AFTER
  // releasing the scheduler lock (never invoke an external callback under lock).
  auto pre = impl_->preemption;
  for (auto& ev : dec.priority_inversions) {
    if (pre) pre->request_preemption(ev.holder, ev.requester, ev.action);
  }
  return dec;
}

GrantAdjudication Scheduler::revalidateGrant(const CollectiveGrant& g) { std::unique_lock l(impl_->mu); return impl_->revalidate_grant(g); }

bool Scheduler::handoff(const CollectiveGrant& g) {
  std::shared_ptr<CollectiveFabricPort> fabric;
  std::optional<CollectiveGrant> gc;
  {
    std::unique_lock l(impl_->mu);
    if (impl_->revalidate_grant(g) != GrantAdjudication::VALID) return false;
    auto rit = impl_->records.find(g.request); if (rit == impl_->records.end()) return false;
    try { rit->second.lifecycle.transition(LifecycleState::DISPATCHING); } catch (const scheduler_error&) { return false; }
    auto gs = impl_->grants.find(g.id);
    if (gs != impl_->grants.end()) { gs->second.handed_off = true; gs->second.grant = g; }
    fabric = impl_->fabric;
    gc = g;
  }
  if (!gc) return false;
  bool accepted = fabric ? fabric->accept(*gc) : true;
  bool revoke = false;
  {
    std::unique_lock l(impl_->mu);
    auto rit = impl_->records.find(gc->request);
    if (rit != impl_->records.end()) {
      auto& rec = rit->second;
      if (rec.lifecycle.state() == LifecycleState::DISPATCHING && accepted) {
        try { rec.lifecycle.transition(LifecycleState::ACTIVE); } catch (const scheduler_error&) {}
      }
      auto gs = impl_->grants.find(gc->id);
      if (gs != impl_->grants.end()) {
        bool isActive = (rec.lifecycle.state() == LifecycleState::ACTIVE);
        gs->second.active = isActive;
        if (isActive) { ++impl_->active_grants; } else { revoke = true; }
      } else { revoke = true; }
    } else { revoke = true; }
  }
  if (revoke && fabric) fabric->revoke(*gc);
  return accepted && !revoke;
}

bool Scheduler::complete(const CollectiveGrant& g, const WorkerBootId& wb, const SourceBootId& sb) {
  WorkloadId release_wl; bool release = false;
  {
    std::unique_lock l(impl_->mu);
    auto rit = impl_->records.find(g.request); if (rit == impl_->records.end()) return false;
    auto& rec = rit->second;
    auto gs = impl_->grants.find(g.id); if (gs == impl_->grants.end()) return false;
    auto& gstate = gs->second;
    if (gstate.terminal) return false;
    if (g.evidence.epoch.value != impl_->epoch.value) return false;  // stale epoch
    if (rec.lifecycle.state() != LifecycleState::ACTIVE && rec.lifecycle.state() != LifecycleState::DISPATCHING) return false;
    if (!rec.current_grant || rec.current_grant->id.value != g.id.value || rec.current_grant->gen.value != g.gen.value) return false;
    if (rec.request.gen.value != g.request_gen.value) return false;
    if (rec.request.collective_gen.value != g.collective_gen.value) return false;
    // Participant authority: all participant gens must still be current AND ready.
    bool boot_ok = false;
    for (std::size_t i = 0; i < g.participants.size(); ++i) {
      auto pit = impl_->participants.find(g.participants[i]);
      if (pit == impl_->participants.end()) return false;
      if (pit->second.gen.value != g.participant_gens[i].value) return false;
      if (!pit->second.ready || pit->second.readiness_source == ProvenanceStatus::UNKNOWN) return false;
      if (pit->second.worker_boot.value == wb.value) {
        if (pit->second.source_boot.value == sb.value) boot_ok = true;
      }
    }
    if (!boot_ok) return false;
    try { rec.lifecycle.transition(LifecycleState::COMPLETED); } catch (const scheduler_error&) { return false; }
    gstate.terminal = true; gstate.active = false; gstate.completed_by = wb; gstate.completed_by_source = sb;
    if (impl_->active_grants > 0) --impl_->active_grants;
    if (rec.current_grant) rec.current_grant->active = false;
    release_wl = rec.request.workload; release = true;
  }
  if (release && impl_->broker) impl_->broker->release(release_wl);
  return true;
}

void Scheduler::fenceWorkerBoot(WorkerId worker, WorkerBootId boot) {
  std::unique_lock l(impl_->mu);
  for (auto& [pid, p] : impl_->participants) {
    (void)pid;
    if (p.worker.value == worker.value && p.worker_boot.value == boot.value) {
      p.ready = false; p.readiness_source = ProvenanceStatus::UNKNOWN;
    }
  }
  for (auto& [rid, rec] : impl_->records) {
    if (is_terminal_state(rec.lifecycle.state())) continue;
    for (auto pid : rec.request.participants) {
      auto pit = impl_->participants.find(pid); if (pit == impl_->participants.end()) continue;
      if (pit->second.worker.value == worker.value && pit->second.worker_boot.value == boot.value) {
        try {
          switch (rec.lifecycle.state()) {
            case LifecycleState::READY: case LifecycleState::ELIGIBLE: case LifecycleState::QUEUED:
            case LifecycleState::GRANTED: case LifecycleState::DISPATCHING: case LifecycleState::ACTIVE:
              rec.lifecycle.transition(LifecycleState::REVALIDATION_REQUIRED); break;
            default: break;
          }
        } catch (const scheduler_error&) {}
        if (rec.current_grant) { auto gs = impl_->grants.find(rec.current_grant->id); if (gs != impl_->grants.end()) { gs->second.terminal = true; gs->second.active = false; } }
      }
    }
  }
}

Scheduler::Info Scheduler::info() const {
  std::shared_lock l(impl_->mu);
  Info inf;
  std::vector<std::pair<CollectiveRequestId, std::uint64_t>> v;
  for (const auto& [id, rec] : impl_->records) {
    ++inf.records;
    if (rec.lifecycle.state() == LifecycleState::READY) ++inf.ready;
    if (rec.lifecycle.state() == LifecycleState::QUEUED || rec.lifecycle.state() == LifecycleState::ELIGIBLE) ++inf.queued;
    if (rec.lifecycle.state() == LifecycleState::RETIRED) ++inf.retired;
    if (rec.lifecycle.state() == LifecycleState::ACTIVE) {}
    if (rec.hard_eligible && rec.lifecycle.state() != LifecycleState::GRANTED && rec.lifecycle.state() != LifecycleState::ACTIVE) v.push_back({id, rec.enqueue_seq});
  }
  inf.participants = impl_->participants.size();
  inf.plans = impl_->plans.size();
  inf.active_grants = impl_->active_grants;
  inf.capacity_gen = impl_->capacity_gen;
  inf.congestion_gen = impl_->congestion_gen;
  inf.reservation_gen = impl_->reservation_gen;
  inf.epoch = impl_->epoch;
  inf.now_ns = impl_->now.value();
  std::sort(v.begin(), v.end(), [this](const auto& a, const auto& b) {
    auto ca = impl_->records.at(a.first).request.collective.value;
    auto cb = impl_->records.at(b.first).request.collective.value;
    if (ca != cb) return ca < cb;
    if (a.second != b.second) return a.second < b.second;
    return a.first.value < b.first.value;
  });
  for (auto& e : v) inf.next_eligible.push_back(e.first);
  return inf;
}

void Scheduler::resetDynamicStateForRecovery() {
  std::unique_lock l(impl_->mu);
  impl_->participants.clear();
  impl_->plans.clear();
  impl_->deps.clear();
  impl_->reservations.clear();
  impl_->congestion.reset();
  impl_->capacity.reset();
  impl_->participant_requests.clear();
  impl_->congestion_gen = CongestionGeneration(0);
  impl_->capacity_gen = CapacityGeneration(0);
  impl_->reservation_gen = ReservationGeneration(0);
  impl_->active_grants = 0;
  impl_->grants.clear();
  for (auto& [id, rec] : impl_->records) {
    (void)id;
    auto st = rec.lifecycle.state();
    if (st != LifecycleState::COMPLETED && st != LifecycleState::CANCELLED &&
        st != LifecycleState::SUPERSEDED && st != LifecycleState::STALE && st != LifecycleState::RETIRED) {
      try { rec.lifecycle.transition(LifecycleState::REVALIDATION_REQUIRED); } catch (const scheduler_error&) { rec.lifecycle = Lifecycle(LifecycleState::REVALIDATION_REQUIRED); }
    }
    rec.current_grant.reset();
    rec.hard_eligible = false; rec.last_readiness = Readiness::UNKNOWN;
  }
}

std::optional<CollectiveGrant> Scheduler::grantById(CollectiveGrantId id) const {
  std::shared_lock l(impl_->mu);
  auto it = impl_->grants.find(id);
  if (it == impl_->grants.end()) return std::nullopt;
  return it->second.grant;
}


// ---------------------------------------------------------------------------
// Persistence hooks (the Impl/Record types are complete here).
// ---------------------------------------------------------------------------
namespace {
bool persist_enum_ok(LifecycleState v) noexcept {
  switch (v) {
    case LifecycleState::DECLARED: case LifecycleState::WAITING: case LifecycleState::READY:
    case LifecycleState::ELIGIBLE: case LifecycleState::QUEUED: case LifecycleState::GRANTED:
    case LifecycleState::DISPATCHING: case LifecycleState::ACTIVE: case LifecycleState::DRAINING:
    case LifecycleState::COMPLETED: case LifecycleState::CANCELLED: case LifecycleState::FAILED:
    case LifecycleState::REVALIDATION_REQUIRED: case LifecycleState::SUPERSEDED: case LifecycleState::STALE:
    case LifecycleState::RETIRED: return true;
  }
  return false;
}
void ser_req(PersistenceWriter& w, const CollectiveRequest& r) {
  w.id(r.id.value); w.id(r.gen.value); w.id(r.collective.value); w.id(r.collective_gen.value);
  w.u8(static_cast<std::uint8_t>(r.kind));
  w.u64(r.payload_bytes);
  w.u64(r.participants.size()); for (auto v : r.participants) w.id(v.value);
  w.u64(r.participant_gens.size()); for (auto v : r.participant_gens) w.id(v.value);
  w.u64(r.per_participant_payload.size()); for (auto v : r.per_participant_payload) w.u64(v);
  w.id(r.plan_id.value); w.id(r.plan_gen.value);
  w.u64(r.dependencies.size());
  for (const auto& d : r.dependencies) { w.id(d.id.value); w.id(d.gen.value); w.u8(static_cast<std::uint8_t>(d.kind)); w.u8(static_cast<std::uint8_t>(d.provenance)); }
  w.u64(r.resource_claims.size());
  for (const auto& c : r.resource_claims) { w.id(c.resource.value); w.id(c.gen.value); w.f64(c.units); w.u8(c.exclusive ? 1 : 0); w.u8(static_cast<std::uint8_t>(c.provenance)); }
  w.u64(r.exclusion_classes.size()); for (const auto& s2 : r.exclusion_classes) w.str(s2);
  w.i32(r.priority.value); w.id(r.priority.gen.value); w.u8(r.priority.authoritative ? 1 : 0);
  w.u64(r.window.earliest_start_ns); w.u64(r.window.latest_start_ns); w.u64(r.window.deadline_ns);
  w.u64(r.window.expected_duration_ns); w.u64(r.window.slack_ns); w.u8(static_cast<std::uint8_t>(r.window.source));
  w.u8(static_cast<std::uint8_t>(r.overlap));
  w.u8(static_cast<std::uint8_t>(r.fairness_class));
  w.u8(r.reservation_gen ? 1 : 0); if (r.reservation_gen) w.id(r.reservation_gen->value);
  w.id(r.workload.value); w.id(r.workload_gen.value);
  w.u8(static_cast<std::uint8_t>(r.queue_class));
  w.u32(r.max_retries); w.u32(r.retry_count);
  w.id(r.policy_gen.value);
  w.u8(static_cast<std::uint8_t>(r.provenance));
}
CollectiveRequest de_req(PersistenceReader& r) {
  CollectiveRequest q;
  q.id = CollectiveRequestId(r.id()); q.gen = CollectiveRequestGeneration(r.id());
  q.collective = CollectiveId(r.id()); q.collective_gen = CollectiveGeneration(r.id());
  q.kind = static_cast<CollectiveKind>(r.u8());
  q.payload_bytes = r.u64();
  auto np = r.u64(); for (std::uint64_t i = 0; i < np; ++i) q.participants.push_back(ParticipantId(r.id()));
  auto ng = r.u64(); for (std::uint64_t i = 0; i < ng; ++i) q.participant_gens.push_back(ParticipantGeneration(r.id()));
  auto nb = r.u64(); for (std::uint64_t i = 0; i < nb; ++i) q.per_participant_payload.push_back(r.u64());
  q.plan_id = CommunicationPlanId(r.id()); q.plan_gen = CommunicationPlanGeneration(r.id());
  auto nd = r.u64(); for (std::uint64_t i = 0; i < nd; ++i) { DependencyRequirement d; d.id = DependencyId(r.id()); d.gen = DependencyGeneration(r.id()); d.kind = static_cast<DependencyKind>(r.u8()); d.provenance = static_cast<ProvenanceStatus>(r.u8()); q.dependencies.push_back(d); }
  auto nc = r.u64(); for (std::uint64_t i = 0; i < nc; ++i) { ResourceClaim c; c.resource = ResourceId(r.id()); c.gen = ResourceGeneration(r.id()); c.units = r.f64(); c.exclusive = r.u8() != 0; c.provenance = static_cast<ProvenanceStatus>(r.u8()); q.resource_claims.push_back(c); }
  auto ne = r.u64(); for (std::uint64_t i = 0; i < ne; ++i) q.exclusion_classes.push_back(r.str());
  q.priority.value = r.i32(); q.priority.gen = PriorityGeneration(r.id()); q.priority.authoritative = r.u8() != 0;
  q.window.earliest_start_ns = r.u64(); q.window.latest_start_ns = r.u64(); q.window.deadline_ns = r.u64();
  q.window.expected_duration_ns = r.u64(); q.window.slack_ns = r.u64(); q.window.source = static_cast<ProvenanceStatus>(r.u8());
  q.overlap = static_cast<OverlapMode>(r.u8()); q.fairness_class = static_cast<FairnessClass>(r.u8());
  if (r.u8()) q.reservation_gen = ReservationGeneration(r.id());
  q.workload = WorkloadId(r.id()); q.workload_gen = WorkloadGeneration(r.id());
  q.queue_class = static_cast<QueueClass>(r.u8());
  q.max_retries = r.u32(); q.retry_count = r.u32();
  q.policy_gen = PolicyGeneration(r.id()); q.provenance = static_cast<ProvenanceStatus>(r.u8());
  return q;
}
void ser_fair(PersistenceWriter& w, const FairnessAccount& f) {
  w.u8(static_cast<std::uint8_t>(f.cls)); w.u64(f.observed_grants); w.f64(f.observed_service); w.f64(f.deficit);
  w.u64(f.wait_ticks); w.u64(f.consecutive_bypasses); w.u8(f.starved ? 1 : 0); w.f64(f.target_share);
}
FairnessAccount de_fair(PersistenceReader& r) {
  FairnessAccount f; f.cls = static_cast<FairnessClass>(r.u8()); f.observed_grants = r.u64(); f.observed_service = r.f64(); f.deficit = r.f64();
  f.wait_ticks = r.u64(); f.consecutive_bypasses = r.u64(); f.starved = r.u8() != 0; f.target_share = r.f64();
  return f;
}
}  // namespace

void Scheduler::serializeState(PersistenceWriter& w) const {
  std::shared_lock l(impl_->mu);
  w.u64(impl_->epoch.value);
  w.u64(impl_->next_request_id.value);
  w.u64(impl_->next_schedule_id.value);
  w.u64(impl_->next_grant_id.value);
  w.u64(impl_->next_group_id.value);
  w.u64(impl_->next_schedule_gen.value);
  w.u64(impl_->next_grant_gen.value);
  w.u64(impl_->next_group_gen.value);
  w.u64(impl_->next_seq);
  w.u64(impl_->policy_gen.value);
  w.u64(impl_->records.size());
  for (const auto& [id, rec] : impl_->records) {
    (void)id;
    ser_req(w, rec.request);
    w.u8(static_cast<std::uint8_t>(rec.lifecycle.state()));
    w.u64(rec.enqueue_seq);
    w.u64(rec.age_ticks);
    ser_fair(w, rec.fairness);
  }
}

void Scheduler::deserializeState(PersistenceReader& r) {
  CoordinatorEpoch epoch(r.u64());
  std::uint64_t nrid = r.u64(); std::uint64_t nsid = r.u64(); std::uint64_t ngid = r.u64(); std::uint64_t ngrpid = r.u64();
  std::uint64_t nschedule_gen = r.u64(); std::uint64_t ngrant_gen = r.u64(); std::uint64_t ngroup_gen = r.u64();
  std::uint64_t nseq = r.u64(); std::uint64_t npol = r.u64();
  std::uint64_t nrec = r.u64();
  if (nrec > impl_->policy.bounds.max_history_records) throw_bounds("persistence record count exceeds bound");
  std::vector<Scheduler::Record> recs;
  recs.reserve(static_cast<std::size_t>(nrec));
  for (std::uint64_t i = 0; i < nrec; ++i) {
    Scheduler::Record rec;
    rec.request = de_req(r);
    auto lc = static_cast<LifecycleState>(r.u8());
    if (!persist_enum_ok(lc)) throw_malformed("invalid lifecycle enum in persistence");
    rec.lifecycle = Lifecycle(lc);
    rec.enqueue_seq = r.u64();
    rec.age_ticks = r.u64();
    rec.fairness = de_fair(r);
    recs.push_back(std::move(rec));
  }
  std::unique_lock l(impl_->mu);
  impl_->epoch = epoch;
  impl_->next_request_id = CollectiveRequestId(nrid);
  impl_->next_schedule_id = CollectiveScheduleId(nsid);
  impl_->next_grant_id = CollectiveGrantId(ngid);
  impl_->next_group_id = CollectiveGroupId(ngrpid);
  impl_->next_schedule_gen = CollectiveScheduleGeneration(nschedule_gen);
  impl_->next_grant_gen = CollectiveGrantGeneration(ngrant_gen);
  impl_->next_group_gen = CollectiveGroupGeneration(ngroup_gen);
  impl_->next_seq = nseq;
  impl_->policy_gen = PolicyGeneration(npol);
  impl_->records.clear();
  impl_->participant_requests.clear();
  impl_->grants.clear();
  impl_->active_grants = 0;
  for (auto& rec : recs) {
    for (auto pid : rec.request.participants) impl_->participant_requests[pid].insert(rec.request.id);
    impl_->records.emplace(rec.request.id, std::move(rec));
  }
}
}  // namespace collective_scheduler