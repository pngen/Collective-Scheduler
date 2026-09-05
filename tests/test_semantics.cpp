#include <collective_scheduler/scheduler.hpp>
#include "test_common.hpp"
#include "test_framework.hpp"

using namespace collective_scheduler;
using namespace tdata;

namespace {
// Drain a scheduler: repeatedly schedule, handoff + complete the first grant.
// Returns the order of collective ids that were granted/completed.
std::vector<std::uint64_t> drain(Scheduler& s, int max_cycles = 200) {
  std::vector<std::uint64_t> order;
  for (int i = 0; i < max_cycles; ++i) {
    auto dec = s.schedule();
    if (dec.selected.empty()) break;
    // handoff+complete every selected grant we can.
    bool progress = false;
    for (auto gid : dec.grants) {
      auto g = s.grantById(gid);
      if (!g) continue;
      if (s.handoff(*g) && s.complete(*g, WorkerBootId(11), SourceBootId(100))) {
        order.push_back(g->collective.value);
        progress = true;
      }
    }
    if (!progress) break;
  }
  return order;
}
}  // namespace

TEST(semantics_determinism_insertion_order) {
  auto make = [](bool reversed, Scheduler& s) {
    s.publishParticipant(P(1, 1, 1, 11)); s.publishParticipant(P(2, 1, 1, 11));
    s.publishParticipant(P(3, 1, 1, 11)); s.publishParticipant(P(4, 1, 1, 18));
    s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
    s.publishPlan(Plan(2, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(2), ParticipantId(3)}));
    s.publishPlan(Plan(3, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(3), ParticipantId(4)}));
    if (!reversed) {
      s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1, OverlapMode::OVERLAP_LIMITED));
      s.submit(Req(2, CollectiveKind::ALL_REDUCE, {ParticipantId(2), ParticipantId(3)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 2, 1, OverlapMode::OVERLAP_LIMITED));
      s.submit(Req(3, CollectiveKind::ALL_REDUCE, {ParticipantId(3), ParticipantId(4)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 3, 1, OverlapMode::OVERLAP_LIMITED));
    } else {
      s.submit(Req(3, CollectiveKind::ALL_REDUCE, {ParticipantId(3), ParticipantId(4)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 3, 1, OverlapMode::OVERLAP_LIMITED));
      s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1, OverlapMode::OVERLAP_LIMITED));
      s.submit(Req(2, CollectiveKind::ALL_REDUCE, {ParticipantId(2), ParticipantId(3)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 2, 1, OverlapMode::OVERLAP_LIMITED));
    }
  };
  Scheduler s1, s2; make(false, s1); make(true, s2);
  auto a = drain(s1);
  auto b = drain(s2);
  REQUIRE(a.size() >= 3);
  CHECK_EQ(a.size(), b.size());
  for (std::size_t i = 0; i < a.size(); ++i) CHECK_EQ(a[i], b[i]);
}

TEST(semantics_deadline_ordering) {
  Scheduler s;
  s.publishParticipant(P(1, 1, 1, 11)); s.publishParticipant(P(2, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  auto near = tdata::Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1);
  near.window.deadline_ns = 100; near.window.expected_duration_ns = 10;  // near deadline
  auto far = tdata::Req(2, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1);
  far.window.deadline_ns = 100000000000UL; far.window.expected_duration_ns = 10;  // long slack
  auto c1 = s.submit(std::move(near));
  auto c2 = s.submit(std::move(far));
  auto dec = s.schedule();
  REQUIRE(dec.selected.size() == 1);
  CHECK_EQ(dec.selected[0].value, c1.value);  // near-deadline first
}

TEST(semantics_deadline_does_not_bypass_hard_constraint) {
  Scheduler s;
  s.publishParticipant(P(1, 1, 1, 11)); s.publishParticipant(P(2, 1, 1, 11));
  // C1 references stale participant generation (gen 2, but participant is gen 1).
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  auto staleNear = tdata::Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(99), ParticipantGeneration(99)}, 1, 1);
  staleNear.window.deadline_ns = 100; staleNear.window.expected_duration_ns = 10;
  auto ok = tdata::Req(2, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1);
  ok.window.deadline_ns = 100000000000UL; ok.window.expected_duration_ns = 10;
  auto c1 = s.submit(std::move(staleNear));
  auto c2 = s.submit(std::move(ok));
  auto dec = s.schedule();
  REQUIRE(dec.selected.size() == 1);
  CHECK_NEQ(dec.selected[0].value, c1.value);  // stale near-deadline cannot win
  CHECK_EQ(dec.selected[0].value, c2.value);
}
TEST(semantics_fairness_starvation_prevention) {
  // Sustained high-priority competition: a fresh high-priority collective always
  // contends for the same resource, so the low-priority collective is continuously
  // bypassed. The scheduler's anti-starvation guarantee must eventually promote it.
  Scheduler::Options opts;
  opts.policy.max_bypass_count = 3;
  opts.policy.aging_growth = 0.05;
  opts.policy.w_starvation_risk = 5.0;
  opts.policy.w_fairness_deficit = 5.0;
  Scheduler s(opts);
  s.publishParticipant(P(1, 1, 1, 11)); s.publishParticipant(P(2, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  auto low = tdata::Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1);
  low.priority.value = 1; low.priority.authoritative = true;
  s.submit(std::move(low));
  std::uint64_t serial = 100;
  bool low_granted = false;
  int granted_cycle = -1;
  for (int cycle = 0; cycle < 40; ++cycle) {
    auto hr = Req(serial++, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1);
    hr.priority.value = 5; hr.priority.authoritative = true;
    s.submit(std::move(hr));
    auto dec = s.schedule();
    if (dec.selected.empty()) break;
    for (auto gid : dec.grants) {
      auto g = s.grantById(gid);
      if (!g) continue;
      bool completed = s.handoff(*g) && s.complete(*g, WorkerBootId(11), SourceBootId(100));
      if (completed && g->collective.value == 1) { low_granted = true; granted_cycle = cycle; }
    }
    if (low_granted) break;
  }
  CHECK(low_granted);
  // It had to accumulate at least max_bypass_count bypasses before being promoted.
  CHECK(granted_cycle >= static_cast<int>(opts.policy.max_bypass_count));
}

TEST(semantics_priority_inversion_detected) {
  Scheduler s;
  s.publishParticipant(P(1, 1, 1, 11)); s.publishParticipant(P(2, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  auto low = tdata::Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1);
  low.priority.value = 1; low.priority.authoritative = true; low.priority.gen = PriorityGeneration(1);
  auto cl = s.submit(std::move(low));
  auto dec1 = s.schedule();
  REQUIRE(dec1.selected.size() == 1);
  auto g = s.grantById(dec1.grants[0]); REQUIRE(g);
  CHECK(s.handoff(*g));  // low-priority now ACTIVE
  auto high = tdata::Req(2, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1);
  high.priority.value = 10; high.priority.authoritative = true; high.priority.gen = PriorityGeneration(1);
  auto ch = s.submit(std::move(high));
  auto dec2 = s.schedule();
  CHECK(dec2.selected.empty());  // high cannot run (conflicts with ACTIVE low)
  REQUIRE(dec2.priority_inversions.size() >= 1);
  CHECK_EQ(dec2.priority_inversions[0].holder.value, cl.value);
  CHECK_EQ(dec2.priority_inversions[0].requester.value, ch.value);
  CHECK(dec2.priority_inversions[0].action != PriorityInversionAction::NO_ACTION);
}

TEST(semantics_priority_inversion_unknown_evidence_no_action) {
  Scheduler s;
  s.publishParticipant(P(1, 1, 1, 11)); s.publishParticipant(P(2, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  auto low = tdata::Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1);
  low.priority.value = 1; low.priority.authoritative = true;
  auto cl = s.submit(std::move(low));
  auto dec1 = s.schedule(); auto g = s.grantById(dec1.grants[0]); REQUIRE(g); CHECK(s.handoff(*g));
  // High priority is NON-authoritative (UNKNOWN evidence) -> no aggressive inversion action.
  auto high = tdata::Req(2, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1);
  high.priority.value = 10; high.priority.authoritative = false;
  auto ch = s.submit(std::move(high));
  auto dec2 = s.schedule();
  CHECK_EQ(dec2.priority_inversions.size(), 0);
}

TEST(semantics_lifecycle_illegal_transitions) {
  bool threw = false;
  Lifecycle l(LifecycleState::DECLARED);
  try { l.transition(LifecycleState::ACTIVE); } catch (const scheduler_error&) { threw = true; }
  CHECK(threw);
  Lifecycle w(LifecycleState::WAITING);
  threw = false; try { w.transition(LifecycleState::ACTIVE); } catch (const scheduler_error&) { threw = true; } CHECK(threw);
  Lifecycle c(LifecycleState::COMPLETED);
  threw = false; try { c.transition(LifecycleState::ACTIVE); } catch (const scheduler_error&) { threw = true; } CHECK(threw);
  Lifecycle ca(LifecycleState::CANCELLED);
  threw = false; try { ca.transition(LifecycleState::COMPLETED); } catch (const scheduler_error&) { threw = true; } CHECK(threw);
  Lifecycle sp(LifecycleState::SUPERSEDED);
  threw = false; try { sp.transition(LifecycleState::GRANTED); } catch (const scheduler_error&) { threw = true; } CHECK(threw);
  Lifecycle ok(LifecycleState::DECLARED);
  ok.transition(LifecycleState::WAITING); ok.transition(LifecycleState::READY);
  CHECK(ok.state() == LifecycleState::READY);
}
TEST(semantics_plan_invalidation_race) {
  Scheduler s;
  s.publishParticipant(P(1, 1, 1, 11)); s.publishParticipant(P(2, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  auto id = s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1));
  auto dec = s.schedule();
  auto g = s.grantById(dec.grants[0]); REQUIRE(g);
  // Communication Planner advances the plan generation after grant / before handoff.
  s.publishPlan(Plan(1, 2, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  CHECK(s.revalidateGrant(*g) == GrantAdjudication::STALE_GRANT);
  CHECK(!s.handoff(*g));   // no execution occurs
}

TEST(semantics_congestion_race) {
  Scheduler s;
  s.publishParticipant(P(1, 1, 1, 11)); s.publishParticipant(P(2, 1, 1, 11));
  s.publishParticipant(P(3, 1, 1, 11)); s.publishParticipant(P(4, 1, 1, 11));
  auto pl1 = Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}); pl1.links = {LinkId(100)};
  auto pl2 = Plan(2, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(3), ParticipantId(4)}); pl2.links = {LinkId(101)};
  s.publishPlan(pl1); s.publishPlan(pl2);
  auto c1 = s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1, OverlapMode::OVERLAP_ALLOWED));
  auto c2 = s.submit(Req(2, CollectiveKind::ALL_REDUCE, {ParticipantId(3), ParticipantId(4)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 2, 1, OverlapMode::OVERLAP_ALLOWED));
  auto dec = s.schedule();
  REQUIRE(dec.selected.size() == 2);  // overlap initially allowed
  // Congestion advances with a hard ceiling -> previously allowed overlap is no longer allowed.
  CongestionState cong; cong.gen = CongestionGeneration(5); cong.hard_ceiling = true; cong.hard_ceiling_utilization = 0.5; cong.utilization = 0.9; cong.residual_bandwidth = 0; cong.is_congested = true;
  s.publishCongestion(cong);
  // Old grants become stale (congestion generation advanced).
  for (auto gid : dec.grants) { auto g = s.grantById(gid); REQUIRE(g); CHECK(s.revalidateGrant(*g) == GrantAdjudication::STALE_GRANT); }
  // Fresh schedule serializes / blocks under the hard ceiling.
  auto dec2 = s.schedule();
  CHECK(dec2.selected.size() <= 1);
}

TEST(semantics_reservation_race) {
  Scheduler s;
  s.publishParticipant(P(1, 1, 1, 11)); s.publishParticipant(P(2, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  Reservation res; res.gen = ReservationGeneration(1); res.valid = true; res.strength = ReservationStrength::HARD;
  s.publishReservation(res);
  auto r = Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1);
  r.reservation_gen = ReservationGeneration(1);
  auto id = s.submit(std::move(r));
  auto dec = s.schedule();
  auto g = s.grantById(dec.grants[0]); REQUIRE(g);
  // Reservation advances to generation 2 before handoff -> stale ReservationGeneration rejects.
  Reservation res2; res2.gen = ReservationGeneration(2); res2.valid = true; res2.strength = ReservationStrength::HARD;
  s.publishReservation(res2);
  CHECK(s.revalidateGrant(*g) == GrantAdjudication::STALE_GRANT);
  CHECK(!s.handoff(*g));
}

TEST(semantics_dependency_race) {
  Scheduler s;
  s.publishParticipant(P(1, 1, 1, 11)); s.publishParticipant(P(2, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  DependencyState dep; dep.id = DependencyId(1); dep.gen = DependencyGeneration(1); dep.satisfied = true; dep.kind = DependencyKind::TENSOR_READY;
  s.publishDependency(dep);
  auto r = Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1);
  r.dependencies.push_back({DependencyId(1), DependencyGeneration(1), DependencyKind::TENSOR_READY});
  auto id = s.submit(std::move(r));
  auto dec1 = s.schedule();
  REQUIRE(dec1.selected.size() == 1);
  auto g = s.grantById(dec1.grants[0]); REQUIRE(g);
  // Dependency invalidates / advances before execution -> old grant rejects.
  DependencyState dep2; dep2.id = DependencyId(1); dep2.gen = DependencyGeneration(2); dep2.satisfied = false; dep2.kind = DependencyKind::TENSOR_READY;
  s.publishDependency(dep2);
  CHECK(s.revalidateGrant(*g) == GrantAdjudication::STALE_GRANT);
  auto dec2 = s.schedule();
  CHECK(dec2.selected.empty());  // not eligible until fresh dependency evidence
}

TEST(semantics_supersession_race) {
  Scheduler s;
  s.publishParticipant(P(1, 1, 1, 11)); s.publishParticipant(P(2, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  auto id = s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1));
  auto dec = s.schedule();
  auto g = s.grantById(dec.grants[0]); REQUIRE(g);
  // Generation 2 supersedes the request (changes plan).
  auto r2 = Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1);
  r2.id = id; r2.gen = CollectiveRequestGeneration(2);
  s.supersede(std::move(r2));
  CHECK(s.revalidateGrant(*g) == GrantAdjudication::SUPERSEDED);
  CHECK(!s.handoff(*g));
  CHECK(!s.complete(*g, WorkerBootId(11), SourceBootId(100)));
  auto dec2 = s.schedule();  // generation 2 may proceed with a fresh grant
}

TEST(semantics_cancellation_race) {
  Scheduler s;
  s.publishParticipant(P(1, 1, 1, 11)); s.publishParticipant(P(2, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  auto id = s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1));
  auto dec = s.schedule();
  auto g = s.grantById(dec.grants[0]); REQUIRE(g);
  s.cancel(id);
  CHECK(s.revalidateGrant(*g) == GrantAdjudication::CANCELLED);
  CHECK(!s.handoff(*g));
  CHECK(!s.complete(*g, WorkerBootId(11), SourceBootId(100)));  // cancellled must not complete
  auto dec2 = s.schedule();
  CHECK(dec2.selected.empty());
}

TEST(semantics_epoch_race) {
  Scheduler s;
  s.publishParticipant(P(1, 1, 1, 11)); s.publishParticipant(P(2, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  auto id = s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1));
  auto dec = s.schedule();
  auto g = s.grantById(dec.grants[0]); REQUIRE(g);
  s.advanceCoordinatorEpoch(CoordinatorEpoch(2));
  CHECK(s.revalidateGrant(*g) == GrantAdjudication::EPOCH_MISMATCH);
  CHECK(!s.complete(*g, WorkerBootId(11), SourceBootId(100)));
}
