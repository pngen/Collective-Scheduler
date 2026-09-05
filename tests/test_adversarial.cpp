#include <collective_scheduler/scheduler.hpp>
#include <collective_scheduler/persistence.hpp>
#include "test_common.hpp"
#include "test_framework.hpp"

#include <limits>

using namespace collective_scheduler;
using namespace tdata;

TEST(adversarial_malformed_request_rejected) {
  Scheduler s;
  for (int i = 1; i <= 2; ++i) s.publishParticipant(P(i, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  // Empty participant set.
  auto r0 = Req(1, CollectiveKind::ALL_REDUCE, {}, {}, 1, 1); bool threw = false; try { s.submit(std::move(r0)); } catch (const scheduler_error&) { threw = true; } CHECK(threw);
  // Duplicate participants.
  auto r1 = Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(1)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1); threw = false; try { s.submit(std::move(r1)); } catch (const scheduler_error&) { threw = true; } CHECK(threw);
  // Impossible window (latest < earliest).
  auto r2 = Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1); r2.window.earliest_start_ns = 100; r2.window.latest_start_ns = 50; threw = false; try { s.submit(std::move(r2)); } catch (const scheduler_error&) { threw = true; } CHECK(threw);
  // NaN/Inf resource units.
  auto r3 = Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1); ResourceClaim rc; rc.units = std::numeric_limits<double>::infinity(); r3.resource_claims.push_back(rc); threw = false; try { s.submit(std::move(r3)); } catch (const scheduler_error&) { threw = true; } CHECK(threw);
  // UNKNOWN collective kind must not gain semantics.
  auto r4 = Req(1, CollectiveKind::UNKNOWN, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1); threw = false; try { s.submit(std::move(r4)); } catch (const scheduler_error&) { threw = true; } CHECK(threw);
}

TEST(adversarial_stale_authority_rejected) {
  Scheduler s;
  for (int i = 1; i <= 2; ++i) s.publishParticipant(P(i, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  auto id = s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1));
  auto dec = s.schedule(); auto g = s.grantById(dec.grants[0]); REQUIRE(g);
  // Stale participant generation at revalidation.
  s.publishParticipant(P(1, 2, 1, 11));  // advance participant 1 gen to 2
  CHECK(s.revalidateGrant(*g) == GrantAdjudication::STALE_GRANT);
  CHECK(!s.handoff(*g));
}

TEST(adversarial_unknown_readiness_not_promoted) {
  Scheduler s;
  // Participant present but readiness source UNKNOWN -> not READY.
  Participant p = P(1, 1, 1, 11); p.ready = false; p.readiness_source = ProvenanceStatus::UNKNOWN;
  s.publishParticipant(p);
  s.publishParticipant(P(2, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  auto id = s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1, OverlapMode::OVERLAP_ALLOWED));
  auto dec = s.schedule();
  CHECK(dec.selected.empty());  // UNKNOWN readiness -> never silently READY
}

TEST(adversarial_duplicate_completion_rejected) {
  Scheduler s;
  for (int i = 1; i <= 2; ++i) s.publishParticipant(P(i, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  auto id = s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1));
  auto dec = s.schedule(); auto g = s.grantById(dec.grants[0]); REQUIRE(g); CHECK(s.handoff(*g));
  CHECK(s.complete(*g, WorkerBootId(11), SourceBootId(100)));
  // Duplicate completion must reject (exactly one terminal outcome).
  CHECK(!s.complete(*g, WorkerBootId(11), SourceBootId(100)));
}

TEST(adversarial_completion_before_start_rejected) {
  Scheduler s;
  for (int i = 1; i <= 2; ++i) s.publishParticipant(P(i, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  auto id = s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1));
  auto dec = s.schedule(); auto g = s.grantById(dec.grants[0]); REQUIRE(g);
  // Not handed off / not active.
  CHECK(!s.complete(*g, WorkerBootId(11), SourceBootId(100)));
}

TEST(adversarial_generation_never_regresses) {
  Scheduler s;
  s.publishParticipant(P(1, 1, 1, 11));
  bool threw = false;
  try { s.publishParticipant(P(1, 0, 1, 11)); } catch (const scheduler_error&) { threw = true; }
  CHECK(threw);  // participant generation regress rejected
}

TEST(adversarial_persistence_trailing_garbage_rejected) {
  Scheduler s;
  for (int i = 1; i <= 2; ++i) s.publishParticipant(P(i, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1));
  auto bytes = Persistence::save(s);
  auto bad = bytes; bad.push_back(0x55);  // trailing garbage
  Scheduler s2; bool threw = false;
  // Trailing garbage is detected by the checksum (the byte changes the checksum region). Actually appending after checksum -> expect_end catches it.
  try { Persistence::restore(s2, bad); threw = false; } catch (const scheduler_error&) { threw = true; }
  // Appending a byte shifts the checksum window; the stored checksum mismatches.
  CHECK(threw);
}
