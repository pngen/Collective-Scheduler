#include <collective_scheduler/persistence.hpp>
#include "test_common.hpp"
#include "test_framework.hpp"
#include <exception>

using namespace collective_scheduler;
using namespace tdata;

TEST(persistence_roundtrip_completed_survives) {
  Scheduler s;
  s.publishParticipant(P(1, 1, 1, 11));
  s.publishParticipant(P(2, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  auto id = s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1));
  auto dec = s.schedule();
  REQUIRE(dec.selected.size() == 1);
  auto g = s.grantById(dec.grants[0]);
  REQUIRE(g.has_value());
  REQUIRE(s.handoff(*g));
  REQUIRE(s.complete(*g, WorkerBootId(11), SourceBootId(100)));
  auto bytes = Persistence::save(s);
  CHECK(!bytes.empty());

  Scheduler s2;
  Persistence::restore(s2, bytes);
  auto inf = s2.info();
  CHECK_EQ(inf.records, 1);
  // The completed collective's durable identity survives.
  CHECK_EQ(inf.active_grants, 0);
}

TEST(persistence_recovery_is_conservative) {
  Scheduler s;
  s.publishParticipant(P(1, 1, 1, 11));
  s.publishParticipant(P(2, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  auto id = s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1));
  auto dec = s.schedule();
  auto g = s.grantById(dec.grants[0]);
  REQUIRE(g.has_value());
  REQUIRE(s.handoff(*g));  // ACTIVE, not completed
  auto bytes = Persistence::save(s);

  Scheduler s2;
  Persistence::restore(s2, bytes);
  // Dynamic evidence cleared: participants gone, record revalidation-required.
  auto inf = s2.info();
  CHECK_EQ(inf.participants, 0);
  CHECK_EQ(inf.active_grants, 0);
  // The old grant must not be executable: no fresh participants / plan / epoch.
  auto dec2 = s2.schedule();
  CHECK(dec2.selected.empty());
}

TEST(persistence_corruption_rejected) {
  Scheduler s;
  s.publishParticipant(P(1, 1, 1, 11));
  s.publishParticipant(P(2, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  auto id = s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1));
  auto bytes = Persistence::save(s);

  // Corrupt a byte in the body -> checksum mismatch must throw.
  auto bad = bytes;
  bad[20] ^= 0xff;
  auto threw = false;
  Scheduler s2;
  try { Persistence::restore(s2, bad); } catch (const scheduler_error&) { threw = true; }
  CHECK(threw);

  // Bad version must reject.
  auto badver = bytes;
  badver[4] = 0xff;  // version byte
  threw = false;
  try { Persistence::restore(s2, badver); } catch (const scheduler_error&) { threw = true; }
  CHECK(threw);

  // Truncation must reject.
  auto trunc = std::vector<std::uint8_t>(bytes.begin(), bytes.begin() + 6);
  threw = false;
  try { Persistence::restore(s2, trunc); } catch (const scheduler_error&) { threw = true; }
  CHECK(threw);
}
