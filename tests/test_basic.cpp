#include <collective_scheduler/scheduler.hpp>
#include "test_framework.hpp"

using namespace collective_scheduler;

namespace {
Participant P(std::uint64_t id, std::uint64_t gen, std::uint64_t worker, std::uint64_t boot) {
  Participant p;
  p.id = ParticipantId(id); p.gen = ParticipantGeneration(gen);
  p.worker = WorkerId(worker); p.worker_boot = WorkerBootId(boot);
  p.source = SourceId(1); p.source_boot = SourceBootId(100);
  p.device = DeviceId(id); p.node = NodeId(1); p.nic = NicId(id);
  p.capability_gen = CapabilityGeneration(1); p.health_gen = HealthGeneration(1); p.topology_gen = TopologyGeneration(1);
  p.ready = true; p.readiness_source = ProvenanceStatus::REAL; p.provenance = ProvenanceStatus::REAL;
  return p;
}
CommunicationPlan Plan(std::uint64_t id, std::uint64_t gen, CollectiveKind kind, std::vector<ParticipantId> parts, double bw = 1000000.0) {
  CommunicationPlan pl;
  pl.id = CommunicationPlanId(id); pl.gen = CommunicationPlanGeneration(gen); pl.kind = kind;
  pl.topology_gen = TopologyGeneration(1); pl.participants = parts;
  pl.valid = true; pl.provenance = ProvenanceStatus::SYNTHETIC;
  pl.estimated_duration_ns = 1000; pl.estimated_bandwidth = bw;
  return pl;
}
CollectiveRequest Req(std::uint64_t collective, CollectiveKind kind, std::vector<ParticipantId> parts, std::vector<ParticipantGeneration> gens, std::uint64_t planId, std::uint64_t planGen, OverlapMode overlap = OverlapMode::UNKNOWN) {
  CollectiveRequest r;
  r.collective = CollectiveId(collective); r.collective_gen = CollectiveGeneration(1); r.kind = kind;
  r.participants = std::move(parts); r.participant_gens = std::move(gens);
  r.plan_id = CommunicationPlanId(planId); r.plan_gen = CommunicationPlanGeneration(planGen);
  r.workload_gen = WorkloadGeneration(1); r.overlap = overlap;
  r.priority.gen = PriorityGeneration(1); r.priority.authoritative = true;
  r.window.expected_duration_ns = 1000; r.queue_class = QueueClass::STANDARD;
  return r;
}
}  // namespace

TEST(schedule_single_serial) {
  Scheduler s;
  s.publishParticipant(P(1, 1, 1, 11));
  s.publishParticipant(P(2, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  auto id = s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1));
  auto inf = s.info();
  CHECK_EQ(inf.records, 1);
  auto dec = s.schedule();
  CHECK_EQ(dec.selected.size(), 1);
  CHECK_EQ(dec.selected[0].value, id.value);
  auto g = s.grantById(dec.grants[0]);
  REQUIRE(g.has_value());
  CHECK(s.handoff(*g));
  CHECK(s.complete(*g, WorkerBootId(11), SourceBootId(100)));
  auto inf2 = s.info();
  CHECK_EQ(inf2.active_grants, 0);
  // Stale completion must reject.
  CHECK(!s.complete(*g, WorkerBootId(11), SourceBootId(100)));
}

TEST(schedule_conflict_serializes) {
  Scheduler s;
  s.publishParticipant(P(1, 1, 1, 11)); s.publishParticipant(P(2, 1, 1, 11)); s.publishParticipant(P(3, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  s.publishPlan(Plan(2, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(2), ParticipantId(3)}));
  // C1 uses A+B, C2 uses B+C -> conflict through B.
  auto c1 = s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1));
  auto c2 = s.submit(Req(2, CollectiveKind::ALL_REDUCE, {ParticipantId(2), ParticipantId(3)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 2, 1));
  auto dec1 = s.schedule();
  CHECK_EQ(dec1.selected.size(), 1);  // only one may run (they conflict through B)
  auto f = s.info().next_eligible;
  CHECK_EQ(f.size(), 1);  // the other waits
  auto g1 = s.grantById(dec1.grants[0]);
  REQUIRE(g1.has_value());
  CHECK(s.handoff(*g1));
  CHECK(s.complete(*g1, WorkerBootId(11), SourceBootId(100)));
  auto dec2 = s.schedule();
  CHECK_EQ(dec2.selected.size(), 1);  // now the blocked one is eligible
}

TEST(schedule_disjoint_overlap) {
  Scheduler s;
  s.publishParticipant(P(1, 1, 1, 11)); s.publishParticipant(P(2, 1, 1, 11));
  s.publishParticipant(P(3, 1, 1, 11)); s.publishParticipant(P(4, 1, 1, 11));
  // disjoint resource/link sets -> C1 and C2 can overlap.
  auto pl1 = Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)});
  auto pl2 = Plan(2, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(3), ParticipantId(4)});
  pl1.links = {LinkId(100)}; pl2.links = {LinkId(101)};
  s.publishPlan(pl1); s.publishPlan(pl2);
  auto c1 = s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1, OverlapMode::OVERLAP_ALLOWED));
  auto c2 = s.submit(Req(2, CollectiveKind::ALL_REDUCE, {ParticipantId(3), ParticipantId(4)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 2, 1, OverlapMode::OVERLAP_ALLOWED));
  auto dec = s.schedule();
  CHECK_EQ(dec.selected.size(), 2);  // both may overlap
  CHECK_EQ(dec.overlap_groups.size(), 1);
}
