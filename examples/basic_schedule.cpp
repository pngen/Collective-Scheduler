// Basic example: declare collectives, establish a conflict, obtain a
// deterministic grant/order, complete the first, obtain the next grant.
#include <collective_scheduler/scheduler.hpp>
#include <cstdio>

namespace cs = collective_scheduler;
using namespace cs;

static Participant mk_participant(std::uint64_t id) { Participant p; p.id = ParticipantId(id); p.gen = ParticipantGeneration(1); p.worker = WorkerId(1); p.worker_boot = WorkerBootId(11); p.source = SourceId(1); p.source_boot = SourceBootId(100); p.device = DeviceId(id); p.node = NodeId(1); p.nic = NicId(id); p.capability_gen = CapabilityGeneration(1); p.health_gen = HealthGeneration(1); p.topology_gen = TopologyGeneration(1); p.ready = true; p.readiness_source = ProvenanceStatus::REAL; p.provenance = ProvenanceStatus::REAL; return p; }

int main() {
  Scheduler s;
  s.publishParticipant(mk_participant(1)); s.publishParticipant(mk_participant(2)); s.publishParticipant(mk_participant(3));
  CommunicationPlan pl1; pl1.id = CommunicationPlanId(1); pl1.gen = CommunicationPlanGeneration(1); pl1.kind = CollectiveKind::ALL_REDUCE; pl1.topology_gen = TopologyGeneration(1); pl1.participants = {ParticipantId(1), ParticipantId(2)}; pl1.valid = true; pl1.provenance = ProvenanceStatus::SYNTHETIC;
  CommunicationPlan pl2; pl2.id = CommunicationPlanId(2); pl2.gen = CommunicationPlanGeneration(1); pl2.kind = CollectiveKind::ALL_REDUCE; pl2.topology_gen = TopologyGeneration(1); pl2.participants = {ParticipantId(2), ParticipantId(3)}; pl2.valid = true; pl2.provenance = ProvenanceStatus::SYNTHETIC;
  s.publishPlan(pl1); s.publishPlan(pl2);

  auto submit = [&](std::uint64_t c, std::vector<ParticipantId> parts, std::uint64_t plan) { CollectiveRequest r; r.collective = CollectiveId(c); r.collective_gen = CollectiveGeneration(1); r.kind = CollectiveKind::ALL_REDUCE; r.participants = parts; r.participant_gens.assign(parts.size(), ParticipantGeneration(1)); r.plan_id = CommunicationPlanId(plan); r.plan_gen = CommunicationPlanGeneration(1); r.workload_gen = WorkloadGeneration(1); r.priority.gen = PriorityGeneration(1); r.priority.authoritative = true; r.window.expected_duration_ns = 1000; return s.submit(std::move(r)); };
  auto c1 = submit(1, {ParticipantId(1), ParticipantId(2)}, 1);
  auto c2 = submit(2, {ParticipantId(2), ParticipantId(3)}, 2);  // conflicts with C1 via B

  auto dec1 = s.schedule();
  std::printf("first schedule selected %zu collective(s)\n", dec1.selected.size());
  auto g1 = s.grantById(dec1.grants[0]);
  if (g1) { std::printf("handoff C%llu accepted=%d\n", (unsigned long long)g1->collective.value, (int)s.handoff(*g1)); std::printf("complete C%llu ok=%d\n", (unsigned long long)g1->collective.value, (int)s.complete(*g1, WorkerBootId(11), SourceBootId(100))); }
  auto dec2 = s.schedule();
  std::printf("second schedule selected %zu collective(s)\n", dec2.selected.size());
  auto g2 = s.grantById(dec2.grants[0]);
  if (g2) std::printf("handoff C%llu accepted=%d\n", (unsigned long long)g2->collective.value, (int)s.handoff(*g2));
  std::printf("example complete\n");
  return 0;
}