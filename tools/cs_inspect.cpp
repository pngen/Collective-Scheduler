// CLI inspection tool: prints scheduler state for a demo scenario.
#include <collective_scheduler/scheduler.hpp>
#include <collective_scheduler/enums.hpp>
#include <cstdio>
#include <string>

namespace cs = collective_scheduler;
using namespace cs;

int main() {
  Scheduler s;
  for (int i = 1; i <= 4; ++i) { Participant p; p.id = ParticipantId(i); p.gen = ParticipantGeneration(1); p.worker = WorkerId(1); p.worker_boot = WorkerBootId(11); p.source = SourceId(1); p.source_boot = SourceBootId(100); p.device = DeviceId(i); p.node = NodeId(1); p.nic = NicId(i); p.capability_gen = CapabilityGeneration(1); p.health_gen = HealthGeneration(1); p.topology_gen = TopologyGeneration(1); p.ready = true; p.readiness_source = ProvenanceStatus::REAL; p.provenance = ProvenanceStatus::REAL; s.publishParticipant(p); }
  CommunicationPlan pl; pl.id = CommunicationPlanId(1); pl.gen = CommunicationPlanGeneration(1); pl.kind = CollectiveKind::ALL_REDUCE; pl.topology_gen = TopologyGeneration(1); pl.participants = {ParticipantId(1), ParticipantId(2)}; pl.valid = true; pl.provenance = ProvenanceStatus::SYNTHETIC; s.publishPlan(pl);
  for (int c = 1; c <= 3; ++c) { CollectiveRequest r; r.collective = CollectiveId(c); r.collective_gen = CollectiveGeneration(1); r.kind = CollectiveKind::ALL_REDUCE; r.participants = {ParticipantId(c), ParticipantId(c % 4 + 1)}; r.participant_gens = {ParticipantGeneration(1), ParticipantGeneration(1)}; r.plan_id = CommunicationPlanId(1); r.plan_gen = CommunicationPlanGeneration(1); r.workload_gen = WorkloadGeneration(1); r.priority.gen = PriorityGeneration(1); r.priority.authoritative = true; r.window.expected_duration_ns = 1000; s.submit(std::move(r)); }
  auto inf = s.info();
  std::printf("records=%zu participants=%zu plans=%zu active_grants=%zu epoch=%llu\n", inf.records, inf.participants, inf.plans, inf.active_grants, (unsigned long long)inf.epoch.value);
  std::printf("next_eligible: "); for (auto id : inf.next_eligible) std::printf("%llu ", (unsigned long long)id.value); std::printf("\n");
  auto dec = s.schedule();
  std::printf("selected (schedule gen %llu): ", (unsigned long long)dec.gen.value); for (auto id : dec.selected) std::printf("%llu ", (unsigned long long)id.value); std::printf("\n");
  std::printf("ranking factors (top): "); for (auto& f : dec.ranking_factors) std::printf("%s=%g ", std::string(name_of(f.kind)).c_str(), f.value); std::printf("\n");
  return 0;
}