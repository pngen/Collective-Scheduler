#pragma once
#include <collective_scheduler/scheduler.hpp>

namespace tdata {
using namespace collective_scheduler;

inline Participant P(std::uint64_t id, std::uint64_t gen, std::uint64_t worker, std::uint64_t boot) {
  Participant p;
  p.id = ParticipantId(id); p.gen = ParticipantGeneration(gen);
  p.worker = WorkerId(worker); p.worker_boot = WorkerBootId(boot);
  p.source = SourceId(1); p.source_boot = SourceBootId(100);
  p.device = DeviceId(id); p.node = NodeId(1); p.nic = NicId(id);
  p.capability_gen = CapabilityGeneration(1); p.health_gen = HealthGeneration(1); p.topology_gen = TopologyGeneration(1);
  p.ready = true; p.readiness_source = ProvenanceStatus::REAL; p.provenance = ProvenanceStatus::REAL;
  return p;
}

inline CommunicationPlan Plan(std::uint64_t id, std::uint64_t gen, CollectiveKind kind, std::vector<ParticipantId> parts, double bw = 1000000.0) {
  CommunicationPlan pl;
  pl.id = CommunicationPlanId(id); pl.gen = CommunicationPlanGeneration(gen); pl.kind = kind;
  pl.topology_gen = TopologyGeneration(1); pl.participants = parts;
  pl.valid = true; pl.provenance = ProvenanceStatus::SYNTHETIC;
  pl.estimated_duration_ns = 1000; pl.estimated_bandwidth = bw;
  return pl;
}

inline CollectiveRequest Req(std::uint64_t collective, CollectiveKind kind, std::vector<ParticipantId> parts, std::vector<ParticipantGeneration> gens, std::uint64_t planId, std::uint64_t planGen, OverlapMode overlap = OverlapMode::UNKNOWN) {
  CollectiveRequest r;
  r.collective = CollectiveId(collective); r.collective_gen = CollectiveGeneration(1); r.kind = kind;
  r.participants = std::move(parts); r.participant_gens = std::move(gens);
  r.plan_id = CommunicationPlanId(planId); r.plan_gen = CommunicationPlanGeneration(planGen);
  r.workload_gen = WorkloadGeneration(1); r.overlap = overlap;
  r.priority.gen = PriorityGeneration(1); r.priority.authoritative = true;
  r.window.expected_duration_ns = 1000; r.queue_class = QueueClass::STANDARD;
  return r;
}
}  // namespace tdata