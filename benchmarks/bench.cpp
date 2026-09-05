// Collective Scheduler — meaningful-scale benchmarks.
#include <collective_scheduler/scheduler.hpp>

#include <chrono>
#include <cstdio>
#include <vector>

using namespace collective_scheduler;

static void mk(int nparts, Scheduler& s) {
  for (int i = 1; i <= nparts; ++i) { Participant p; p.id = ParticipantId(i); p.gen = ParticipantGeneration(1); p.worker = WorkerId(1); p.worker_boot = WorkerBootId(11); p.source = SourceId(1); p.source_boot = SourceBootId(100); p.device = DeviceId(i); p.node = NodeId(1); p.nic = NicId(i); p.capability_gen = CapabilityGeneration(1); p.health_gen = HealthGeneration(1); p.topology_gen = TopologyGeneration(1); p.ready = true; p.readiness_source = ProvenanceStatus::REAL; p.provenance = ProvenanceStatus::REAL; s.publishParticipant(p); }
}

static void submit_n(Scheduler& s, int n, int nparts) {
  for (int c = 0; c < n; ++c) {
    CollectiveRequest r; r.collective = CollectiveId(100000 + c); r.collective_gen = CollectiveGeneration(1); r.kind = CollectiveKind::ALL_REDUCE;
    r.participants = {ParticipantId(1 + (c % (nparts - 1))), ParticipantId(nparts)}; r.participant_gens = {ParticipantGeneration(1), ParticipantGeneration(1)};
    r.plan_id = CommunicationPlanId(1); r.plan_gen = CommunicationPlanGeneration(1); r.workload_gen = WorkloadGeneration(1);
    r.priority.gen = PriorityGeneration(1); r.priority.authoritative = true; r.priority.value = c % 10; r.window.expected_duration_ns = 1000;
    try { s.submit(std::move(r)); } catch (...) {}
  }
}

template <typename F> double ms(F f) { auto t0 = std::chrono::steady_clock::now(); f(); auto t1 = std::chrono::steady_clock::now(); return std::chrono::duration<double, std::milli>(t1 - t0).count(); }

int main() {
  const int nparts = 256;
  // Publish plans once.
  for (const int n : {100, 1000, 10000}) {
    Scheduler s; mk(nparts, s);
    CommunicationPlan pl; pl.id = CommunicationPlanId(1); pl.gen = CommunicationPlanGeneration(1); pl.kind = CollectiveKind::ALL_REDUCE; pl.topology_gen = TopologyGeneration(1); pl.participants = {ParticipantId(1), ParticipantId(nparts)}; pl.valid = true; pl.provenance = ProvenanceStatus::SYNTHETIC; s.publishPlan(pl);
    double ingest = ms([&]{ submit_n(s, n, nparts); });
    double schedule_ms = ms([&]{ for (int i = 0; i < 50; ++i) { auto d = s.schedule(); (void)d; } });
    std::printf("n=%d  ingest=%.2fms  schedule(x50)=%.2fms\n", n, ingest, schedule_ms);
    std::fflush(stdout);
  }
  // One heavily-contended participant.
  {
    Scheduler s; mk(nparts, s);
    CommunicationPlan pl; pl.id = CommunicationPlanId(1); pl.gen = CommunicationPlanGeneration(1); pl.kind = CollectiveKind::ALL_REDUCE; pl.topology_gen = TopologyGeneration(1); pl.participants = {ParticipantId(1), ParticipantId(2)}; pl.valid = true; pl.provenance = ProvenanceStatus::SYNTHETIC; s.publishPlan(pl);
    submit_n(s, 1000, nparts);
    double t = ms([&]{ auto d = s.schedule(); (void)d; });
    std::printf("contended n=1000 schedule=%.2fms\n", t); std::fflush(stdout);
  }
  return 0;
}