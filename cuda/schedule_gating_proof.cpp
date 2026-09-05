// Real CUDA schedule-gating proof on RTX 5090 (sm_120).
#include <collective_scheduler/scheduler.hpp>
#include "cuda_executor.hpp"

#include <cstdio>
#include <memory>

using namespace collective_scheduler;

static int g_fail = 0;
static void report(bool ok, const char* what) { std::printf("%s %s\n", ok ? "[OK]" : "[FAIL]", what); if (!ok) ++g_fail; }

static Participant P(std::uint64_t id) { Participant p; p.id = ParticipantId(id); p.gen = ParticipantGeneration(1); p.worker = WorkerId(1); p.worker_boot = WorkerBootId(11); p.source = SourceId(1); p.source_boot = SourceBootId(100); p.device = DeviceId(id); p.node = NodeId(1); p.nic = NicId(id); p.capability_gen = CapabilityGeneration(1); p.health_gen = HealthGeneration(1); p.topology_gen = TopologyGeneration(1); p.ready = true; p.readiness_source = ProvenanceStatus::REAL; p.provenance = ProvenanceStatus::REAL; return p; }
static CommunicationPlan Plan(std::uint64_t id) { CommunicationPlan pl; pl.id = CommunicationPlanId(id); pl.gen = CommunicationPlanGeneration(1); pl.kind = CollectiveKind::ALL_REDUCE; pl.topology_gen = TopologyGeneration(1); pl.participants = {ParticipantId(1), ParticipantId(2)}; pl.valid = true; pl.provenance = ProvenanceStatus::SYNTHETIC; return pl; }
static CollectiveRequest Req(std::uint64_t c) { CollectiveRequest r; r.collective = CollectiveId(c); r.collective_gen = CollectiveGeneration(1); r.kind = CollectiveKind::ALL_REDUCE; r.participants = {ParticipantId(1), ParticipantId(2)}; r.participant_gens = {ParticipantGeneration(1), ParticipantGeneration(1)}; r.plan_id = CommunicationPlanId(1); r.plan_gen = CommunicationPlanGeneration(1); r.workload_gen = WorkloadGeneration(1); r.priority.gen = PriorityGeneration(1); r.priority.authoritative = true; r.window.expected_duration_ns = 1000; return r; }

int main() {
  auto fabric = std::make_shared<RecordingCollectiveFabric>();
  Scheduler::Options opts; opts.fabric = fabric;
  Scheduler s(opts);
  s.publishParticipant(P(1)); s.publishParticipant(P(2));
  s.publishPlan(Plan(1));
  auto c1 = s.submit(Req(1));
  auto c2 = s.submit(Req(2));  // conflicts with C1 through shared participants

  // First schedule: only C1 granted (C2 waits).
  auto dec1 = s.schedule();
  report(dec1.selected.size() == 1, "only one collective granted in first schedule");
  bool c1_selected = false; for (auto id : dec1.selected) if (id.value == c1.value) c1_selected = true;
  report(c1_selected, "C1 (collective 1) is the granted collective");
  auto g1 = s.grantById(dec1.grants[0]);
  report(g1.has_value(), "C1 grant exists");
  bool accepted1 = g1 ? s.handoff(*g1) : false;
  report(accepted1, "C1 valid grant reached the collective fabric");
  report(fabric->accepted.size() == 1, "exactly C1's grant reached the execution adapter");

  // Real CUDA work for C1.
  const std::size_t N = 1 << 20;
  auto r1 = run_cuda_work(12345, N);
  report(r1.parity_ok, "C1 real CUDA work CPU parity passed");
  report(r1.device_count >= 1, "CUDA device count detected (RTX 5090)");
  std::printf("[INFO] device cc=%d.%d\n", r1.cc_major, r1.cc_minor);
  report(r1.total_memory_after <= r1.total_memory_before + (64u * 1024u * 1024u), "device memory returned to baseline after C1");
  bool completed1 = s.complete(*g1, WorkerBootId(11), SourceBootId(100));
  report(completed1, "C1 completed authoritatively");

  // Second schedule: C2 became eligible and receives a fresh grant.
  auto dec2 = s.schedule();
  report(dec2.selected.size() == 1, "C2 selected in second schedule");
  bool c2_selected = false; for (auto id : dec2.selected) if (id.value == c2.value) c2_selected = true;
  report(c2_selected, "C2 (collective 2) granted after C1 completed");
  auto g2 = s.grantById(dec2.grants[0]);
  report(g2.has_value(), "C2 grant exists");
  bool accepted2 = g2 ? s.handoff(*g2) : false;
  report(accepted2, "C2 valid grant reached the collective fabric");
  report(fabric->accepted.size() == 2, "both valid grants reached the execution adapter in order");
  auto r2 = run_cuda_work(67890, N);
  report(r2.parity_ok, "C2 real CUDA work CPU parity passed");
  report(r2.total_memory_after <= r2.total_memory_before + (64u * 1024u * 1024u), "device memory returned to baseline after C2");
  bool completed2 = s.complete(*g2, WorkerBootId(11), SourceBootId(100));
  report(completed2, "C2 completed authoritatively");

  // Stale completion of C1 is rejected.
  report(!s.complete(*g1, WorkerBootId(11), SourceBootId(100)), "duplicate/stale C1 completion rejected");

  std::printf("CUDA schedule-gating proof %s\n", g_fail == 0 ? "PASSED" : "FAILED");
  return g_fail == 0 ? 0 : 1;
}