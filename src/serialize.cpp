// Collective Scheduler — request/fairness serialization (implementation).
#include <collective_scheduler/serialize.hpp>

namespace collective_scheduler {

void serialize_request(PersistenceWriter& w, const CollectiveRequest& r) {
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

CollectiveRequest deserialize_request(PersistenceReader& r) {
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

void serialize_fairness(PersistenceWriter& w, const FairnessAccount& f) {
  w.u8(static_cast<std::uint8_t>(f.cls)); w.u64(f.observed_grants); w.f64(f.observed_service); w.f64(f.deficit);
  w.u64(f.wait_ticks); w.u64(f.consecutive_bypasses); w.u8(f.starved ? 1 : 0); w.f64(f.target_share);
}
FairnessAccount deserialize_fairness(PersistenceReader& r) {
  FairnessAccount f; f.cls = static_cast<FairnessClass>(r.u8()); f.observed_grants = r.u64(); f.observed_service = r.f64(); f.deficit = r.f64();
  f.wait_ticks = r.u64(); f.consecutive_bypasses = r.u64(); f.starved = r.u8() != 0; f.target_share = r.f64();
  return f;
}

void serialize_grant(PersistenceWriter& w, const CollectiveGrant& g) {
  w.id(g.id.value); w.id(g.gen.value);
  w.id(g.request.value); w.id(g.request_gen.value);
  w.id(g.collective.value); w.id(g.collective_gen.value);
  w.id(g.schedule.value); w.id(g.schedule_gen.value);
  w.u64(g.participants.size()); for (auto v : g.participants) w.id(v.value);
  w.u64(g.participant_gens.size()); for (auto v : g.participant_gens) w.id(v.value);
  w.id(g.plan.value); w.id(g.plan_gen.value);
  w.id(g.evidence.epoch.value);
  w.id(g.evidence.collective_gen.value); w.id(g.evidence.request_gen.value); w.id(g.evidence.plan_gen.value);
  w.id(g.evidence.participant_gen.value); w.id(g.evidence.dependency_gen.value);
  w.id(g.evidence.reservation_gen.value); w.id(g.evidence.congestion_gen.value); w.id(g.evidence.capacity_gen.value);
  w.id(g.evidence.policy_gen.value); w.id(g.evidence.priority_gen.value); w.id(g.evidence.fairness_gen.value);
  w.id(g.evidence.topology_gen.value); w.id(g.evidence.capability_gen.value); w.id(g.evidence.health_gen.value);
  w.u8(g.revalidation.require_fresh_participants ? 1 : 0); w.u8(g.revalidation.require_fresh_plan ? 1 : 0);
  w.u8(g.revalidation.require_fresh_reservation ? 1 : 0); w.u8(g.revalidation.require_fresh_dependencies ? 1 : 0);
  w.u8(g.revalidation.require_fresh_congestion ? 1 : 0); w.u8(g.revalidation.require_fresh_capacity ? 1 : 0);
  w.u8(g.revalidation.require_fresh_execution_authority ? 1 : 0); w.u8(g.revalidation.require_current_epoch ? 1 : 0);
  w.id(g.workload.value);
  w.id(g.execution_attempt.value); w.id(g.execution_attempt_gen.value);
  w.id(g.policy_gen.value);
  w.u8(g.active ? 1 : 0); w.u8(g.terminal ? 1 : 0);
}
CollectiveGrant deserialize_grant(PersistenceReader& r) {
  CollectiveGrant g;
  g.id = CollectiveGrantId(r.id()); g.gen = CollectiveGrantGeneration(r.id());
  g.request = CollectiveRequestId(r.id()); g.request_gen = CollectiveRequestGeneration(r.id());
  g.collective = CollectiveId(r.id()); g.collective_gen = CollectiveGeneration(r.id());
  g.schedule = CollectiveScheduleId(r.id()); g.schedule_gen = CollectiveScheduleGeneration(r.id());
  auto np = r.u64(); for (std::uint64_t i = 0; i < np; ++i) g.participants.push_back(ParticipantId(r.id()));
  auto ng = r.u64(); for (std::uint64_t i = 0; i < ng; ++i) g.participant_gens.push_back(ParticipantGeneration(r.id()));
  g.plan = CommunicationPlanId(r.id()); g.plan_gen = CommunicationPlanGeneration(r.id());
  g.evidence.epoch = CoordinatorEpoch(r.id());
  g.evidence.collective_gen = CollectiveGeneration(r.id()); g.evidence.request_gen = CollectiveRequestGeneration(r.id()); g.evidence.plan_gen = CommunicationPlanGeneration(r.id());
  g.evidence.participant_gen = ParticipantGeneration(r.id()); g.evidence.dependency_gen = DependencyGeneration(r.id());
  g.evidence.reservation_gen = ReservationGeneration(r.id()); g.evidence.congestion_gen = CongestionGeneration(r.id()); g.evidence.capacity_gen = CapacityGeneration(r.id());
  g.evidence.policy_gen = PolicyGeneration(r.id()); g.evidence.priority_gen = PriorityGeneration(r.id()); g.evidence.fairness_gen = FairnessGeneration(r.id());
  g.evidence.topology_gen = TopologyGeneration(r.id()); g.evidence.capability_gen = CapabilityGeneration(r.id()); g.evidence.health_gen = HealthGeneration(r.id());
  g.revalidation.require_fresh_participants = r.u8() != 0; g.revalidation.require_fresh_plan = r.u8() != 0;
  g.revalidation.require_fresh_reservation = r.u8() != 0; g.revalidation.require_fresh_dependencies = r.u8() != 0;
  g.revalidation.require_fresh_congestion = r.u8() != 0; g.revalidation.require_fresh_capacity = r.u8() != 0;
  g.revalidation.require_fresh_execution_authority = r.u8() != 0; g.revalidation.require_current_epoch = r.u8() != 0;
  g.workload = WorkloadId(r.id());
  g.execution_attempt = ExecutionId(r.id()); g.execution_attempt_gen = ExecutionGeneration(r.id());
  g.policy_gen = PolicyGeneration(r.id());
  g.active = r.u8() != 0; g.terminal = r.u8() != 0;
  return g;
}

void serialize_participant(PersistenceWriter& w, const Participant& p) {
  w.id(p.id.value); w.id(p.gen.value); w.id(p.worker.value); w.id(p.worker_boot.value);
  w.id(p.source.value); w.id(p.source_boot.value);
  w.id(p.device.value); w.id(p.node.value); w.id(p.nic.value);
  w.id(p.capability_gen.value); w.id(p.health_gen.value); w.id(p.topology_gen.value);
  w.u32(p.process_id); w.u8(p.ready ? 1 : 0); w.u8(static_cast<std::uint8_t>(p.readiness_source)); w.u8(static_cast<std::uint8_t>(p.provenance));
}
Participant deserialize_participant(PersistenceReader& r) {
  Participant p;
  p.id = ParticipantId(r.id()); p.gen = ParticipantGeneration(r.id()); p.worker = WorkerId(r.id()); p.worker_boot = WorkerBootId(r.id());
  p.source = SourceId(r.id()); p.source_boot = SourceBootId(r.id());
  p.device = DeviceId(r.id()); p.node = NodeId(r.id()); p.nic = NicId(r.id());
  p.capability_gen = CapabilityGeneration(r.id()); p.health_gen = HealthGeneration(r.id()); p.topology_gen = TopologyGeneration(r.id());
  p.process_id = r.u32(); p.ready = r.u8() != 0; p.readiness_source = static_cast<ProvenanceStatus>(r.u8()); p.provenance = static_cast<ProvenanceStatus>(r.u8());
  return p;
}

void serialize_plan(PersistenceWriter& w, const CommunicationPlan& pl) {
  w.id(pl.id.value); w.id(pl.gen.value); w.u8(static_cast<std::uint8_t>(pl.kind)); w.id(pl.topology_gen.value);
  w.u64(pl.participants.size()); for (auto v : pl.participants) w.id(v.value);
  w.u64(pl.flows.size()); for (auto v : pl.flows) w.id(v.value);
  w.u64(pl.resources.size()); for (auto v : pl.resources) w.id(v.value);
  w.u64(pl.links.size()); for (auto v : pl.links) w.id(v.value);
  w.u64(pl.paths.size()); for (auto v : pl.paths) w.id(v.value);
  w.u64(pl.byte_count); w.f64(pl.estimated_bandwidth); w.u64(pl.estimated_duration_ns); w.u64(pl.communication_cost);
  w.u8(static_cast<std::uint8_t>(pl.provenance)); w.u8(pl.valid ? 1 : 0);
}
CommunicationPlan deserialize_plan(PersistenceReader& r) {
  CommunicationPlan pl;
  pl.id = CommunicationPlanId(r.id()); pl.gen = CommunicationPlanGeneration(r.id()); pl.kind = static_cast<CollectiveKind>(r.u8()); pl.topology_gen = TopologyGeneration(r.id());
  auto np = r.u64(); for (std::uint64_t i = 0; i < np; ++i) pl.participants.push_back(ParticipantId(r.id()));
  auto nf = r.u64(); for (std::uint64_t i = 0; i < nf; ++i) pl.flows.push_back(FlowId(r.id()));
  auto nr = r.u64(); for (std::uint64_t i = 0; i < nr; ++i) pl.resources.push_back(ResourceId(r.id()));
  auto nl = r.u64(); for (std::uint64_t i = 0; i < nl; ++i) pl.links.push_back(LinkId(r.id()));
  auto npth = r.u64(); for (std::uint64_t i = 0; i < npth; ++i) pl.paths.push_back(PathId(r.id()));
  pl.byte_count = r.u64(); pl.estimated_bandwidth = r.f64(); pl.estimated_duration_ns = r.u64(); pl.communication_cost = r.u64();
  pl.provenance = static_cast<ProvenanceStatus>(r.u8()); pl.valid = r.u8() != 0;
  return pl;
}

void serialize_congestion(PersistenceWriter& w, const CongestionState& c) {
  w.id(c.gen.value); w.f64(c.utilization); w.f64(c.residual_bandwidth); w.u8(c.is_congested ? 1 : 0);
  w.u8(c.hard_ceiling ? 1 : 0); w.f64(c.hard_ceiling_utilization); w.u8(c.backpressure_intent ? 1 : 0);
  w.u64(c.bottlenecks.size()); for (auto v : c.bottlenecks) w.id(v.value);
  w.u8(static_cast<std::uint8_t>(c.provenance));
}
CongestionState deserialize_congestion(PersistenceReader& r) {
  CongestionState c; c.gen = CongestionGeneration(r.id()); c.utilization = r.f64(); c.residual_bandwidth = r.f64(); c.is_congested = r.u8() != 0;
  c.hard_ceiling = r.u8() != 0; c.hard_ceiling_utilization = r.f64(); c.backpressure_intent = r.u8() != 0;
  auto nb = r.u64(); for (std::uint64_t i = 0; i < nb; ++i) c.bottlenecks.push_back(LinkId(r.id()));
  c.provenance = static_cast<ProvenanceStatus>(r.u8());
  return c;
}

void serialize_reservation(PersistenceWriter& w, const Reservation& res) {
  w.id(res.gen.value); w.u8(static_cast<std::uint8_t>(res.strength)); w.f64(res.reserved_bandwidth);
  w.u64(res.resources.size()); for (auto v : res.resources) w.id(v.value);
  w.u64(res.links.size()); for (auto v : res.links) w.id(v.value);
  w.u64(res.window_start_ns); w.u64(res.window_end_ns); w.u8(static_cast<std::uint8_t>(res.provenance)); w.u8(res.valid ? 1 : 0);
}
Reservation deserialize_reservation(PersistenceReader& r) {
  Reservation res; res.gen = ReservationGeneration(r.id()); res.strength = static_cast<ReservationStrength>(r.u8()); res.reserved_bandwidth = r.f64();
  auto nr = r.u64(); for (std::uint64_t i = 0; i < nr; ++i) res.resources.push_back(ResourceId(r.id()));
  auto nl = r.u64(); for (std::uint64_t i = 0; i < nl; ++i) res.links.push_back(LinkId(r.id()));
  res.window_start_ns = r.u64(); res.window_end_ns = r.u64(); res.provenance = static_cast<ProvenanceStatus>(r.u8()); res.valid = r.u8() != 0;
  return res;
}

void serialize_dependency(PersistenceWriter& w, const DependencyState& d) {
  w.id(d.id.value); w.u8(static_cast<std::uint8_t>(d.kind)); w.id(d.gen.value); w.u8(d.satisfied ? 1 : 0); w.u8(static_cast<std::uint8_t>(d.source));
}
DependencyState deserialize_dependency(PersistenceReader& r) {
  DependencyState d; d.id = DependencyId(r.id()); d.kind = static_cast<DependencyKind>(r.u8()); d.gen = DependencyGeneration(r.id()); d.satisfied = r.u8() != 0; d.source = static_cast<ProvenanceStatus>(r.u8());
  return d;
}

void serialize_capacity(PersistenceWriter& w, const CapacityState& c) {
  w.id(c.gen.value); w.f64(c.usable_capacity); w.f64(c.headroom); w.f64(c.confidence); w.u8(static_cast<std::uint8_t>(c.provenance));
}
CapacityState deserialize_capacity(PersistenceReader& r) {
  CapacityState c; c.gen = CapacityGeneration(r.id()); c.usable_capacity = r.f64(); c.headroom = r.f64(); c.confidence = r.f64(); c.provenance = static_cast<ProvenanceStatus>(r.u8());
  return c;
}

}  // namespace collective_scheduler
