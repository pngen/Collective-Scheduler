// Collective Scheduler — the temporal arbitration runtime.
//
// Collective Scheduler 1.0.0
// Copyright 2026 Summon Software Labs.

#pragma once

#include <collective_scheduler/capacity.hpp>
#include <collective_scheduler/config.hpp>
#include <collective_scheduler/conflict.hpp>
#include <collective_scheduler/congestion.hpp>
#include <collective_scheduler/dependency.hpp>
#include <collective_scheduler/domains.hpp>
#include <collective_scheduler/enums.hpp>
#include <collective_scheduler/fairness.hpp>
#include <collective_scheduler/lifecycle.hpp>
#include <collective_scheduler/overlap.hpp>
#include <collective_scheduler/participant.hpp>
#include <collective_scheduler/plan.hpp>
#include <collective_scheduler/ports.hpp>
#include <collective_scheduler/readiness.hpp>
#include <collective_scheduler/request.hpp>
#include <collective_scheduler/reservation.hpp>
#include <collective_scheduler/scheduling.hpp>
#include <collective_scheduler/time.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

namespace collective_scheduler {

class PersistenceWriter;
class PersistenceReader;

class Scheduler {
 public:
  struct Options {
    PolicyConfig policy;
    std::shared_ptr<ResourceBrokerPort> broker;
    std::shared_ptr<CollectiveFabricPort> fabric;
    std::shared_ptr<PreemptionFabricPort> preemption;
    CoordinatorEpoch start_epoch = CoordinatorEpoch(1);
  };

  explicit Scheduler(Options opts = {});
  ~Scheduler();

  Scheduler(const Scheduler&) = delete;
  Scheduler& operator=(const Scheduler&) = delete;

  void publishParticipant(Participant p);
  void publishPlan(CommunicationPlan plan);
  void publishDependency(DependencyState dep);
  void publishReservation(Reservation res);
  void publishCongestion(CongestionState cong);
  void publishCapacity(CapacityState cap);
  void setPolicy(PolicyConfig cfg);

  CollectiveRequestId submit(CollectiveRequest req);
  void supersede(CollectiveRequest new_request);
  void cancel(CollectiveRequestId id);

  void advanceCoordinatorEpoch(CoordinatorEpoch epoch);
  void advanceTime(Nanoseconds d);
  CoordinatorEpoch coordinatorEpoch() const;

  ScheduleDecision schedule();
  GrantAdjudication revalidateGrant(const CollectiveGrant& grant);
  bool handoff(const CollectiveGrant& grant);
  bool complete(const CollectiveGrant& grant, const WorkerBootId& completer_boot, const SourceBootId& source_boot);

  std::optional<CollectiveGrant> grantById(CollectiveGrantId id) const;

  // Fence a worker boot: any evidence from this boot becomes stale.
  void fenceWorkerBoot(WorkerId worker, WorkerBootId boot);

  struct Info {
    std::size_t records = 0;
    std::size_t participants = 0;
    std::size_t plans = 0;
    std::size_t queued = 0;
    std::size_t ready = 0;
    std::size_t eligible = 0;
    std::size_t active_grants = 0;
    std::size_t retired = 0;
    CapacityGeneration capacity_gen;
    CongestionGeneration congestion_gen;
    ReservationGeneration reservation_gen;
    CoordinatorEpoch epoch;
    Nanoseconds now_ns = 0;
    std::vector<CollectiveRequestId> next_eligible;
  };
  Info info() const;

  void resetDynamicStateForRecovery();
  CapacityGeneration capacityGeneration() const;
  CongestionGeneration congestionGeneration() const;
  ReservationGeneration reservationGeneration() const;

 private:
  friend class Persistence;
  class Impl;
  struct Record;
  struct GrantState;
  std::shared_ptr<Impl> impl_;

  // Persistence hooks; implemented in scheduler.cpp where Impl/Record are complete.
  void serializeState(PersistenceWriter& w) const;
  void deserializeState(PersistenceReader& r);
};

}  // namespace collective_scheduler