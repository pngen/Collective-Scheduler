// Collective Scheduler — narrow integration ports.
//
// Collective Scheduler 1.0.0
// Copyright 2026 Summon Software Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <collective_scheduler/domains.hpp>
#include <collective_scheduler/enums.hpp>
#include <collective_scheduler/request.hpp>
#include <collective_scheduler/scheduling.hpp>

#include <functional>

namespace collective_scheduler {

// The scheduler consumes external evidence and hands off execution. It never
// reimplements dependency traversal, congestion measurement, reservation
// creation, route computation, or resource arbitration. Each adjacent runtime
// exposes a narrow port implemented by an adapter.

// Resource Broker: owns actual scarce-resource claims. A schedule decision is
// not resource ownership. The scheduler must validate mandatory claims before
// granting and release them on cancellation / completion failure.
class ResourceBrokerPort {
 public:
  virtual ~ResourceBrokerPort() = default;

  // Returns true if all claims can be satisfied without committing.
  virtual bool can_claim(const WorkloadId& workload,
                         const std::vector<ResourceClaim>& claims) const = 0;
  // Atomically claim the resources; returns false if any mandatory claim fails.
  virtual bool claim(const WorkloadId& workload,
                     const std::vector<ResourceClaim>& claims) = 0;
  // Release resources previously claimed for the workload.
  virtual void release(const WorkloadId& workload) = 0;
  // Return a bounded, deterministic typing of the claim reject reason.
  virtual RejectionReason last_rejection() const { return RejectionReason::NONE; }
};

// Collective Fabric: owns collective execution semantics. The scheduler hands
// over an authoritative grant; the fabric returns accepted / started / progress
// / completed / failed / cancelled. This port is the handoff boundary.
class CollectiveFabricPort {
 public:
  virtual ~CollectiveFabricPort() = default;
  // Hand a valid grant to the fabric. The fabric decides whether to accept and
  // begin execution. Returns true if accepted.
  virtual bool accept(const CollectiveGrant& grant) = 0;
  // Notify the fabric that an in-flight grant must not proceed (cancellation).
  virtual void revoke(const CollectiveGrant& grant) = 0;
};

// Preemption Fabric: owns safe interruption. Collective Scheduler emits a
// bounded, typed preemption request; it never performs unsafe preemption.
class PreemptionFabricPort {
 public:
  virtual ~PreemptionFabricPort() = default;
  virtual void request_preemption(const CollectiveRequestId& holder,
                                  const CollectiveRequestId& requester,
                                  PriorityInversionAction action) = 0;
};

// A no-op resource broker that always satisfies claims and deterministically
// rejects nothing. Useful as a default and in tests that ignore scarce claims.
class AlwaysGrantingResourceBroker final : public ResourceBrokerPort {
 public:
  bool can_claim(const WorkloadId&, const std::vector<ResourceClaim>&) const override { return true; }
  bool claim(const WorkloadId&, const std::vector<ResourceClaim>&) override { return true; }
  void release(const WorkloadId&) override {}
};

// A no-op collective fabric that tracks accepted grants for inspection.
class RecordingCollectiveFabric final : public CollectiveFabricPort {
 public:
  bool accept(const CollectiveGrant& grant) override {
    accepted.push_back(grant);
    return true;
  }
  void revoke(const CollectiveGrant&) override { revoked = true; }
  std::vector<CollectiveGrant> accepted;
  bool revoked = false;
};

}  // namespace collective_scheduler
