// Collective Scheduler — declared identity/generation domains.
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

#include <collective_scheduler/identity.hpp>

namespace collective_scheduler {

// Macro declarations. Each domain gets its own compile-time tag so that an
// Id and a Gen of different domains are distinct C++ types and can never be
// accidentally interchanged. Never collapse these into generic integers.
#define CS_ID(NAME) struct NAME##_domain_tag; using NAME = Id<NAME##_domain_tag>;
#define CS_GEN(NAME) struct NAME##_domain_tag; using NAME = Gen<NAME##_domain_tag>;

// --- Identity domains -------------------------------------------------------
CS_ID(CollectiveId)
CS_ID(CollectiveRequestId)
CS_ID(CollectiveScheduleId)
CS_ID(CollectiveGrantId)
CS_ID(CollectiveExecutionId)
CS_ID(CollectiveGroupId)
CS_ID(ParticipantId)
CS_ID(ParticipantGroupId)
CS_ID(CommunicationPlanId)
CS_ID(FlowId)
CS_ID(DependencyId)
CS_ID(ResourceId)
CS_ID(LinkId)
CS_ID(PathId)
CS_ID(DeviceId)
CS_ID(NodeId)
CS_ID(NicId)
CS_ID(MemoryDomainId)
CS_ID(WorkloadId)
CS_ID(ExecutionId)
CS_ID(WorkerId)
CS_ID(WorkerBootId)
CS_ID(SourceId)
CS_ID(SourceBootId)

// --- Generation / epoch domains ---------------------------------------------
CS_GEN(CollectiveGeneration)
CS_GEN(CollectiveRequestGeneration)
CS_GEN(CollectiveScheduleGeneration)
CS_GEN(CollectiveGrantGeneration)
CS_GEN(CollectiveExecutionGeneration)
CS_GEN(CollectiveGroupGeneration)
CS_GEN(ParticipantGeneration)
CS_GEN(ParticipantGroupGeneration)
CS_GEN(CommunicationPlanGeneration)
CS_GEN(FlowGeneration)
CS_GEN(DependencyGeneration)
CS_GEN(ResourceGeneration)
CS_GEN(LinkGeneration)
CS_GEN(PathGeneration)
CS_GEN(DeviceGeneration)
CS_GEN(NodeGeneration)
CS_GEN(NicGeneration)
CS_GEN(MemoryDomainGeneration)
CS_GEN(CapacityGeneration)
CS_GEN(ReservationGeneration)
CS_GEN(CongestionGeneration)
CS_GEN(TopologyGeneration)
CS_GEN(CapabilityGeneration)
CS_GEN(HealthGeneration)
CS_GEN(WorkloadGeneration)
CS_GEN(ExecutionGeneration)
CS_GEN(PolicyGeneration)
CS_GEN(PriorityGeneration)
CS_GEN(SloGeneration)
CS_GEN(FairnessGeneration)
CS_GEN(QueueGeneration)
CS_GEN(CoordinatorEpoch)
CS_GEN(AuthorityGeneration)
CS_GEN(RecoveryGeneration)
CS_GEN(RevalidationGeneration)

#undef CS_ID
#undef CS_GEN

}  // namespace collective_scheduler
