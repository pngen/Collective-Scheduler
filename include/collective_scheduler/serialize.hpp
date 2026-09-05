// Collective Scheduler — shared request/fairness serialization helpers.
//
// Collective Scheduler 1.0.0
// Copyright 2026 Summon Software Labs.

#pragma once

#include <collective_scheduler/capacity.hpp>
#include <collective_scheduler/congestion.hpp>
#include <collective_scheduler/dependency.hpp>
#include <collective_scheduler/fairness.hpp>
#include <collective_scheduler/participant.hpp>
#include <collective_scheduler/persistence.hpp>
#include <collective_scheduler/plan.hpp>
#include <collective_scheduler/request.hpp>
#include <collective_scheduler/reservation.hpp>
#include <collective_scheduler/scheduling.hpp>

namespace collective_scheduler {

void serialize_request(PersistenceWriter& w, const CollectiveRequest& r);
CollectiveRequest deserialize_request(PersistenceReader& r);
void serialize_fairness(PersistenceWriter& w, const FairnessAccount& f);
FairnessAccount deserialize_fairness(PersistenceReader& r);

void serialize_grant(PersistenceWriter& w, const CollectiveGrant& g);
CollectiveGrant deserialize_grant(PersistenceReader& r);
void serialize_participant(PersistenceWriter& w, const Participant& p);
Participant deserialize_participant(PersistenceReader& r);
void serialize_plan(PersistenceWriter& w, const CommunicationPlan& p);
CommunicationPlan deserialize_plan(PersistenceReader& r);
void serialize_congestion(PersistenceWriter& w, const CongestionState& c);
CongestionState deserialize_congestion(PersistenceReader& r);
void serialize_reservation(PersistenceWriter& w, const Reservation& r);
Reservation deserialize_reservation(PersistenceReader& r);
void serialize_dependency(PersistenceWriter& w, const DependencyState& d);
DependencyState deserialize_dependency(PersistenceReader& r);
void serialize_capacity(PersistenceWriter& w, const CapacityState& c);
CapacityState deserialize_capacity(PersistenceReader& r);

}  // namespace collective_scheduler