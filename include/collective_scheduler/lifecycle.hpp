// Collective Scheduler — guarded lifecycle state machine.
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

#include <collective_scheduler/enums.hpp>
#include <collective_scheduler/error.hpp>

namespace collective_scheduler {

// Explicit guarded lifecycle for a collective. Illegal transitions are rejected
// with std::logic_error-style scheduler_error. The state machine is deliberately
// conservative: a stale grant may not "become" ACTIVE, a cancelled collective may
// not become COMPLETED via a stale result, a superseded request may not be
// re-granted.
class Lifecycle {
 public:
  constexpr Lifecycle() noexcept = default;
  explicit constexpr Lifecycle(LifecycleState s) noexcept : state_(s) {}

  LifecycleState state() const noexcept { return state_; }

  [[nodiscard]] static constexpr bool can_transition(LifecycleState from, LifecycleState to) noexcept {
    switch (from) {
      case LifecycleState::DECLARED:
        return to == LifecycleState::WAITING || to == LifecycleState::READY ||
               to == LifecycleState::CANCELLED || to == LifecycleState::REVALIDATION_REQUIRED ||
               to == LifecycleState::SUPERSEDED || to == LifecycleState::STALE ||
               to == LifecycleState::RETIRED || to == LifecycleState::FAILED;
      case LifecycleState::WAITING:
        return to == LifecycleState::READY || to == LifecycleState::REVALIDATION_REQUIRED ||
               to == LifecycleState::CANCELLED || to == LifecycleState::SUPERSEDED ||
               to == LifecycleState::STALE || to == LifecycleState::RETIRED ||
               to == LifecycleState::FAILED;
      case LifecycleState::READY:
        return to == LifecycleState::ELIGIBLE || to == LifecycleState::WAITING ||
               to == LifecycleState::REVALIDATION_REQUIRED || to == LifecycleState::CANCELLED ||
               to == LifecycleState::SUPERSEDED || to == LifecycleState::STALE ||
               to == LifecycleState::RETIRED || to == LifecycleState::FAILED;
      case LifecycleState::ELIGIBLE:
        return to == LifecycleState::QUEUED || to == LifecycleState::CANCELLED ||
               to == LifecycleState::REVALIDATION_REQUIRED || to == LifecycleState::SUPERSEDED ||
               to == LifecycleState::STALE || to == LifecycleState::RETIRED ||
               to == LifecycleState::FAILED;
      case LifecycleState::QUEUED:
        return to == LifecycleState::GRANTED || to == LifecycleState::WAITING ||
               to == LifecycleState::CANCELLED || to == LifecycleState::REVALIDATION_REQUIRED ||
               to == LifecycleState::SUPERSEDED || to == LifecycleState::STALE ||
               to == LifecycleState::RETIRED || to == LifecycleState::FAILED;
      case LifecycleState::GRANTED:
        return to == LifecycleState::DISPATCHING || to == LifecycleState::CANCELLED ||
               to == LifecycleState::REVALIDATION_REQUIRED || to == LifecycleState::SUPERSEDED ||
               to == LifecycleState::STALE || to == LifecycleState::RETIRED ||
               to == LifecycleState::FAILED;
      case LifecycleState::DISPATCHING:
        return to == LifecycleState::ACTIVE || to == LifecycleState::CANCELLED ||
               to == LifecycleState::FAILED || to == LifecycleState::REVALIDATION_REQUIRED ||
               to == LifecycleState::SUPERSEDED || to == LifecycleState::STALE ||
               to == LifecycleState::RETIRED;
      case LifecycleState::ACTIVE:
        return to == LifecycleState::COMPLETED || to == LifecycleState::DRAINING ||
               to == LifecycleState::FAILED || to == LifecycleState::CANCELLED ||
               to == LifecycleState::REVALIDATION_REQUIRED || to == LifecycleState::SUPERSEDED ||
               to == LifecycleState::STALE || to == LifecycleState::RETIRED;
      case LifecycleState::DRAINING:
        return to == LifecycleState::COMPLETED || to == LifecycleState::ACTIVE ||
               to == LifecycleState::FAILED || to == LifecycleState::SUPERSEDED ||
               to == LifecycleState::STALE || to == LifecycleState::RETIRED;
      case LifecycleState::COMPLETED:
        return to == LifecycleState::RETIRED;
      case LifecycleState::CANCELLED:
        return to == LifecycleState::RETIRED;
      case LifecycleState::FAILED:
        return to == LifecycleState::REVALIDATION_REQUIRED || to == LifecycleState::RETIRED;
      case LifecycleState::REVALIDATION_REQUIRED:
        return to == LifecycleState::READY || to == LifecycleState::WAITING ||
               to == LifecycleState::CANCELLED || to == LifecycleState::SUPERSEDED ||
               to == LifecycleState::STALE || to == LifecycleState::RETIRED ||
               to == LifecycleState::FAILED;
      case LifecycleState::SUPERSEDED:
      case LifecycleState::STALE:
        return to == LifecycleState::RETIRED;
      case LifecycleState::RETIRED:
        return false;
    }
    return false;
  }

  void transition(LifecycleState to) {
    if (!can_transition(state_, to)) {
      std::string msg = "illegal lifecycle transition ";
      msg += std::string(name_of(state_));
      msg += " -> ";
      msg += std::string(name_of(to));
      throw_transition(std::move(msg));
    }
    state_ = to;
  }

 private:
  LifecycleState state_ = LifecycleState::DECLARED;
};

}  // namespace collective_scheduler
