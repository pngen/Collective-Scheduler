// Collective Scheduler — time model.
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

#include <cstdint>

namespace collective_scheduler {

// Nanosecond durations / absolute timestamps. Never use wall-clock values as
// scheduling authority without attaching explicit source semantics (see
// ProvenanceStatus). For deterministic scheduling the scheduler advances an
// explicit logical time origin; wall-clock real deadlines are only consulted
// for admitted absolute-deadline collectives.
using Nanoseconds = std::uint64_t;
using Milliseconds = std::uint64_t;

inline constexpr Nanoseconds kNanosPerMilli = 1'000'000;

// A monotonically increasing logical scheduler time. Tests and deterministic
// proofs advance it explicitly; the scheduler never silently reads the wall
// clock to mutate authoritative state.
class LogicalTime {
 public:
  Nanoseconds value() const noexcept { return value_; }
  void advance(Nanoseconds d) noexcept { value_ += d; }
  void set(Nanoseconds v) noexcept { value_ = v; }

 private:
  Nanoseconds value_ = 0;
};

// Saturating duration difference, never negative.
inline constexpr Nanoseconds saturating_sub(Nanoseconds a, Nanoseconds b) noexcept {
  return a > b ? (a - b) : 0;
}

}  // namespace collective_scheduler
