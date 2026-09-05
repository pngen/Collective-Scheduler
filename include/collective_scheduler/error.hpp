// Collective Scheduler — error types.
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

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>

namespace collective_scheduler {

// A coarse error code for scheduler failures (exception safety and protocol).
enum class ErrorCode : std::uint8_t {
  OK,
  INVALID_ARGUMENT,
  MALFORMED_REQUEST,
  ARITHMETIC_OVERFLOW,
  OUT_OF_BOUNDS,
  DUPLICATE_IDENTIFIER,
  STALE_AUTHORITY,
  ILLEGAL_TRANSITION,
  NOT_FOUND,
  ALREADY_PRESENT,
  CONCURRENT_MODIFICATION,
  PERSISTENCE_CORRUPTION,
  PERSISTENCE_UNSUPPORTED_VERSION,
  PROTOCOL_ERROR,
  RESOURCE_EXHAUSTED,
  BAD_STATE,
  INTERNAL,
};

// Thrown for recoverable-but-error conditions (malformed requests, illegal
// transitions, persistence corruption, protocol defects). Carries a precise
// message plus a coarse code and, where useful, a rejecting reason.
class scheduler_error : public std::runtime_error {
 public:
  scheduler_error(ErrorCode code, std::string message)
      : std::runtime_error(std::move(message)), code_(code) {}
  scheduler_error(ErrorCode code, RejectionReason reason, std::string message)
      : std::runtime_error(std::move(message)), code_(code), reason_(reason) {}

  ErrorCode code() const noexcept { return code_; }
  RejectionReason rejection_reason() const noexcept { return reason_; }

 private:
  ErrorCode code_ = ErrorCode::INTERNAL;
  RejectionReason reason_ = RejectionReason::NONE;
};

[[noreturn]] inline void throw_invalid(std::string msg) {
  throw scheduler_error(ErrorCode::INVALID_ARGUMENT, std::move(msg));
}
[[noreturn]] inline void throw_malformed(std::string msg) {
  throw scheduler_error(ErrorCode::MALFORMED_REQUEST, std::move(msg));
}
[[noreturn]] inline void throw_overflow(std::string msg) {
  throw scheduler_error(ErrorCode::ARITHMETIC_OVERFLOW, std::move(msg));
}
[[noreturn]] inline void throw_bounds(std::string msg) {
  throw scheduler_error(ErrorCode::OUT_OF_BOUNDS, std::move(msg));
}
[[noreturn]] inline void throw_transition(std::string msg) {
  throw scheduler_error(ErrorCode::ILLEGAL_TRANSITION, std::move(msg));
}

}  // namespace collective_scheduler
