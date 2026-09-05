// Collective Scheduler — fairness and starvation-prevention model.
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

#include <cstdint>

namespace collective_scheduler {

// Per-collective fairness accounting. Deficit-based: a collective that has been
// under-served relative to its class share accumulates deficit. Aging and
// famine classification are bounded and deterministic.
struct FairnessAccount {
  FairnessClass cls = FairnessClass::DEFAULT;
  std::uint64_t observed_grants = 0;
  double observed_service = 0.0;
  double deficit = 0.0;
  std::uint64_t wait_ticks = 0;          // scheduler epochs waited
  std::uint64_t consecutive_bypasses = 0;  // times ranked but not selected
  bool starved = false;                   // current starvation classification
  // target share weight (0..1) assigned to the class by policy
  double target_share = 0.0;

  bool operator==(const FairnessAccount&) const = default;
};

// Fairness / starvation policy parameters (bounded, deterministic).
struct FairnessPolicy {
  double protected_min_share = 0.05;
  double aging_growth = 0.05;
  std::uint64_t max_bypass_count = 16;
  std::uint64_t window_grants = 64;  // grants over which the share is measured
  bool starvation_enabled = true;
};

// Pure helpers. Given an account and policy, compute how much starvation
// pressure should promote this collective. Returns a value in [0, 1] where a
// higher value means "deserves earlier service".
inline constexpr double starvation_pressure(const FairnessAccount& a, const FairnessPolicy& p) noexcept {
  if (!p.starvation_enabled) return 0.0;
  double bypass = static_cast<double>(a.consecutive_bypasses);
  double cap = static_cast<double>(p.max_bypass_count > 0 ? p.max_bypass_count : 1);
  double frac = bypass / cap;
  if (frac > 1.0) frac = 1.0;
  double deficit_term = a.deficit;
  if (deficit_term > 1.0) deficit_term = 1.0;
  double p_pressure = 0.5 * frac + 0.5 * deficit_term;
  return p_pressure < 0.0 ? 0.0 : (p_pressure > 1.0 ? 1.0 : p_pressure);
}

// Whether a protected-class collective is below its guaranteed minimum share
// and therefore must be promoted.
inline constexpr bool below_protected_share(const FairnessAccount& a, const FairnessPolicy& p) noexcept {
  if (a.cls != FairnessClass::PROTECTED) return false;
  if (a.observed_grants >= p.window_grants) return false;
  return a.observed_service < p.protected_min_share;
}

}  // namespace collective_scheduler
