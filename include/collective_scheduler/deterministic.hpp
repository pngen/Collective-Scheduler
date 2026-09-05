// Collective Scheduler — deterministic utilities.
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
#include <collective_scheduler/identity.hpp>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <string>
#include <vector>

namespace collective_scheduler {

// Sort and de-duplicate in place. All participant sets, plans, groups, and
// exclusion classes are normalized to sorted-unique so that identity equality
// and conflict queries are order-independent.
inline void sort_unique(std::vector<ParticipantId>& v) {
  std::sort(v.begin(), v.end());
  v.erase(std::unique(v.begin(), v.end()), v.end());
}
inline void sort_unique_gens(std::vector<ParticipantGeneration>& v) {
  std::sort(v.begin(), v.end());
  v.erase(std::unique(v.begin(), v.end()), v.end());
}
inline void sort_unique_ids(std::vector<ResourceId>& v) { std::sort(v.begin(), v.end()); v.erase(std::unique(v.begin(), v.end()), v.end()); }
inline void sort_unique_links(std::vector<LinkId>& v) { std::sort(v.begin(), v.end()); v.erase(std::unique(v.begin(), v.end()), v.end()); }
inline void sort_unique_flows(std::vector<FlowId>& v) { std::sort(v.begin(), v.end()); v.erase(std::unique(v.begin(), v.end()), v.end()); }
inline void sort_unique_strings(std::vector<std::string>& v) { std::sort(v.begin(), v.end()); v.erase(std::unique(v.begin(), v.end()), v.end()); }

// FNV-1a 64-bit. Deterministic across platforms; never rely on std::hash.
inline std::uint64_t fnv1a64(const void* data, std::size_t len, std::uint64_t seed = 0xcbf29ce484222325ULL) noexcept {
  const auto* p = static_cast<const unsigned char*>(data);
  std::uint64_t h = seed ^ 0x9e3779b97f4a7c15ULL;
  for (std::size_t i = 0; i < len; ++i) {
    h ^= static_cast<std::uint64_t>(p[i]);
    h *= 0x100000001b3ULL;
  }
  return h;
}
inline std::uint64_t fnv1a64(std::uint64_t v) noexcept { return fnv1a64(&v, sizeof(v)); }

// SplitMix64 — a tiny deterministic PRNG for property tests. Seed values are
// recorded on failure and reproduced exactly.
class SplitMix64 {
 public:
  explicit SplitMix64(std::uint64_t seed) noexcept : state_(seed) {}
  std::uint64_t next() noexcept {
    std::uint64_t z = (state_ += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
  }
  std::uint64_t bounded(std::uint64_t n) noexcept { return n ? next() % n : 0; }
  double unit() noexcept { return (next() >> 11) * (1.0 / 9007199254740992.0); }  // [0,1)

 private:
  std::uint64_t state_;
};

// Integer total order on strong id/gen values (uint64). Useful as a final
// deterministic tie-breaker.
inline bool lt_value(std::uint64_t a, std::uint64_t b) noexcept { return a < b; }

// A strict, total comparator for doubles that treats NaN deterministically
// (NaN sorts last, never compares equal to a number, never silently promotes).
inline int cmp_double(double a, double b) noexcept {
  const bool na = std::isnan(a);
  const bool nb = std::isnan(b);
  if (na && nb) return 0;
  if (na) return 1;     // NaN is greater (sorts last)
  if (nb) return -1;
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

// Stable weak ordering helper for rank tuples: compare a vector of doubles as a
// lexicographic key, treating NaN as "worst" and equal values as equal.
inline int cmp_rank(const std::vector<double>& a, const std::vector<double>& b) noexcept {
  const std::size_t n = std::min(a.size(), b.size());
  for (std::size_t i = 0; i < n; ++i) {
    int c = cmp_double(a[i], b[i]);
    if (c != 0) return c;
  }
  if (a.size() < b.size()) return -1;
  if (a.size() > b.size()) return 1;
  return 0;
}

}  // namespace collective_scheduler
