// Collective Scheduler — strongly typed identities and generations.
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
#include <functional>
#include <limits>
#include <ostream>
#include <type_traits>

namespace collective_scheduler {

// ---------------------------------------------------------------------------
// Tagged identifiers and generations.
//
// Every semantic authority domain (a collective, a participant, a worker boot,
// a plan, a reservation, ...) gets its own compile-time Tag. An Id<Tag> and a
// Gen<Tag> are distinct C++ types and can never be silently interchanged.
// Domains that need both an identity and a generation declare both; the two are
// still distinct because they use different template kinds.
//
// Storage is an unsigned 64-bit counter. Value 0 is reserved to mean
// "unset / absent" (a null id or an unimprinted generation). Concrete values
// are non-zero and monotonically assigned; generations never regress within a
// domain.
// ---------------------------------------------------------------------------

template <typename Tag>
struct Id {
  using value_type = std::uint64_t;
  value_type value = 0;

  constexpr Id() noexcept = default;
  constexpr explicit Id(value_type v) noexcept : value(v) {}

  constexpr bool null() const noexcept { return value == 0; }
  constexpr explicit operator bool() const noexcept { return value != 0; }

  constexpr bool operator==(const Id& o) const noexcept { return value == o.value; }
  constexpr bool operator!=(const Id& o) const noexcept { return value != o.value; }
  constexpr bool operator<(const Id& o) const noexcept { return value < o.value; }
  constexpr bool operator<=(const Id& o) const noexcept { return value <= o.value; }
  constexpr bool operator>(const Id& o) const noexcept { return value > o.value; }
  constexpr bool operator>=(const Id& o) const noexcept { return value >= o.value; }

  constexpr Id& operator++() noexcept {
    if (value < (std::numeric_limits<value_type>::max)()) ++value;
    return *this;
  }
  constexpr Id operator++(int) noexcept { Id t = *this; ++(*this); return t; }
};

template <typename Tag>
struct Gen {
  using value_type = std::uint64_t;
  value_type value = 0;

  constexpr Gen() noexcept = default;
  constexpr explicit Gen(value_type v) noexcept : value(v) {}

  constexpr bool null() const noexcept { return value == 0; }
  constexpr explicit operator bool() const noexcept { return value != 0; }

  constexpr bool operator==(const Gen& o) const noexcept { return value == o.value; }
  constexpr bool operator!=(const Gen& o) const noexcept { return value != o.value; }
  constexpr bool operator<(const Gen& o) const noexcept { return value < o.value; }
  constexpr bool operator<=(const Gen& o) const noexcept { return value <= o.value; }
  constexpr bool operator>(const Gen& o) const noexcept { return value > o.value; }
  constexpr bool operator>=(const Gen& o) const noexcept { return value >= o.value; }

  constexpr Gen& operator++() noexcept {
    if (value < (std::numeric_limits<value_type>::max)()) ++value;
    return *this;
  }
  constexpr Gen operator++(int) noexcept { Gen t = *this; ++(*this); return t; }
};

namespace detail {
template <typename T>
struct is_id_or_gen : std::false_type {};
template <typename Tag>
struct is_id_or_gen<Id<Tag>> : std::true_type {};
template <typename Tag>
struct is_id_or_gen<Gen<Tag>> : std::true_type {};
}  // namespace detail

template <typename T>
inline constexpr bool is_id_or_gen_v = detail::is_id_or_gen<T>::value;

}  // namespace collective_scheduler

namespace std {
template <typename Tag>
struct hash<::collective_scheduler::Id<Tag>> {
  std::size_t operator()(const ::collective_scheduler::Id<Tag>& x) const noexcept {
    return std::hash<std::uint64_t>{}(x.value);
  }
};
template <typename Tag>
struct hash<::collective_scheduler::Gen<Tag>> {
  std::size_t operator()(const ::collective_scheduler::Gen<Tag>& x) const noexcept {
    return std::hash<std::uint64_t>{}(x.value);
  }
};
}  // namespace std
