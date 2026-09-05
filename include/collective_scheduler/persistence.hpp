// Collective Scheduler — versioned persistence.
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
#include <collective_scheduler/error.hpp>
#include <collective_scheduler/scheduler.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace collective_scheduler {

// Deterministic binary codec. All multi-byte values are written little-endian;
// no pointers, sockets, or process-local addresses are ever serialized as
// authority. All counts are bounds-checked before allocation.
class PersistenceWriter {
 public:
  void u8(std::uint8_t v);
  void u16(std::uint16_t v);
  void u32(std::uint32_t v);
  void u64(std::uint64_t v);
  void i32(std::int32_t v);
  void f64(double v);
  void str(const std::string& s);
  void bytes(const std::uint8_t* p, std::size_t n);
  void id(std::uint64_t v) { u64(v); }
  std::size_t size() const { return buf_.size(); }
  const std::uint8_t* data() const { return buf_.data(); }
  void reserve(std::size_t n);
  std::vector<std::uint8_t> take();

 private:
  std::vector<std::uint8_t> buf_;
};

// Bounds-checked reader. Throws scheduler_error on truncation / malformed /
// oversized / invalid content. Never reads past the buffer.
class PersistenceReader {
 public:
  PersistenceReader(const std::uint8_t* p, std::size_t n, std::size_t max);

  std::uint8_t u8();
  std::uint16_t u16();
  std::uint32_t u32();
  std::uint64_t u64();
  std::int32_t i32();
  double f64();
  std::string str();
  void bytes(std::uint8_t* p, std::size_t n);
  std::uint64_t id() { return u64(); }
  bool at_end() const { return pos_ == len_; }
  std::size_t remaining() const { return len_ - pos_; }
  void expect_end() const;

 private:
  void need(std::size_t n) const;
  const std::uint8_t* p_;
  std::size_t len_;
  std::size_t pos_ = 0;
  std::size_t max_;
};

// Save / restore the Scheduler's durable scheduling state.
class Persistence {
 public:
  static constexpr std::uint32_t kMagic = 0x43535058u;  // "CSPX"
  static constexpr std::uint16_t kVersion = 1;

  // Serialize durable state. Does NOT include dynamic live evidence.
  static std::vector<std::uint8_t> save(const Scheduler& s);

  // Restore durable state into `s`. After restore, dynamic evidence is
  // conservatively invalidated: the caller should republish participants /
  // plans / reservations / congestion / capacity and run recovery.
  static void restore(Scheduler& s, const std::vector<std::uint8_t>& bytes);

  // Used for testing / adversarial corruption rejection.
  static std::vector<std::uint8_t> header() ;
};

}  // namespace collective_scheduler