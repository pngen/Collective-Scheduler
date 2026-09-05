// Collective Scheduler — versioned framed TCP protocol.
//
// Collective Scheduler 1.0.0
// Copyright 2026 Summon Software Labs.

#pragma once

#include <collective_scheduler/domains.hpp>
#include <collective_scheduler/enums.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace collective_scheduler {

// Framed, versioned, checksummed wire messages. Every frame carries enough
// authority (sequence + typed message) for the receiver to reject stale traffic.
enum class MsgType : std::uint16_t {
  HELLO = 0, REGISTER = 1, PUBLISH_PARTICIPANT = 2, PUBLISH_READINESS = 3,
  SUBMIT_COLLECTIVE = 4, UPDATE_DEPENDENCY = 5, UPDATE_PRIORITY = 6, UPDATE_PLAN = 7,
  UPDATE_RESERVATION = 8, UPDATE_CONGESTION = 9, UPDATE_CAPACITY = 10, QUERY_SCHEDULE = 11,
  GRANT = 12, EXECUTION_ACCEPTED = 13, START = 14, PROGRESS = 15, COMPLETE = 16, FAIL = 17,
  CANCEL = 18, SUPERSEDE = 19, FENCE_WORKER = 20, REVALIDATE = 21, SAVE = 22, SHUTDOWN = 23,
  ERROR = 24,
};

constexpr std::uint32_t CS_PROTOCOL_MAGIC = 0x43535052u;  // "CSPR"
constexpr std::uint16_t CS_PROTOCOL_VERSION = 1;
constexpr std::size_t CS_MAX_PROTOCOL_PAYLOAD = 16 * 1024 * 1024;  // 16 MiB

struct Msg {
  MsgType type = MsgType::HELLO;
  std::uint64_t seq = 0;
  std::vector<std::uint8_t> payload;
};

// Encode one frame. The checksum is an FNV-1a over the header + payload.
std::vector<std::uint8_t> encode_frame(MsgType type, std::uint64_t seq, const std::vector<std::uint8_t>& payload);

// Decode as many complete frames as are buffered in `buffer`, appending the
// resulting messages to `out`. Mutates `buffer` to drop consumed bytes.
// Returns false if a frame is malformed (bad magic/version, oversized, or bad
// checksum); throws on truncation handled by callers via the return value.
bool decode_frames(std::vector<std::uint8_t>& buffer, std::vector<Msg>& out);

// Validate a message type enum value is known.
bool is_known_message_type(std::uint16_t v) noexcept;

}  // namespace collective_scheduler