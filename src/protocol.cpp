// Collective Scheduler — versioned framed TCP protocol (implementation).
// Collective Scheduler 1.0.0
// Copyright 2026 Summon Software Labs.

#include <collective_scheduler/protocol.hpp>
#include <collective_scheduler/deterministic.hpp>

#include <cstdint>

namespace collective_scheduler {

namespace {
constexpr std::size_t kHeaderSize = 20;  // magic(4)+version(2)+type(2)+seq(8)+len(4)
constexpr std::size_t kChecksumSize = 8;
constexpr std::uint64_t kProtoSeed = 0x9d6b3f5a7c1e2843ULL;

std::uint32_t le32(const std::uint8_t* p) noexcept {
  return static_cast<std::uint32_t>(p[0]) | (static_cast<std::uint32_t>(p[1]) << 8) |
         (static_cast<std::uint32_t>(p[2]) << 16) | (static_cast<std::uint32_t>(p[3]) << 24);
}
std::uint16_t le16(const std::uint8_t* p) noexcept {
  return static_cast<std::uint16_t>(p[0]) | (static_cast<std::uint16_t>(p[1]) << 8);
}
std::uint64_t le64(const std::uint8_t* p) noexcept {
  std::uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<std::uint64_t>(p[i]) << (8 * i);
  return v;
}
void put_le16(std::vector<std::uint8_t>& b, std::uint16_t v) {
  b.push_back(static_cast<std::uint8_t>(v & 0xff));
  b.push_back(static_cast<std::uint8_t>((v >> 8) & 0xff));
}
void put_le32(std::vector<std::uint8_t>& b, std::uint32_t v) {
  for (int i = 0; i < 4; ++i) b.push_back(static_cast<std::uint8_t>((v >> (8 * i)) & 0xff));
}
void put_le64(std::vector<std::uint8_t>& b, std::uint64_t v) {
  for (int i = 0; i < 8; ++i) b.push_back(static_cast<std::uint8_t>((v >> (8 * i)) & 0xff));
}
}  // namespace

bool is_known_message_type(std::uint16_t v) noexcept {
  switch (static_cast<MsgType>(v)) {
    case MsgType::HELLO: case MsgType::REGISTER: case MsgType::PUBLISH_PARTICIPANT:
    case MsgType::PUBLISH_READINESS: case MsgType::SUBMIT_COLLECTIVE: case MsgType::UPDATE_DEPENDENCY:
    case MsgType::UPDATE_PRIORITY: case MsgType::UPDATE_PLAN: case MsgType::UPDATE_RESERVATION:
    case MsgType::UPDATE_CONGESTION: case MsgType::UPDATE_CAPACITY: case MsgType::QUERY_SCHEDULE:
    case MsgType::GRANT: case MsgType::EXECUTION_ACCEPTED: case MsgType::START: case MsgType::PROGRESS:
    case MsgType::COMPLETE: case MsgType::FAIL: case MsgType::CANCEL: case MsgType::SUPERSEDE:
    case MsgType::FENCE_WORKER: case MsgType::REVALIDATE: case MsgType::SAVE: case MsgType::SHUTDOWN:
    case MsgType::ERROR:
      return true;
  }
  return false;
}

std::vector<std::uint8_t> encode_frame(MsgType type, std::uint64_t seq, const std::vector<std::uint8_t>& payload) {
  std::vector<std::uint8_t> buf;
  buf.reserve(kHeaderSize + payload.size() + kChecksumSize);
  put_le32(buf, CS_PROTOCOL_MAGIC);
  put_le16(buf, CS_PROTOCOL_VERSION);
  put_le16(buf, static_cast<std::uint16_t>(type));
  put_le64(buf, seq);
  put_le32(buf, static_cast<std::uint32_t>(payload.size()));
  buf.insert(buf.end(), payload.begin(), payload.end());
  put_le64(buf, fnv1a64(buf.data(), buf.size(), kProtoSeed));
  return buf;
}

bool decode_frames(std::vector<std::uint8_t>& buffer, std::vector<Msg>& out) {
  std::size_t off = 0;
  while (buffer.size() - off >= kHeaderSize) {
    const std::uint8_t* p = buffer.data() + off;
    if (le32(p) != CS_PROTOCOL_MAGIC) return false;
    if (le16(p + 4) != CS_PROTOCOL_VERSION) return false;
    std::uint16_t type = le16(p + 6);
    if (!is_known_message_type(type)) return false;
    std::uint64_t seq = le64(p + 8);
    std::uint32_t plen = le32(p + 16);
    if (plen > CS_MAX_PROTOCOL_PAYLOAD) return false;
    std::size_t frame_len = kHeaderSize + static_cast<std::size_t>(plen) + kChecksumSize;
    if (buffer.size() - off < frame_len) break;  // incomplete; need more bytes
    std::uint64_t stored = le64(p + kHeaderSize + plen);
    std::uint64_t calc = fnv1a64(p, kHeaderSize + static_cast<std::size_t>(plen), kProtoSeed);
    if (stored != calc) return false;
    Msg m;
    m.type = static_cast<MsgType>(type);
    m.seq = seq;
    m.payload.assign(p + kHeaderSize, p + kHeaderSize + plen);
    out.push_back(std::move(m));
    off += frame_len;
  }
  if (off > 0) buffer.erase(buffer.begin(), buffer.begin() + static_cast<std::ptrdiff_t>(off));
  return true;
}

}  // namespace collective_scheduler