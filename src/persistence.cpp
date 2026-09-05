// Collective Scheduler — versioned persistence (implementation).
//
// Collective Scheduler 1.0.0
// Copyright 2026 Summon Software Labs.

#include <collective_scheduler/persistence.hpp>
#include <collective_scheduler/deterministic.hpp>

#include <bit>

namespace collective_scheduler {

void PersistenceWriter::u8(std::uint8_t v) { buf_.push_back(v); }
void PersistenceWriter::u16(std::uint16_t v) {
  buf_.push_back(static_cast<std::uint8_t>(v & 0xff));
  buf_.push_back(static_cast<std::uint8_t>((v >> 8) & 0xff));
}
void PersistenceWriter::u32(std::uint32_t v) {
  for (int i = 0; i < 4; ++i) buf_.push_back(static_cast<std::uint8_t>((v >> (8 * i)) & 0xff));
}
void PersistenceWriter::u64(std::uint64_t v) {
  for (int i = 0; i < 8; ++i) buf_.push_back(static_cast<std::uint8_t>((v >> (8 * i)) & 0xff));
}
void PersistenceWriter::i32(std::int32_t v) { u32(static_cast<std::uint32_t>(v)); }
void PersistenceWriter::f64(double v) { u64(std::bit_cast<std::uint64_t>(v)); }
void PersistenceWriter::bytes(const std::uint8_t* p, std::size_t n) { buf_.insert(buf_.end(), p, p + n); }
void PersistenceWriter::str(const std::string& s) { u64(s.size()); bytes(reinterpret_cast<const std::uint8_t*>(s.data()), s.size()); }
void PersistenceWriter::reserve(std::size_t n) { buf_.reserve(n); }
std::vector<std::uint8_t> PersistenceWriter::take() { return std::move(buf_); }

PersistenceReader::PersistenceReader(const std::uint8_t* p, std::size_t n, std::size_t max)
    : p_(p), len_(n), max_(max) {}
void PersistenceReader::need(std::size_t n) const {
  if (n > max_) throw_malformed("persistence read exceeds max size");
  if (pos_ + n > len_) throw_malformed("persistence truncation");
}
std::uint8_t PersistenceReader::u8() { need(1); return p_[pos_++]; }
std::uint16_t PersistenceReader::u16() { need(2); std::uint16_t v = static_cast<std::uint16_t>(p_[pos_]) | static_cast<std::uint16_t>(p_[pos_ + 1] << 8); pos_ += 2; return v; }
std::uint32_t PersistenceReader::u32() { need(4); std::uint32_t v = 0; for (int i = 0; i < 4; ++i) v |= static_cast<std::uint32_t>(p_[pos_ + i]) << (8 * i); pos_ += 4; return v; }
std::uint64_t PersistenceReader::u64() { need(8); std::uint64_t v = 0; for (int i = 0; i < 8; ++i) v |= static_cast<std::uint64_t>(p_[pos_ + i]) << (8 * i); pos_ += 8; return v; }
std::int32_t PersistenceReader::i32() { return static_cast<std::int32_t>(u32()); }
double PersistenceReader::f64() { std::uint64_t v = u64(); return std::bit_cast<double>(v); }
std::string PersistenceReader::str() {
  auto n = u64();
  if (n > max_) throw_malformed("persistence string exceeds max");
  need(n);
  std::string s(reinterpret_cast<const char*>(p_ + pos_), n);
  pos_ += n;
  return s;
}
void PersistenceReader::bytes(std::uint8_t* out, std::size_t n) { need(n); std::memcpy(out, p_ + pos_, n); pos_ += n; }
void PersistenceReader::expect_end() const { if (pos_ != len_) throw_malformed("persistence trailing garbage"); }

constexpr std::uint64_t kCheckSeed = 0x4b2f9a8d1c7e3f5aULL;

std::vector<std::uint8_t> Persistence::save(const Scheduler& s) {
  PersistenceWriter w;
  w.u32(kMagic);
  w.u16(kVersion);
  s.serializeState(w);
  auto ck = fnv1a64(w.data(), w.size(), kCheckSeed);
  w.u64(ck);
  return w.take();
}

void Persistence::restore(Scheduler& s, const std::vector<std::uint8_t>& bytes) {
  if (bytes.size() < 4 + 2 + 8) throw_malformed("persistence truncated");
  PersistenceReader head(bytes.data(), bytes.size(), bytes.size());
  std::uint32_t magic = head.u32();
  std::uint16_t ver = head.u16();
  if (magic != kMagic) throw_malformed("bad persistence magic");
  if (ver != kVersion) throw_malformed("unsupported persistence version");
  std::size_t body_len = bytes.size() - 8;
  std::uint64_t stored = 0;
  for (int i = 0; i < 8; ++i) stored |= static_cast<std::uint64_t>(bytes[body_len + i]) << (8 * i);
  std::uint64_t calc = fnv1a64(bytes.data(), body_len, kCheckSeed);
  if (stored != calc) throw_malformed("persistence checksum mismatch");
  std::size_t state_len = body_len - 6;
  PersistenceReader r(bytes.data() + 6, state_len, state_len);
  s.deserializeState(r);
  r.expect_end();
  // Conservative recovery: dynamic evidence must not silently remain current.
  s.resetDynamicStateForRecovery();
}

std::vector<std::uint8_t> Persistence::header() {
  PersistenceWriter w;
  w.u32(kMagic); w.u16(kVersion);
  return w.take();
}

}  // namespace collective_scheduler