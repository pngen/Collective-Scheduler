#include <collective_scheduler/protocol.hpp>
#include "test_framework.hpp"

using namespace collective_scheduler;

TEST(protocol_frame_roundtrip) {
  std::vector<std::uint8_t> payload = {1, 2, 3, 4, 5};
  auto frame = encode_frame(MsgType::SUBMIT_COLLECTIVE, 42, payload);
  CHECK(!frame.empty());
  std::vector<std::uint8_t> buf = frame;
  std::vector<Msg> out;
  REQUIRE(decode_frames(buf, out));
  REQUIRE(out.size() == 1);
  CHECK(out[0].type == MsgType::SUBMIT_COLLECTIVE);
  CHECK_EQ(out[0].seq, 42);
  CHECK_EQ(out[0].payload.size(), payload.size());
  CHECK(out[0].payload == payload);
  CHECK(buf.empty());
}

TEST(protocol_partial_reads) {
  auto frame = encode_frame(MsgType::HELLO, 7, {9, 8, 7});
  std::vector<std::uint8_t> buf;
  std::vector<Msg> out;
  // feed one byte at a time; must handle partial reads.
  for (std::size_t i = 0; i < frame.size(); ++i) {
    buf.push_back(frame[i]);
    bool ok = decode_frames(buf, out);
    REQUIRE(ok);
  }
  REQUIRE(out.size() == 1);
  CHECK_EQ(out[0].seq, 7);
}

TEST(protocol_malformed_rejected) {
  auto frame = encode_frame(MsgType::REGISTER, 1, {1, 2, 3});
  // Bad magic.
  auto bad = frame; bad[0] ^= 0xff;
  std::vector<std::uint8_t> buf = bad;
  std::vector<Msg> out;
  CHECK(!decode_frames(buf, out));
  // Bad version.
  bad = frame; bad[4] ^= 0xff;
  buf = bad; out.clear(); CHECK(!decode_frames(buf, out));
  // Bad checksum (flip a payload byte).
  bad = frame; bad[20] ^= 0xff;
  buf = bad; out.clear(); CHECK(!decode_frames(buf, out));
  // Oversized payload length.
  bad = frame; bad[16] = 0xff; bad[17] = 0xff; bad[18] = 0xff; bad[19] = 0xff;
  buf = bad; out.clear(); CHECK(!decode_frames(buf, out));
}

TEST(protocol_multiple_frames) {
  auto f1 = encode_frame(MsgType::HELLO, 1, {});
  auto f2 = encode_frame(MsgType::REGISTER, 2, {10});
  auto f3 = encode_frame(MsgType::SUBMIT_COLLECTIVE, 3, {20, 30});
  std::vector<std::uint8_t> buf;
  buf.insert(buf.end(), f1.begin(), f1.end());
  buf.insert(buf.end(), f2.begin(), f2.end());
  buf.insert(buf.end(), f3.begin(), f3.end());
  std::vector<Msg> out;
  REQUIRE(decode_frames(buf, out));
  CHECK_EQ(out.size(), 3);
  CHECK_EQ(out[0].seq, 1); CHECK_EQ(out[1].seq, 2); CHECK_EQ(out[2].seq, 3);
}
