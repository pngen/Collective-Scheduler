// Collective Scheduler — reference multiprocess worker (TCP).
#include <collective_scheduler/protocol.hpp>
#include <collective_scheduler/serialize.hpp>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

using namespace collective_scheduler;

namespace {
bool send_all(int fd, const std::uint8_t* p, std::size_t n) {
  std::size_t off = 0;
  while (off < n) {
#ifdef _WIN32
    int sent = ::send(fd, reinterpret_cast<const char*>(p + off), static_cast<int>(n - off), 0);
#else
    ssize_t sent = ::send(fd, p + off, n - off, 0);
#endif
    if (sent <= 0) return false;
    off += static_cast<std::size_t>(sent);
  }
  return true;
}
bool send_frame(int fd, MsgType type, std::uint64_t seq, const std::vector<std::uint8_t>& payload) {
  auto frame = encode_frame(type, seq, payload);
  return send_all(fd, frame.data(), frame.size());
}
std::uint64_t arg_u64(const char* s) { return std::strtoull(s, nullptr, 10); }
}  // namespace

int main(int argc, char** argv) {
  int port = 0; std::uint64_t worker = 0, boot = 0;
  std::vector<std::uint64_t> participants;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--port" && i + 1 < argc) port = std::atoi(argv[++i]);
    else if (a == "--worker" && i + 1 < argc) worker = arg_u64(argv[++i]);
    else if (a == "--boot" && i + 1 < argc) boot = arg_u64(argv[++i]);
    else if (a == "--participant" && i + 1 < argc) { participants.push_back(arg_u64(argv[++i])); }
  }
#ifdef _WIN32
  WSADATA d; if (WSAStartup(MAKEWORD(2, 2), &d) != 0) return 1;
  int fd = static_cast<int>(::socket(AF_INET, SOCK_STREAM, 0));
#else
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
#endif
  sockaddr_in addr; std::memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET; addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(static_cast<unsigned short>(port));
  if (::connect(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) return 2;

  send_frame(fd, MsgType::HELLO, 0, {});
  // Register worker + boot.
  { PersistenceWriter w; w.id(worker); w.id(boot); send_frame(fd, MsgType::REGISTER, 1, w.take()); }
  // Publish each owned participant.
  for (auto pid : participants) {
    Participant p;
    p.id = ParticipantId(pid); p.gen = ParticipantGeneration(1);
    p.worker = WorkerId(worker); p.worker_boot = WorkerBootId(boot);
    p.source = SourceId(1); p.source_boot = SourceBootId(100);
    p.device = DeviceId(pid); p.node = NodeId(1); p.nic = NicId(pid);
    p.capability_gen = CapabilityGeneration(1); p.health_gen = HealthGeneration(1); p.topology_gen = TopologyGeneration(1);
    p.ready = true; p.readiness_source = ProvenanceStatus::REAL; p.provenance = ProvenanceStatus::REAL;
    PersistenceWriter w; serialize_participant(w, p);
    send_frame(fd, MsgType::PUBLISH_PARTICIPANT, 2, w.take());
  }

  std::vector<std::uint8_t> buf;
  std::vector<Msg> msgs;
  std::uint8_t tmp[65536];
  bool quit = false;
  while (!quit) {
#ifdef _WIN32
    int n = ::recv(fd, reinterpret_cast<char*>(tmp), sizeof(tmp), 0);
#else
    int n = static_cast<int>(::recv(fd, tmp, sizeof(tmp), 0));
#endif
    if (n <= 0) break;
    buf.insert(buf.end(), tmp, tmp + n);
    msgs.clear();
    if (!decode_frames(buf, msgs)) break;
    for (auto& m : msgs) {
      if (m.type == MsgType::GRANT) {
        PersistenceReader r(m.payload.data(), m.payload.size(), m.payload.size());
        auto g = deserialize_grant(r);
        r.expect_end();
        bool am_participant = false;
        for (auto pid : g.participants) for (auto mine : participants) if (pid.value == mine) am_participant = true;
        if (am_participant && !g.terminal) {
          PersistenceWriter w; serialize_grant(w, g); w.id(g.workload.value);
          // EXECUTION_ACCEPTED then COMPLETE with this worker's authority.
          send_frame(fd, MsgType::EXECUTION_ACCEPTED, m.seq, w.take());
          std::this_thread::sleep_for(std::chrono::milliseconds(5));
          PersistenceWriter cw; serialize_grant(cw, g); cw.id(boot); cw.id(100);
          send_frame(fd, MsgType::COMPLETE, m.seq + 1, cw.take());
        }
      } else if (m.type == MsgType::SHUTDOWN) {
        quit = true;
      }
    }
  }
  send_frame(fd, MsgType::SHUTDOWN, 999, {});
  return 0;
}