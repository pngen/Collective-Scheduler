// CUDA worker process: performs real schedule-gated CUDA work on grant.
#include <collective_scheduler/protocol.hpp>
#include <collective_scheduler/serialize.hpp>
#include "cuda_executor.hpp"

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
bool send_all(int fd, const std::uint8_t* p, std::size_t n) { std::size_t o = 0; while (o < n) { int s = ::send(fd, reinterpret_cast<const char*>(p + o), (int)(n - o), 0); if (s <= 0) return false; o += (std::size_t)s; } return true; }
bool send_frame(int fd, MsgType t, std::uint64_t seq, const std::vector<std::uint8_t>& p) { auto f = encode_frame(t, seq, p); return send_all(fd, f.data(), f.size()); }
}  // namespace
int main(int argc, char** argv) {
  int port = 0; std::uint64_t worker = 0, boot = 0; std::vector<std::uint64_t> participants;
  for (int i = 1; i < argc; ++i) { std::string a = argv[i]; if (a == "--port" && i + 1 < argc) port = std::atoi(argv[++i]); else if (a == "--worker" && i + 1 < argc) worker = std::strtoull(argv[++i], nullptr, 10); else if (a == "--boot" && i + 1 < argc) boot = std::strtoull(argv[++i], nullptr, 10); else if (a == "--participant" && i + 1 < argc) participants.push_back(std::strtoull(argv[++i], nullptr, 10)); }
#ifdef _WIN32
  WSADATA d; if (WSAStartup(MAKEWORD(2, 2), &d) != 0) return 1;
  int fd = (int)::socket(AF_INET, SOCK_STREAM, 0);
#else
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
#endif
  sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = htons((unsigned short)port);
  if (::connect(fd, (sockaddr*)&a, sizeof(a)) == SOCKET_ERROR) return 2;
  send_frame(fd, MsgType::HELLO, 0, {});
  { PersistenceWriter w; w.id(worker); w.id(boot); send_frame(fd, MsgType::REGISTER, 1, w.take()); }
  for (auto pid : participants) { Participant p; p.id = ParticipantId(pid); p.gen = ParticipantGeneration(1); p.worker = WorkerId(worker); p.worker_boot = WorkerBootId(boot); p.source = SourceId(1); p.source_boot = SourceBootId(100); p.device = DeviceId(pid); p.node = NodeId(1); p.nic = NicId(pid); p.capability_gen = CapabilityGeneration(1); p.health_gen = HealthGeneration(1); p.topology_gen = TopologyGeneration(1); p.ready = true; p.readiness_source = ProvenanceStatus::REAL; p.provenance = ProvenanceStatus::REAL; PersistenceWriter w; serialize_participant(w, p); send_frame(fd, MsgType::PUBLISH_PARTICIPANT, 2, w.take()); }
  std::vector<std::uint8_t> buf; std::vector<Msg> msgs; std::uint8_t tmp[65536];
  bool quit = false;
  while (!quit) {
    int n = ::recv(fd, reinterpret_cast<char*>(tmp), sizeof(tmp), 0); if (n <= 0) break;
    buf.insert(buf.end(), tmp, tmp + n); msgs.clear(); if (!decode_frames(buf, msgs)) break;
    for (auto& m : msgs) {
      if (m.type == MsgType::GRANT) {
        PersistenceReader r(m.payload.data(), m.payload.size(), m.payload.size()); auto g = deserialize_grant(r); r.expect_end();
        bool am = false; for (auto pid : g.participants) for (auto mine : participants) if (pid.value == mine) am = true;
        if (am && !g.terminal) {
          std::printf("[cuda_worker] executing grant for collective %llu on worker %llu boot %llu\n", (unsigned long long)g.collective.value, (unsigned long long)worker, (unsigned long long)boot);
          auto res = run_cuda_work(boot, 1u << 20);
          std::printf("[cuda_worker] parity=%s device_count=%d cc=%d.%d\n", res.parity_ok ? "OK" : "BAD", res.device_count, res.cc_major, res.cc_minor);
          PersistenceWriter cw; serialize_grant(cw, g); cw.id(boot); cw.id(100);
          send_frame(fd, MsgType::COMPLETE, m.seq + 1, cw.take());
        }
      } else if (m.type == MsgType::SHUTDOWN) { quit = true; }
    }
  }
  send_frame(fd, MsgType::SHUTDOWN, 999, {});
  return 0;
}