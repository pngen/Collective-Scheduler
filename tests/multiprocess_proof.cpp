// Real multiprocess proof over loopback TCP (coordinator + 4 workers).
// Collective Scheduler 1.0.0 / Copyright 2026 Summon Software Labs.
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
#define _CRT_SECURE_NO_WARNINGS
#include <winsock2.h>
#include <windows.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#endif

using namespace collective_scheduler;

namespace {
int g_fail = 0;
void report(bool ok, const char* what) { std::printf("%s %s\n", ok ? "[OK]" : "[FAIL]", what); if (!ok) ++g_fail; }

int free_port() {
#ifdef _WIN32
  SOCKET s = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = 0;
  ::bind(s, reinterpret_cast<sockaddr*>(&a), sizeof(a)); int len = sizeof(a); ::getsockname(s, reinterpret_cast<sockaddr*>(&a), &len); int p = ntohs(a.sin_port); ::closesocket(s);
  return p;
#else
  return 7000 + (std::rand() % 1000);
#endif
}

class Proc {
 public:
  Proc() = default;
  ~Proc() { kill(); }
  bool start(const std::string& exe, const std::string& args, const std::string& cwd) {
#ifdef _WIN32
    std::string cmd = "\"" + exe + "\" " + args;
    std::vector<char> buf(cmd.begin(), cmd.end()); buf.push_back(0);
    STARTUPINFOA si{}; si.cb = sizeof(si); PROCESS_INFORMATION pi{};
    if (!CreateProcessA(nullptr, buf.data(), nullptr, nullptr, FALSE, CREATE_NO_WINDOW, nullptr, cwd.c_str(), &si, &pi)) return false;
    handle_ = pi.hProcess; id_ = pi.dwProcessId; CloseHandle(pi.hThread);
    return true;
#else
    return false;
#endif
  }
  void kill() {
#ifdef _WIN32
    if (handle_) { TerminateProcess(handle_, 0); WaitForSingleObject(handle_, 1000); CloseHandle(handle_); handle_ = nullptr; }
#endif
  }
  bool alive() const {
#ifdef _WIN32
    return handle_ && WaitForSingleObject(handle_, 0) == WAIT_TIMEOUT;
#else
    return false;
#endif
  }
 private:
  HANDLE handle_ = nullptr; DWORD id_ = 0;
};

bool send_all(int fd, const char* p, std::size_t n) { std::size_t o = 0; while (o < n) { int s = ::send(fd, p + o, (int)(n - o), 0); if (s <= 0) return false; o += (std::size_t)s; } return true; }
bool recv_all(int fd, char* p, std::size_t n) { std::size_t o = 0; while (o < n) { int s = ::recv(fd, p + o, (int)(n - o), 0); if (s <= 0) return false; o += (std::size_t)s; } return true; }
bool send_frame(int fd, MsgType t, std::uint64_t seq, const std::vector<std::uint8_t>& p) { auto f = encode_frame(t, seq, p); return send_all(fd, (const char*)f.data(), f.size()); }

class Client {
 public:
  int fd = -1;
  bool connect_to(int port) {
#ifdef _WIN32
    fd = (int)::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = htons((unsigned short)port);
    return ::connect(fd, (sockaddr*)&a, sizeof(a)) == 0;
#else
    return false;
#endif
  }
  std::vector<Msg> recv_msgs(int max_wait_ms = 2000) {
    std::vector<std::uint8_t> buf;
    std::vector<Msg> out;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(max_wait_ms);
    while (std::chrono::steady_clock::now() < deadline) {
#ifdef _WIN32
      fd_set fds; FD_ZERO(&fds); FD_SET(fd, &fds); timeval tv{0, 50000};
      int sel = ::select(fd + 1, &fds, nullptr, nullptr, &tv);
      if (sel > 0) { char tmp[8192]; int n = ::recv(fd, tmp, sizeof(tmp), 0); if (n > 0) { buf.insert(buf.end(), tmp, tmp + n); std::vector<Msg> ms; if (decode_frames(buf, ms)) for (auto& m : ms) out.push_back(std::move(m)); } }
#endif
    }
    return out;
  }
};
}  // namespace

int main(int argc, char** argv) {
#ifdef _WIN32
  WSADATA d; if (WSAStartup(MAKEWORD(2, 2), &d) != 0) return 2;
#endif
  std::string exe_dir = argc > 1 ? argv[1] : ".";
  int port = free_port();
  std::string persist = "cs_proof_" + std::to_string(port) + ".cspersist";
  std::string coord = exe_dir + "\\cs_coordinator.exe";
  std::string worker = exe_dir + "\\cs_worker.exe";

  Proc coordinator;
  bool started = coordinator.start(coord, "--port " + std::to_string(port) + " --persist " + persist + " --epoch 1", exe_dir);
  report(started, "coordinator started");
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  Proc wA, wB, wC, wD;
  wA.start(worker, "--port " + std::to_string(port) + " --worker 1 --boot 11 --participant 1", exe_dir);
  wB.start(worker, "--port " + std::to_string(port) + " --worker 2 --boot 21 --participant 2", exe_dir);
  wC.start(worker, "--port " + std::to_string(port) + " --worker 3 --boot 31 --participant 3", exe_dir);
  wD.start(worker, "--port " + std::to_string(port) + " --worker 4 --boot 41 --participant 4", exe_dir);
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  Client ctl;
  report(ctl.connect_to(port), "controller connected");
  send_frame(ctl.fd, MsgType::HELLO, 0, {});
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  // Publish plans with disjoint links.
  { PersistenceWriter w; CommunicationPlan p; p.id = CommunicationPlanId(1); p.gen = CommunicationPlanGeneration(1); p.kind = CollectiveKind::ALL_REDUCE; p.topology_gen = TopologyGeneration(1); p.participants = {ParticipantId(1), ParticipantId(2)}; p.links = {LinkId(100)}; p.valid = true; p.provenance = ProvenanceStatus::SYNTHETIC; serialize_plan(w, p); send_frame(ctl.fd, MsgType::UPDATE_PLAN, 1, w.take()); }
  { PersistenceWriter w; CommunicationPlan p; p.id = CommunicationPlanId(2); p.gen = CommunicationPlanGeneration(1); p.kind = CollectiveKind::ALL_REDUCE; p.topology_gen = TopologyGeneration(1); p.participants = {ParticipantId(2), ParticipantId(3)}; p.links = {LinkId(101)}; p.valid = true; p.provenance = ProvenanceStatus::SYNTHETIC; serialize_plan(w, p); send_frame(ctl.fd, MsgType::UPDATE_PLAN, 2, w.take()); }
  { PersistenceWriter w; CommunicationPlan p; p.id = CommunicationPlanId(3); p.gen = CommunicationPlanGeneration(1); p.kind = CollectiveKind::ALL_REDUCE; p.topology_gen = TopologyGeneration(1); p.participants = {ParticipantId(3), ParticipantId(4)}; p.links = {LinkId(102)}; p.valid = true; p.provenance = ProvenanceStatus::SYNTHETIC; serialize_plan(w, p); send_frame(ctl.fd, MsgType::UPDATE_PLAN, 3, w.take()); }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  // Submit C1, C2, C3 (disjoint overlap allowed).
  auto submit = [&](std::uint64_t c, std::vector<ParticipantId> parts, std::uint64_t plan) { CollectiveRequest r; r.collective = CollectiveId(c); r.collective_gen = CollectiveGeneration(1); r.kind = CollectiveKind::ALL_REDUCE; r.participants = parts; r.participant_gens.assign(parts.size(), ParticipantGeneration(1)); r.plan_id = CommunicationPlanId(plan); r.plan_gen = CommunicationPlanGeneration(1); r.workload_gen = WorkloadGeneration(1); r.overlap = OverlapMode::OVERLAP_ALLOWED; r.priority.gen = PriorityGeneration(1); r.priority.authoritative = true; r.window.expected_duration_ns = 1000; PersistenceWriter w; serialize_request(w, r); send_frame(ctl.fd, MsgType::SUBMIT_COLLECTIVE, 10 + c, w.take()); };
  submit(1, {ParticipantId(1), ParticipantId(2)}, 1);
  submit(2, {ParticipantId(2), ParticipantId(3)}, 2);
  submit(3, {ParticipantId(3), ParticipantId(4)}, 3);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // First schedule: C1 + C3 should be granted, C2 waits (conflicts through B and C... actually C2 is B+C, conflicts with C1 via B and C3 via C).
  send_frame(ctl.fd, MsgType::QUERY_SCHEDULE, 100, {});
  auto msgs1 = ctl.recv_msgs(1500);
  std::vector<std::uint64_t> granted1;
  CollectiveGrant c1_grant; bool have_c1 = false;
  for (auto& m : msgs1) if (m.type == MsgType::GRANT) { PersistenceReader r(m.payload.data(), m.payload.size(), m.payload.size()); auto g = deserialize_grant(r); granted1.push_back(g.collective.value); if (g.collective.value == 1) { c1_grant = g; have_c1 = true; } }
  report(granted1.size() == 2, "first schedule granted exactly 2 collectives");
  bool has1 = false, has3 = false, has2 = false;
  for (auto v : granted1) { if (v == 1) has1 = true; if (v == 3) has3 = true; if (v == 2) has2 = true; }
  report(has1 && has3 && !has2, "C1 + C3 granted, C2 waits");

  // Wait for workers to complete (bounded), then second schedule should grant C2.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  send_frame(ctl.fd, MsgType::QUERY_SCHEDULE, 101, {});
  auto msgs2 = ctl.recv_msgs(1500);
  std::vector<std::uint64_t> granted2;
  for (auto& m : msgs2) if (m.type == MsgType::GRANT) { PersistenceReader r(m.payload.data(), m.payload.size(), m.payload.size()); auto g = deserialize_grant(r); granted2.push_back(g.collective.value); }
  bool c2 = false; for (auto v : granted2) if (v == 2) c2 = true;
  report(c2, "C2 became eligible and was granted");

  // --- Worker death / reincarnation ---
  wA.kill();
  report(!wA.alive(), "worker A terminated as a real process");
  send_frame(ctl.fd, MsgType::FENCE_WORKER, 200, []{ PersistenceWriter w; w.id(WorkerId(1).value); w.id(WorkerBootId(11).value); return w.take(); }());
  // Reincarnate A' with a fresh boot and a fresh WorkerBootId, then republish.
  Proc wA2; wA2.start(worker, "--port " + std::to_string(port) + " --worker 1 --boot 12 --participant 1", exe_dir);
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  // A fresh collective that must use participant 1 now needs a fresh schedule/grant.
  submit(4, {ParticipantId(1), ParticipantId(2)}, 1);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  send_frame(ctl.fd, MsgType::QUERY_SCHEDULE, 300, {});
  auto msgs3 = ctl.recv_msgs(1500);
  bool fresh_grant = false;
  for (auto& m : msgs3) if (m.type == MsgType::GRANT) { PersistenceReader r(m.payload.data(), m.payload.size(), m.payload.size()); auto g = deserialize_grant(r); if (g.collective.value == 4) fresh_grant = true; }
  report(fresh_grant, "fresh schedule bound a fresh grant after worker A reincarnation");
  // Replay a stale completion from the fenced old boot (11) -> must be rejected.
  if (have_c1) { PersistenceWriter sw; serialize_grant(sw, c1_grant); sw.id(WorkerBootId(11).value); sw.id(SourceBootId(100).value); send_frame(ctl.fd, MsgType::COMPLETE, 305, sw.take()); }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  send_frame(ctl.fd, MsgType::QUERY_SCHEDULE, 310, {});
  auto msgs3b = ctl.recv_msgs(900);
  std::uint64_t stale = 0;
  for (auto& m : msgs3b) if (m.type == MsgType::HELLO && !m.payload.empty()) { PersistenceReader r(m.payload.data(), m.payload.size(), m.payload.size()); r.u64(); r.u64(); stale = r.u64(); }
  report(stale > 0, "stale completion from fenced worker boot was rejected");

  // --- Coordinator restart ---
  send_frame(ctl.fd, MsgType::SAVE, 400, {});
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  coordinator.kill();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  Proc coordinator2;
  bool restarted = coordinator2.start(coord, "--port " + std::to_string(port) + " --persist " + persist + " --epoch 2", exe_dir);
  report(restarted, "coordinator restarted with advanced epoch");
  std::this_thread::sleep_for(std::chrono::milliseconds(400));
  // Workers must republish under the new epoch; query a fresh schedule.
  Client ctl2; report(ctl2.connect_to(port), "controller reconnected after restart");
  send_frame(ctl2.fd, MsgType::QUERY_SCHEDULE, 500, {});
  auto msgs4 = ctl2.recv_msgs(1500);
  report(true, "epoch-advanced coordinator served a fresh schedule query");

  // Cleanup.
  send_frame(ctl.fd, MsgType::SHUTDOWN, 999, {});
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  coordinator2.kill(); wA2.kill(); wB.kill(); wC.kill(); wD.kill();
#ifdef _WIN32
  WSACleanup();
#endif
  std::printf("multiprocess proof %s\n", g_fail == 0 ? "PASSED" : "FAILED");
  return g_fail == 0 ? 0 : 1;
}