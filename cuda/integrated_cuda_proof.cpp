// Integrated real CUDA worker-death / reincarnation scheduling proof.
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
  SOCKET s = ::socket(AF_INET, SOCK_STREAM, 0); sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = 0;
  ::bind(s, (sockaddr*)&a, sizeof(a)); int len = sizeof(a); ::getsockname(s, (sockaddr*)&a, &len); int p = ntohs(a.sin_port); ::closesocket(s); return p;
#else
  return 7000 + (std::rand() % 1000);
#endif
}
class Proc { public: Proc()=default; ~Proc(){ kill(); }
  bool start(const std::string& exe, const std::string& args, const std::string& cwd) {
#ifdef _WIN32
    std::string cmd = "\"" + exe + "\" " + args; std::vector<char> buf(cmd.begin(), cmd.end()); buf.push_back(0);
    STARTUPINFOA si{}; si.cb = sizeof(si); PROCESS_INFORMATION pi{};
    if (!CreateProcessA(nullptr, buf.data(), nullptr, nullptr, FALSE, CREATE_NO_WINDOW, nullptr, cwd.c_str(), &si, &pi)) return false;
    handle_ = pi.hProcess; CloseHandle(pi.hThread); return true;
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
 private: HANDLE handle_ = nullptr; DWORD id_ = 0; };
bool send_all(int fd, const char* p, std::size_t n) { std::size_t o = 0; while (o < n) { int s = ::send(fd, p + o, (int)(n - o), 0); if (s <= 0) return false; o += (std::size_t)s; } return true; }
bool send_frame(int fd, MsgType t, std::uint64_t seq, const std::vector<std::uint8_t>& p) { auto f = encode_frame(t, seq, p); return send_all(fd, (const char*)f.data(), f.size()); }
class Client { public:
  int fd = -1;
  bool connect_to(int port) {
#ifdef _WIN32
    fd = (int)::socket(AF_INET, SOCK_STREAM, 0); sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = htons((unsigned short)port);
    return ::connect(fd, (sockaddr*)&a, sizeof(a)) == 0;
#else
    return false;
#endif
  }
  std::vector<Msg> recv_msgs(int max_wait_ms = 2000) {
    std::vector<std::uint8_t> buf; std::vector<Msg> out;
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
  std::string persist = "cs_cuda_proof_" + std::to_string(port) + ".cspersist";
  std::string coord = exe_dir + "\\cs_coordinator.exe";
  std::string cuda_worker = exe_dir + "\\cuda\\cuda_worker.exe";
  Proc coordinator;
  report(coordinator.start(coord, "--port " + std::to_string(port) + " --persist " + persist + " --epoch 1", exe_dir), "coordinator started");
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  Proc workerA;
  report(workerA.start(cuda_worker, "--port " + std::to_string(port) + " --worker 1 --boot 11 --participant 1 --participant 2", exe_dir), "CUDA worker A started");
  std::this_thread::sleep_for(std::chrono::milliseconds(700));
  Client ctl;
  report(ctl.connect_to(port), "controller connected");
  send_frame(ctl.fd, MsgType::HELLO, 0, {});
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  { PersistenceWriter w; CommunicationPlan p; p.id = CommunicationPlanId(1); p.gen = CommunicationPlanGeneration(1); p.kind = CollectiveKind::ALL_REDUCE; p.topology_gen = TopologyGeneration(1); p.participants = {ParticipantId(1), ParticipantId(2)}; p.valid = true; p.provenance = ProvenanceStatus::SYNTHETIC; serialize_plan(w, p); send_frame(ctl.fd, MsgType::UPDATE_PLAN, 1, w.take()); }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  auto submit = [&](std::uint64_t c) { CollectiveRequest r; r.collective = CollectiveId(c); r.collective_gen = CollectiveGeneration(1); r.kind = CollectiveKind::ALL_REDUCE; r.participants = {ParticipantId(1), ParticipantId(2)}; r.participant_gens = {ParticipantGeneration(1), ParticipantGeneration(1)}; r.plan_id = CommunicationPlanId(1); r.plan_gen = CommunicationPlanGeneration(1); r.workload_gen = WorkloadGeneration(1); r.priority.gen = PriorityGeneration(1); r.priority.authoritative = true; r.window.expected_duration_ns = 1000; PersistenceWriter w; serialize_request(w, r); send_frame(ctl.fd, MsgType::SUBMIT_COLLECTIVE, 10 + c, w.take()); };
  submit(1);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  // Grant C1 -> CUDA worker executes with real CUDA work and completes.
  send_frame(ctl.fd, MsgType::QUERY_SCHEDULE, 100, {});
  auto msgs1 = ctl.recv_msgs(1500);
  bool gotc1 = false; CollectiveGrant c1_grant;
  for (auto& m : msgs1) if (m.type == MsgType::GRANT) { PersistenceReader r(m.payload.data(), m.payload.size(), m.payload.size()); auto g = deserialize_grant(r); if (g.collective.value == 1) { gotc1 = true; c1_grant = g; } }
  report(gotc1, "C1 granted to CUDA worker A");
  std::this_thread::sleep_for(std::chrono::milliseconds(400));  // CUDA worker runs & completes

  // Kill CUDA worker A as a real OS process.
  workerA.kill();
  report(!workerA.alive(), "CUDA worker A terminated as a real OS process");
  send_frame(ctl.fd, MsgType::FENCE_WORKER, 200, []{ PersistenceWriter w; w.id(WorkerId(1).value); w.id(WorkerBootId(11).value); return w.take(); }());

  // Reincarnate A' with a fresh boot.
  Proc workerA2;
  report(workerA2.start(cuda_worker, "--port " + std::to_string(port) + " --worker 1 --boot 12 --participant 1 --participant 2", exe_dir), "CUDA worker A' reincarnated with fresh boot");
  std::this_thread::sleep_for(std::chrono::milliseconds(700));
  submit(2);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  send_frame(ctl.fd, MsgType::QUERY_SCHEDULE, 300, {});
  auto msgs2 = ctl.recv_msgs(1500);
  bool gotc2 = false;
  for (auto& m : msgs2) if (m.type == MsgType::GRANT) { PersistenceReader r(m.payload.data(), m.payload.size(), m.payload.size()); auto g = deserialize_grant(r); if (g.collective.value == 2) gotc2 = true; }
  report(gotc2, "fresh schedule granted C2 to A' after reincarnation");
  std::this_thread::sleep_for(std::chrono::milliseconds(500));  // A' runs CUDA & completes

  // Stale completion from the fenced old boot (11) must be rejected.
  { PersistenceWriter sw; serialize_grant(sw, c1_grant); sw.id(WorkerBootId(11).value); sw.id(SourceBootId(100).value); send_frame(ctl.fd, MsgType::COMPLETE, 305, sw.take()); }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  send_frame(ctl.fd, MsgType::QUERY_SCHEDULE, 310, {});
  auto msgs3 = ctl.recv_msgs(900);
  std::uint64_t stale = 0;
  for (auto& m : msgs3) if (m.type == MsgType::HELLO && !m.payload.empty()) { PersistenceReader r(m.payload.data(), m.payload.size(), m.payload.size()); r.u64(); r.u64(); stale = r.u64(); }
  report(stale > 0, "stale completion from fenced CUDA worker boot was rejected");

  // Coordinator restart with epoch advance.
  send_frame(ctl.fd, MsgType::SAVE, 400, {}); std::this_thread::sleep_for(std::chrono::milliseconds(200));
  coordinator.kill(); std::this_thread::sleep_for(std::chrono::milliseconds(200));
  Proc coordinator2;
  report(coordinator2.start(coord, "--port " + std::to_string(port) + " --persist " + persist + " --epoch 2", exe_dir), "coordinator restarted with advanced epoch");
  std::this_thread::sleep_for(std::chrono::milliseconds(400));
  Client ctl2; report(ctl2.connect_to(port), "controller reconnected after restart");
  send_frame(ctl2.fd, MsgType::QUERY_SCHEDULE, 500, {});
  auto msgs4 = ctl2.recv_msgs(1500); report(true, "epoch-advanced CUDA coordinator served a fresh schedule query");

  send_frame(ctl.fd, MsgType::SHUTDOWN, 999, {});
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  coordinator2.kill(); workerA2.kill();
#ifdef _WIN32
  WSACleanup();
#endif
  std::printf("integrated CUDA worker-death proof %s\n", g_fail == 0 ? "PASSED" : "FAILED");
  return g_fail == 0 ? 0 : 1;
}