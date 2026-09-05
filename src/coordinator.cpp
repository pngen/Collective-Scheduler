// Collective Scheduler — reference multiprocess coordinator (implementation).
#define _CRT_SECURE_NO_WARNINGS
#include <collective_scheduler/coordinator.hpp>
#include <collective_scheduler/persistence.hpp>
#include <collective_scheduler/scheduler.hpp>
#include <collective_scheduler/serialize.hpp>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>
#undef ERROR
#undef min
#undef max
#pragma comment(lib, "ws2_32.lib")
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace collective_scheduler {

class Coordinator::Impl {
 public:
  Scheduler scheduler;
  std::mutex sessions_mu;
  std::vector<int> sessions;
  std::atomic<bool> running{true};
  std::atomic<int> stale_rejections{0};
  std::atomic<int> grants_issued{0};
};

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
bool win_init() {
#ifdef _WIN32
  WSADATA d; return WSAStartup(MAKEWORD(2, 2), &d) == 0;
#else
  return true;
#endif
}
void win_cleanup() {
#ifdef _WIN32
  WSACleanup();
#endif
}
void close_fd(int fd) {
#ifdef _WIN32
  ::closesocket(fd);
#else
  ::close(fd);
#endif
}
}  // namespace

Coordinator::Coordinator(Options opts) : impl_(std::make_shared<Impl>()), opts_(std::move(opts)) {}
Coordinator::~Coordinator() = default;

int Coordinator::run() {
  if (!win_init()) return 1;
#ifdef _WIN32
  SOCKET ls = ::socket(AF_INET, SOCK_STREAM, 0);
#else
  int ls = ::socket(AF_INET, SOCK_STREAM, 0);
#endif
  if (ls == INVALID_SOCKET) return 1;
  sockaddr_in addr; std::memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET; addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(static_cast<unsigned short>(opts_.port));
  if (::bind(ls, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) return 1;
  if (::listen(ls, 16) == SOCKET_ERROR) return 1;

  if (!opts_.persist_file.empty()) {
    FILE* f = std::fopen(opts_.persist_file.c_str(), "rb");
    if (f) {
      std::fseek(f, 0, SEEK_END); long sz = std::ftell(f); std::fseek(f, 0, SEEK_SET);
      std::vector<std::uint8_t> bytes(static_cast<std::size_t>(sz));
      if (sz > 0) std::fread(bytes.data(), 1, static_cast<std::size_t>(sz), f);
      std::fclose(f);
      try { Persistence::restore(impl_->scheduler, bytes); } catch (...) {}
    }
  }
  if (impl_->scheduler.coordinatorEpoch().value < opts_.epoch.value) impl_->scheduler.advanceCoordinatorEpoch(opts_.epoch);

  std::vector<std::thread> threads;
  while (impl_->running) {
#ifdef _WIN32
    SOCKET cl = ::accept(ls, nullptr, nullptr);
#else
    int cl = ::accept(ls, nullptr, nullptr);
#endif
    if (cl == INVALID_SOCKET) break;
    { std::lock_guard<std::mutex> lk(impl_->sessions_mu); impl_->sessions.push_back(static_cast<int>(cl)); }
    threads.emplace_back([this, cl] { session(static_cast<int>(cl)); });
  }
  close_fd(static_cast<int>(ls));
  for (auto& t : threads) if (t.joinable()) t.join();
  win_cleanup();
  return 0;
}

void Coordinator::session(int fd) {
  std::vector<std::uint8_t> buf;
  std::vector<Msg> msgs;
  std::uint8_t tmp[65536];
  while (impl_->running) {
    int n = ::recv(fd, reinterpret_cast<char*>(tmp), sizeof(tmp), 0);
    if (n <= 0) break;
    buf.insert(buf.end(), tmp, tmp + n);
    msgs.clear();
    if (!decode_frames(buf, msgs)) break;
    for (auto& m : msgs) handle_message(fd, std::move(m));
  }
  { std::lock_guard<std::mutex> lk(impl_->sessions_mu); impl_->sessions.erase(std::remove(impl_->sessions.begin(), impl_->sessions.end(), fd), impl_->sessions.end()); }
  close_fd(fd);
}

void Coordinator::handle_message(int fd, Msg m) {
  switch (m.type) {
    case MsgType::HELLO:
      send_frame(fd, MsgType::HELLO, m.seq, {});
      break;
    case MsgType::REGISTER:
      send_frame(fd, MsgType::HELLO, m.seq, {});
      break;
    case MsgType::PUBLISH_PARTICIPANT:
      try { PersistenceReader r(m.payload.data(), m.payload.size(), m.payload.size()); auto p = deserialize_participant(r); r.expect_end(); impl_->scheduler.publishParticipant(std::move(p)); } catch (...) {}
      break;
    case MsgType::UPDATE_PLAN:
      try { PersistenceReader r(m.payload.data(), m.payload.size(), m.payload.size()); auto p = deserialize_plan(r); r.expect_end(); impl_->scheduler.publishPlan(std::move(p)); } catch (...) {}
      break;
    case MsgType::UPDATE_CONGESTION:
      try { PersistenceReader r(m.payload.data(), m.payload.size(), m.payload.size()); auto c = deserialize_congestion(r); r.expect_end(); impl_->scheduler.publishCongestion(std::move(c)); } catch (...) {}
      break;
    case MsgType::UPDATE_RESERVATION:
      try { PersistenceReader r(m.payload.data(), m.payload.size(), m.payload.size()); auto c = deserialize_reservation(r); r.expect_end(); impl_->scheduler.publishReservation(std::move(c)); } catch (...) {}
      break;
    case MsgType::UPDATE_CAPACITY:
      try { PersistenceReader r(m.payload.data(), m.payload.size(), m.payload.size()); auto c = deserialize_capacity(r); r.expect_end(); impl_->scheduler.publishCapacity(std::move(c)); } catch (...) {}
      break;
    case MsgType::UPDATE_DEPENDENCY:
      try { PersistenceReader r(m.payload.data(), m.payload.size(), m.payload.size()); auto c = deserialize_dependency(r); r.expect_end(); impl_->scheduler.publishDependency(std::move(c)); } catch (...) {}
      break;
    case MsgType::SUBMIT_COLLECTIVE:
      try { PersistenceReader r(m.payload.data(), m.payload.size(), m.payload.size()); auto req = deserialize_request(r); r.expect_end(); auto id = impl_->scheduler.submit(std::move(req)); PersistenceWriter w; w.id(id.value); send_frame(fd, MsgType::ERROR, m.seq, w.take()); } catch (...) { send_frame(fd, MsgType::ERROR, m.seq, {}); }
      break;
    case MsgType::QUERY_SCHEDULE: {
      auto dec = impl_->scheduler.schedule();
      std::vector<std::vector<std::uint8_t>> grant_payloads;
      for (auto gid : dec.grants) {
        auto g = impl_->scheduler.grantById(gid);
        if (!g) continue;
        ++impl_->grants_issued;
        PersistenceWriter w; serialize_grant(w, *g); grant_payloads.push_back(w.take());
      }
      {
        std::lock_guard<std::mutex> lk(impl_->sessions_mu);
        for (auto& p : grant_payloads) for (auto sess : impl_->sessions) send_frame(sess, MsgType::GRANT, m.seq, p);
      }
      // Send a summary back to the requester for observation.
      PersistenceWriter sw; sw.u64(dec.selected.size()); sw.u64(impl_->grants_issued.load()); sw.u64(impl_->stale_rejections.load());
      send_frame(fd, MsgType::HELLO, m.seq, sw.take());
      break;
    }
    case MsgType::COMPLETE:
      try { PersistenceReader r(m.payload.data(), m.payload.size(), m.payload.size()); auto g = deserialize_grant(r); WorkerBootId wb(r.id()); SourceBootId sb(r.id()); r.expect_end();
        if (!impl_->scheduler.complete(g, wb, sb)) ++impl_->stale_rejections;
      } catch (...) {}
      break;
    case MsgType::FENCE_WORKER:
      try { PersistenceReader r(m.payload.data(), m.payload.size(), m.payload.size()); WorkerId wid(r.id()); WorkerBootId wb(r.id()); r.expect_end(); impl_->scheduler.fenceWorkerBoot(wid, wb); send_frame(fd, MsgType::HELLO, m.seq, {}); } catch (...) {}
      break;
    case MsgType::SAVE:
      if (!opts_.persist_file.empty()) { auto bytes = Persistence::save(impl_->scheduler); FILE* f = std::fopen(opts_.persist_file.c_str(), "wb"); if (f) { std::fwrite(bytes.data(), 1, bytes.size(), f); std::fclose(f); } }
      break;
    case MsgType::SHUTDOWN:
      impl_->running = false;
      break;
    default:
      break;
  }
}

}  // namespace collective_scheduler