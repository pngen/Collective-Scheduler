#include <collective_scheduler/scheduler.hpp>
#include "test_common.hpp"
#include "test_framework.hpp"

#include <atomic>
#include <thread>
#include <vector>

using namespace collective_scheduler;
using namespace tdata;

TEST(concurrency_readiness_vs_scheduling) {
  Scheduler s;
  for (int i = 1; i <= 4; ++i) s.publishParticipant(P(i, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  auto id = s.submit(Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1));
  std::atomic<bool> stop{false};
  std::vector<std::thread> ts;
  ts.emplace_back([&] { while (!stop) s.schedule(); });
  ts.emplace_back([&] { for (int i = 0; i < 200; ++i) s.publishParticipant(P(1, 1, 1, 11)); });
  ts.emplace_back([&] { for (int i = 0; i < 200; ++i) { auto inf = s.info(); (void)inf; } });
  ts.emplace_back([&] { for (int i = 0; i < 200; ++i) s.cancel(id); });
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  stop = true;
  for (auto& t : ts) t.join();
  CHECK(true);  // no crash / no deadlock
}

TEST(concurrency_grant_vs_cancel) {
  Scheduler s;
  for (int i = 1; i <= 2; ++i) s.publishParticipant(P(i, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  std::atomic<bool> done{false};
  std::thread t1([&] {
    for (int i = 0; i < 50; ++i) {
      auto id = s.submit(Req(100 + i, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1));
      auto dec = s.schedule();
      if (!dec.grants.empty()) { auto g = s.grantById(dec.grants[0]); if (g) s.handoff(*g); }
      s.cancel(id);
    }
    done = true;
  });
  std::thread t2([&] { while (!done) { auto inf = s.info(); (void)inf; auto dec = s.schedule(); (void)dec; } });
  t1.join(); t2.join();
  CHECK(s.info().active_grants >= 0);
}

TEST(concurrency_many_querying) {
  Scheduler s;
  for (int i = 1; i <= 4; ++i) s.publishParticipant(P(i, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  for (int i = 1; i <= 8; ++i) s.submit(Req(i, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1));
  std::vector<std::thread> ts;
  for (int t = 0; t < 8; ++t) ts.emplace_back([&] { for (int i = 0; i < 100; ++i) { auto inf = s.info(); (void)inf; auto dec = s.schedule(); (void)dec; } });
  for (auto& t : ts) t.join();
  CHECK(true);
}
