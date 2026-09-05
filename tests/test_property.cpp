#include <collective_scheduler/scheduler.hpp>
#include <collective_scheduler/deterministic.hpp>
#include "test_common.hpp"
#include "test_framework.hpp"

#include <algorithm>
#include <cstdio>

using namespace collective_scheduler;
using namespace tdata;

namespace {
// Build a scheduler with `n` collectives drawn from a bounded set of shared
// participants so that conflicts are dense/sparse depending on the seed.
void build(int n, std::uint64_t seed, Scheduler& s) {
  SplitMix64 rng(seed);
  for (int i = 1; i <= 8; ++i) s.publishParticipant(P(i, 1, 1, 11));
  for (int i = 1; i <= 4; ++i) s.publishPlan(Plan(i, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  for (int c = 0; c < n; ++c) {
    std::uint64_t cid = 1000 + c;
    auto pa = 1 + rng.bounded(6);
    auto pb = 1 + rng.bounded(6);
    if (pb == pa) pb = (pb % 6) + 1;
    auto r = Req(cid, CollectiveKind::ALL_REDUCE, {ParticipantId(pa), ParticipantId(pb)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1, rng.bounded(2) ? OverlapMode::OVERLAP_ALLOWED : OverlapMode::UNKNOWN);
    r.priority.value = static_cast<std::int32_t>(rng.bounded(10));
    r.priority.authoritative = rng.bounded(2) == 0;
    try { s.submit(std::move(r)); } catch (...) {}
  }
}
// Deterministic completion order of collective ids.
std::vector<std::uint64_t> drain_order(Scheduler& s, int maxc = 400) {
  std::vector<std::uint64_t> order;
  for (int i = 0; i < maxc; ++i) {
    auto dec = s.schedule();
    if (dec.selected.empty()) break;
    bool prog = false;
    for (auto gid : dec.grants) { auto g = s.grantById(gid); if (!g) continue; if (s.handoff(*g) && s.complete(*g, WorkerBootId(11), SourceBootId(100))) { order.push_back(g->collective.value); prog = true; } }
    if (!prog) break;
  }
  return order;
}
}  // namespace

TEST(property_determinism_identical_inputs) {
  for (std::uint64_t seed = 1; seed <= 20; ++seed) {
    Scheduler a, b; build(8, seed, a); build(8, seed, b);
    auto oa = drain_order(a); auto ob = drain_order(b);
    CHECK_EQ(oa.size(), ob.size());
    for (std::size_t i = 0; i < oa.size() && i < ob.size(); ++i) CHECK_EQ(oa[i], ob[i]);
  }
}

TEST(property_insertion_order_independent) {
  // Same set of collectives submitted in different orders -> same drain order (by collective id).
  auto run = [](bool rev) {
    Scheduler s;
    for (int i = 1; i <= 4; ++i) s.publishParticipant(P(i, 1, 1, 11));
    for (int i = 1; i <= 3; ++i) s.publishPlan(Plan(i, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(i), ParticipantId(i % 4 + 1)}));
    std::vector<int> order = rev ? std::vector<int>{3, 1, 2} : std::vector<int>{1, 2, 3};
    for (auto c : order) s.submit(Req(c, CollectiveKind::ALL_REDUCE, {ParticipantId(c), ParticipantId(c % 4 + 1)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, c, 1, OverlapMode::OVERLAP_LIMITED));
    return drain_order(s);
  };
  auto a = run(false); auto b = run(true);
  CHECK_EQ(a.size(), b.size());
  for (std::size_t i = 0; i < a.size() && i < b.size(); ++i) CHECK_EQ(a[i], b[i]);
}

TEST(property_hard_ineligible_never_wins) {
  SplitMix64 rng(7);
  Scheduler s;
  for (int i = 1; i <= 4; ++i) s.publishParticipant(P(i, 1, 1, 11));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  // A stale-participant collective (gen mismatch) is never granted.
  auto stale = Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(99), ParticipantGeneration(99)}, 1, 1);
  stale.priority.value = 100; stale.priority.authoritative = true;
  auto ok = Req(2, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, {ParticipantGeneration(1), ParticipantGeneration(1)}, 1, 1);
  s.submit(std::move(stale)); s.submit(std::move(ok));
  auto dec = s.schedule();
  CHECK(dec.selected.empty() || dec.selected[0].value != 1);  // stale (collective 1) cannot win
}
