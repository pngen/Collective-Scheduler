// Collective Scheduler — minimal self-contained test framework.
//
// Collective Scheduler 1.0.0
// Copyright 2026 Summon Software Labs.

#pragma once

#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <sstream>
#include <string>
#include <vector>

namespace cstest {

struct Failure {
  std::string file; int line; std::string msg;
};
inline std::vector<Failure>& failures() { static std::vector<Failure> f; return f; }
inline int& checks() { static int c = 0; return c; }
inline void record(const char* file, int line, const std::string& msg) {
  failures().push_back({file, line, msg});
}
inline void check(bool cond, const char* file, int line, const std::string& msg) {
  ++checks();
  if (!cond) record(file, line, msg);
}

template <typename T>
std::string to_str(const T& v) {
  std::ostringstream os; os << v; return os.str();
}
inline std::string to_str(const std::string& v) { return v; }
inline std::string to_str(const char* v) { return v ? std::string(v) : std::string("(null)"); }
inline std::string to_str(bool v) { return v ? "true" : "false"; }

template <typename A, typename B>
void eq(const A& a, const B& b, const char* file, int line, const char* ea, const char* eb) {
  ++checks();
  if (!(a == b)) {
    std::ostringstream os;
    os << ea << " == " << eb << "  (" << to_str(a) << " vs " << to_str(b) << ")";
    record(file, line, os.str());
  }
}

#define CHECK(cond) ::cstest::check((cond), __FILE__, __LINE__, #cond)
#define REQUIRE(cond) do { if (!(cond)) { ::cstest::record(__FILE__, __LINE__, #cond); return; } } while (0)
#define CHECK_EQ(a, b) ::cstest::eq((a), (b), __FILE__, __LINE__, #a, #b)

using TestFn = std::function<void()>;
struct TestCase { std::string name; TestFn fn; };
inline std::vector<TestCase>& registry() { static std::vector<TestCase> r; return r; }
inline int& selected_total() { static int t = -1; return t; }

struct Registrar {
  Registrar(const char* name, TestFn fn) { registry().push_back({name, fn}); }
};

#define TEST(name) \
  static void cstest_fn_##name(); \
  static ::cstest::Registrar cstest_reg_##name(#name, cstest_fn_##name); \
  static void cstest_fn_##name()

inline int run_all(int argc, char** argv) {
  std::string filter;
  if (argc > 1) filter = argv[1];
  int executed = 0;
  for (auto& t : registry()) {
    if (!filter.empty() && t.name.find(filter) == std::string::npos) continue;
    int before = static_cast<int>(failures().size());
    t.fn();
    ++executed;
    std::size_t newF = failures().size() - before;
    if (newF == 0) std::printf("[ OK ] %s\n", t.name.c_str());
    else std::printf("[FAIL] %s\n", t.name.c_str());
  }
  std::printf("\n%d test(s) executed, %zu check failure(s), %d check(s)\n", executed, failures().size(), checks());
  if (!failures().empty()) {
    for (auto& f : failures()) std::printf("  %s:%d: %s\n", f.file.c_str(), f.line, f.msg.c_str());
    return 1;
  }
  return 0;
}

}  // namespace cstest