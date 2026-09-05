// Collective Scheduler — coordinator executable entry point.
#include <collective_scheduler/coordinator.hpp>
#include <cstdio>
#include <cstdlib>
#include <string>

int main(int argc, char** argv) {
  collective_scheduler::Coordinator::Options opts;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--port" && i + 1 < argc) opts.port = std::atoi(argv[++i]);
    else if (a == "--persist" && i + 1 < argc) opts.persist_file = argv[++i];
    else if (a == "--epoch" && i + 1 < argc) opts.epoch = collective_scheduler::CoordinatorEpoch(static_cast<std::uint64_t>(std::atoll(argv[++i])));
  }
  collective_scheduler::Coordinator c(opts);
  return c.run();
}