// Collective Scheduler — reference multiprocess coordinator.
#pragma once
#include <collective_scheduler/domains.hpp>
#include <collective_scheduler/protocol.hpp>
#include <cstdint>
#include <memory>
#include <string>
namespace collective_scheduler {
class Coordinator {
 public:
  struct Options {
    int port = 0;
    std::string persist_file;
    CoordinatorEpoch epoch = CoordinatorEpoch(1);
  };
  explicit Coordinator(Options opts);
  ~Coordinator();
  int run();   // blocks; returns process exit code
 private:
  class Impl;
  std::shared_ptr<Impl> impl_;
  Options opts_;
  void session(int fd);
  void handle_message(int fd, Msg m);
};
}  // namespace collective_scheduler