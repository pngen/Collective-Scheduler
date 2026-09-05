// Collective Scheduler — CUDA reference execution adapter.
#pragma once
#include <cstddef>
#include <cstdint>
namespace collective_scheduler {
struct CudaWorkResult {
  bool parity_ok = false;
  std::uint64_t kernel_sum = 0;
  std::size_t total_memory_before = 0;
  std::size_t total_memory_after = 0;
  std::size_t free_memory_before = 0;
  std::size_t free_memory_after = 0;
  int device_count = 0;
  int cc_major = 0;
  int cc_minor = 0;
};
// Runs a bounded, real CUDA workload (H2D, real kernel, D2H, CPU parity) and
// frees every resource. The collective topology/semantics are SYNTHETIC: this
// proves the scheduler gates real accelerator work on a single device, not
// physical multi-GPU collective execution.
CudaWorkResult run_cuda_work(std::uint64_t seed, std::size_t elements);
}  // namespace collective_scheduler