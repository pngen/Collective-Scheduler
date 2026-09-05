// Collective Scheduler — CUDA reference execution adapter (implementation).
// The kernel / topology semantics are explicitly SYNTHETIC (single device).
#include "cuda_executor.hpp"
#include <cuda_runtime.h>

namespace collective_scheduler {
namespace {
__global__ void cs_axpy_kernel(float* out, const float* in, float a, int n) {
  int i = blockIdx.x * blockDim.x + threadIdx.x;
  if (i < n) out[i] = a * in[i] + 1.0f;
}
std::size_t used_bytes() {
  std::size_t freeb = 0, total = 0;
  if (cudaMemGetInfo(&freeb, &total) != cudaSuccess) return total;
  return total > freeb ? (total - freeb) : 0;
}
}  // namespace

CudaWorkResult run_cuda_work(std::uint64_t seed, std::size_t elements) {
  CudaWorkResult r;
  const std::size_t bytes = elements * sizeof(float);
  int dev = 0;
  if (cudaGetDeviceCount(&dev) == cudaSuccess) r.device_count = dev;
  cudaDeviceProp prop;
  if (cudaGetDeviceProperties(&prop, 0) == cudaSuccess) { r.cc_major = prop.major; r.cc_minor = prop.minor; }
  r.total_memory_before = used_bytes();
  std::size_t freeb = 0, total = 0; if (cudaMemGetInfo(&freeb, &total) == cudaSuccess) r.free_memory_before = freeb;

  float* h_in = nullptr; float* h_out = nullptr;
  float* d_in = nullptr; float* d_out = nullptr;
  bool ok_allocs = cudaMallocHost(&h_in, bytes) == cudaSuccess && cudaMallocHost(&h_out, bytes) == cudaSuccess &&
                      cudaMalloc(&d_in, bytes) == cudaSuccess && cudaMalloc(&d_out, bytes) == cudaSuccess;
  if (ok_allocs) {
    for (std::size_t i = 0; i < elements; ++i) h_in[i] = static_cast<float>((seed + i * 2654435761ULL) % 1000);
    cudaMemcpy(d_in, h_in, bytes, cudaMemcpyHostToDevice);
    int n = static_cast<int>(elements);
    cs_axpy_kernel<<<(n + 255) / 256, 256>>>(d_out, d_in, 2.0f, n);
    cudaDeviceSynchronize();
    cudaMemcpy(h_out, d_out, bytes, cudaMemcpyDeviceToHost);

    std::uint64_t outsum = 0, parsum = 0;
    for (std::size_t i = 0; i < elements; ++i) { outsum += static_cast<std::uint64_t>(h_out[i]); parsum += static_cast<std::uint64_t>(2.0f * h_in[i] + 1.0f); }
    r.kernel_sum = outsum;
    r.parity_ok = (outsum == parsum);

    cudaFree(d_in); cudaFree(d_out); cudaFreeHost(h_in); cudaFreeHost(h_out);
  }
  cudaDeviceSynchronize();
  r.total_memory_after = used_bytes();
  if (cudaMemGetInfo(&freeb, &total) == cudaSuccess) r.free_memory_after = freeb;
  return r;
}

}  // namespace collective_scheduler