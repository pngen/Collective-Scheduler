# Collective Scheduler

Collective Scheduler is an open-source, vendor-neutral C++20 runtime for scheduling competing collective communication operations across heterogeneous AI infrastructure using dependencies, priorities, deadlines, topology, congestion, reservations, fairness, and current execution authority.

**It answers one systems question:**

> Which collective operation should run now, which must wait, which may safely overlap, and which schedule remains authoritative as participants, dependencies, congestion, reservations, and infrastructure state change?

## Why it exists

A feasible collective communication plan does not answer **when** that collective should execute. Multiple collectives may simultaneously be individually valid while competing for accelerators, communication links, NICs, PCIe roots, host/pinned-memory staging, copy engines, collective participants, overlapping process groups, bandwidth reservations, dependent tensors, limited execution windows, latency-critical model phases, checkpoint/recovery traffic, higher-priority work, and current congestion headroom.

Collective Scheduler makes collective execution order and authority explicit. It distinguishes **collective definition → readiness → eligibility → conflict analysis → scheduling decision → grant → execution handoff → active execution → completion/failure → revalidation → supersession** and decides, deterministically, which competing collective may run now.

Collective Scheduler is **hard-correct first, authority-bound always, deterministic under equal authoritative inputs, fair over time, and explicit about uncertainty.**

## Systems boundary

A collective being feasible does not mean it should execute now. Collective Scheduler owns **temporal arbitration among competing collective operations.** It does not own the other runtimes' responsibilities:

- **Collective Fabric** owns collective execution semantics and real collective runtime behavior.
- **Communication Planner** constructs collective communication plans (routes, paths, stages).
- **Fabric Scheduler** places workloads.
- **Congestion Fabric** measures/governs live congestion and bounded local backpressure intent.
- **Reservation Fabric** owns advance commitments.
- **Capacity Fabric** owns capacity modeling and usable-capacity forecasts.
- **Resource Broker** owns actual scarce-resource claims.
- **Dependency Fabric** owns dependency readiness and invalidation.
- **Execution Fabric** owns execution-attempt authority.
- **Workload Fabric** owns workload lifecycle.
- **Preemption Fabric** owns safe interruption.
- Topology/NUMA/PCIe/fleet/runtime layers own physical facts.
- **SLO Fabric** owns global SLO contracts.
- **Interference Observatory** and **Contention Governor** own broader cross-workload interference analysis.

Collective Scheduler owns scheduling requests, schedule identity/generation, readiness evaluation using supplied dependency state, conflict detection, participant-overlap detection, communication-resource overlap detection, hard eligibility filtering, temporal ordering, deterministic priority arbitration, deadline-aware ordering, bounded overlap decisions, fairness, starvation prevention, priority-inversion handling, reservation-aware scheduling, congestion-aware scheduling, scheduling windows, grant/revoke semantics, active collective accounting, schedule revalidation, schedule supersession, fallback ordering, deterministic explanations, durable scheduling history, conservative recovery, and bounded handoff to Collective Fabric / execution adapters.

## Defining thesis

**Collective scheduling is not collective planning.** A communication plan describes *how* participants may exchange data; Collective Scheduler decides *when* competing plans may execute, which must serialize, which may overlap, and which generation-bound grant is still authoritative now.

## What is implemented

The runtime is a single `Scheduler` that arbitrates among submitted collectives. Key facets:

- **Strongly typed identities and generations.** Every semantic authority domain (Collective, Participant, Worker, WorkerBoot, Source, SourceBoot, CommunicationPlan, Dependency, Reservation, Congestion, Capacity, Priority, Policy, Fairness, CoordinatorEpoch, and many more) is a distinct, non-interchangeable C++ type carrying a `uint64` value. Generations never regress within a domain; a stale generation can never satisfy readiness, authorize a plan, protect bandwidth, or complete work.
- **Explicit collective request model** with participant sets + generations, payload, plan reference, dependencies, resource claims, reservation binding, priority, scheduling window (earliest/latest/deadline), overlap permission, fairness class, workload/execution authority, and provenance. Malformed requests (duplicate/empty participants, impossible windows, NaN/Inf, overflow, unknown kind, broken references) are rejected.
- **Participant model** binding identity/authority to worker boot, device, node, NIC, capability/health/topology generations, and readiness provenance. Participant liveness is not durable authority.
- **Readiness model**: READY, WAITING_DEPENDENCY, WAITING_PARTICIPANT, WAITING_COMMUNICATION_PLAN, WAITING_RESERVATION, WAITING_RESOURCE, WAITING_CAPACITY, BLOCKED_CONGESTION, BLOCKED_POLICY, REVALIDATION_REQUIRED, STALE, UNKNOWN. UNKNOWN is never treated as READY.
- **Hard eligibility filtering** before ranking: current collective/participant generations, participant availability, current plan validity, satisfied dependencies, valid reservation, capacity, congestion ceiling, scheduling-window validity, and execution authority. Rejections are explicit. A hard-ineligible collective never wins on priority, deadline, or fairness score.
- **First-class conflict model** (participant, resource, link, bandwidth, reservation, dependency, policy, UNKNOWN) with severity and provenance. UNKNOWN conflict evidence never authorizes overlap.
- **Bounded overlap model** (EXCLUSIVE, OVERLAP_ALLOWED, OVERLAP_LIMITED, SERIALIZE, UNKNOWN) with explicit overlap groups. Overlap is allowed only when constraints and resource evidence permit it.
- **Guarded lifecycle state machine** (DECLARED→WAITING→READY→ELIGIBLE→QUEUED→GRANTED→DISPATCHING→ACTIVE→DRAINING→COMPLETED/CANCELLED/FAILED/REVALIDATION_REQUIRED/SUPERSEDED/STALE/RETIRED). Illegal transitions (DECLARED→ACTIVE, COMPLETED→ACTIVE, SUPERSEDED→GRANTED, CANCELLED→COMPLETED) are rejected.
- **Deterministic arbitration** with named ranking factors (deadline urgency, priority, starvation/pressure, fairness deficit, age, expected duration, reservation fit, communication cost, resource footprint, congestion penalty, participant availability) combined into an explicit weighted scale, with a stable content-based (collective identity) tiebreak so insertion order cannot change the schedule. Determinism is property-tested.
- **Deadline / scheduling-window semantics**: deadline urgency orders eligible collectives; a missed deadline yields an explicit state, and deadline pressure never overrides stale authority, missing participants, hard dependencies, resource conflicts, or invalid plans.
- **Fairness and starvation prevention**: deficit-based accounting with aging, protected classes, and an explicit maximum-bypass guarantee that promotes an eligible collective after a bounded number of consecutive bypasses. Starvation prevention never violates hard safety.
- **Priority-inversion detection**: at most a lower-priority ACTIVE collective blocking a higher-priority eligible one yields a typed action (DRAIN_CURRENT / DEFER_NEW_CONFLICTING_WORK / REQUEST_PREEMPTION) emitted through the Preemption Fabric port; the scheduler never performs unsafe preemption. UNKNOWN priority evidence produces no aggressive action.
- **Grant semantics**: an explicit, generation-bound grant carrying evidence generations and revalidation requirements. Before handoff the grant is revalidated; a stale grant rejects and is never silently patched.
- **Completion authority**: completion must match collective identity/generation, grant generation, participant authority, WorkerBootId, CoordinatorEpoch, and current lifecycle. Duplicate, stale, post-cancellation, post-supersession, old-epoch, and stale-worker completions are rejected; exactly one terminal outcome survives.
- **Persistence**: a versioned binary format with magic, version, bounds-checked lengths, and an FNV-1a integrity checksum. Corrupt/truncated/oversized/invalid-enum/trailing-garbage state is rejected. Deterministic serialization never persists pointers, sockets, or process-local addresses as authority. Dynamic live evidence is not persisted.
- **Conservative recovery**: after a coordinator restart, durable history persists while dynamic evidence (participant readiness, plans, congestion, capacity, reservations, ACTIVE execution) is conservatively invalidated. Recovered GRANTED/ACTIVE work does not blindly remain executable; it requires fresh evidence and a fresh schedule generation.
- **Real multiprocess reference deployment**: a framed, checksummed, versioned TCP protocol (HELLO, REGISTER, PUBLISH_PARTICIPANT, SUBMIT_COLLECTIVE, UPDATE_PLAN/CONGESTION/RESERVATION/CAPACITY/DEPENDENCY, QUERY_SCHEDULE, GRANT, COMPLETE, FENCE_WORKER, SAVE, SHUTDOWN, ERROR) over real loopback TCP. A coordinator process + independent worker processes register with fresh WorkerBootIds, publish participants, submit collectives, receive grants, and complete with authoritative authority.
- **Adjacent-runtime ports**: narrow, pluggable interfaces for Resource Broker, Collective Fabric, and Preemption Fabric, implemented by reference adapters. No scheduler callback is ever invoked while the scheduler lock is held.

## Physical validation boundary

The current host has one physical NVIDIA RTX 5090 (compute capability 12.0, sm_120). There is no physical multi-GPU collective fabric, so **no physical NCCL/RCCL multi-GPU collective execution is claimed.**

The runtime is validated with **real CUDA to prove that Collective Scheduler gates real accelerator work**, while the multi-participant collective topology and semantics are **explicitly SYNTHETIC**. Two proofs run on the RTX 5090:

1. **Real CUDA schedule-gating proof** (`cuda/schedule_gating_proof`): two conflicting collective-like execution jobs; C1 receives a valid grant, C2 waits; only C1's grant reaches the CUDA execution adapter; C1 performs real CUDA work (cudaGetDeviceCount, cudaGetDeviceProperties, cudaMalloc, cudaMallocHost, H2D, real kernel, synchronize, D2H, CPU parity, cudaFree, cudaFreeHost); C1 completion under matching authority unlocks C2; C2 receives a fresh grant and performs real CUDA work; CPU parity passes for both; device memory returns to measured baseline.
2. **Integrated real CUDA worker-death / reincarnation scheduling proof** (`cuda/integrated_cuda_proof`): an independent CUDA worker process discovers the RTX 5090, publishes readiness, performs bounded real CUDA work, and completes with matching authority; the worker is killed as a real OS process, its WorkerBootId is fenced, its participant readiness becomes stale, and a stale replay is rejected; a fresh worker process reincarnates with a fresh PID/WorkerBootId, republishes, and the scheduler generates a fresh schedule/grant for a new collective that performs real CUDA work. The device memory returns to baseline and old boot/grant/completion evidence remains permanently rejected.

Both proofs demonstrate real accelerator work is schedule-gated. They do not demonstrate physical multi-GPU collective execution.

## Building

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build
```

With CUDA reference proofs (on a host with an NVIDIA GPU and CUDA toolkit):

```bash
cmake -S . -B build-cuda -G Ninja -DCMAKE_BUILD_TYPE=Release -DCS_ENABLE_CUDA=ON -DCMAKE_CUDA_ARCHITECTURES=120
cmake --build build-cuda
ctest --test-dir build-cuda
```

On MSVC, first-party C++ compiles with `/W4 /WX`; Debug and Release both build with zero first-party warnings. The CUDA host compilation uses the strict warnings forwarded to the nvcc host compiler (the single nvcc-generated stub warning, C4211, is explicitly suppressed because it is not first-party code).

## Packaging

The project installs an exported CMake target `CollectiveScheduler::CollectiveScheduler` with a package config/version. A downstream consumer uses:

```cmake
find_package(CollectiveScheduler CONFIG REQUIRED)
target_link_libraries(app PRIVATE CollectiveScheduler::CollectiveScheduler)
```

An independent downstream consumer is validated with `find_package` against the installed package (see `tools/consumer`).

## Testing

The suite covers request validation, readiness, conflict, overlap, deterministic scheduling, insertion-order permutations, deadlines, fairness/starvation, priority inversion, lifecycle legality, plan/congestion/reservation/dependency/cancellation/supersession/epoch races, persistence/corruption/recovery, property-based randomized invariants, genuine multithreaded concurrency, adversarial stale-authority and malformed-input rejection, protocol framing/malformed-frame rejection, the real multiprocess proof, and the real CUDA schedule-gating and integrated CUDA worker-death proofs.

No test uses a wall-clock timeout to pass; the suite runs naturally. Deliberate worker-kill and process-restart are scenario stimuli.

## License

Apache License 2.0. Copyright 2026 Summon Software Labs. No telemetry transmission.