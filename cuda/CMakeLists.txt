# CUDA reference execution adapter and proof executables.
if(NOT CMAKE_CUDA_COMPILER AND DEFINED CMAKE_CUDA_COMPILER)
endif()
set(CMAKE_CUDA_STANDARD 17)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)
if(NOT DEFINED CMAKE_CUDA_ARCHITECTURES)
  set(CMAKE_CUDA_ARCHITECTURES 120)
endif()

add_library(cuda_reference STATIC cuda_executor.cu)
target_include_directories(cuda_reference PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(cuda_schedule_gating schedule_gating_proof.cpp)
target_link_libraries(cuda_schedule_gating PRIVATE CollectiveScheduler::CollectiveScheduler cuda_reference)
target_include_directories(cuda_schedule_gating PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(cuda_integrated integrated_cuda_proof.cpp)
target_link_libraries(cuda_integrated PRIVATE CollectiveScheduler::CollectiveScheduler cuda_reference)
target_include_directories(cuda_integrated PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(cuda_worker cuda_worker.cpp)
target_link_libraries(cuda_worker PRIVATE CollectiveScheduler::CollectiveScheduler cuda_reference)
target_include_directories(cuda_worker PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

if(MSVC)
  foreach(tgt cuda_schedule_gating cuda_integrated cuda_worker)
    target_compile_options(${tgt} PRIVATE /W4 /WX)
  endforeach()
endif()

add_test(NAME cs_cuda_schedule_gating COMMAND cuda_schedule_gating)
add_test(NAME cs_cuda_integrated COMMAND cuda_integrated "$<TARGET_FILE_DIR:cs_coordinator>")

install(TARGETS cuda_worker RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")