cmake_minimum_required(VERSION 3.24)
project(CollectiveScheduler VERSION 1.0.0 LANGUAGES CXX)

include(GNUInstallDirs)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(CS_ENABLE_CUDA "Build CUDA reference execution adapter and proofs" OFF)
option(CS_BUILD_TESTS "Build tests" ON)
option(CS_BUILD_BENCHMARKS "Build benchmarks" ON)
option(CS_BUILD_EXAMPLES "Build examples" ON)
option(CS_BUILD_TOOLS "Build CLI tools" ON)

set(CS_WARNINGS "")
if(MSVC)
  set(CS_WARNINGS /W4 /WX)
  # Apply strict warnings to C++; forward the same flags to the host compiler of
  # CUDA translation units so /W4 /WX (were meaningful) also apply there without
  # confusing the nvcc option parser.
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:/W4> $<$<COMPILE_LANGUAGE:CXX>:/WX> $<$<COMPILE_LANGUAGE:CXX>:/utf-8>)
  add_compile_options($<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=/W4> $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=/WX> $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=/utf-8> $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=/wd4211>)
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  add_compile_options(-Wall -Wextra -Wpedantic -Werror)
endif()

# Core library (CUDA-free).
set(CS_CORE_SOURCES
    src/conflict.cpp
    src/scheduler.cpp
    src/persistence.cpp
    src/serialize.cpp
    src/protocol.cpp
)

add_library(collective_scheduler STATIC ${CS_CORE_SOURCES})
add_library(CollectiveScheduler::CollectiveScheduler ALIAS collective_scheduler)
set_target_properties(collective_scheduler PROPERTIES EXPORT_NAME CollectiveScheduler)
target_include_directories(collective_scheduler
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_compile_features(collective_scheduler PUBLIC cxx_std_20)

if(MSVC)
  target_compile_options(collective_scheduler PRIVATE ${CS_WARNINGS})
  target_compile_options(collective_scheduler PRIVATE /utf-8)
  target_compile_options(collective_scheduler PRIVATE /bigobj)
endif()

# Reference multiprocess coordinator & worker (real TCP).
add_executable(cs_coordinator src/coordinator_main.cpp src/coordinator.cpp)
target_link_libraries(cs_coordinator PRIVATE CollectiveScheduler::CollectiveScheduler)
add_executable(cs_worker src/worker.cpp)
target_link_libraries(cs_worker PRIVATE CollectiveScheduler::CollectiveScheduler)
if(MSVC)
  target_compile_options(cs_coordinator PRIVATE /W4 /WX /utf-8)
  target_compile_options(cs_worker PRIVATE /W4 /WX /utf-8)
endif()

if(CS_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
if(CS_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()
if(CS_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
if(CS_BUILD_TOOLS)
  add_subdirectory(tools)
endif()

include(CMakePackageConfigHelpers)
install(TARGETS cs_coordinator cs_worker RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")

set(INSTALL_TARGETS collective_scheduler)
install(TARGETS ${INSTALL_TARGETS}
        EXPORT CollectiveSchedulerTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT CollectiveSchedulerTargets
        FILE CollectiveSchedulerTargets.cmake
        NAMESPACE CollectiveScheduler::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/CollectiveScheduler)

configure_package_config_file(cmake/CollectiveSchedulerConfig.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/CollectiveSchedulerConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/CollectiveScheduler)
write_basic_package_version_file("${CMAKE_CURRENT_BINARY_DIR}/CollectiveSchedulerConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/CollectiveSchedulerConfig.cmake"
              "${CMAKE_CURRENT_BINARY_DIR}/CollectiveSchedulerConfigVersion.cmake"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/CollectiveScheduler)

if(CS_ENABLE_CUDA)
  enable_language(CUDA)
  if(NOT DEFINED CMAKE_CUDA_ARCHITECTURES)
    set(CMAKE_CUDA_ARCHITECTURES 120)
  endif()
  set(CMAKE_CUDA_STANDARD 17)
  set(CMAKE_CUDA_STANDARD_REQUIRED ON)
  add_subdirectory(cuda)
endif()