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
  add_compile_options(${CS_WARNINGS})
  add_compile_options(/utf-8)
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  add_compile_options(-Wall -Wextra -Wpedantic -Werror)
endif()

# Core library (CUDA-free).
set(CS_CORE_SOURCES
    src/conflict.cpp
    src/scheduler.cpp
    src/persistence.cpp
)

add_library(collective_scheduler STATIC ${CS_CORE_SOURCES})
add_library(CollectiveScheduler::CollectiveScheduler ALIAS collective_scheduler)
target_include_directories(collective_scheduler
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_compile_features(collective_scheduler PUBLIC cxx_std_20)

if(MSVC)
  target_compile_options(collective_scheduler PRIVATE ${CS_WARNINGS})
  target_compile_options(collective_scheduler PRIVATE /utf-8)
  target_compile_options(collective_scheduler PRIVATE /bigobj)
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
  add_subdirectory(cuda)
endif()