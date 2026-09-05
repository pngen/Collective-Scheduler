file(GLOB CS_TEST_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/test_*.cpp")
add_executable(cs_tests ${CS_TEST_SOURCES})
target_link_libraries(cs_tests PRIVATE CollectiveScheduler::CollectiveScheduler)
target_include_directories(cs_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
if(MSVC)
  target_compile_options(cs_tests PRIVATE /W4 /WX /utf-8 /bigobj)
endif()
add_test(NAME cs_tests COMMAND cs_tests)

# Per-area aliases registered as individual CTest tests that call the runner with a filter.
set(CS_TEST_AREAS basic readiness conflict overlap determinism deadline fairness starvation priority_inversion lifecycle grant plan_invalidation congestion reservation dependency cancellation supersession persistence property concurrency adversarial multiprocess cuda worker_death coordinator_restart distribution hygiene)
foreach(area ${CS_TEST_AREAS})
  add_test(NAME cs_${area} COMMAND cs_tests ${area})
endforeach()

# Real multiprocess proof (coordinator + workers over real TCP).
add_executable(multiprocess_proof multiprocess_proof.cpp)
target_link_libraries(multiprocess_proof PRIVATE CollectiveScheduler::CollectiveScheduler)
if(MSVC)
  target_compile_options(multiprocess_proof PRIVATE /W4 /WX /utf-8)
endif()
add_test(NAME cs_mp_proof COMMAND multiprocess_proof "$<TARGET_FILE_DIR:cs_coordinator>")