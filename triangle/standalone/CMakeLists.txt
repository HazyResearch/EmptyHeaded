cmake_minimum_required(VERSION 3.16)
project(emptyheaded_triangle_runner LANGUAGES CXX)

get_filename_component(DEFAULT_EH_ROOT "${CMAKE_CURRENT_LIST_DIR}/../.." ABSOLUTE)
set(EMPTYHEADED_ROOT "${DEFAULT_EH_ROOT}" CACHE PATH
    "Root of the historical EmptyHeaded source tree")
set(EMPTYHEADED_GENERATED_DIR
    "${EMPTYHEADED_ROOT}/triangle/generated/g16" CACHE PATH
    "Directory containing the portable generated run_0.hpp")
set(EMPTYHEADED_DATABASE_ROOT
    "${EMPTYHEADED_GENERATED_DIR}/database" CACHE PATH
    "Directory containing EmptyHeaded relations and encodings")
set(EMPTYHEADED_NUM_THREADS "16" CACHE STRING
    "Compile-time EmptyHeaded query worker count (independent of persisted trie partitions)")
option(EMPTYHEADED_CYCLIC_OUTER
       "Assign generated outer-domain keys cyclically to worker slots" OFF)
set(EXPECTED_TRIANGLE_COUNT "435586" CACHE STRING
    "Expected oriented triangle count")
set(EMPTYHEADED_TBB_INCLUDE_DIR "" CACHE PATH
    "Optional oneTBB include directory")
set(EMPTYHEADED_TBB_LIBRARY_DIR "" CACHE PATH
    "Optional directory containing libtbb")

if(NOT EXISTS "${EMPTYHEADED_GENERATED_DIR}/run_0.hpp")
  message(FATAL_ERROR "run_0.hpp not found in ${EMPTYHEADED_GENERATED_DIR}")
endif()
if(NOT EXISTS "${EMPTYHEADED_DATABASE_ROOT}/relations")
  message(FATAL_ERROR
          "EmptyHeaded database not found in ${EMPTYHEADED_DATABASE_ROOT}")
endif()

find_path(TBB_INCLUDE_DIR tbb/parallel_sort.h
  HINTS "${EMPTYHEADED_TBB_INCLUDE_DIR}")
find_library(TBB_LIBRARY NAMES tbb
  HINTS "${EMPTYHEADED_TBB_LIBRARY_DIR}")
if(NOT TBB_INCLUDE_DIR OR NOT TBB_LIBRARY)
  message(FATAL_ERROR
          "oneTBB headers/library not found; set EMPTYHEADED_TBB_INCLUDE_DIR "
          "and EMPTYHEADED_TBB_LIBRARY_DIR")
endif()

find_package(Threads REQUIRED)

set(STORAGE_ROOT "${EMPTYHEADED_ROOT}/storage_engine")
set(STORAGE_SOURCES
  "${STORAGE_ROOT}/src/utils/thread_pool.cpp"
  "${STORAGE_ROOT}/src/utils/parallel.cpp"
  "${STORAGE_ROOT}/src/utils/MemoryBuffer.cpp"
  "${STORAGE_ROOT}/src/utils/MMapBuffer.cpp"
  "${STORAGE_ROOT}/src/utils/ParMemoryBuffer.cpp"
  "${STORAGE_ROOT}/src/utils/ParMMapBuffer.cpp"
  "${STORAGE_ROOT}/src/Encoding.cpp"
  "${STORAGE_ROOT}/src/Trie.cpp"
  "${STORAGE_ROOT}/src/TrieBuilder.cpp"
  "${STORAGE_ROOT}/src/TrieIterator.cpp")

add_executable(eh_triangle_runner triangle_runner.cpp ${STORAGE_SOURCES})
target_compile_features(eh_triangle_runner PRIVATE cxx_std_14)
target_compile_options(eh_triangle_runner PRIVATE
  -O3 -mavx2 -march=native -mtune=native
  -fno-strict-aliasing -fwrapv -Wall -Wextra -Wno-unused-function)
target_compile_definitions(eh_triangle_runner PRIVATE
  NDEBUG
  "NUM_THREADS_IN=${EMPTYHEADED_NUM_THREADS}"
  "EXPECTED_TRIANGLE_COUNT=${EXPECTED_TRIANGLE_COUNT}"
  "EMPTYHEADED_DATABASE_ROOT=\"${EMPTYHEADED_DATABASE_ROOT}\"")
if(EMPTYHEADED_CYCLIC_OUTER)
  target_compile_definitions(eh_triangle_runner PRIVATE
    EMPTYHEADED_CYCLIC_OUTER=1)
endif()
target_include_directories(eh_triangle_runner PRIVATE
  "${EMPTYHEADED_ROOT}/triangle/include"
  "${EMPTYHEADED_GENERATED_DIR}"
  "${STORAGE_ROOT}/include"
  "${TBB_INCLUDE_DIR}")
target_link_libraries(eh_triangle_runner PRIVATE
  "${TBB_LIBRARY}" Threads::Threads)
