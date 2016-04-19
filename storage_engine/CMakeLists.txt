cmake_minimum_required(VERSION 2.8)

project(emptyheaded)

include_directories(include)

set (CMAKE_CXX_FLAGS "-mavx2 -fopenmp -lmkl_intel_lp64 -lmkl_intel_thread -lmkl_core -fPIC -std=c++0x -pedantic -O3 -Wall -Wno-unused-function -Wextra -march=native -mtune=native -Drestrict=__restrict__ -DNUM_THREADS_IN=${NUM_THREADS}")

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

link_libraries(jemalloc tbb pthread iomp5 mkl_intel_lp64 mkl_core mkl_intel_thread)
subdirs(src)
subdirs(apps)
subdirs(test)
