cmake_minimum_required(VERSION 2.8)

project(emptyheaded)

include_directories(include)

if (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
	SET(CMAKE_C_COMPILER gcc-5)
	SET(CMAKE_CXX_COMPILER g++-5)
endif()

set (CMAKE_CXX_FLAGS "-mavx2 -std=c++0x -pedantic -O3 -Wall -Wno-unused-function -Wextra -march=native -mtune=native -DNUM_THREADS_IN=${NUM_THREADS}")

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

link_libraries(jemalloc tbb pthread)
subdirs(src)
subdirs(apps)
