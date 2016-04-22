#ifndef COMMON_H
#define COMMON_H

#include <x86intrin.h>
#include <unordered_map>
#include <ctime>
#include <sys/time.h>
#include <sys/resource.h>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <functional>
#include <iterator>
#include <algorithm>  // for std::find
#include <cstring>
#include <string>
#include <sys/mman.h>
#include <fcntl.h>    /* For O_RDWR */
#include <unistd.h>   /* For open(), creat() */
#include <math.h>
#include <unistd.h>
#include <tuple>
#include <cstdarg>
#include <set>
#include "assert.h"
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <vector>
#include "tbb/parallel_sort.h"

#ifdef NUM_THREADS_IN
static size_t NUM_THREADS = NUM_THREADS_IN;
static size_t get_num_threads(){return NUM_THREADS;}
#endif

const static size_t GIGABYTE = 1073741824;
//Needed for parallelization, prevents false sharing of cache lines
#define BLOCK_SIZE 64 //must be a multiple of 64
#define PADDING 300
#define MAX_THREADS 512
#define VECTORIZE 1

#define RELATION_DENSITY_THRESHOLD 0.8
#define VECTOR_DENSITY_THRESHOLD (1/128)
#define MIN_BITSET_LENGTH 2

//CONSTANTS THAT SHOULD NOT CHANGE
#define SHORTS_PER_REG 8
#define INTS_PER_REG 4
#define BYTES_PER_REG 16
#define BYTES_PER_CACHELINE 64

#define NUM_NUMA 4
#define SOCKET_THREADS 12
#define MAX_MEMORY 10 //GB

namespace type{
  enum file : uint8_t{
    csv = 0,
    tsv = 1,
    binary = 2
  };

  enum primitive: uint8_t{
    BOOL = 0,
    UINT32 = 1,
    UINT64 = 2,
    STRING = 3
  };

  enum layout: uint8_t {
    BITSET = 0,
    UINTEGER = 1
  };
}

#endif
