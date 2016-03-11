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
#include <iterator>
#include <algorithm>  // for std::find
#include <cstring>
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

#define NUM_THREADS 1

const static size_t GIGABYTE = 1073741824;
//Needed for parallelization, prevents false sharing of cache lines
#define PADDING 300
#define MAX_THREADS 512
#define VECTORIZE 1

//CONSTANTS THAT SHOULD NOT CHANGE
#define SHORTS_PER_REG 8
#define INTS_PER_REG 4
#define BYTES_PER_REG 16
#define BYTES_PER_CACHELINE 64

#define NUM_NUMA 4
#define SOCKET_THREADS 12
#define MAX_MEMORY 10 //GB

#define BITS_PER_WORD 64
#define ADDRESS_BITS_PER_WORD 6
#define BYTES_PER_WORD 8

namespace common{
  static size_t bitset_length = 2;
  static double bitset_req = 128.0;//256.0;

  inline bool is_sparse(size_t length, size_t range) {
    if(length > bitset_length){
      const bool sparse = (((double)range/length) > (bitset_req));
      return sparse;
    }
    return true;
  }
}

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
    RANGE_BITSET = 0,
    UINTEGER = 1,
    HYBRID = 2,
    BLOCK_BITSET = 3,
    BLOCK = 4,
    NOT_VALID = 8

  };

}

#endif
