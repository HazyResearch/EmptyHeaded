#ifndef DEBUG_H
#define DEBUG_H

#include <chrono>
#include "common.hpp"

namespace debug{
  static void allocateStack(){
    const rlim_t kStackSize = 64L * 1024L * 1024L;   // min stack size = 64 Mb
    struct rlimit rl;
    int result;

    result = getrlimit(RLIMIT_STACK, &rl);
    if (result == 0){
      if (rl.rlim_cur < kStackSize){
        rl.rlim_cur = kStackSize;
        result = setrlimit(RLIMIT_STACK, &rl);
        if (result != 0){
          fprintf(stderr, "setrlimit returned result = %d\n", result);
        }
      }
    }
  }

  static void _mm256_print_ps(__m256 x) {
    float *data = new float[8];
    _mm256_storeu_ps(&data[0],x);
    for(size_t i =0 ; i < 8; i++){
      std::cout << "Data[" << i << "]: " << data[i] << std::endl;
    }
    delete[] data;
  }
  static void _mm128i_print_shorts(__m128i x) {
    std::cout << "Data[" << 0 << "]: " << _mm_extract_epi16(x,0) << std::endl;
    std::cout << "Data[" << 1 << "]: " << _mm_extract_epi16(x,1) << std::endl;
    std::cout << "Data[" << 2 << "]: " << _mm_extract_epi16(x,2) << std::endl;
    std::cout << "Data[" << 3 << "]: " << _mm_extract_epi16(x,3) << std::endl;
    std::cout << "Data[" << 4 << "]: " << _mm_extract_epi16(x,4) << std::endl;
    std::cout << "Data[" << 5 << "]: " << _mm_extract_epi16(x,5) << std::endl;
    std::cout << "Data[" << 6 << "]: " << _mm_extract_epi16(x,6) << std::endl;
    std::cout << "Data[" << 7 << "]: " << _mm_extract_epi16(x,7) << std::endl;
  }
  static void _mm128i_print(__m128i x) {
    std::cout << "Data[" << 0 << "]: " << _mm_extract_epi32(x,0) << std::endl;
    std::cout << "Data[" << 1 << "]: " << _mm_extract_epi32(x,1) << std::endl;
    std::cout << "Data[" << 2 << "]: " << _mm_extract_epi32(x,2) << std::endl;
    std::cout << "Data[" << 3 << "]: " << _mm_extract_epi32(x,3) << std::endl;
  }
  static void _mm256i_print(__m256i x) {
    int *data = new int[8];
    _mm256_storeu_si256((__m256i*)&data[0],x);
    for(size_t i =0 ; i < 8; i++){
      std::cout << "Data[" << i << "]: " << data[i] << std::endl;
    }
    delete[] data;
  }
}
#endif
