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
  
  //Thanks stack overflow.
  static inline float _mm256_reduce_add_ps(__m256 x) {
    /* ( x3+x7, x2+x6, x1+x5, x0+x4 ) */
    const int imm = 1;
    const __m128 x128 = _mm_add_ps(_mm256_extractf128_ps(x, imm), _mm256_castps256_ps128(x));
    /* ( -, -, x1+x3+x5+x7, x0+x2+x4+x6 ) */
    const __m128 x64 = _mm_add_ps(x128, _mm_movehl_ps(x128, x128));
    /* ( -, -, -, x0+x1+x2+x3+x4+x5+x6+x7 ) */
    const __m128 x32 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
    /* Conversion to float is a no-op on x86-64 */
    return _mm_cvtss_f32(x32);
  }
}
#endif
