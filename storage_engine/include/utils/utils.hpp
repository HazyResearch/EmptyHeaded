/******************************************************************************
*
* Author: Christopher R. Aberger
*
* Some common utility functions.
******************************************************************************/

#ifndef _UTILS_H
#define _UTILS_H

#include "timer.hpp"
#include "io.hpp"
#include "parallel.hpp"
#include "thread_pool.hpp"
#include "debug.hpp"
#include "common.hpp"
#include "ParMMapBuffer.hpp"
#include "ParMemoryBuffer.hpp"

namespace utils {

  template<class A>
  inline A zero(){
    return (A)0;
  }

  template<>
  inline BufferIndex zero(){
    return BufferIndex(0,0);
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

  //Look for a key, pass in a pointer to an array and a start and end index.
  inline long binary_search(const uint32_t * const data, size_t first, size_t last, uint32_t search_key){
   long index;
   if (first > last){
    index = -1;
   } else{
    size_t mid = (last+first)/2;
    if (search_key == data[mid])
      index = mid;
    else{
      if (search_key < data[mid]){
        if(mid == 0)
          index = -1;
        else
          index = binary_search(data,first,mid-1,search_key);
      }
      else
        index = binary_search(data,mid+1,last,search_key);
    }
   } // end if
   return index;
  } // end binarySearch
}

#endif
