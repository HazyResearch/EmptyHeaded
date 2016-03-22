#ifndef _INTERSECT_BITSET_H_
#define _INTERSECT_BITSET_H_

namespace ops{

  inline void set_intersect_bitset(
      Meta *meta,
      uint64_t * const restrict C, 
      const uint64_t * const restrict A,
      const uint64_t a_si,
      const uint64_t s_a,
      const uint64_t * const restrict B,
      const uint64_t b_si,
      const uint64_t s_b
    ){
    size_t count = 0;

    const bool a_big = a_si > b_si;
    const uint64_t start_index = (a_big) ? a_si : b_si;
    const uint64_t a_start_index = (a_big) ? 0:(b_si-a_si);
    const uint64_t b_start_index = (a_big) ? (a_si-b_si):0;

    const uint64_t end_index = ((a_si+s_a) > (b_si+s_b)) ? (b_si+s_b):(a_si+s_a);
    const uint64_t total_size = (start_index > end_index) ? 0:(end_index-start_index);

    const size_t b_size = total_size*64;
    size_t i = 0;
    while((i+255) < b_size){
      const size_t vector_index = (i/BITS_PER_WORD);
      const __m256 m1 = _mm256_loadu_ps((float*)(A + a_start_index + vector_index));
      const __m256 m2 = _mm256_loadu_ps((float*)(B + b_start_index + vector_index));
      const __m256 r = _mm256_and_ps(m1, m2);
    
      _mm256_storeu_ps((float*)(C+vector_index), r);
      
      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,0));
      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,1));
      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,2));
      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,3));

      i += 256;
    }  
    //64 bits per word
    for(; i < b_size; i+=64){
      const size_t vector_index = (i/BITS_PER_WORD);
      const uint64_t r = A[vector_index+a_start_index] & B[vector_index+b_start_index]; 
      count += _mm_popcnt_u64(r);      
      C[vector_index] = r;
    }
    
    meta->start = start_index*BITS_PER_WORD;
    meta->end = (end_index+1)*BITS_PER_WORD;
    meta->cardinality = count;
    meta->type = type::BITSET;
  }
}

#endif