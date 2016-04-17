#ifndef _INTERSECT_BITSET_H_
#define _INTERSECT_BITSET_H_

namespace ops{

  struct BS_BS_VOID{
    template<class CA, class AA, class BA>
    static inline void compute_avx(
      CA * restrict c, 
      const AA * restrict a, 
      const BA * restrict b){
      (void) c; (void) a; (void)b;
      return;
    }
    template<class CA, class AA, class BA>
    static inline void compute_word(
      CA * const restrict c, 
      const AA * const restrict a, 
      const BA * const restrict b){
      (void) c; (void) a; (void)b;
      return;
    }
  };

  template<class AGG,class CA, class AA, class BA>
  inline void set_intersect_bitset(
      Meta *meta,
      uint64_t * const restrict C,
      CA * const restrict annoC, 
      const uint64_t * const restrict A,
      const AA * const restrict annoA,
      const uint64_t a_si,
      const uint64_t s_a,
      const uint64_t * const restrict B,
      const BA * const restrict annoB,
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

      AGG:: template compute_avx<CA,AA,BA>(annoC,annoA,annoB);

      i += 256;
    }  
    //64 bits per word
    for(; i < b_size; i+=64){
      const size_t vector_index = (i/BITS_PER_WORD);
      const uint64_t r = A[vector_index+a_start_index] & B[vector_index+b_start_index]; 
      count += _mm_popcnt_u64(r);      
      C[vector_index] = r;
      AGG:: template compute_word<CA,AA,BA>(annoC,annoA,annoB);
    }
    
    meta->start = start_index*BITS_PER_WORD;
    meta->end = (end_index*BITS_PER_WORD)-1;
    meta->cardinality = count;
    meta->type = type::BITSET;
  }

  template <class AGG,class A, class B, class C>
  inline Vector<EHVector,A,ParMemoryBuffer> bitset_bitset_intersect(
    const size_t tid,
    const size_t alloc_size,
    ParMemoryBuffer * restrict m,
    const Vector<EHVector,B,ParMemoryBuffer>& rare, 
    const Vector<EHVector,C,ParMemoryBuffer>& freq){

    const size_t index = m->get_offset(tid);
    uint8_t *buffer = m->get_next(tid,alloc_size);
    Meta* meta = new(buffer) Meta();
    uint64_t *out = (uint64_t*)(buffer+sizeof(Meta));

    if(rare.get_meta()->cardinality == 0 || freq.get_meta()->cardinality == 0
      || (rare.get_meta()->start > freq.get_meta()->end)
      || (freq.get_meta()->start > rare.get_meta()->end)){
      meta->cardinality = 0;
      meta->start = 0;
      meta->end = 0;
      meta->type = type::BITSET;
    } else {
      ops::set_intersect_bitset<AGG,A,B,C>(
        meta,
        out,
        (A*)NULL,
        (const uint64_t * const)freq.get_index_data(),
        (B*)NULL,
        BITSET::word_index(freq.get_meta()->start),
        BITSET::get_num_data_words(freq.get_meta()),
        (const uint64_t * const)rare.get_index_data(),
        (C*)NULL,
        BITSET::word_index(rare.get_meta()->start),
        BITSET::get_num_data_words(rare.get_meta()));
    } 
    BufferIndex bi;
    bi.tid = tid;
    bi.index = index;
    return Vector<EHVector,A,ParMemoryBuffer>(m,bi);
  }


}

#endif