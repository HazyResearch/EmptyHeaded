#ifndef _INTERSECT_BITSET_H_
#define _INTERSECT_BITSET_H_

namespace ops{

  template<class CA>
  struct BS_BS_VOID{
    inline void init(){}
    inline void compute_avx(
      CA * restrict c,
      const size_t c_offset, 
      const CA * restrict a,
      const size_t a_offset, 
      const CA * restrict b,
      const size_t b_offset){
      (void) c; (void) a; (void)b;
      (void) c_offset; (void) a_offset; (void) b_offset;
      return;
    }
    inline void compute_word(
      CA * restrict c,
      const size_t c_offset, 
      const CA * restrict a,
      const size_t a_offset, 
      const CA * restrict b,
      const size_t b_offset){
      (void) c; (void) a; (void)b;
      (void) c_offset; (void) a_offset; (void) b_offset;
      return;
    }
    inline CA finish(CA * restrict c){(void)c; return (CA)NULL;}
  };

  template<class CA>
  struct BS_BS_SUM{
    __m256 r;
    float result;
    inline void init(){}
    inline void compute_avx(
      CA * restrict c,
      const size_t c_offset, 
      const CA * restrict a,
      const size_t a_offset, 
      const CA * restrict b,
      const size_t b_offset){
      (void) c; (void) a; (void)b;
      (void) c_offset; (void) a_offset; (void) b_offset;
      return;
    }
    inline void compute_word(
      CA * restrict c,
      const size_t c_offset, 
      const CA * restrict a,
      const size_t a_offset, 
      const CA * restrict b,
      const size_t b_offset){
      (void) c; (void) a; (void)b;
      (void) c_offset; (void) a_offset; (void) b_offset;
      return;
    }
    inline CA finish(CA * restrict c){(void)c; return (CA)NULL;}
  };

  template<>
  void BS_BS_SUM<float>::init(){
    r = _mm256_set1_ps(0.0f);
  }

  template<>
  void BS_BS_SUM<float>::compute_avx(
    float * restrict c,
    const size_t c_offset, 
    const float * restrict a,
    const size_t a_offset, 
    const float * restrict b,
    const size_t b_offset){
    (void) c; (void) c_offset;
    const size_t blocks_per_reg = (256/8);
    for(size_t i = 0; i < blocks_per_reg; i++){
      const __m256 m_b_1 = _mm256_loadu_ps(&a[i*8+a_offset]);
      const __m256 m_b_2 = _mm256_loadu_ps(&b[i*8+b_offset]);
      r = _mm256_fmadd_ps(m_b_1,m_b_2,r);
    }
  }

  template<>
  inline void BS_BS_SUM<float>::compute_word(
    float * restrict c,
    const size_t c_offset, 
    const float * restrict a,
    const size_t a_offset, 
    const float * restrict b,
    const size_t b_offset){
    (void) c; (void) c_offset;
    const size_t blocks_per_reg = (64/8);
    for(size_t i = 0; i < blocks_per_reg; i++){
      const __m256 m_b_1 = _mm256_loadu_ps(&a[i*8+a_offset]);
      const __m256 m_b_2 = _mm256_loadu_ps(&b[i*8+b_offset]);
      r = _mm256_fmadd_ps(m_b_1,m_b_2,r);
    }
  }

  template<>
  inline float BS_BS_SUM<float>::finish(float * restrict c){
    (void) c;
    return utils::_mm256_reduce_add_ps(r);
  }

  template<class AGG,class CA, class AA, class BA>
  inline CA set_intersect_bitset(
      Meta *meta,
      uint64_t * const restrict C,
      CA * const restrict annoC, 
      const uint64_t * const restrict A,
      const CA * const restrict annoA,
      const uint64_t a_si,
      const uint64_t s_a,
      const uint64_t * const restrict B,
      const CA * const restrict annoB,
      const uint64_t b_si,
      const uint64_t s_b
    ){
    size_t count = 0;

    AGG agg;
    agg.init();
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

      agg.compute_avx(
        annoC,
        vector_index,
        annoA,
        vector_index+a_start_index,
        annoB,
        vector_index+b_start_index);

      i += 256;
    }  
    //64 bits per word
    for(; i < b_size; i+=64){
      const size_t vector_index = (i/BITS_PER_WORD);
      const uint64_t r = A[vector_index+a_start_index] & B[vector_index+b_start_index]; 
      count += _mm_popcnt_u64(r);      
      C[vector_index] = r;

      agg.compute_word(
        annoC,
        vector_index,
        annoA,
        vector_index+a_start_index,
        annoB,
        vector_index+b_start_index);
    }

    meta->start = start_index*BITS_PER_WORD;
    meta->end = (end_index*BITS_PER_WORD)-1;
    meta->cardinality = count;
    meta->type = type::BITSET;

    return agg.finish(annoC);
  }

  template <class AGG, class A, class BV, class B, class CV, class C>
  inline Vector<EHVector,A,ParMemoryBuffer> bitset_bitset_intersect(
    const size_t tid,
    const size_t alloc_size,
    ParMemoryBuffer * restrict m,
    const Vector<BV,B,ParMemoryBuffer>& rare, 
    const Vector<CV,C,ParMemoryBuffer>& freq){

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
        (A*)NULL,
        BITSET::word_index(freq.get_meta()->start),
        BITSET::get_num_data_words(freq.get_meta()),
        (const uint64_t * const)rare.get_index_data(),
        (A*)NULL,
        BITSET::word_index(rare.get_meta()->start),
        BITSET::get_num_data_words(rare.get_meta()));
    } 
    BufferIndex bi;
    bi.tid = tid;
    bi.index = index;
    return Vector<EHVector,A,ParMemoryBuffer>(m,bi);
  }

  template <class AGG,class A>
  inline A agg_bitset_bitset_intersect(
    const size_t tid,
    const size_t alloc_size,
    ParMemoryBuffer * restrict m,
    const Vector<BLASVector,A,ParMemoryBuffer>& rare, 
    const Vector<BLASVector,A,ParMemoryBuffer>& freq){

    const size_t index = m->get_offset(tid);
    uint8_t *buffer = m->get_next(tid,alloc_size);
    Meta* meta = new(buffer) Meta();
    uint64_t *out = (uint64_t*)(buffer+sizeof(Meta));

    A anno_result = (A)0;

    if(rare.get_meta()->cardinality == 0 || freq.get_meta()->cardinality == 0
      || (rare.get_meta()->start > freq.get_meta()->end)
      || (freq.get_meta()->start > rare.get_meta()->end)){
      meta->cardinality = 0;
      meta->start = 0;
      meta->end = 0;
      meta->type = type::BITSET;
    } else {
      anno_result = ops::set_intersect_bitset<AGG,A,A,A>(
        meta,
        out,
        &anno_result,//FIXME->figure out where annotation is
        (const uint64_t * const)freq.get_index_data(),
        freq.get_annotation(),
        BITSET::word_index(freq.get_meta()->start),
        BITSET::get_num_data_words(freq.get_meta()),
        (const uint64_t * const)rare.get_index_data(),
        rare.get_annotation(),
        BITSET::word_index(rare.get_meta()->start),
        BITSET::get_num_data_words(rare.get_meta()));
    } 
    BufferIndex bi;
    bi.tid = tid;
    bi.index = index;
    return anno_result;
  }


}

#endif