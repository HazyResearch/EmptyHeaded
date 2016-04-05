#ifndef _INTERSECT_DENSE_H_
#define _INTERSECT_DENSE_H_

#include "vector/DenseVector.hpp"
#include "intersect_bitset.hpp"

namespace ops{
  /*

  Interesection should allocate the output vector ahead of time. 
  This means also the annotations which will be filled in.

  Intersections should be optimized for mutliplying aggregations.
  While scanning over the indices we should also be streaming the 
  aggregations and filling them in (possibly aggregating them).
  
  Dense intersections actually just multiply vectors.
  */

  template <class A, class B, class C>
  inline Vector<DenseVector,A,ParMemoryBuffer> alloc_and_intersect(
    const size_t tid,
    const size_t alloc_size,
    ParMemoryBuffer * restrict m,
    const Vector<DenseVector,B,ParMemoryBuffer>& rare, 
    const Vector<DenseVector,C,ParMemoryBuffer>& freq){

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
      ops::set_intersect_bitset(
        meta,
        out,
        (const uint64_t * const)freq.get_data(),
        BITSET::word_index(freq.get_meta()->start),
        BITSET::get_num_data_words(freq.get_meta()),
        (const uint64_t * const)rare.get_data(),
        BITSET::word_index(rare.get_meta()->start),
        BITSET::get_num_data_words(rare.get_meta()));
    } 
    BufferIndex bi;
    bi.tid = tid;
    bi.index = index;
    return Vector<DenseVector,A,ParMemoryBuffer>(m,bi);
  }

  template <class A, class B>
  inline Vector<DenseVector,void*,ParMemoryBuffer> agg_intersect(
    const size_t tid,
    ParMemoryBuffer * restrict m,
    const Vector<DenseVector,A,ParMemoryBuffer>& rare, 
    const Vector<DenseVector,B,ParMemoryBuffer>& freq){

    //run intersection.
    const size_t alloc_size = 
       std::min(rare.get_num_bytes(),freq.get_num_bytes());
    
    Vector<DenseVector,void*,ParMemoryBuffer> result = 
      alloc_and_intersect<void*,A,B>(tid,alloc_size,m,rare,freq);
    
    m->roll_back(tid,alloc_size);
    return result;
  }

  //Materilize (allocate memory & run intersection)
  inline float agg_intersect(
    const size_t tid,
    ParMemoryBuffer * restrict m,
    const Vector<DenseVector,float,ParMemoryBuffer>& rare, 
    const Vector<DenseVector,float,ParMemoryBuffer>& freq){

    //run intersection.
    const size_t alloc_size = 
       std::min(rare.get_num_bytes(),freq.get_num_bytes());
    
    Vector<DenseVector,float,ParMemoryBuffer> result = 
      alloc_and_intersect<float,float,float>(tid,alloc_size,m,rare,freq);

    float anno = 0.0;      
    result.foreach_index([&](const uint32_t index, const uint32_t data){
      (void) index;
      //have some field set in vector see if it is annotated or not,
      //use default field if set otherwise actually lookup the annotation.
      anno += rare.get(data)*freq.get(data);
    });

    m->roll_back(tid,alloc_size);
    return anno;
  }

  //Materilize (allocate memory & run intersection)
  inline float simd_agg_intersect(
    const size_t tid,
    ParMemoryBuffer * restrict m,
    const Vector<DenseVector,float,ParMemoryBuffer>& rare, 
    const Vector<DenseVector,float,ParMemoryBuffer>& freq){

    //run intersection.
    const size_t alloc_size = 
       std::min(rare.get_num_bytes(),freq.get_num_bytes());
       
    Vector<DenseVector,float,ParMemoryBuffer> result = 
      alloc_and_intersect<float,float,float>(tid,alloc_size,m,rare,freq);

    const size_t elems_per_reg = 8;
    const size_t num_avx_per_block = std::max((int)8,(int)(BLOCK_SIZE/elems_per_reg));

    __m256 r = _mm256_set1_ps(0.0f);
    const float * const restrict rare_anno = (float*)rare.get_annotation();
    const float * const restrict freq_anno = (float*)freq.get_annotation();
    //std::cout << "NUM AVX PER BLOCK: " << num_avx_per_block << std::endl;
    for(size_t i = 0; i < num_avx_per_block; i++){
      //std::cout << rare_anno[i*elems_per_reg] << " " << freq_anno[i*elems_per_reg] << std::endl;
      const __m256 m_b_1 = _mm256_loadu_ps(&rare_anno[i*elems_per_reg]);
      const __m256 m_b_2 = _mm256_loadu_ps(&freq_anno[i*elems_per_reg]);

      r = _mm256_fmadd_ps(m_b_1,m_b_2,r);
    }
    const float anno = utils::_mm256_reduce_add_ps(r);

    m->roll_back(tid,alloc_size);

    return anno;
  }
}
#endif