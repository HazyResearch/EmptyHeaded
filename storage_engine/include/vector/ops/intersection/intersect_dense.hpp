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

    if(rare.meta->cardinality == 0 || freq.meta->cardinality == 0
      || (rare.meta->start > freq.meta->end)
      || (freq.meta->start > rare.meta->end)){
      meta->cardinality = 0;
      meta->start = 0;
      meta->end = 0;
      meta->type = type::BITSET;
    } else {
      ops::set_intersect_bitset(
        meta,
        out,
        (const uint64_t * const)freq.get_data(),
        BITSET::word_index(freq.meta->start),
        BITSET::get_num_data_words(freq.meta),
        (const uint64_t * const)rare.get_data(),
        BITSET::word_index(rare.meta->start),
        BITSET::get_num_data_words(rare.meta));
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
}
#endif