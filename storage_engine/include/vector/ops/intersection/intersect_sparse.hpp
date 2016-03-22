#ifndef _INTERSECT_SPARSE_H_
#define _INTERSECT_SPARSE_H_

#include "vector/SparseVector.hpp"
#include "intersect_uinteger.hpp"

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
  inline Vector<SparseVector,A,ParMemoryBuffer> alloc_and_intersect(
    const size_t tid,
    const size_t alloc_size,
    ParMemoryBuffer * restrict m,
    const Vector<SparseVector,B,ParMemoryBuffer>& rare, 
    const Vector<SparseVector,C,ParMemoryBuffer>& freq){

    const size_t index = m->get_offset(tid);
    uint8_t *buffer = m->get_next(tid,alloc_size);
    Meta* meta = new(buffer) Meta();
    uint32_t *out = (uint32_t*)(buffer+sizeof(Meta));

    if(rare.meta->cardinality == 0 || freq.meta->cardinality == 0
      || (rare.meta->start > freq.meta->end) 
      || (freq.meta->start > rare.meta->end)){
      meta->cardinality = 0;
      meta->start = 0;
      meta->end = 0;
      meta->type = type::UINTEGER;
    } else if((rare.meta->cardinality/freq.meta->cardinality) >= 32) {
      set_intersect_galloping(
        meta,
        out,
        (const uint32_t * const)freq.get_data(),
        freq.meta->cardinality,
        (const uint32_t * const)rare.get_data(),
        rare.meta->cardinality);
    } else if((freq.meta->cardinality/rare.meta->cardinality) >= 32) {
      set_intersect_galloping(
        meta,
        out,
        (const uint32_t * const)rare.get_data(),
        rare.meta->cardinality,
        (const uint32_t * const)freq.get_data(),
        freq.meta->cardinality);
    } else {
      set_intersect_shuffle(
        meta,
        out,
        (const uint32_t * const)rare.get_data(),
        rare.meta->cardinality,
        (const uint32_t * const)freq.get_data(),
        freq.meta->cardinality);  
    }
    BufferIndex bi;
    bi.tid = tid;
    bi.index = index;
    return Vector<SparseVector,A,ParMemoryBuffer>(m,bi);
  }

  //Materilize (allocate memory & run intersection)
  inline float agg_intersect(
    const size_t tid,
    ParMemoryBuffer * restrict m,
    const Vector<SparseVector,float,ParMemoryBuffer>& rare, 
    const Vector<SparseVector,float,ParMemoryBuffer>& freq){

    //run intersection.
    const size_t alloc_size = sizeof(Meta)+
      (sizeof(uint32_t)*
       std::min(rare.meta->cardinality,freq.meta->cardinality));
    
    Vector<SparseVector,float,ParMemoryBuffer> result = 
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
  template <class A, class B>
  inline Vector<SparseVector,void*,ParMemoryBuffer> agg_intersect(
    const size_t tid,
    ParMemoryBuffer * restrict m,
    const Vector<SparseVector,A,ParMemoryBuffer>& rare, 
    const Vector<SparseVector,B,ParMemoryBuffer>& freq){

    //run intersection.
    const size_t alloc_size = sizeof(Meta)+
      (sizeof(uint32_t)*
      std::min(rare.meta->cardinality,freq.meta->cardinality));
    
    Vector<SparseVector,void*,ParMemoryBuffer> result = 
      alloc_and_intersect<void*,A,B>(tid,alloc_size,m,rare,freq);
    m->roll_back(tid,alloc_size);
    return result;
  }


  //Materilize (allocate memory & run intersection)
  template <class A, class B, class C>
  inline Vector<SparseVector,A,ParMemoryBuffer> mat_intersect(
    const size_t tid,
    ParMemoryBuffer * restrict m,
    const Vector<SparseVector,B,ParMemoryBuffer>& rare, 
    const Vector<SparseVector,C,ParMemoryBuffer>& freq){

    const size_t alloc_size = sizeof(Meta)+
      ((sizeof(uint32_t)+sizeof(A))*
       std::min(rare.meta->cardinality,freq.meta->cardinality));
    return alloc_and_intersect<A,B,C>(tid,alloc_size,m,rare,freq);
  }
}
#endif