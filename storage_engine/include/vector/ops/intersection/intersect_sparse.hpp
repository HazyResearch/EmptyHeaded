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
  inline Vector<SparseVector,A,MemoryBuffer> alloc_and_intersect(
    const size_t alloc_size,
    MemoryBuffer *m,
    const Vector<SparseVector,B,MemoryBuffer>& rare, 
    const Vector<SparseVector,C,MemoryBuffer>& freq){

    const size_t index = m->get_offset();
    uint8_t *buffer = m->get_next(alloc_size);
    Meta* meta = new(buffer) Meta();
    uint32_t *out = (uint32_t*)(buffer+sizeof(Meta));

    if(rare.meta->cardinality == 0 || freq.meta->cardinality == 0){
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
    return Vector<SparseVector,A,MemoryBuffer>(m,index);
  }

  //Materilize (allocate memory & run intersection)
  inline float agg_intersect(
    MemoryBuffer *m,
    const Vector<SparseVector,float,MemoryBuffer>& rare, 
    const Vector<SparseVector,float,MemoryBuffer>& freq){

    //run intersection.
    const size_t alloc_size = sizeof(Meta)*
      sizeof(uint32_t)*
      std::min(rare.meta->cardinality,freq.meta->cardinality);
    
    Vector<SparseVector,float,MemoryBuffer> result = 
      alloc_and_intersect<float,float,float>(alloc_size,m,rare,freq);

    float anno = 0.0;
    result.foreach_index([&](const uint32_t index, const uint32_t data){
      (void) index;
      //have some field set in vector see if it is annotated or not,
      //use default field if set otherwise actually lookup the annotation.
      anno += rare.get(data)*freq.get(data);
    });

    m->roll_back(alloc_size);
    return anno;
  }

  //Materilize (allocate memory & run intersection)
  template <class A, class B, class C>
  inline Vector<SparseVector,A,MemoryBuffer> mat_intersect(
    MemoryBuffer *m,
    const Vector<SparseVector,B,MemoryBuffer>& rare, 
    const Vector<SparseVector,C,MemoryBuffer>& freq){

    const size_t alloc_size = sizeof(Meta)*
      sizeof(uint32_t)*
      std::min(rare.meta->cardinality,freq.meta->cardinality);
    return alloc_and_intersect<A,B,C>(alloc_size,m,rare,freq);
  }
}
#endif