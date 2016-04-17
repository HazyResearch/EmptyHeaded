#ifndef _INTERSECT_DENSE_H_
#define _INTERSECT_DENSE_H_

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

  struct VOID_AGG {
    int a = 0;
  };

  template <class A, class B, class C>
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
      ops::set_intersect_bitset(
        meta,
        out,
        (const uint64_t * const)freq.get_index_data(),
        BITSET::word_index(freq.get_meta()->start),
        BITSET::get_num_data_words(freq.get_meta()),
        (const uint64_t * const)rare.get_index_data(),
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