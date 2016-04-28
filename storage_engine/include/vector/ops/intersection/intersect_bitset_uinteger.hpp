#ifndef _INTERSECT_BITSET_UINTEGER_H_
#define _INTERSECT_BITSET_UINTEGER_H_

namespace ops{
 
  template <class AGG, class A, class B, class C>
  inline Vector<EHVector,A,ParMemoryBuffer> bitset_uinteger_intersect(
    const size_t tid,
    const size_t alloc_size,
    ParMemoryBuffer * restrict m,
    const Vector<EHVector,B,ParMemoryBuffer>& bs_set, 
    const Vector<EHVector,C,ParMemoryBuffer>& uint_set){

    const size_t index = m->get_offset(tid);
    uint8_t *buffer = m->get_next(tid,alloc_size);
    Meta* meta = new(buffer) Meta();
    uint32_t *out = (uint32_t*)(buffer+sizeof(Meta));

    uint32_t *start_out = out;

    meta->cardinality = 0;
    meta->start = 0;
    meta->end = 0;
    meta->type = type::UINTEGER;

    if( !(bs_set.get_meta()->cardinality == 0 || uint_set.get_meta()->cardinality == 0
      || (bs_set.get_meta()->start > uint_set.get_meta()->end) 
      || (uint_set.get_meta()->start > bs_set.get_meta()->end)) ) {

      uint_set.foreach_index([&](const uint32_t index, const uint32_t data){
        (void) index;
        if(!bs_set.contains(data)){
          meta->cardinality++;
          meta->end = data;
          *out++ = data;
        }
      });
      if(meta->cardinality > 0)
        meta->start = *start_out;
    }
    BufferIndex bi;
    bi.tid = tid;
    bi.index = index;
    return Vector<EHVector,A,ParMemoryBuffer>(m,bi);
  }

}
#endif