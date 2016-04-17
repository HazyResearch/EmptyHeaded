#ifndef _INTERSECT_H_
#define _INTERSECT_H_

#include "intersect_dense.hpp"

namespace ops{
  //types of aggregates
  //AGG_VOID
  //AGG_SUM
  template <class A, class B>
  inline Vector<EHVector,void*,ParMemoryBuffer> agg_intersect(
    const size_t tid,
    ParMemoryBuffer * restrict m,
    const Vector<EHVector,A,ParMemoryBuffer>& rare, 
    const Vector<EHVector,B,ParMemoryBuffer>& freq
  ){

    const size_t alloc_size = 
       std::min(rare.get_num_index_bytes(),freq.get_num_index_bytes());

    switch(rare.get_type()){
      case type::BITSET : {
        switch(freq.get_type()){
          case type::BITSET :
            return bitset_bitset_intersect<void*,A,B>(tid,alloc_size,m,rare,freq);
          break;
          case type::UINTEGER : 
            //bitset_uinteger_intersect<AGG>();
          break;
        }
      }
      case type::UINTEGER : {
        switch(freq.get_type()){
          case type::BITSET : 
            //bitset_uinteger_intersect<AGG>();
          break;
          case type::UINTEGER : 
            //uinteger_uinteger_intersect<AGG>();
          break;
        }
      default:
        std::cout << "TYPE ERROR" << std::endl;
        throw;
      break;
      }
    }
    return bitset_bitset_intersect<void*,A,B>(tid,alloc_size,m,rare,freq);
  }
}
#endif