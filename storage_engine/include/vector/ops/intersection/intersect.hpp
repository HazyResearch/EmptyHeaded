#ifndef _INTERSECT_H_
#define _INTERSECT_H_

#include "intersect_bitset.hpp"
#include "intersect_uinteger.hpp"
#include "intersect_bitset_uinteger.hpp"

namespace ops{
  //types of aggregates
  //AGG_VOID
  //AGG_SUM
  template <class AGG,class C,class A, class B>
  inline Vector<EHVector,C,ParMemoryBuffer> agg_intersect(
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
            return ops::bitset_bitset_intersect<AGG,C,A,B>(
              tid,
              alloc_size,
              m,
              rare,
              freq);
          break;
          case type::UINTEGER : 
            return ops::bitset_bitset_intersect<AGG,C,A,B>(
              tid,
              alloc_size,
              m,
              rare,
              freq);
          break;
        }
      }
      case type::UINTEGER : {
        switch(freq.get_type()){
          case type::BITSET : 
            return ops::bitset_bitset_intersect<AGG,C,A,B>(
              tid,
              alloc_size,
              m,
              freq,
              rare);
          break;
          case type::UINTEGER : 
            return ops::uinteger_uinteger_intersect<AGG,C,A,B>(
              tid,
              alloc_size,
              m,
              rare,
              freq);
          break;
        }
      default:
        std::cout << "TYPE ERROR" << std::endl;
        throw;
      break;
      }
    }
    return ops::bitset_bitset_intersect<AGG,C,A,B>(tid,alloc_size,m,rare,freq);
  }


  template <class AGG,class A>
  inline A agg_intersect(
    const size_t tid,
    ParMemoryBuffer * restrict m,
    const Vector<BLASVector,A,ParMemoryBuffer>& rare, 
    const Vector<BLASVector,A,ParMemoryBuffer>& freq
  ){

    const size_t alloc_size = 
       std::min(rare.get_num_index_bytes(),freq.get_num_index_bytes());

    switch(rare.get_type()){
      case type::BITSET : {
        switch(freq.get_type()){
          case type::BITSET :
            return ops::agg_bitset_bitset_intersect<AGG,A>(tid,alloc_size,m,rare,freq);
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
    return ops::agg_bitset_bitset_intersect<AGG,A>(tid,alloc_size,m,rare,freq);
  }

}
#endif