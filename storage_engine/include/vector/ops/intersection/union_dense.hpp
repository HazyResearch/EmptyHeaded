#ifndef _UNION_H_
#define _UNION_H_

#include "union_bitset.hpp"

namespace ops{
  template <class A>
  inline Vector<BLASVector,void*,ParMemoryBuffer> union_in_place(
    const Vector<BLASVector,void*,ParMemoryBuffer>& freq,
    const Vector<BLASVector,A,ParMemoryBuffer>& rare){

    ops::set_union_bitset(
      freq.get_meta(),
      (uint64_t *)freq.get_index_data(),
      (const uint64_t * const)freq.get_index_data(),
      BITSET::word_index(freq.get_meta()->start),
      BITSET::get_num_data_words(freq.get_meta()),
      (const uint64_t * const)rare.get_data(),
      BITSET::word_index(rare.get_meta()->start),
      BITSET::get_num_data_words(rare.get_meta()));

    return freq;
  }
}
#endif