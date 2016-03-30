#ifndef _UNION_DENSE_H_
#define _UNION_DENSE_H_

#include "vector/DenseVector.hpp"
#include "union_bitset.hpp"

namespace ops{
  template <class A>
  inline Vector<DenseVector,void*,ParMemoryBuffer> union_in_place(
    const Vector<DenseVector,void*,ParMemoryBuffer>& freq,
    const Vector<DenseVector,A,ParMemoryBuffer>& rare){

    ops::set_union_bitset(
      freq.get_meta(),
      (uint64_t *)freq.get_data(),
      (const uint64_t * const)freq.get_data(),
      BITSET::word_index(freq.get_meta()->start),
      BITSET::get_num_data_words(freq.get_meta()),
      (const uint64_t * const)rare.get_data(),
      BITSET::word_index(rare.get_meta()->start),
      BITSET::get_num_data_words(rare.get_meta()));

    return freq;
  }
}
#endif