#ifndef _UNION_H_
#define _UNION_H_

#include "union_bitset.hpp"

namespace ops{
  template <class A>
  inline Vector<BLASVector,void*,ParMemoryBuffer> union_in_place(
    const Vector<BLASVector,void*,ParMemoryBuffer>& freq,
    const Vector<BLASVector,A,ParMemoryBuffer>& rare){

    ops::set_union_bitset<BS_BS_UNION_VOID<void*>,void*>(
      (void*)NULL,
      freq.get_meta(),
      (uint64_t *)freq.get_index_data(),
      (void**)NULL,
      (const uint64_t * const)freq.get_index_data(),
      (void**)NULL,
      BITSET::word_index(freq.get_meta()->start),
      BITSET::get_num_data_words(freq.get_meta()),
      (const uint64_t * const)rare.get_index_data(),
      (void**)NULL,
      BITSET::word_index(rare.get_meta()->start),
      BITSET::get_num_data_words(rare.get_meta()));

    return freq;
  }
  template <class A>
  inline Vector<BLASVector,void*,ParMemoryBuffer> union_in_place(
    const Vector<BLASVector,void*,ParMemoryBuffer>& freq,
    const Vector<EHVector,A,ParMemoryBuffer>& rare){

    ops::set_union_bitset<BS_BS_UNION_VOID<void*>,void*>(
      (void*)NULL,
      freq.get_meta(),
      (uint64_t *)freq.get_index_data(),
      (void**)NULL,
      (const uint64_t * const)freq.get_index_data(),
      (void**)NULL,
      BITSET::word_index(freq.get_meta()->start),
      BITSET::get_num_data_words(freq.get_meta()),
      (const uint64_t * const)rare.get_index_data(),
      (void**)NULL,
      BITSET::word_index(rare.get_meta()->start),
      BITSET::get_num_data_words(rare.get_meta()));

    return freq;
  }
  template <class A>
  inline Vector<BLASVector,A,ParMemoryBuffer> union_in_place(
    const A mult_value,
    const Vector<BLASVector,A,ParMemoryBuffer>& freq,
    const Vector<EHVector,A,ParMemoryBuffer>& rare){

    ops::set_union_bitset<BS_BS_ALPHA_SUM<float>,float>(
      mult_value,
      freq.get_meta(),
      (uint64_t *)freq.get_index_data(),
      (A*)freq.get_annotation(),
      (const uint64_t * const)freq.get_index_data(),
      (A*)freq.get_annotation(),
      BITSET::word_index(freq.get_meta()->start),
      BITSET::get_num_data_words(freq.get_meta()),
      (const uint64_t * const)rare.get_index_data(),
      (A*)rare.get_annotation(),
      BITSET::word_index(rare.get_meta()->start),
      BITSET::get_num_data_words(rare.get_meta()));

    return freq;
  }
}
#endif