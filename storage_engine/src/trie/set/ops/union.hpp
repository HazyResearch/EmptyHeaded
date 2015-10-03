#ifndef _UNION_H_
#define _UNION_H_

namespace ops{

  inline bool atomic_union(Set<range_bitset> set, uint32_t data) {
    //there better be capacity to perform the op
    assert(set.number_of_bytes > 0);
    const size_t num_data_words = range_bitset::get_number_of_words(set.number_of_bytes);
    const uint64_t offset = ((uint64_t*)set.data)[0];
    uint64_t* A64 = (uint64_t*)(set.data+sizeof(uint64_t));

    const size_t word = range_bitset::word_index(data);

    //there better be capacity to perform the op
    assert(word >= offset && word < (offset+num_data_words));

    const uint64_t set_bit = ((uint64_t) 1 << (data%BITS_PER_WORD));
    const uint64_t old_value = __sync_fetch_and_or((uint64_t*)&A64[word-offset],set_bit);
    return !(old_value & set_bit);
  }
}

#endif