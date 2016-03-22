#ifndef _BITSET_H_
#define _BITSET_H_

#include "utils/utils.hpp"
#include "vector/Meta.hpp"

#define BITS_PER_WORD 64
#define ADDRESS_BITS_PER_WORD 6
#define BYTES_PER_WORD 8

struct BITSET{
  //compute word of data
  static inline size_t word_index(const uint32_t bit_index){
    return bit_index >> ADDRESS_BITS_PER_WORD;
  }
  //check if a bit is set
  static inline bool is_set(
    const uint32_t index, 
    const uint64_t * const in_array, 
    const uint64_t start_index){
    return (in_array)[word_index(index)-start_index] & ((uint64_t) 1 << (index%BITS_PER_WORD));
  }
  //set a bit
  static inline void set_bit(
    const uint32_t index, 
    uint64_t * const in_array, 
    const uint64_t start_index){
    *(in_array + ((index >> ADDRESS_BITS_PER_WORD)-start_index)) |= ((uint64_t)1 << (index & 0x3F));
  }

  static inline size_t get_num_data_words(const Meta * const restrict meta){
    const size_t num_words = 
      word_index(meta->end)-word_index(meta->start);
    return (meta->cardinality>0) ? (num_words+1): 0;
  }

  template <class A, class M>
  static inline A get(
    const uint32_t data, 
    const Meta * const restrict meta, 
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex){

    const size_t num_words = get_num_data_words(meta);
    const A* const restrict values = 
      (const A* const restrict)
      (memoryBuffer->get_address(bufferIndex)+
        sizeof(Meta)+
        BYTES_PER_WORD*num_words+
        sizeof(A)*(data-meta->start));
    return *values;
  }

  template <class A, class M>
  static inline A get(
    const uint32_t index,
    const uint32_t data,
    const Meta * const restrict meta, 
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex){
    (void) index;
    return get<A,M>(data,meta,memoryBuffer,bufferIndex);
  }

  template <class A, class M>
  static inline void set(
    const uint32_t index,
    const uint32_t data,
    const A& restrict value,
    const Meta * const restrict meta, 
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex)
  {
    (void) index;
    const size_t num_words = get_num_data_words(meta);
    A* const restrict values = 
      (A* const restrict)
      (memoryBuffer->get_address(bufferIndex)+
        sizeof(Meta)+
        BYTES_PER_WORD*num_words+
        sizeof(A)*(data-meta->start));
    *values = value;
  }


  //Iterates over set applying a lambda.
  template <class A, class M, typename F>
  static inline void foreach(
    F f,
    const Meta * const restrict meta, 
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex) 
  {
    const size_t num_words = get_num_data_words(meta);
    const size_t offset = word_index(meta->start);
    size_t index = 0;
    if(num_words > 0){
      for(size_t i = 0; i < num_words; i++){
        const uint64_t * const restrict A64_data = 
          (const uint64_t* const restrict)
          (memoryBuffer->get_address(bufferIndex)+
            sizeof(Meta)+
            sizeof(uint64_t)*i);
        const uint64_t cur_word = *A64_data;
        if(cur_word != 0) {
          for(size_t j = 0; j < BITS_PER_WORD; j++){
            if((cur_word >> j) % 2) {
              const uint32_t data = BITS_PER_WORD *(i+offset) + j;
              const A* const restrict values = 
                (const A* const restrict)
                (memoryBuffer->get_address(bufferIndex)+
                  sizeof(Meta)+
                  sizeof(uint64_t)*num_words+
                  sizeof(A)*(data-meta->start));
              f(index++,data,*values);
            }
          }
        }
      }
    }
  }

  template <class M, typename F>
  static inline void parforeach_index(
    F f,
    const Meta * const restrict meta, 
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex) 
  {
    const size_t num_words = get_num_data_words(meta);
    const size_t offset = word_index(meta->start);
    par::for_range(0, num_words, 1,
     [&](const size_t tid, const size_t i) {
        const uint64_t * const restrict A64_data = 
          (const uint64_t* const restrict)
          (memoryBuffer->get_address(bufferIndex)+
            sizeof(Meta)+
            sizeof(uint64_t)*i);
        const uint64_t cur_word = *A64_data;
        if(cur_word != 0) {
          for(size_t j = 0; j < BITS_PER_WORD; j++){
            if((cur_word >> j) % 2) {
              const uint32_t data = BITS_PER_WORD *(i+offset) + j;
              f(tid,i*BITS_PER_WORD,data);
            }
          }
        }
     });
  }

  //Iterates over set applying a lambda.
  template <class M, typename F>
  static inline void foreach_index(
    F f,
    const Meta * const restrict meta, 
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex)
  {
    const size_t num_words = get_num_data_words(meta);
    size_t index = 0;
    if(num_words > 0){
      const uint64_t offset = word_index(meta->start);
      for(size_t i = 0; i < num_words; i++){
        const uint64_t * const restrict A64_data = 
          (const uint64_t* const restrict)
          (memoryBuffer->get_address(bufferIndex)+
            sizeof(Meta)+
            sizeof(uint64_t)*i);
        const uint64_t cur_word = *A64_data;
        if(cur_word != 0) {
          for(size_t j = 0; j < BITS_PER_WORD; j++){
            if((cur_word >> j) % 2) {
              f(index++,BITS_PER_WORD *(i+offset) + j);
            }
          }
        }
      }
    }
  }

  //constructors
  static inline void from_vector(
    uint8_t* buffer,
    const uint32_t * const restrict input_data,
    const size_t input_length )
  {
    if(input_length > 0){
      const uint64_t offset = BITSET::word_index(input_data[0]);

      uint64_t* R64_data = (uint64_t*)(buffer);
      size_t word = offset;
      size_t i = 0;

      const size_t num_words = (word_index(input_data[input_length-1])-offset)+1;
      memset(R64_data,(uint8_t)0,num_words*sizeof(uint64_t));

      while(i < input_length){
        uint32_t cur = input_data[i];
        word = word_index(cur);

        uint64_t set_value = (uint64_t) 1 << (cur % BITS_PER_WORD);
        bool same_word = true;
        ++i;
        while(i<input_length && same_word){
          if(word_index(input_data[i])==word){
            cur = input_data[i];
            set_value |= ((uint64_t) 1 << (cur%BITS_PER_WORD));
            ++i;
          } else same_word = false;
        }
        R64_data[word-offset] = set_value;
      }
    }
  }

  //constructors
  /*
  Should we consider mixing annotations and indices.
  */
  template<class A>
  static inline void from_vector(
    uint8_t* const restrict buffer,
    const uint32_t * const restrict input_data,
    const A * const restrict values,
    const size_t input_length ) {

    from_vector(buffer,input_data,input_length);
    const uint64_t offset = BITSET::word_index(input_data[0]);
    const size_t num_words = input_length > 0 ? word_index(input_data[input_length-1])-offset+1:0;
    A* anno_buffer = (A*)(buffer+(sizeof(uint64_t)*num_words));
    size_t input_index = 0;
    const size_t range = input_length > 0 ? 
      input_data[input_length-1]-input_data[0]+1 : 
      0;
    const uint32_t start = input_data[0];
    for(size_t i = 0; i < range; i++){
      if((i+start) == input_data[input_index]){
        anno_buffer[i] = values[input_index];
        ++input_index;
      } else{
        anno_buffer[i] = utils::zero<A>();
      }
    }
  }

};

#endif