#ifndef _RANGE_range_bitset_H_
#define _RANGE_range_bitset_H_
/*
THIS CLASS IMPLEMENTS THE FUNCTIONS ASSOCIATED WITH THE range_bitset LAYOUT.
*/

#include "../ops/sse_masks.hpp"

class range_bitset{
  public:
    static size_t word_index(const uint32_t bit_index);
    static bool is_set(const uint32_t index, const uint64_t *in_array, const uint64_t start_index);
    static void set(const uint32_t index, uint64_t *in_array, const uint64_t start_index);
    static size_t get_number_of_bytes(const size_t length, const size_t range);

    static long find(uint32_t key, const uint8_t *data_in, const size_t number_of_bytes, const type::layout t);
    static std::tuple<size_t,bool> find(uint32_t start_index, uint32_t key, const uint8_t *data_in, const size_t number_of_bytes, const type::layout t);

    static type::layout get_type();
    static std::tuple<size_t,type::layout> build(uint8_t *r_in, const uint32_t *data, const size_t length);
    static size_t get_number_of_words(size_t num_bytes);
    static size_t get_num_set(const uint32_t key, const uint64_t data, const uint32_t offset);
    static void set_indices(const uint8_t *data_in,
        const size_t cardinality,
        const size_t number_of_bytes,
        const type::layout t);

    template<typename F>
    static void foreach(
        F f,
        const uint8_t *data_in,
        const size_t cardinality,
        const size_t number_of_bytes,
        const type::layout t);

    template<class M, typename F>
    static void foreach(
        const size_t index,
        M* memoryBuffer,
        F f,
        const size_t cardinality,
        const size_t number_of_bytes,
        const type::layout t);

    template<typename F>
    static void foreach_index(
        F f,
        const uint8_t *data_in,
        const size_t cardinality,
        const size_t number_of_bytes,
        const type::layout t);

    template<typename F>
    static void foreach_until(
        F f,
        const uint8_t *data_in,
        const size_t cardinality,
        const size_t number_of_bytes,
        const type::layout t);

    template<typename F>
    static size_t par_foreach(
      F f,
      const uint8_t* A,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t);

    template<typename F>
    static size_t par_foreach_index(
      F f,
      const uint8_t* A,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t);

    template<typename F>
    static size_t static_par_foreach_index(
      F f,
      const uint8_t* A,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t);
};
inline size_t range_bitset::get_number_of_words(size_t num_bytes){
  return ((num_bytes-sizeof(uint64_t))/(BYTES_PER_WORD+sizeof(uint32_t)));
}
//compute word of data
inline size_t range_bitset::word_index(const uint32_t bit_index){
  return bit_index >> ADDRESS_BITS_PER_WORD;
}
//check if a bit is set
inline bool range_bitset::is_set(const uint32_t index, const uint64_t * const in_array, const uint64_t start_index){
  return (in_array)[word_index(index)-start_index] & ((uint64_t) 1 << (index%BITS_PER_WORD));
}
//check if a bit is set
inline void range_bitset::set(const uint32_t index, uint64_t * const in_array, const uint64_t start_index){
  *(in_array + ((index >> ADDRESS_BITS_PER_WORD)-start_index)) |= ((uint64_t)1 << (index & 0x3F));
}
inline type::layout range_bitset::get_type(){
  return type::RANGE_BITSET;
}

//Called before build to get proper alloc size
inline size_t range_bitset::get_number_of_bytes(const size_t length, const size_t range){
  (void) length;
  //overestimate by 1 word because the range could cross a word boundary ie range 10 of [60,70]
  //is 2 words
  return ( (word_index(range)+2) * (sizeof(uint64_t)+sizeof(uint32_t)) ) + sizeof(uint64_t); 
}

//Copies data from input array of ints to our set data r_in
inline std::tuple<size_t,type::layout> range_bitset::build(uint8_t *R, const uint32_t *A, const size_t s_a){
  if(s_a > 0){
    const uint64_t offset = word_index(A[0]);
    ((uint64_t*)R)[0] = offset; 

    uint64_t* R64_data = (uint64_t*)(R+sizeof(uint64_t));
    size_t word = word_index(A[0]);
    size_t i = 0;

    const size_t num_words = (word_index(A[s_a-1])-offset)+1;
    memset(R64_data,(uint8_t)0,num_words*sizeof(uint64_t));
    uint32_t* R32_index = (uint32_t*)(&R64_data[num_words]);

    while(i < s_a){
      uint32_t cur = A[i];
      word = word_index(cur);

      R32_index[word-offset] = i;
      uint64_t set_value = (uint64_t) 1 << (cur % BITS_PER_WORD);
      bool same_word = true;
      ++i;
      while(i<s_a && same_word){
        if(word_index(A[i])==word){
          cur = A[i];
          set_value |= ((uint64_t) 1 << (cur%BITS_PER_WORD));
          ++i;
        } else same_word = false;
      }
      R64_data[word-offset] = set_value;
    }
    const size_t num_bytes = (num_words)*(BYTES_PER_WORD+sizeof(uint32_t)) + sizeof(uint64_t);
    return std::make_pair(num_bytes,type::RANGE_BITSET);
  }
  return std::make_pair(0,type::RANGE_BITSET);
}
//Iterates over set applying a lambda.
template<typename F>
inline void range_bitset::foreach_until(
    F f,
    const uint8_t *A,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout type) {
  (void) cardinality; (void) type;

  if(number_of_bytes > 0){
    const size_t num_data_words = get_number_of_words(number_of_bytes);
    const uint64_t offset = ((uint64_t*)A)[0];
    const uint64_t* A64 = (uint64_t*)(A+sizeof(uint64_t));
    for(size_t i = 0; i < num_data_words; i++){
      const uint64_t cur_word = A64[i];
      for(size_t j = 0; j < BITS_PER_WORD; j++){
        if((cur_word >> j) % 2){
          if(f(BITS_PER_WORD*(i+offset) + j))
            break;
        }
      }
    }
  }
}

//Iterates over set applying a lambda.
template<typename F>
inline void range_bitset::foreach_index(
    F f,
    const uint8_t * const A,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout type) {
  (void) cardinality; (void) type;

  if(number_of_bytes > 0 && cardinality > 0){
    const size_t num_data_words = get_number_of_words(number_of_bytes);
    const uint64_t offset = ((uint64_t*)A)[0];
    const uint64_t* A64_data = (uint64_t*)(A+sizeof(uint64_t));
    const uint32_t* A32_index = (uint32_t*)(A64_data+num_data_words);

    for(size_t i = 0; i < num_data_words; i++){
      const uint64_t cur_word = *A64_data;
      uint32_t index = *A32_index;
      if(cur_word != 0) {
        for(size_t j = 0; j < BITS_PER_WORD; j++){
          if((cur_word >> j) % 2) {
            f(index++,BITS_PER_WORD *(i+offset) + j);
          }
        }
      }
      A64_data++;
      A32_index++;
    }
  }
}

//Iterates over set applying a lambda.
template<typename F>
inline void range_bitset::foreach(
    F f,
    const uint8_t * const A,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout type) {
  (void) cardinality; (void) type;

  if(number_of_bytes > 0 && cardinality > 0){
    const size_t num_data_words = get_number_of_words(number_of_bytes);
    const uint64_t offset = ((uint64_t*)A)[0];
    const uint64_t* A64_data = (uint64_t*)(A+sizeof(uint64_t));

    for(size_t i = 0; i < num_data_words; i++){
      const uint64_t cur_word = *A64_data;
      if(cur_word != 0) {
        for(size_t j = 0; j < BITS_PER_WORD; j++){
          if((cur_word >> j) % 2) {
            f(BITS_PER_WORD *(i+offset) + j);
          }
        }
      }
      A64_data++;
    }
  }
}

//Iterates over set applying a lambda.
template<class M, typename F>
inline void range_bitset::foreach(
    const size_t index,
    M* memoryBuffer,
    F f,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout type) {
  (void) cardinality; (void) type;

  size_t word_index = 0;
  uint8_t *A = (uint8_t*) memoryBuffer->get_address(index);
  if(number_of_bytes > 0 && cardinality > 0){
    const size_t num_data_words = get_number_of_words(number_of_bytes);
    const uint64_t offset = ((uint64_t*)A)[0];
    uint64_t* A64_data = (uint64_t*)(A+sizeof(uint64_t));

    for(size_t i = 0; i < num_data_words; i++){
      A64_data = (uint64_t*)(
        memoryBuffer->get_address(
          word_index*sizeof(uint64_t) +
          index +
          sizeof(uint64_t)));
      const uint64_t cur_word = *A64_data;
      if(cur_word != 0) {
        for(size_t j = 0; j < BITS_PER_WORD; j++){
          if((cur_word >> j) % 2) {
            f(BITS_PER_WORD *(i+offset) + j);
          }
        }
      }
      word_index++;
    }
  }
}

//Iterates over set applying a lambda.
inline void range_bitset::set_indices(
    const uint8_t * const A,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout type) {
  (void) cardinality; (void) type;

  if(number_of_bytes > 0){
    const size_t num_data_words = get_number_of_words(number_of_bytes);
    const uint64_t* A64_data = (uint64_t*)(A+sizeof(uint64_t));
    uint32_t* A32_index = (uint32_t*)(A64_data+num_data_words);

    size_t count = 0;
    for(size_t i = 0; i < num_data_words; i++){
      const uint64_t cur_word = *A64_data;
      *A32_index = count;
      count += _mm_popcnt_u64(cur_word);
      A64_data++;
      A32_index++;
    }
  }
}

// Iterates over set applying a lambda in parallel.
template<typename F>
inline size_t range_bitset::par_foreach(
      F f,
      const uint8_t* A,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t) {
   (void) t; (void) cardinality;

  if(number_of_bytes > 0){
    const size_t num_data_words = get_number_of_words(number_of_bytes);
    const uint64_t offset = ((uint64_t*)A)[0];
    const uint64_t* A64 = (uint64_t*)(A+sizeof(uint64_t));
    return par::for_range(0, num_data_words, 2,
           [&f, &A64, cardinality,offset](size_t tid, size_t i) {
              const uint64_t cur_word = A64[i];
              if(cur_word != 0) {
                for(size_t j = 0; j < BITS_PER_WORD; j++){
                  const uint32_t curr_nb = BITS_PER_WORD*(i+offset) + j;
                  if((cur_word >> j) % 2) {
                    f(tid, curr_nb);
                  }
                }
              }
           });
  }

  return 1;
}

// Iterates over set applying a lambda in parallel.
template<typename F>
inline size_t range_bitset::par_foreach_index(
      F f,
      const uint8_t* A,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t) {
   (void) t; (void) cardinality;

  if(number_of_bytes > 0){
    const size_t num_data_words = get_number_of_words(number_of_bytes);
    const uint64_t offset = ((uint64_t*)A)[0];
    const uint64_t* A64 = (uint64_t*)(A+sizeof(uint64_t));
    const uint32_t* A32_index = (uint32_t*)(A64+num_data_words);

    return par::for_range(0, num_data_words, 1,
           [&](size_t tid, size_t i) {
              const uint64_t cur_word = A64[i];
              uint32_t index = A32_index[i];
              if(cur_word != 0) {
                for(size_t j = 0; j < BITS_PER_WORD; j++){
                  const uint32_t curr_nb = BITS_PER_WORD*(i+offset) + j;
                  if((cur_word >> j) % 2) {
                    f(tid,index++,curr_nb);
                  }
                }
              }
           });
  }

  return 1;
}

// Iterates over set applying a lambda in parallel.
template<typename F>
inline size_t range_bitset::static_par_foreach_index(
      F f,
      const uint8_t* A,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t) {
   (void) t; (void) cardinality;

  if(number_of_bytes > 0){
    const size_t num_data_words = get_number_of_words(number_of_bytes);
    const uint64_t offset = ((uint64_t*)A)[0];
    const uint64_t* A64 = (uint64_t*)(A+sizeof(uint64_t));
    const uint32_t* A32_index = (uint32_t*)(A64+num_data_words);

    return par::for_range(0, num_data_words,
           [&](size_t tid, size_t i) {
              const uint64_t cur_word = A64[i];
              uint32_t index = A32_index[i];
              if(cur_word != 0) {
                for(size_t j = 0; j < BITS_PER_WORD; j++){
                  const uint32_t curr_nb = BITS_PER_WORD*(i+offset) + j;
                  if((cur_word >> j) % 2) {
                    f(tid,index++,curr_nb);
                  }
                }
              }
           });
  }

  return 1;
}

inline size_t range_bitset::get_num_set(const uint32_t key, const uint64_t data, const uint32_t offset){
  const uint64_t masked_word = data & (masks::find_mask[(key%BITS_PER_WORD)]);
  return _mm_popcnt_u64(masked_word) + offset;
}

inline long range_bitset::find(uint32_t key, const uint8_t *A, const size_t number_of_bytes, const type::layout t){
  (void) t;
  if(number_of_bytes > 0){
    const size_t num_data_words = get_number_of_words(number_of_bytes);
    const uint64_t offset = ((uint64_t*)A)[0];
    const uint64_t* A64 = (uint64_t*)(A+sizeof(uint64_t));
    const uint32_t* A32_index = (uint32_t*)(A64+num_data_words);

    const size_t word = word_index(key);
    if(word >= offset && word < (offset+num_data_words)){
      if(is_set(key,A64,offset)){
        //figure out how many in word set before one we want, then add up index
        return get_num_set(key,(A64)[word-offset],A32_index[word-offset]);
      }
    }
  }
  return -1;
}

inline std::tuple<size_t,bool> range_bitset::find(uint32_t start_index,
  uint32_t key, 
  const uint8_t *data_in, 
  const size_t number_of_bytes,
  const type::layout t){

  (void) start_index;
  const long find_index = find(key,data_in,number_of_bytes,t);
  return std::make_tuple(find_index,find_index != -1);
}


#endif