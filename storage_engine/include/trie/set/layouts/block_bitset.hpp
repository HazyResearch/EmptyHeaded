#ifndef _BLOCK_BITSET_H_
#define _BLOCK_BITSET_H_
/*
THIS CLASS IMPLEMENTS THE FUNCTIONS ASSOCIATED WITH THE BITSET LAYOUT.
*/

#define BLOCK_SIZE 256
#define ADDRESS_BITS_PER_BLOCK 8
#define WORDS_PER_BLOCK 4

class block_bitset{
  public:
    static size_t word_index(const uint32_t bit_index);
    static bool is_set(const uint32_t index, const uint64_t *in_array, const uint64_t start_index);
    static void set(const uint32_t index, uint64_t *in_array, const uint64_t start_index);

    static long find(uint32_t key, const uint8_t *data_in, const size_t number_of_bytes, const type::layout t);
    static type::layout get_type();
    static std::tuple<size_t,type::layout> build(uint8_t *r_in, const uint32_t *data, const size_t length);
    static std::tuple<size_t,size_t,type::layout> get_flattened_data(const uint8_t *set_data, const size_t cardinality);

    template<typename F>
    static void foreach(
        F f,
        const uint8_t *data_in,
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
    static void foreach_index(
        F f,
        const uint8_t *data_in,
        const size_t cardinality,
        const size_t number_of_bytes,
        const type::layout t,
        const uint32_t index_offs);

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
    static size_t par_foreach_index(
      F f,
      const uint8_t* A,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t,
      const uint32_t index_offs);

};
//compute word of data
inline size_t block_bitset::word_index(const uint32_t bit_index){
  return bit_index >> ADDRESS_BITS_PER_WORD;
}
//check if a bit is set
inline bool block_bitset::is_set(const uint32_t index, const uint64_t * const in_array, const uint64_t start_index){
  return (in_array)[word_index(index)-start_index] & ((uint64_t) 1 << (index%BITS_PER_WORD));
}
//check if a bit is set
inline void block_bitset::set(const uint32_t index, uint64_t * const in_array, const uint64_t start_index){
  *(in_array + ((index >> ADDRESS_BITS_PER_WORD)-start_index)) |= ((uint64_t)1 << (index & 0x3F));
}
inline type::layout block_bitset::get_type(){
  return type::BLOCK_BITSET;
}

inline void pack_block(uint64_t *R, const uint32_t *A, const size_t s_a){
  memset(R,(uint8_t)0,BLOCK_SIZE/8);
  for(size_t i = 0; i < s_a; i++){
    const uint32_t cur = A[i];
    const size_t block_bit = cur % BLOCK_SIZE;
    const size_t block_word = block_bit / BITS_PER_WORD;
    R[block_word] |= ((uint64_t) 1 << (block_bit%BITS_PER_WORD)); 
  }

}
inline bool in_range(const uint32_t value, const size_t block_id){
  return (value >= (block_id*BLOCK_SIZE)) && 
        (value < ((block_id+1)*BLOCK_SIZE));
}
//Copies data from input array of ints to our set data r_in
inline std::tuple<size_t,type::layout> block_bitset::build(uint8_t *R, const uint32_t *A, const size_t s_a){
  if(s_a > 0){
    size_t i = 0;
    const uint8_t * const R_start = R;
    while(i < s_a){
      const size_t block_id = A[i] / BLOCK_SIZE;
      const size_t block_start_index = i;      
      while(i < s_a && in_range(A[i],block_id)) {
        i++;
      }
      *(uint32_t*)(R) = block_id;
      *(uint32_t*)(R+sizeof(uint32_t)) = block_start_index;
      pack_block((uint64_t*)(R+2*sizeof(uint32_t)),&A[block_start_index],(i-block_start_index));
      R += WORDS_PER_BLOCK*sizeof(uint64_t)+2*sizeof(uint32_t);
    }
    const size_t num_bytes = R-R_start;
    return std::make_pair(num_bytes,type::BLOCK_BITSET);
  }
  return std::make_pair(0,type::BLOCK_BITSET);
}

//Iterates over set applying a lambda.
template<typename F>
inline void block_bitset::foreach_until(
    F f,
    const uint8_t *A,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout type) {
  (void) cardinality; (void) type; (void) f; (void) number_of_bytes; (void) A;
  std::cout << "ERROR: NOT IMPLEMENTED" << std::endl;
  abort();
}

//Iterates over set applying a lambda.
template<typename F>
inline void block_bitset::foreach(
    F f,
    const uint8_t * const A,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout type) {
  (void) cardinality; (void) type;

  uint8_t *A_ptr = const_cast<uint8_t*>(A); 
  if(number_of_bytes > 0){
    const size_t A_num_blocks = number_of_bytes/(2*sizeof(uint32_t)+(BLOCK_SIZE/8));
    for(size_t i = 0; i < A_num_blocks; i++){
      const uint64_t * const data = (uint64_t*)(A_ptr+2*sizeof(uint32_t));
      const uint32_t offset = *(uint32_t*)A_ptr;
      for(size_t j = 0; j < BLOCK_SIZE; j++){
        const size_t word = j / BITS_PER_WORD;
        const size_t bit = j % BITS_PER_WORD;
        if((data[word] >> bit) % 2) {
          f(offset*BLOCK_SIZE + j);
        }
      }
      A_ptr += 2*sizeof(uint32_t)+WORDS_PER_BLOCK*sizeof(uint64_t);
    }
  }
}

//Iterates over set applying a lambda.
template<typename F>
inline void block_bitset::foreach_index(
    F f,
    const uint8_t * const A,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout type,
    const uint32_t index_offs) {
  (void) cardinality; (void) type;

  uint8_t *A_ptr = const_cast<uint8_t*>(A); 
  if(number_of_bytes > 0){
    const size_t A_num_blocks = number_of_bytes/(2*sizeof(uint32_t)+(BLOCK_SIZE/8));
    for(size_t i = 0; i < A_num_blocks; i++){
      const uint64_t * const data = (uint64_t*)(A_ptr+2*sizeof(uint32_t));
      const uint32_t offset = *(uint32_t*)A_ptr;
      uint32_t index = *(uint32_t*)(A_ptr+sizeof(uint32_t));
      for(size_t j = 0; j < BLOCK_SIZE; j++){
        const size_t word = j / BITS_PER_WORD;
        const size_t bit = j % BITS_PER_WORD;
        if((data[word] >> bit) % 2) {
          f(index_offs+index++,offset*BLOCK_SIZE + j);
        }
      }
      A_ptr += 2*sizeof(uint32_t)+WORDS_PER_BLOCK*sizeof(uint64_t);
    }
  }
}

template<typename F>
inline void block_bitset::foreach_index(
    F f,
    const uint8_t * const A,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout type){
  foreach_index(f,A,cardinality,number_of_bytes,type,0);
}

// Iterates over set applying a lambda in parallel.
template<typename F>
inline size_t block_bitset::par_foreach(
      F f,
      const uint8_t* A,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t) {
  (void) cardinality; (void) t;

  const uint32_t data_offset = 2*sizeof(uint32_t)+WORDS_PER_BLOCK*sizeof(uint64_t);
  if(number_of_bytes > 0){
    const size_t A_num_blocks = number_of_bytes/(2*sizeof(uint32_t)+(BLOCK_SIZE/8));
    return par::for_range(0, A_num_blocks, 1,
     [&](size_t tid, size_t i) {
        const uint32_t offset = *(uint32_t*)(A+i*data_offset);
        uint64_t * const data = (uint64_t*)(A+2*sizeof(uint32_t)+i*data_offset);
        for(size_t j = 0; j < BLOCK_SIZE; j++){
          const size_t word = j / BITS_PER_WORD;
          const size_t bit = j % BITS_PER_WORD;
          if((data[word] >> bit) % 2) {
            f(tid,offset*BLOCK_SIZE + j);
          }
        }
    });
  }
  return 0;
}

// Iterates over set applying a lambda in parallel.
template<typename F>
inline size_t block_bitset::par_foreach_index(
      F f,
      const uint8_t* A,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t,
      const uint32_t index_offs) {
  (void) cardinality; (void) t;

  const uint32_t data_offset = 2*sizeof(uint32_t)+WORDS_PER_BLOCK*sizeof(uint64_t);
  if(number_of_bytes > 0){
    const size_t A_num_blocks = number_of_bytes/(2*sizeof(uint32_t)+(BLOCK_SIZE/8));
    return par::for_range(0, A_num_blocks, 1,
     [&](size_t tid, size_t i) {
        const uint32_t offset = *(uint32_t*)(A+i*data_offset);
        uint64_t * const data = (uint64_t*)(A+2*sizeof(uint32_t)+i*data_offset);
        uint32_t index = *(uint32_t*)(A+i*data_offset+sizeof(uint32_t));
        for(size_t j = 0; j < BLOCK_SIZE; j++){
          const size_t word = j / BITS_PER_WORD;
          const size_t bit = j % BITS_PER_WORD;
          if((data[word] >> bit) % 2) {
            f(tid,index_offs+index++,offset*BLOCK_SIZE + j);
          }
        }
    });
  }
  return 0;
}

template<typename F>
inline size_t block_bitset::par_foreach_index(
      F f,
      const uint8_t* A,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t){
  return par_foreach_index(f,A,cardinality,number_of_bytes,t,0);
}

inline long block_bitset::find(uint32_t key, 
  const uint8_t *data_in, 
  const size_t number_of_bytes,
  const type::layout t){
  (void) t;

  const uint32_t data_offset = 2+(WORDS_PER_BLOCK*2);
  if(number_of_bytes > 0){
    //always have at least one block in this code
    const size_t A_num_blocks = number_of_bytes/(2*sizeof(uint32_t)+(BLOCK_SIZE/8));
    long block_index = utils::binary_search((uint32_t*)data_in,0,A_num_blocks-1,key/BLOCK_SIZE,[&](uint32_t d){return d*data_offset;});
    if(block_index != -1){
      uint8_t *A_BLOCK = const_cast<uint8_t*>(data_in)+block_index*(2*sizeof(uint32_t)+WORDS_PER_BLOCK*sizeof(uint64_t));
      uint64_t *A64 = (uint64_t*)(A_BLOCK+2*sizeof(uint32_t));
      size_t block_bit = key % BLOCK_SIZE;
      size_t word = block_bit/BITS_PER_WORD;
      size_t bit = block_bit % BITS_PER_WORD;
      if((A64[word] >> bit) % 2) {
        size_t index = *(uint32_t*)(A_BLOCK+sizeof(uint32_t));
        for(size_t i = 0 ; i < word; i++){
          index += _mm_popcnt_u64(A64[i]);
        }
        uint64_t masked_word = A64[word] & masks::find_mask[bit];
        return index+_mm_popcnt_u64(masked_word);
      }
    }
  }
  return -1;
}

#endif