#ifndef _BITSET_INTERSECTION_H_
#define _BITSET_INTERSECTION_H_

namespace ops{
  inline size_t get_num_set(const uint32_t key, const uint64_t data, const uint32_t offset){
    const uint64_t masked_word = data & (masks::find_mask[(key%BITS_PER_WORD)]);
    return _mm_popcnt_u64(masked_word) + offset;
  }
  //////
  struct bs_aggregate_null{
     static inline void write(uint64_t *r, size_t j, uint64_t data){
      (void)r;(void)j;(void)data;
      return;
     }
     static inline uint8_t* adjust_write_pointer(uint8_t *C,size_t count,size_t old_count,size_t bytes_per_block){
        (void) C; (void) count; (void) old_count; (void) bytes_per_block;
        return NULL;
     }
     static inline uint8_t* advanceC(uint8_t *C, size_t num){
      (void) C; (void) num;
      return NULL;
     }
    static inline uint64_t*  write_range_index(uint8_t *d_in, uint64_t start_index){
      (void) d_in; (void) start_index;
      return NULL;
    }
    static inline uint32_t* index_data(uint64_t * const C, size_t total_size){
      (void) C; (void) total_size;
      return NULL;
    }
    static inline void write_block_header(uint32_t *C, const uint32_t data, const uint32_t count){
      (void) C; (void) data; (void) count;
      return;
    }
    template<typename F>
    static inline size_t unpack_range(size_t count, uint64_t x, const uint64_t offset, F f,
      const uint64_t A, const uint64_t B, const uint32_t *A_index, const uint32_t *B_index,
      uint64_t *result, size_t r_index, uint32_t *index_data){

      (void) A, (void) B; (void) A_index; (void) B_index;(void) offset; (void)f;
      (void) result; (void) index_data; (void) r_index;

      count += _mm_popcnt_u64(x);      
      return count;
    }

    template<typename F>
    static inline std::tuple<size_t,uint32_t,uint32_t> unpack_block(size_t count, uint64_t x, const uint64_t offset, F f,
      const uint64_t A, const uint64_t B, uint32_t A_index, uint32_t B_index,
      uint64_t *result, size_t r_index){

      (void) A, (void) B; (void) A_index; (void) B_index;(void) offset; (void)f;
      (void) result; (void) r_index;

      count += _mm_popcnt_u64(x);      
      return std::make_tuple(count,0,0);
    }
    template<typename F>
    static inline size_t range(const __m256 r, const __m256 A, const __m256 B, 
      size_t count, uint64_t *result, size_t r_index, uint32_t *index_data, const uint64_t offset, F f, 
      const uint32_t *A_index, const uint32_t *B_index){

      (void) A; (void) B; (void) result; (void) index_data; (void) offset;
      (void) f; (void) A_index; (void) B_index; (void) r_index;

      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,0));
      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,1));
      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,2));
      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,3));

      return count;
    }
    template<typename F>
    static inline std::tuple<size_t,uint32_t,uint32_t> block(const __m256 r, const __m256 A, const __m256 B, 
      size_t count, uint64_t *result, size_t r_index, const uint64_t offset, F f, 
      uint32_t A_index, uint32_t B_index){

      (void) A; (void) B; (void) result; (void) offset;
      (void) f; (void) A_index; (void) B_index; (void) r_index;

      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,0));
      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,1));
      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,2));
      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,3));

      return std::make_tuple(count,0,0);
    }
  };

  /////
  struct bs_materialize_null{
     static inline void write(uint64_t *r, size_t j, uint64_t data){
      r[j] = data;
     }
     static inline uint8_t* adjust_write_pointer(uint8_t *C,size_t count,size_t old_count,size_t bytes_per_block){
        if(old_count != count){
          C += bytes_per_block;
        }
        return C;
     }
     static inline uint8_t* advanceC(uint8_t *C, size_t num){
      return C+num;
     }
    static inline uint64_t*  write_range_index(uint8_t *d_in, uint64_t start_index){
      uint64_t * const C = (uint64_t*)(d_in+sizeof(uint64_t));
      uint64_t * const c_index = (uint64_t*) d_in;
      c_index[0] = start_index;
      return C;
    }
    static inline uint32_t* index_data(uint64_t * const C, size_t total_size){
      return (uint32_t*)(total_size+C);
    }
    static inline void write_block_header(uint32_t *C, const uint32_t data, const uint32_t count){
      *C = data;
      *(C+1) = count; 
    }

    template<typename F>
    static inline size_t unpack_range(size_t count, uint64_t x, const uint64_t offset, F f,
      const uint64_t A, const uint64_t B, const uint32_t *A_index, const uint32_t *B_index,
      uint64_t *result, size_t r_index, uint32_t *index_data){

      (void) A, (void) B; (void) A_index; (void) B_index;(void) offset; (void)f;

      index_data[r_index] = count;
      count += _mm_popcnt_u64(x);      
      result[r_index] = x;
      return count;
    }

    template<typename F>
    static inline std::tuple<size_t,uint32_t,uint32_t> unpack_block(size_t count, uint64_t x, const uint64_t offset, F f,
      const uint64_t A, const uint64_t B, uint32_t A_index, uint32_t B_index,
      uint64_t *result, size_t r_index){

      (void) A, (void) B; (void) A_index; (void) B_index; (void) offset; (void)f;

      count += _mm_popcnt_u64(x);      
      result[r_index] = x;

      return std::make_tuple(count,0,0);
    }

    template<typename F>
    static inline size_t range(const __m256 r, const __m256 A, const __m256 B, 
      size_t count, uint64_t *result, size_t r_index, uint32_t *index_data, const uint64_t offset, F f, 
      const uint32_t *A_index, const uint32_t *B_index){

      (void) A; (void) B; (void) result; (void) index_data; (void) offset;
      (void) f; (void) A_index; (void) B_index;
      _mm256_storeu_ps((float*)(result+r_index), r);

      index_data[0+r_index] = count;
      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,0));
      index_data[1+r_index] = count;
      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,1));
      index_data[2+r_index] = count;
      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,2));
      index_data[3+r_index] = count;
      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,3));

      return count;
    }
    template<typename F>
    static inline std::tuple<size_t,uint32_t,uint32_t> block(const __m256 r, const __m256 A, const __m256 B, 
      size_t count, uint64_t *result, size_t r_index, const uint64_t offset, F f, 
      uint32_t A_index, uint32_t B_index){

      (void) A; (void) B; (void) result; (void) offset;
      (void) f; (void) A_index; (void) B_index;
      _mm256_storeu_ps((float*)(result+r_index), r);

      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,0));
      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,1));
      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,2));
      count += _mm_popcnt_u64(_mm256_extract_epi64((__m256i)r,3));

      return std::make_tuple(count,0,0);
    }
  };

  //////
  struct bs_unpack_materialize{
     static inline void write(uint64_t *r, size_t j, uint64_t data){
      r[j] = data;
     }
     static inline uint8_t* adjust_write_pointer(uint8_t *C,size_t count,size_t old_count,size_t bytes_per_block){
        if(old_count != count){
          C += bytes_per_block;
        }
        return C;
     }
     static inline uint8_t* advanceC(uint8_t *C, size_t num){
      return C+num;
     }
    static inline uint64_t*  write_range_index(uint8_t *d_in, uint64_t start_index){
      uint64_t * const C = (uint64_t*)(d_in+sizeof(uint64_t));
      uint64_t * const c_index = (uint64_t*) d_in;
      c_index[0] = start_index;
      return C;
    }
    static inline uint32_t* index_data(uint64_t * const C, size_t total_size){
      return (uint32_t*)(total_size+C);
    }
    static inline void write_block_header(uint32_t *C, const uint32_t data, const uint32_t count){
      *C = data;
      *(C+1) = count; 
    }

    template<typename F>
    static inline std::tuple<size_t,uint32_t,uint32_t> unpack_block(size_t count, uint64_t x, const uint64_t offset, F f,
      const uint64_t A, const uint64_t B, uint32_t A_index, uint32_t B_index,
      uint64_t *result, size_t r_index){

      for(size_t j = 0; j < BITS_PER_WORD; j++){
        if((x >> j) % 2) {
          const uint32_t a_i = get_num_set(offset+j,A,A_index);
          const uint32_t b_i = get_num_set(offset+j,B,B_index);
          const size_t ret = f(offset+j,a_i,b_i);
          count += ret;
          x ^= (-ret ^ x) & (1 << j); //sets the j'th bit to the output of the function
        }
      }
      result[r_index] = x;
      A_index += _mm_popcnt_u64(A);
      B_index += _mm_popcnt_u64(B);
      return std::make_tuple(count,A_index,B_index);
    }

    template<typename F>
    static inline size_t unpack_range(size_t count, uint64_t x, const uint64_t offset, F f,
      const uint64_t A, const uint64_t B, const uint32_t *A_index, const uint32_t *B_index,
      uint64_t *result, size_t r_index, uint32_t *index_data){

      index_data[r_index] = count;
      for(size_t j = 0; j < BITS_PER_WORD; j++){
        if((x >> j) % 2) {
          const uint32_t a_i = get_num_set(offset+j,A,*A_index);
          const uint32_t b_i = get_num_set(offset+j,B,*B_index);
          const size_t ret = f(offset+j,a_i,b_i);
          count += ret;
          x ^= (-ret ^ x) & (1 << j); //sets the j'th bit to the output of the function
        }
      }
      result[r_index] = x;
      return count;
    }
    template<typename F>
    static inline size_t range(const __m256 r, const __m256 A, const __m256 B, 
      size_t count, uint64_t *result, size_t r_index, uint32_t *index_data, const uint64_t offset, F f, 
      const uint32_t *A_index, const uint32_t *B_index){

      count = unpack_range(
        count,
        _mm256_extract_epi64((__m256i)r,0),offset+0*BITS_PER_WORD,f,
        _mm256_extract_epi64((__m256i)A,0),_mm256_extract_epi64((__m256i)B,0),
        A_index+0,B_index+0,result,0+r_index,index_data);
      count = unpack_range(
        count,
        _mm256_extract_epi64((__m256i)r,1),offset+1*BITS_PER_WORD,f,
        _mm256_extract_epi64((__m256i)A,1),_mm256_extract_epi64((__m256i)B,1),
        A_index+1,B_index+1,result,1+r_index,index_data);
      count = unpack_range(
        count,
        _mm256_extract_epi64((__m256i)r,2),offset+2*BITS_PER_WORD,f,
        _mm256_extract_epi64((__m256i)A,2),_mm256_extract_epi64((__m256i)B,2),
        A_index+2,B_index+2,result,2+r_index,index_data);
      count = unpack_range(
        count,
        _mm256_extract_epi64((__m256i)r,3),offset+3*BITS_PER_WORD,f,
        _mm256_extract_epi64((__m256i)A,3),_mm256_extract_epi64((__m256i)B,3),
        A_index+3,B_index+3,result,3+r_index,index_data);
      
      return count;
    }
    template<typename F>
    static inline std::tuple<size_t,uint32_t,uint32_t> block(const __m256 r, const __m256 A, const __m256 B, 
      size_t count, uint64_t *result, size_t r_index, const uint64_t offset, F f, 
      uint32_t A_index, uint32_t B_index){

        auto tup = unpack_block(
          count,
          _mm256_extract_epi64((__m256i)r,0),offset+0*BITS_PER_WORD,f,
          _mm256_extract_epi64((__m256i)A,0),_mm256_extract_epi64((__m256i)B,0),
          A_index,B_index,result,0+r_index);
        count = std::get<0>(tup);
        A_index = std::get<1>(tup);
        B_index = std::get<2>(tup);
        tup = unpack_block(
          count,
          _mm256_extract_epi64((__m256i)r,1),offset+1*BITS_PER_WORD,f,
          _mm256_extract_epi64((__m256i)A,1),_mm256_extract_epi64((__m256i)B,1),
          A_index,B_index,result,1+r_index);
        count = std::get<0>(tup);
        A_index = std::get<1>(tup);
        B_index = std::get<2>(tup);
        tup = unpack_block(
          count,
          _mm256_extract_epi64((__m256i)r,2),offset+2*BITS_PER_WORD,f,
          _mm256_extract_epi64((__m256i)A,2),_mm256_extract_epi64((__m256i)B,2),
          A_index,B_index,result,2+r_index);
        count = std::get<0>(tup);
        A_index = std::get<1>(tup);
        B_index = std::get<2>(tup);
        tup = unpack_block(
          count,
          _mm256_extract_epi64((__m256i)r,3),offset+3*BITS_PER_WORD,f,
          _mm256_extract_epi64((__m256i)A,3),_mm256_extract_epi64((__m256i)B,3),
          A_index,B_index,result,3+r_index);
        count = std::get<0>(tup);
        A_index = std::get<1>(tup);
        B_index = std::get<2>(tup);

      return std::make_tuple(count,A_index,B_index);
    }
  };

  /////
  struct bs_unpack_aggregate{
     static inline void write(uint64_t *r, size_t j, uint64_t data){
      (void)r;(void)j;(void)data;
      return;
     }
     static inline uint8_t* adjust_write_pointer(uint8_t *C,size_t count,size_t old_count,size_t bytes_per_block){
        (void) C; (void) count; (void) old_count; (void) bytes_per_block;
        return NULL;
     }
     static inline uint8_t* advanceC(uint8_t *C, size_t num){
      (void) C; (void) num;
      return NULL;
     }
    static inline uint64_t*  write_range_index(uint8_t *d_in, uint64_t start_index){
      (void) d_in; (void) start_index;
      return NULL;
    }
    static inline uint32_t* index_data(uint64_t * const C, size_t total_size){
      (void) C; (void) total_size;
      return NULL;
    }
    static inline void write_block_header(uint32_t *C, const uint32_t data, const uint32_t count){
      (void) C; (void) data; (void) count;
      return;
    }
    template<typename F>
    static inline size_t unpack_range(size_t count, uint64_t x, const uint64_t offset, F f,
      const uint64_t A, const uint64_t B, const uint32_t *A_index, const uint32_t *B_index,
      uint64_t *result, size_t r_index, uint32_t *index_data){

      (void) index_data; (void) result; (void) r_index;

      for(size_t j = 0; j < BITS_PER_WORD; j++){
        if((x >> j) % 2) {
          const uint32_t a_i = get_num_set(offset+j,A,*A_index);
          const uint32_t b_i = get_num_set(offset+j,B,*B_index);
          const size_t ret = f(offset+j,a_i,b_i);
          count += ret;
        }
      }
      return count;
    }
    template<typename F>
    static inline std::tuple<size_t,uint32_t,uint32_t> unpack_block(size_t count, uint64_t x, const uint64_t offset, F f,
      const uint64_t A, const uint64_t B, uint32_t A_index, uint32_t B_index,
      uint64_t *result, size_t r_index){

      (void) result; (void) r_index;

      for(size_t j = 0; j < BITS_PER_WORD; j++){
        if((x >> j) % 2) {
          const uint32_t a_i = get_num_set(offset+j,A,A_index);
          const uint32_t b_i = get_num_set(offset+j,B,B_index);
          const size_t ret = f(offset+j,a_i,b_i);
          count += ret;
        }
      }
      A_index += _mm_popcnt_u64(A);
      B_index += _mm_popcnt_u64(B);
      return std::make_tuple(count,A_index,B_index);    
    }
    template<typename F>
    static inline size_t range(const __m256 r, const __m256 A, const __m256 B, 
      size_t count, uint64_t *result, size_t r_index, uint32_t *index_data, const uint64_t offset, F f, 
      const uint32_t *A_index, const uint32_t *B_index){

      count = unpack_range(
          count,
          _mm256_extract_epi64((__m256i)r,0),offset+0*BITS_PER_WORD,f,
          _mm256_extract_epi64((__m256i)A,0),_mm256_extract_epi64((__m256i)B,0),
          A_index+0,B_index+0,result,0+r_index,index_data);

      count = unpack_range(
          count,
          _mm256_extract_epi64((__m256i)r,1),offset+1*BITS_PER_WORD,f,
          _mm256_extract_epi64((__m256i)A,1),_mm256_extract_epi64((__m256i)B,1),
          A_index+1,B_index+1,result,1+r_index,index_data);

      count = unpack_range(
          count,
          _mm256_extract_epi64((__m256i)r,2),offset+2*BITS_PER_WORD,f,
          _mm256_extract_epi64((__m256i)A,2),_mm256_extract_epi64((__m256i)B,2),
          A_index+2,B_index+2,result,2+r_index,index_data);

      count = unpack_range(
          count,
          _mm256_extract_epi64((__m256i)r,3),offset+3*BITS_PER_WORD,f,
          _mm256_extract_epi64((__m256i)A,3),_mm256_extract_epi64((__m256i)B,3),
          A_index+3,B_index+3,result,3+r_index,index_data);

      return count;
    }
    template<typename F>
    static inline std::tuple<size_t,uint32_t,uint32_t> block(const __m256 r, const __m256 A, const __m256 B, 
      size_t count, uint64_t *result, size_t r_index, const uint64_t offset, F f, 
      uint32_t A_index, uint32_t B_index){


      auto tup = unpack_block(
        count,
        _mm256_extract_epi64((__m256i)r,0),offset+0*BITS_PER_WORD,f,
        _mm256_extract_epi64((__m256i)A,0),_mm256_extract_epi64((__m256i)B,0),
        A_index,B_index,result,0+r_index);
      count = std::get<0>(tup);
      A_index = std::get<1>(tup);
      B_index = std::get<2>(tup);

      tup = unpack_block(
        count,
        _mm256_extract_epi64((__m256i)r,1),offset+1*BITS_PER_WORD,f,
        _mm256_extract_epi64((__m256i)A,1),_mm256_extract_epi64((__m256i)B,1),
        A_index,B_index,result,1+r_index);
      count = std::get<0>(tup);
      A_index = std::get<1>(tup);
      B_index = std::get<2>(tup);

      tup = unpack_block(
        count,
        _mm256_extract_epi64((__m256i)r,2),offset+2*BITS_PER_WORD,f,
        _mm256_extract_epi64((__m256i)A,2),_mm256_extract_epi64((__m256i)B,2),
        A_index,B_index,result,2+r_index);
      count = std::get<0>(tup);
      A_index = std::get<1>(tup);
      B_index = std::get<2>(tup);

      tup = unpack_block(
        count,
        _mm256_extract_epi64((__m256i)r,3),offset+3*BITS_PER_WORD,f,
        _mm256_extract_epi64((__m256i)A,3),_mm256_extract_epi64((__m256i)B,3),
        A_index,B_index,result,3+r_index);
      count = std::get<0>(tup);
      A_index = std::get<1>(tup);
      B_index = std::get<2>(tup);

      return std::make_tuple(count,A_index,B_index);
    }
  };


  template<typename F, typename FA, typename FB, typename FCA, typename FCB, typename FEA, typename FEB>
  inline void find_matching_offsets(const uint8_t *A, 
    const size_t lenA,
    const size_t increment_a, 
    FA fa,
    FCA fca,
    FEA fea,
    const uint8_t *B,
    const size_t lenB, 
    const size_t increment_b,
    FB fb,
    FCB fcb,
    FEB feb, 
    F f){

      uint32_t a_index = 0;
      uint32_t b_index = 0;
      const uint8_t *endA = A + lenA*increment_a;
      const uint8_t *endB = B + lenB*increment_b;

      if (lenA == 0){
        feb(B,endB,increment_b);
        return;
      } else if(lenB == 0){
        fea(A,endA,increment_a);
        return;
      }

      while (1) {
          while (fa(*((uint32_t*)A)) < fb(*((uint32_t*)B))) {
  SKIP_FIRST_COMPARE:
              fca(*(uint32_t*)A);
              A += increment_a;
              ++a_index;
              if (A == endA){
                feb(B,endB,increment_b);
                return;
              }
          }
          while (fa(*((uint32_t*)A)) > fb(*((uint32_t*)B)) ) {
              fcb(*(uint32_t*)B);
              B += increment_b;
              ++b_index;
              if (B == endB){
                fea(A,endA,increment_a);
                return;
              }
          }
          if (fa(*((uint32_t*)A)) == fb(*((uint32_t*)B))) {
              auto pair = f(a_index,b_index,*(uint32_t*)A);
              A += increment_a*std::get<0>(pair);
              a_index += std::get<0>(pair);
              B += increment_b*std::get<1>(pair);
              b_index += std::get<1>(pair);
              if (A == endA){
                feb(B,endB,increment_b);
                return;
              } else if (B == endB){
                fea(A,endA,increment_a);
                return;
              }
          } else {
              goto SKIP_FIRST_COMPARE;
          }
      }
      return; // NOTREACHED
  }

  template <class N,typename F>
  inline size_t intersect_range_block(
    uint64_t * const result_data, 
    uint32_t * const index_data, 
    const uint64_t * const A, 
    const uint64_t * const B,
    const size_t b_size,
    const uint64_t offset,
    F f,
    const uint32_t *A32_index,
    const uint32_t *B32_index){
    
    size_t i = 0;
    size_t count = 0;

    #if VECTORIZE == 1
    while((i+255) < b_size){
      const size_t vector_index = (i/BITS_PER_WORD);
      const __m256 m1 = _mm256_loadu_ps((float*)(A + vector_index));
      const __m256 m2 = _mm256_loadu_ps((float*)(B + vector_index));
      const __m256 r = _mm256_and_ps(m1, m2);

      count = N::range(r,m1,m2,count,result_data,vector_index,
        index_data,(offset+vector_index)*BITS_PER_WORD,
        f,A32_index+vector_index,B32_index+vector_index);
    
      i += 256;
    }
    #endif

    //64 bits per word
    for(; i < b_size; i+=64){
      const size_t vector_index = (i/BITS_PER_WORD);
      const uint64_t r = A[vector_index] & B[vector_index]; 
      count = N::unpack_range(count,r,offset+(vector_index)*BITS_PER_WORD,f,
        A[vector_index],B[vector_index],A32_index+vector_index,B32_index+vector_index,
        result_data,vector_index,index_data);
    }

    return count;
  }

  template <class N, typename F>
  inline size_t intersect_block(
    uint64_t * const result_data, 
    const uint64_t * const A, 
    const uint64_t * const B,
    const size_t b_size,
    const uint64_t offset,
    F f,
    uint32_t i_a,
    uint32_t i_b){
    
    size_t i = 0;
    size_t count = 0;

    #if VECTORIZE == 1
    while((i+255) < b_size){
      const size_t vector_index = (i/BITS_PER_WORD);
      const __m256 m1 = _mm256_loadu_ps((float*)(A + vector_index));
      const __m256 m2 = _mm256_loadu_ps((float*)(B + vector_index));
      const __m256 r = _mm256_and_ps(m1, m2);

      auto tup = N::block(r,m1,m2,count,result_data,vector_index,
        (offset+vector_index)*BITS_PER_WORD,
        f,i_a,i_b);

      count = std::get<0>(tup);
      i_a = std::get<1>(tup);
      i_b = std::get<2>(tup);

      i += 256;
    }
    #endif

    //64 bits per word
    for(; i < b_size; i+=BITS_PER_WORD){
      const size_t vector_index = (i/BITS_PER_WORD);
      const uint64_t r = A[vector_index] & B[vector_index]; 
      
      auto tup = N::unpack_block(count,r,(offset+vector_index)*BITS_PER_WORD,f,
        A[vector_index],B[vector_index],i_a,i_b,
        result_data,vector_index);

      count = std::get<0>(tup);
      i_a = std::get<1>(tup);
      i_b = std::get<2>(tup);
    }

    return count;
  }

  template <class N,typename F>
  inline Set<range_bitset>* run_intersection(Set<range_bitset> *C_in, const Set<range_bitset> *A_in, const Set<range_bitset> *B_in, F f){
    long count = 0l;
    C_in->number_of_bytes = 0;
    C_in->range = 0;

    if(A_in->number_of_bytes > 0 && B_in->number_of_bytes > 0){
      const uint64_t *a_index = (uint64_t*) A_in->data;
      const uint64_t *b_index = (uint64_t*) B_in->data;

      const uint64_t * const A = (uint64_t*)(A_in->data+sizeof(uint64_t));
      const uint64_t * const B = (uint64_t*)(B_in->data+sizeof(uint64_t));
      const size_t s_a = range_bitset::get_number_of_words(A_in->number_of_bytes); 
      const size_t s_b = range_bitset::get_number_of_words(B_in->number_of_bytes);

      const bool a_big = a_index[0] > b_index[0];
      const uint64_t start_index = (a_big) ? a_index[0] : b_index[0];
      const uint64_t a_start_index = (a_big) ? 0:(b_index[0]-a_index[0]);
      const uint64_t b_start_index = (a_big) ? (a_index[0]-b_index[0]):0;

      const uint64_t end_index = ((a_index[0]+s_a) > (b_index[0]+s_b)) ? (b_index[0]+s_b):(a_index[0]+s_a);
      const uint64_t total_size = (start_index > end_index) ? 0:(end_index-start_index);

      const uint32_t* A32_index = (uint32_t*)(A+s_a)+a_start_index;
      const uint32_t* B32_index = (uint32_t*)(B+s_b)+b_start_index;

      //16 uint16_ts
      //8 ints
      //4 longs
      uint64_t * const C = N::write_range_index(C_in->data,start_index);
      uint32_t * index_write = N::index_data(C,total_size);

      count = intersect_range_block<N>(C,index_write,A+a_start_index,B+b_start_index,total_size*64,start_index,f,A32_index,B32_index);

      C_in->number_of_bytes = total_size*(sizeof(uint64_t)+sizeof(uint32_t))+sizeof(uint64_t);
      C_in->range = (total_size+start_index)*64; //num words = total size, 64 values per word
    }

    C_in->cardinality = count;
    C_in->type= type::RANGE_BITSET;

    return C_in;
  }
  
  inline Set<range_bitset>* set_intersect(Set<range_bitset> *C_in, const Set<range_bitset> *A_in, const Set<range_bitset> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    return run_intersection<bs_materialize_null>(C_in,A_in,B_in,f);
  }

  inline size_t set_intersect(const Set<range_bitset> *A_in, const Set<range_bitset> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    Set<range_bitset> C_in;
    return run_intersection<bs_aggregate_null>(&C_in,A_in,B_in,f)->cardinality;
  }

  template <typename F>
  inline Set<range_bitset>* set_intersect(Set<range_bitset> *C_in, const Set<range_bitset> *A_in, const Set<range_bitset> *B_in, F f){
    return run_intersection<bs_unpack_materialize>(C_in,A_in,B_in,f);
  }

  template <typename F>
  inline size_t set_intersect(const Set<range_bitset> *A_in, const Set<range_bitset> *B_in, F f){
    Set<range_bitset> C_in;
    return run_intersection<bs_unpack_aggregate>(&C_in,A_in,B_in,f)->cardinality;
  }

  template <typename N, typename F>
  inline Set<block_bitset>* run_intersection(Set<block_bitset> *C_in,const Set<block_bitset> *A_in,const Set<block_bitset> *B_in, F f){
    if(A_in->number_of_bytes == 0 || B_in->number_of_bytes == 0){
      C_in->cardinality = 0;
      C_in->number_of_bytes = 0;
      C_in->type= type::BLOCK_BITSET;
      return C_in;
    }

    const size_t A_num_blocks = A_in->number_of_bytes/(2*sizeof(uint32_t)+(BLOCK_SIZE/8));
    const size_t B_num_blocks = B_in->number_of_bytes/(2*sizeof(uint32_t)+(BLOCK_SIZE/8));

    size_t count = 0;
    const size_t bytes_per_block = (BLOCK_SIZE/8);

    uint8_t *C = C_in->data;
    const uint8_t * const C_start = C_in->data;
    const uint8_t * const A_data = A_in->data+2*sizeof(uint32_t);
    const uint8_t * const B_data = B_in->data+2*sizeof(uint32_t);
    const uint32_t offset = 2*sizeof(uint32_t)+WORDS_PER_BLOCK*sizeof(uint64_t);
    
    auto check_f = [&](uint32_t d){(void) d; return;};
    auto finish_f = [&](const uint8_t *start, const uint8_t *end, size_t increment){
      (void) start, (void) end; (void) increment;
      return;};

    find_matching_offsets(A_in->data,A_num_blocks,offset,[&](uint32_t a){return a;},check_f,finish_f,
        B_in->data,B_num_blocks,offset,[&](uint32_t b){return b;},check_f,finish_f, 
        
        [&](uint32_t a_index, uint32_t b_index, uint32_t data){    
          N::write_block_header((uint32_t*)C,data,count);
          const size_t old_count = count;
          const uint32_t a_i = *(uint32_t*)(A_data+a_index*offset-sizeof(uint32_t)); 
          const uint32_t b_i = *(uint32_t*)(B_data+b_index*offset-sizeof(uint32_t)); 

          C = N::advanceC(C,2*sizeof(uint32_t));
          count += intersect_block<N>((uint64_t*)C,(uint64_t*)(A_data+a_index*offset),(uint64_t*)(B_data+b_index*offset),BLOCK_SIZE,data*WORDS_PER_BLOCK,f,a_i,b_i);
          C = N::adjust_write_pointer(C,count,old_count,bytes_per_block);
          std::cout << count << " " << (C-C_start) << std::endl;

          return std::make_pair(1,1); 
        }
    );   

    std::cout << "num bytes: " << C-C_start << std::endl;
    C_in->cardinality = count;
    C_in->number_of_bytes = C-C_start;
    C_in->type= type::BLOCK_BITSET;

    return C_in;
  }

  inline Set<block_bitset>* set_intersect(Set<block_bitset> *C_in, const Set<block_bitset> *A_in, const Set<block_bitset> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    return run_intersection<bs_materialize_null>(C_in,A_in,B_in,f);
  }

  inline size_t set_intersect(const Set<block_bitset> *A_in, const Set<block_bitset> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    Set<block_bitset> C_in;
    return run_intersection<bs_aggregate_null>(&C_in,A_in,B_in,f)->cardinality;
  }

  template <typename F>
  inline Set<block_bitset>* set_intersect(Set<block_bitset> *C_in, const Set<block_bitset> *A_in, const Set<block_bitset> *B_in, F f){
    return run_intersection<bs_unpack_materialize>(C_in,A_in,B_in,f);
  }

  template <typename F>
  inline size_t set_intersect(const Set<block_bitset> *A_in, const Set<block_bitset> *B_in, F f){
    Set<block_bitset> C_in;
    return run_intersection<bs_unpack_aggregate>(&C_in,A_in,B_in,f)->cardinality;
  }

}
#endif
