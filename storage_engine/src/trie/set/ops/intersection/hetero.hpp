#ifndef _HETERO_INTERSECTION_H_
#define _HETERO_INTERSECTION_H_

namespace ops{
  template<class N,typename F>
  inline size_t probe_block(
    uint32_t *result,
    uint32_t value,
    uint64_t *block,
    F f,
    size_t i_a,
    size_t i_b){
    const uint32_t probe_value = value % BLOCK_SIZE;
    const size_t word_to_check = probe_value / BITS_PER_WORD;
    const size_t bit_to_check = probe_value % BITS_PER_WORD;

    if((block[word_to_check] >> bit_to_check) % 2){
      
      for(size_t i = 0 ; i < word_to_check; i++){
        i_b += _mm_popcnt_u64(block[i]);
      }
      i_b = range_bitset::get_num_set(value,block[word_to_check],i_b);
      
      return N::scalar(value,result,f,i_a,i_b);
    } 
    return 0;
  }

  template<class N, typename F>
  inline Set<uinteger>* run_intersection(Set<uinteger> *C_in,const Set<uinteger> *A_in,const Set<range_bitset> *B_in, F f){
    uint32_t * C = (uint32_t*)C_in->data;
    const uint32_t * const A = (uint32_t*)A_in->data;
    const size_t s_a = A_in->cardinality;
    const size_t s_b = (B_in->number_of_bytes > 0) ? range_bitset::get_number_of_words(B_in->number_of_bytes):0;
    const uint64_t start_index = (B_in->number_of_bytes > 0) ? ((uint64_t*)B_in->data)[0]:0;

    const uint64_t * const B = (uint64_t*)(B_in->data+sizeof(uint64_t));
    const uint32_t * const B_index = (uint32_t*)(B+s_b);

    size_t count = 0;
    for(size_t i = 0; i < s_a; i++){
      const uint32_t cur = A[i];
      const size_t cur_index = range_bitset::word_index(cur);
      if((cur_index < (s_b+start_index)) && (cur_index >= start_index) && range_bitset::is_set(cur,B,start_index)){
        const uint32_t b_index = range_bitset::get_num_set(cur,B[cur_index],B_index[cur_index-start_index]);
        const size_t num_hit = N::scalar(cur,C,f,i,b_index);
        count += num_hit;
        C = N::advanceC(C,num_hit);
      } else if(cur_index >= (s_b+start_index)){
        break;
      }
    }

    C_in->cardinality = count;
    C_in->range = N::range((uint32_t*)C_in->data,C_in->cardinality);
    C_in->number_of_bytes = count*sizeof(uint32_t);
    C_in->type= type::UINTEGER;

    return C_in;
  }

  inline size_t set_intersect(const Set<uinteger> *A_in, const Set<range_bitset> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    Set<uinteger> C_in;
    return run_intersection<unpack_aggregate>(&C_in,A_in,B_in,f)->cardinality;
  }

  inline size_t set_intersect(const Set<range_bitset> *A_in, const Set<uinteger> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    Set<uinteger> C_in;
    return run_intersection<unpack_aggregate>(&C_in,B_in,A_in,f)->cardinality;
  }

  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in, const Set<uinteger> *A_in, const Set<range_bitset> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    return run_intersection<unpack_materialize>(C_in,A_in,B_in,f);
  }

  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in, const Set<range_bitset> *A_in, const Set<uinteger> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    return run_intersection<unpack_materialize>(C_in,B_in,A_in,f);
  }

  template <typename F>
  inline size_t set_intersect(const Set<uinteger> *A_in, const Set<range_bitset> *B_in, F f){
    Set<uinteger> C_in;
    return run_intersection<unpack_uinteger_aggregate>(&C_in,A_in,B_in,f)->cardinality;
  }

  template <typename F>
  inline size_t set_intersect(const Set<range_bitset> *A_in, const Set<uinteger> *B_in, F f){
    Set<uinteger> C_in;
    auto f_in = [&](uint32_t data, uint32_t a_i, uint32_t b_i){return f(data,b_i,a_i);};
    return run_intersection<unpack_uinteger_aggregate>(&C_in,B_in,A_in,f_in)->cardinality;
  }

  template <typename F>
  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in, const Set<uinteger> *A_in, const Set<range_bitset> *B_in, F f){
    return run_intersection<unpack_uinteger_materialize>(C_in,A_in,B_in,f);
  }
  template <typename F>
  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in, const Set<range_bitset> *A_in, const Set<uinteger> *B_in, F f){
    auto f_in = [&](uint32_t data, uint32_t a_i, uint32_t b_i){return f(data,b_i,a_i);};
    return run_intersection<unpack_uinteger_materialize>(C_in,B_in,A_in,f_in);
  }

  template<class N, typename F>
  inline Set<uinteger>* run_intersection(Set<uinteger> *C_in,const Set<uinteger> *A_in,const Set<block_bitset> *B_in, F f){
    if(A_in->number_of_bytes == 0 || B_in->number_of_bytes == 0){
      C_in->cardinality = 0;
      C_in->number_of_bytes = 0;
      C_in->type= type::UINTEGER;
      return C_in;
    }

    const size_t B_num_blocks = B_in->number_of_bytes/(2*sizeof(uint32_t)+(BLOCK_SIZE/8));

    size_t count = 0;
    uint32_t *C = (uint32_t*)C_in->data;

    const uint32_t * const A_data = (uint32_t*)A_in->data;
    const uint8_t * const B_data = B_in->data+2*sizeof(uint32_t);
    const uint32_t offset = 2*sizeof(uint32_t)+WORDS_PER_BLOCK*sizeof(uint64_t);
    
    auto check_f = [&](uint32_t d){(void) d; return;};
    auto finish_f = [&](const uint8_t *start, const uint8_t *end, size_t increment){
      (void) start, (void) end; (void) increment;
      return;};
    find_matching_offsets(A_in->data,A_in->cardinality,sizeof(uint32_t),[&](uint32_t a){return a >> ADDRESS_BITS_PER_BLOCK;},check_f,finish_f,
        B_in->data,B_num_blocks,offset,[&](uint32_t b){return b;},check_f,finish_f,
        
        //the uinteger value is returned in data->not the best interface 
        [&](uint32_t a_index, uint32_t b_index, uint32_t data){    
          const uint32_t start_a_index = a_index;
          const size_t num_hit = probe_block<N>(C,data,(uint64_t*)(B_data+offset*b_index),f,a_index,*(uint32_t*)((B_data+offset*b_index)-sizeof(uint32_t)));
          count += num_hit;
          N::advanceC(C,num_hit);
          ++a_index;
          while( ( &A_data[a_index] < (A_data+A_in->cardinality) ) &&
            (A_data[a_index] >> ADDRESS_BITS_PER_BLOCK) == (data >> ADDRESS_BITS_PER_BLOCK)){
            count += probe_block<N>(&C[count],A_data[a_index],(uint64_t*)(B_data+offset*b_index),f,a_index,*(uint32_t*)((B_data+offset*b_index)-sizeof(uint32_t)));
            ++a_index;
          }
          return std::make_pair(a_index-start_a_index,1);
        }
    );   

    C_in->cardinality = count;
    C_in->number_of_bytes = count*sizeof(uint32_t);
    C_in->type= type::UINTEGER;

    return C_in;
  }


  inline size_t set_intersect(const Set<uinteger> *A_in, const Set<block_bitset> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    Set<uinteger> C_in;
    return run_intersection<unpack_aggregate>(&C_in,A_in,B_in,f)->cardinality;
  }

  inline size_t set_intersect(const Set<block_bitset> *A_in, const Set<uinteger> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    Set<uinteger> C_in;
    return run_intersection<unpack_aggregate>(&C_in,B_in,A_in,f)->cardinality;
  }

  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in, const Set<uinteger> *A_in, const Set<block_bitset> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    return run_intersection<unpack_materialize>(C_in,A_in,B_in,f);
  }

  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in, const Set<block_bitset> *A_in, const Set<uinteger> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    return run_intersection<unpack_materialize>(C_in,B_in,A_in,f);
  }

  template <typename F>
  inline size_t set_intersect(const Set<uinteger> *A_in, const Set<block_bitset> *B_in, F f){
    Set<uinteger> C_in;
    return run_intersection<unpack_uinteger_aggregate>(&C_in,A_in,B_in,f)->cardinality;
  }

  template <typename F>
  inline size_t set_intersect(const Set<block_bitset> *A_in, const Set<uinteger> *B_in, F f){
    Set<uinteger> C_in;
    auto f_in = [&](uint32_t data, uint32_t a_i, uint32_t b_i){return f(data,b_i,a_i);};
    return run_intersection<unpack_uinteger_aggregate>(&C_in,B_in,A_in,f_in)->cardinality;
  }

  template <typename F>
  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in, const Set<uinteger> *A_in, const Set<block_bitset> *B_in, F f){
    return run_intersection<unpack_uinteger_materialize>(C_in,A_in,B_in,f);
  }
  template <typename F>
  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in, const Set<block_bitset> *A_in, const Set<uinteger> *B_in, F f){
    auto f_in = [&](uint32_t data, uint32_t a_i, uint32_t b_i){return f(data,b_i,a_i);};
    return run_intersection<unpack_uinteger_materialize>(C_in,B_in,A_in,f_in);
  }
  
  template<class N, typename F>
  inline Set<block_bitset>* run_intersection(Set<block_bitset> *C_in, const Set<block_bitset> *A_in, const Set<range_bitset> *B_in, F f){
    size_t count = 0;
    size_t num_bytes = 0;

    if(A_in->number_of_bytes > 0 && B_in->number_of_bytes > 0){
      const size_t A_num_blocks = A_in->number_of_bytes/(2*sizeof(uint32_t)+(BLOCK_SIZE/8));
      uint8_t *C = C_in->data;
      const uint32_t offset = 2*sizeof(uint32_t)+WORDS_PER_BLOCK*sizeof(uint64_t);

      const uint64_t *b_index = (uint64_t*) B_in->data;
      const uint64_t * const B = (uint64_t*)(B_in->data+sizeof(uint64_t));
      const size_t s_b = range_bitset::get_number_of_words(B_in->number_of_bytes);
      const uint64_t b_end = (b_index[0]+s_b);
      const uint64_t b_start = b_index[0];

      for(size_t i = 0; i < A_num_blocks; i++){
        uint32_t index = WORDS_PER_BLOCK * (*((uint32_t*)(A_in->data+i*offset)));
        if( (index+WORDS_PER_BLOCK-1) >= b_start && index < b_end){
          size_t j = 0;
          uint64_t *A_data = (uint64_t*)(A_in->data+ i*offset + 2*sizeof(uint32_t));
          uint32_t A_index = *(uint32_t*)(A_in->data+ i*offset + 1*sizeof(uint32_t));
          uint32_t BB_index = *( ((uint32_t*)(B+s_b)) + (index-b_start) );

          N::write_block_header((uint32_t*)C,index/WORDS_PER_BLOCK,count);
          C = N::advanceC(C,2*sizeof(uint32_t));
          const size_t old_count = count;
          while(index < b_start){
            N::write((uint64_t*)C,j,0);
            A_index += _mm_popcnt_u64(A_data[j]);
            index++;
            j++;
          }
          while(j < WORDS_PER_BLOCK && index < b_end){
            const uint64_t result = A_data[j] & B[index-b_start];

            auto tup = N::unpack_block(count,result,index*BITS_PER_WORD,f,
                  A_data[j],B[index-b_start],A_index,BB_index,(uint64_t*)C,j);
            count = std::get<0>(tup);
            A_index = std::get<1>(tup);
            BB_index = std::get<2>(tup);

            j++;
            index++;
          }
          while(j < WORDS_PER_BLOCK){
            N::write((uint64_t*)C,j,0);
            j++;
          }

          C = N::adjust_write_pointer(C,count,old_count,offset);
        }
        if(index >= b_end)
          break;
      }
    }

    C_in->cardinality = count;
    C_in->number_of_bytes = num_bytes;
    C_in->type= type::BLOCK_BITSET;

    return C_in;
  }

  inline size_t set_intersect(const Set<block_bitset> *A_in, const Set<range_bitset> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    Set<block_bitset> C_in;
    return run_intersection<bs_aggregate_null>(&C_in,A_in,B_in,f)->cardinality;
  }

  inline size_t set_intersect(const Set<range_bitset> *A_in, const Set<block_bitset> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    Set<block_bitset> C_in;
    return run_intersection<bs_aggregate_null>(&C_in,B_in,A_in,f)->cardinality;
  }

  inline Set<block_bitset>* set_intersect(Set<block_bitset> *C_in, const Set<block_bitset> *A_in, const Set<range_bitset> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    return run_intersection<bs_materialize_null>(C_in,A_in,B_in,f);
  }

  inline Set<block_bitset>* set_intersect(Set<block_bitset> *C_in, const Set<range_bitset> *A_in, const Set<block_bitset> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    return run_intersection<bs_materialize_null>(C_in,B_in,A_in,f);
  }

  template <typename F>
  inline size_t set_intersect(const Set<block_bitset> *A_in, const Set<range_bitset> *B_in, F f){
    auto f_in = [&](uint32_t data, uint32_t a_i, uint32_t b_i){return f(data,b_i,a_i);};
    Set<block_bitset> C_in;
    return run_intersection<bs_unpack_aggregate>(&C_in,A_in,B_in,f_in)->cardinality;
  }


  template <typename F>
  inline size_t set_intersect(const Set<range_bitset> *A_in, const Set<block_bitset> *B_in, F f){
    Set<block_bitset> C_in;
    return run_intersection<bs_unpack_aggregate>(&C_in,B_in,A_in,f)->cardinality;
  }

  template <typename F>
  inline Set<block_bitset>* set_intersect(Set<block_bitset> *C_in, const Set<block_bitset> *A_in, const Set<range_bitset> *B_in, F f){
    auto f_in = [&](uint32_t data, uint32_t a_i, uint32_t b_i){return f(data,b_i,a_i);};
    return run_intersection<bs_unpack_materialize>(C_in,A_in,B_in,f_in);
  }
  template <typename F>
  inline Set<block_bitset>* set_intersect(Set<block_bitset> *C_in, const Set<range_bitset> *A_in, const Set<block_bitset> *B_in, F f){
    return run_intersection<bs_unpack_materialize>(C_in,B_in,A_in,f);
  }
  
}
#endif
