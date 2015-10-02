#ifndef _BLOCK_INTERSECTION_H_
#define _BLOCK_INTERSECTION_H_

#include "uinteger.hpp"
#include "bitset.hpp"
#include "hetero.hpp"

namespace ops{

  template<class N, typename F>
  inline Set<block>* set_intersect(Set<block> *C_in,const Set<block> *A_in,const Set<block> *B_in, F f){
    if(A_in->number_of_bytes == 0 || B_in->number_of_bytes == 0){
      C_in->cardinality = 0;
      C_in->number_of_bytes = 0;
      C_in->type= type::BLOCK;
      return C_in;
    }

    const size_t A_num_uint_bytes = ((size_t*)A_in->data)[0];
    uint8_t * A_uinteger_data = A_in->data+sizeof(size_t);
    uint8_t * A_new_bs_data = A_in->data+sizeof(size_t)+A_num_uint_bytes;
    const size_t A_num_bs_bytes = A_in->number_of_bytes-(sizeof(size_t)+A_num_uint_bytes);
    const size_t A_uint_card = A_num_uint_bytes/sizeof(uint32_t);

    const size_t B_num_uint_bytes = ((size_t*)B_in->data)[0];
    uint8_t * B_uinteger_data = B_in->data+sizeof(size_t);
    uint8_t * B_new_bs_data = B_in->data+sizeof(size_t)+B_num_uint_bytes;
    const size_t B_num_bs_bytes = B_in->number_of_bytes-(sizeof(size_t)+B_num_uint_bytes);
    const size_t B_uint_card = B_num_uint_bytes/sizeof(uint32_t);


    const Set<uinteger>A_I(A_uinteger_data,A_uint_card,A_num_uint_bytes,type::UINTEGER);
    const Set<uinteger>B_I(B_uinteger_data,B_uint_card,B_num_uint_bytes,type::UINTEGER);

    const Set<block_bitset>A_BS(A_new_bs_data,A_in->cardinality-A_uint_card,A_num_bs_bytes,type::BLOCK_BITSET);
    const Set<block_bitset>B_BS(B_new_bs_data,B_in->cardinality-B_uint_card,B_num_bs_bytes,type::BLOCK_BITSET);

    size_t count = 0;
    
    //const uint32_t offset = 2*sizeof(uint32_t)+WORDS_PER_BLOCK*sizeof(uint64_t);
    uint8_t *C_pointer = C_in->data+sizeof(size_t);
    uint32_t *C_data = (uint32_t*)C_pointer;

    ////////////////////////////////////////////////////////
    //Functions applied for A and B during the intersection
    auto data_functor = [&](uint32_t d){return d;};
    //For each value of A probe the B bitset
    auto check_a = [&](uint32_t a_d){
      //std::cout << "checking a: " << a_d  << std::endl;
      if(B_BS.find(a_d) != -1){
        C_data[count++] = a_d;
        f(a_d);
      }
      return;
    };
    //At the end if we have remaining A elements we must check them
    auto finish_a = [&](const uint8_t *start, const uint8_t *end, size_t increment){
     while(start < end){
      check_a(*(uint32_t*)start);
      start += increment;
     } 
     return;
    };
    auto check_b = [&](uint32_t a_d){
      if(A_BS.find(a_d) != -1){
        C_data[count++] = a_d;
        f(a_d);
      }
      return;
    };
    auto finish_b = [&](const uint8_t *start, const uint8_t *end, size_t increment){
     while(start < end){
      check_b(*(uint32_t*)start);
      start += increment;
     }
     return;
    };
    auto match = [&](uint32_t a_index, uint32_t b_index, uint32_t d){
      (void) a_index; (void) b_index;
      C_data[count++] = d;
      f(d);
      return std::make_pair<uint32_t,uint32_t>(1,1);
    };
    ////////////////////////////////////////////////////////

    find_matching_offsets(A_uinteger_data,A_uint_card,sizeof(uint32_t),data_functor,check_a,finish_a,
      B_uinteger_data,B_uint_card,sizeof(uint32_t),data_functor,check_b,finish_b,match);

    const size_t num_uint = count;
    ((size_t*)C_in->data)[0] = (num_uint*sizeof(uint32_t));
    C_pointer += (num_uint*sizeof(uint32_t));

    Set<block_bitset>BSBS(C_pointer);
    BSBS = ops::set_intersect<N>(&BSBS,&A_BS,&B_BS,f);
    //std::cout << "BS BS count: " << BSBS.cardinality << std::endl;
    count += BSBS.cardinality;

    C_in->cardinality = count;
    C_in->number_of_bytes = sizeof(size_t)+(num_uint*sizeof(uint32_t))+BSBS.number_of_bytes;
    C_in->type= type::BLOCK;

    return C_in;
  }

  inline Set<block>* set_intersect(Set<block> *C_in, const Set<block> *A_in, const Set<block> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    return set_intersect<unpack_null_bs>(C_in,A_in,B_in,f);
  }

  template <typename F>
  inline Set<block>* set_intersect(Set<block> *C_in, const Set<block> *A_in, const Set<block> *B_in, F f){
    return set_intersect<unpack_bitset>(C_in,A_in,B_in,f);
  }



}
#endif
