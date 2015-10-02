#ifndef _SET_INTERSECTION_H_
#define _SET_INTERSECTION_H_

#include "uinteger.hpp"
#include "bitset.hpp"
#include "hetero.hpp"

namespace ops{
  template<typename F>
  Set<hybrid>* set_intersect(Set<hybrid> *C_in,const Set<hybrid> *A_in,const Set<hybrid> *B_in, F f){
    if(A_in->cardinality == 0 || B_in->cardinality == 0){
      C_in->cardinality = 0;
      C_in->number_of_bytes = 0;
      C_in->type = type::UINTEGER;
      return C_in;
    }

    switch (A_in->type) {
        case type::UINTEGER:
          switch (B_in->type) {
            case type::UINTEGER:
              return (Set<hybrid>*)set_intersect((Set<uinteger>*)C_in,(const Set<uinteger>*)A_in,(const Set<uinteger>*)B_in,f);
              break;
            case type::RANGE_BITSET:
              return (Set<hybrid>*)set_intersect((Set<uinteger>*)C_in,(const Set<uinteger>*)A_in,(const Set<range_bitset>*)B_in,f);
              break;
            case type::BLOCK_BITSET:
              return (Set<hybrid>*)set_intersect((Set<uinteger>*)C_in,(const Set<uinteger>*)A_in,(const Set<block_bitset>*)B_in,f);
              break;
            default:
              break;
          }
        break;
        case type::RANGE_BITSET:
          switch (B_in->type) {
            case type::UINTEGER:
              return (Set<hybrid>*)set_intersect((Set<uinteger>*)C_in,(const Set<uinteger>*)B_in,(const Set<range_bitset>*)A_in,f);
            break;
            case type::BLOCK_BITSET:
              return (Set<hybrid>*)set_intersect((Set<block_bitset>*)C_in,(const Set<range_bitset>*)A_in,(const Set<block_bitset>*)B_in,f);
            break;
            case type::RANGE_BITSET:
              return (Set<hybrid>*)set_intersect((Set<range_bitset>*)C_in,(const Set<range_bitset>*)A_in,(const Set<range_bitset>*)B_in,f);
            break;
            default:
            break;
          }
        break;
        case type::BLOCK_BITSET:
          switch (B_in->type) {
            case type::UINTEGER:
              return (Set<hybrid>*)set_intersect((Set<uinteger>*)C_in,(const Set<uinteger>*)B_in,(const Set<block_bitset>*)A_in,f);
            break;
            case type::RANGE_BITSET:
              return (Set<hybrid>*)set_intersect((Set<block_bitset>*)C_in,(const Set<block_bitset>*)A_in,(const Set<range_bitset>*)B_in,f);
            break;
            case type::BLOCK_BITSET:
              return (Set<hybrid>*)set_intersect((Set<block_bitset>*)C_in,(const Set<block_bitset>*)A_in,(const Set<block_bitset>*)B_in,f);
            break;
            default:
            break;
          }
        break;
        default:
        break;
    }

    std::cout << "SET INTERSECTION ERROR 0" << std::endl;
    return C_in;
  }
  Set<hybrid>* set_intersect(Set<hybrid> *C_in, const Set<hybrid> *A_in, const Set<hybrid> *B_in){
    if(A_in->cardinality == 0 || B_in->cardinality == 0){
      C_in->cardinality = 0;
      C_in->number_of_bytes = 0;
      C_in->type = type::UINTEGER;
      return C_in;
    }

    switch (A_in->type) {
        case type::UINTEGER:
          switch (B_in->type) {
            case type::UINTEGER:
              return (Set<hybrid>*)set_intersect((Set<uinteger>*)C_in,(const Set<uinteger>*)A_in,(const Set<uinteger>*)B_in);
              break;
            case type::RANGE_BITSET:
              return (Set<hybrid>*)set_intersect((Set<uinteger>*)C_in,(const Set<uinteger>*)A_in,(const Set<range_bitset>*)B_in);
              break;
            case type::BLOCK_BITSET:
              return (Set<hybrid>*)set_intersect((Set<uinteger>*)C_in,(const Set<uinteger>*)A_in,(const Set<block_bitset>*)B_in);
              break;
            default:
              break;
          }
        break;
        case type::RANGE_BITSET:
          switch (B_in->type) {
            case type::UINTEGER:
              return (Set<hybrid>*)set_intersect((Set<uinteger>*)C_in,(const Set<uinteger>*)B_in,(const Set<range_bitset>*)A_in);
            break;
            case type::BLOCK_BITSET:
              return (Set<hybrid>*)set_intersect((Set<block_bitset>*)C_in,(const Set<range_bitset>*)A_in,(const Set<block_bitset>*)B_in);
            break;
            case type::RANGE_BITSET:
              return (Set<hybrid>*)set_intersect((Set<range_bitset>*)C_in,(const Set<range_bitset>*)A_in,(const Set<range_bitset>*)B_in);
            break;
            default:
            break;
          }
        break;
        case type::BLOCK_BITSET:
          switch (B_in->type) {
            case type::UINTEGER:
              return (Set<hybrid>*)set_intersect((Set<uinteger>*)C_in,(const Set<uinteger>*)B_in,(const Set<block_bitset>*)A_in);
            break;
            case type::RANGE_BITSET:
              return (Set<hybrid>*)set_intersect((Set<block_bitset>*)C_in,(const Set<block_bitset>*)A_in,(const Set<range_bitset>*)B_in);
            break;
            case type::BLOCK_BITSET:
              return (Set<hybrid>*)set_intersect((Set<block_bitset>*)C_in,(const Set<block_bitset>*)A_in,(const Set<block_bitset>*)B_in);
            break;
            default:
            break;
          }
        break;
        default:
        break;
    }
    std::cout << "SET INTERSECTION ERROR 1" << std::endl;
    return C_in;
  }
template<typename F>
size_t set_intersect(const Set<hybrid> *A_in,const Set<hybrid> *B_in, F f){
    if(A_in->cardinality == 0 || B_in->cardinality == 0){
      return 0;
    }

    switch (A_in->type) {
        case type::UINTEGER:
          switch (B_in->type) {
            case type::UINTEGER:
              return set_intersect((const Set<uinteger>*)A_in,(const Set<uinteger>*)B_in,f);
              break;
            case type::RANGE_BITSET:
              return set_intersect((const Set<uinteger>*)A_in,(const Set<range_bitset>*)B_in,f);
              break;
            case type::BLOCK_BITSET:
              return set_intersect((const Set<uinteger>*)A_in,(const Set<block_bitset>*)B_in,f);
              break;
            default:
              break;
          }
        break;
        case type::RANGE_BITSET:
          switch (B_in->type) {
            case type::UINTEGER:
              return set_intersect((const Set<uinteger>*)B_in,(const Set<range_bitset>*)A_in,f);
            break;
            case type::BLOCK_BITSET:
              return set_intersect((const Set<range_bitset>*)A_in,(const Set<block_bitset>*)B_in,f);
            break;
            case type::RANGE_BITSET:
              return set_intersect((const Set<range_bitset>*)A_in,(const Set<range_bitset>*)B_in,f);
            break;
            default:
            break;
          }
        break;
        case type::BLOCK_BITSET:
          switch (B_in->type) {
            case type::UINTEGER:
              return set_intersect((const Set<uinteger>*)B_in,(const Set<block_bitset>*)A_in,f);
            break;
            case type::RANGE_BITSET:
              return set_intersect((const Set<block_bitset>*)A_in,(const Set<range_bitset>*)B_in,f);
            break;
            case type::BLOCK_BITSET:
              return set_intersect((const Set<block_bitset>*)A_in,(const Set<block_bitset>*)B_in,f);
            break;
            default:
            break;
          }
        break;
        default:
        break;
    }

    std::cout << "SET INTERSECTION ERROR 1" << std::endl;
    return 0;
  }
  size_t set_intersect(const Set<hybrid> *A_in,const Set<hybrid> *B_in){
    if(A_in->cardinality == 0 || B_in->cardinality == 0){
      return 0;
    }

    switch (A_in->type) {
        case type::UINTEGER:
          switch (B_in->type) {
            case type::UINTEGER:
              return set_intersect((const Set<uinteger>*)A_in,(const Set<uinteger>*)B_in);
              break;
            case type::RANGE_BITSET:
              return set_intersect((const Set<uinteger>*)A_in,(const Set<range_bitset>*)B_in);
              break;
            case type::BLOCK_BITSET:
              return set_intersect((const Set<uinteger>*)A_in,(const Set<block_bitset>*)B_in);
              break;
            default:
              break;
          }
        break;
        case type::RANGE_BITSET:
          switch (B_in->type) {
            case type::UINTEGER:
              return set_intersect((const Set<uinteger>*)B_in,(const Set<range_bitset>*)A_in);
            break;
            case type::BLOCK_BITSET:
              return set_intersect((const Set<range_bitset>*)A_in,(const Set<block_bitset>*)B_in);
            break;
            case type::RANGE_BITSET:
              return set_intersect((const Set<range_bitset>*)A_in,(const Set<range_bitset>*)B_in);
            break;
            default:
            break;
          }
        break;
        case type::BLOCK_BITSET:
          switch (B_in->type) {
            case type::UINTEGER:
              return set_intersect((const Set<uinteger>*)B_in,(const Set<block_bitset>*)A_in);
            break;
            case type::RANGE_BITSET:
              return set_intersect((const Set<block_bitset>*)A_in,(const Set<range_bitset>*)B_in);
            break;
            case type::BLOCK_BITSET:
              return set_intersect((const Set<block_bitset>*)A_in,(const Set<block_bitset>*)B_in);
            break;
            default:
            break;
          }
        break;
        default:
        break;
    }

    std::cout << "SET INTERSECTION ERROR 3" << std::endl;
    return 0;
  }
}
#endif
