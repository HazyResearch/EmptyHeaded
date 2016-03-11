#ifndef _UINTEGER_H_
#define _UINTEGER_H_

#include "Meta.hpp"

struct UINTEGER{
  //Iterates over set applying a lambda.
  template <class M, typename F>
  static inline void foreach(F f) {
    /*
    uint32_t *data = (uint32_t*) memoryBuffer->get_address(index);
    for(size_t i=0; i<cardinality;i++){
      //std::cout << "FOREACH: " << (void*) memoryBuffer->get_address(0) << std::endl;
      data = (uint32_t*) (memoryBuffer->get_address(index)+(sizeof(uint32_t)*i));
      f(i,*data);
    }
    */
  }

  //constructors
  template <class M>
  static inline void from_vector(
    M* memory_buffer,
    const uint32_t const * input_data,
    const size_t input_length ) {
    std::cout << "HERE" << std::endl;
  };

};

#endif