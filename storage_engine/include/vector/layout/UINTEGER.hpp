#ifndef _UINTEGER_H_
#define _UINTEGER_H_

#include "Meta.hpp"

struct UINTEGER{
  //Iterates over set applying a lambda.
  template <class A, class M, typename F>
  static inline void foreach(F f,Meta* meta, M* memoryBuffer,const size_t index) {
    uint32_t *data = (uint32_t*) memoryBuffer->get_address(index);
    std::cout << (void*) data << std::endl;
    for(size_t i=0; i<meta->cardinality;i++){
      data = (uint32_t*) (memoryBuffer->get_address(index)+(sizeof(uint32_t)*i));
      f(i,*data);
    }
  }

  //constructors
  static inline void from_vector(
    uint8_t* buffer,
    const uint32_t const * input_data,
    const size_t input_length ) {
    memcpy((void*)buffer,(void*)input_data,(input_length*sizeof(uint32_t)));
  };

};

#endif