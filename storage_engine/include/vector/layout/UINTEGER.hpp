#ifndef _UINTEGER_H_
#define _UINTEGER_H_

#include "Meta.hpp"

template<class M>
struct UINTEGER{
  //some basic meta data
  Meta meta;
  Buffer<M> buffer;

  //constructor
  UINTEGER(const Meta& meta_in,
    const Buffer<M>& buffer_in){
    meta = meta_in;
    buffer = buffer_in;
  };

  //Iterates over set applying a lambda.
  template<typename F>
  inline void foreach(F f) {
    /*
    uint32_t *data = (uint32_t*) memoryBuffer->get_address(index);
    for(size_t i=0; i<cardinality;i++){
      //std::cout << "FOREACH: " << (void*) memoryBuffer->get_address(0) << std::endl;
      data = (uint32_t*) (memoryBuffer->get_address(index)+(sizeof(uint32_t)*i));
      f(i,*data);
    }
    */
  }
};

#endif