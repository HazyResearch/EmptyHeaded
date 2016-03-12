#ifndef _UINTEGER_H_
#define _UINTEGER_H_

#include "Meta.hpp"

struct UINTEGER{
  //Iterates over set applying a lambda.
  template <class A, class M, typename F>
  static inline void foreach(F f,Meta* meta, M* memoryBuffer,const size_t index) {
    const size_t anno_offset = sizeof(uint32_t)*meta->cardinality;
    for(size_t i=0; i<meta->cardinality;i++){
      const uint32_t const * data = (const uint32_t const *) 
        (memoryBuffer->get_address(index)+(sizeof(uint32_t)*i));
      const A const * values = (const A const *) 
        (memoryBuffer->get_address(index)+anno_offset+(sizeof(A)*i));
      f(i,*data,*values);
    }
  }

  //Iterates over set applying a lambda.
  template <class M, typename F>
  static inline void foreach_index(F f,Meta* meta, M* memoryBuffer,const size_t index) {
    for(size_t i=0; i<meta->cardinality;i++){
      const uint32_t const * data = (const uint32_t const *) 
        (memoryBuffer->get_address(index)+(sizeof(uint32_t)*i));
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

  //constructors
  /*
  Should we consider mixing annotations and indices.
  */
  template<class A>
  static inline void from_vector(
    uint8_t* buffer,
    const uint32_t const * input_data,
    const A const * values,
    const size_t input_length ) {
    memcpy((void*)buffer,(void*)input_data,(input_length*sizeof(uint32_t)));
    memcpy( (void*)(buffer+(input_length*sizeof(uint32_t))),
      (void*)values,(input_length*sizeof(A)));
    A* anno = (A*)(buffer+(input_length*sizeof(uint32_t)));
  };

};

#endif