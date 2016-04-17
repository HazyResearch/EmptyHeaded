#ifndef _UINTEGER_H_
#define _UINTEGER_H_

#include "utils/utils.hpp"
#include "vector/Meta.hpp"

struct UINTEGER{

  static inline size_t get_num_index_bytes(const Meta * const restrict meta){
    return meta->cardinality*sizeof(uint32_t);
  }

  template <class A, class M>
  static inline A get(
    const uint32_t data, 
    const Meta * const restrict meta, 
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex){

    const uint32_t * const indices = (const uint32_t * const) 
      (memoryBuffer->get_address(bufferIndex)+sizeof(Meta));
    const size_t anno_offset = sizeof(uint32_t)*meta->cardinality;
    const A * const values = (const A * const) 
      (memoryBuffer->get_address(bufferIndex)+sizeof(Meta)+anno_offset);
    const long data_index = utils::binary_search(indices,0,meta->cardinality,data);
    assert(data_index != -1);
    return values[data_index];
  }

  template <class A, class M>
  static inline A get(
    const uint32_t index,
    const uint32_t data,
    const Meta * const restrict meta, 
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex){
    (void) data;
    const size_t anno_offset = sizeof(uint32_t)*meta->cardinality;
    const A * const values = (const A * const) 
      (memoryBuffer->get_address(bufferIndex)+sizeof(Meta)+anno_offset);
    return values[index];
  }

  template <class A, class M>
  static inline void set(
    const uint32_t index,
    const uint32_t data,
    const A& restrict value,
    const Meta * const restrict meta, 
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex)
  {
    (void) data;
    const size_t anno_offset = sizeof(uint32_t)*meta->cardinality;
    A * const values = (A * const) 
      (memoryBuffer->get_address(bufferIndex)+sizeof(Meta)+anno_offset);
    values[index] = value;
  }

  //Iterates over set applying a lambda.
  template <class A, class M, typename F>
  static inline void foreach(
    F f,
    const Meta * const restrict meta, 
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex) 
  {
    const size_t anno_offset = sizeof(uint32_t)*meta->cardinality;
    for(size_t i=0; i<meta->cardinality;i++){
      const uint32_t * const data = (const uint32_t * const) 
        (memoryBuffer->get_address(bufferIndex)+sizeof(Meta)+(sizeof(uint32_t)*i));
      const A * const values = (const A * const) 
        (memoryBuffer->get_address(bufferIndex)+sizeof(Meta)+anno_offset+(sizeof(A)*i));
      f(i,*data,*values);
    }
  }

  template <class M, typename F>
  static inline void parforeach_index(
    F f,
    const Meta * const restrict meta, 
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex) 
  {
    par::for_range(0, meta->cardinality,
     [&](const size_t tid, const size_t i) {
        const uint32_t * const data = (const uint32_t * const) 
          (memoryBuffer->get_address(bufferIndex)+sizeof(Meta)+(sizeof(uint32_t)*i));
        f(tid, (const uint32_t)i, *data);
     });
  }

  //Iterates over set applying a lambda.
  template <class M, typename F>
  static inline void foreach_index(
    F f,
    const Meta * const restrict meta, 
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex)
  {
    for(size_t i=0; i<meta->cardinality;i++){
      std::cout << "HERE" << std::endl;
      const uint32_t * const data = (const uint32_t * const) 
        (memoryBuffer->get_address(bufferIndex)+sizeof(Meta)+(sizeof(uint32_t)*i));
      f(i,*data);
    }
  }

  //constructors
  static inline void from_vector(
    uint8_t* buffer,
    const uint32_t * const restrict input_data,
    const size_t input_length )
  {
    memcpy((void*)buffer,(void*)input_data,(input_length*sizeof(uint32_t)));
  }

  //constructors
  /*
  Should we consider mixing annotations and indices.
  */
  template<class A>
  static inline void from_vector(
    uint8_t* const restrict buffer,
    const uint32_t * const restrict input_data,
    const A * const restrict values,
    const size_t input_length ) {
    memcpy((void*)buffer,(void*)input_data,(input_length*sizeof(uint32_t)));
    memcpy( (void*)(buffer+(input_length*sizeof(uint32_t))),
      (void*)values,(input_length*sizeof(A)));
  }
};

#endif