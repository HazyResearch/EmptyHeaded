#ifndef _BLASVECTOR_H_
#define _BLASVECTOR_H_

#include "layout/UINTEGER.hpp"
#include "layout/BITSET.hpp"

/*
Vectors are laid flat in memory as follows

---------------------------------------------------------
Vector | Indices (uint/bitset/block) | Annotations
---------------------------------------------------------
*/

struct BLASVector{
  //Find the index of a data elem.
  template <class A, class M>
  static inline uint32_t indexOf(
    const uint32_t data) 
  {
      (void) data;
      return 0;
  }

  template <class M>
  static inline uint8_t* get_index_data(
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex
  ){
    return memoryBuffer->get_address(bufferIndex)+sizeof(Meta)+sizeof(size_t);
  }

  template<class M>
  static inline Meta* get_meta(    
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex){
    return new 
      (memoryBuffer->get_address(
        BufferIndex(bufferIndex.tid,
          bufferIndex.index+sizeof(size_t)))) Meta();
  }

  //calls index of then calls get below.
  template <class A, class M>
  static inline A get(
    const uint32_t data,
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex) 
  {
    const Meta * const restrict meta = get_meta<M>(memoryBuffer,bufferIndex);
    const A* anno = (A*)memoryBuffer->anno->get_address(0);
    const size_t anno_offset = *((size_t*)memoryBuffer->get_address(bufferIndex));
    return anno[anno_offset+data-meta->start];
  }

  template <class M>
  static inline type::layout get_type(
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex)
  {
    const Meta * const restrict meta = get_meta<M>(memoryBuffer,bufferIndex);
    return meta->type;
  }

  //calls index of then calls get below.
  template <class M>
  static inline size_t get_num_index_bytes(
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex) 
  {
    const Meta * const restrict meta = get_meta<M>(memoryBuffer,bufferIndex);
    return BITSET::get_num_index_bytes(meta);
  }

  //look up a data value
  template <class A, class M>
  static inline A get(
    const uint32_t index,
    const uint32_t data,
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex)
  {
    (void) index;
    return get<A,M>(data,memoryBuffer,bufferIndex);
  }

  //set an annotation value
  template <class A, class M>
  static inline void set(
    const uint32_t index,
    const uint32_t data, 
    const A value,
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex) 
  {
    (void) index;
    const Meta * const restrict meta = get_meta<M>(memoryBuffer,bufferIndex);
    A* anno = (A*)memoryBuffer->anno->get_address(0);
    const size_t anno_offset = *((size_t*)memoryBuffer->get_address(bufferIndex));
    anno[anno_offset+data-meta->start] = value;
  }

  static inline type::layout get_type() 
  {
    return type::BITSET;
  }

  //look up a data value
  template <class A, class M>
  static inline bool contains(const uint32_t key) {
    (void) key;
    return false;
  }

  //mutable loop (returns data and index)
  template <class A, class M, typename F>
  static inline void foreach(
    F f,
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex)
  {
    const Meta * const restrict meta = get_meta<M>(memoryBuffer,bufferIndex);
    BITSET:: template foreach_index<M>([&](const uint32_t i, const uint32_t d){
      f(i,d,get<A,M>(i,d,memoryBuffer,bufferIndex));
    },meta,memoryBuffer,BufferIndex(bufferIndex.tid,bufferIndex.index+sizeof(size_t)));
  }

  template <class M, typename F>
  static inline void foreach_index(
    F f,
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex)
  {
    const Meta * const restrict meta = get_meta<M>(memoryBuffer,bufferIndex);
    BITSET:: template foreach_index<M>(f,meta,memoryBuffer,BufferIndex(bufferIndex.tid,bufferIndex.index+sizeof(size_t)));
  }

    //mutable loop (returns data and index)
  template <class M, typename F>
  static inline void parforeach_index(
    F f,
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex)
  {
    const Meta * const restrict meta = get_meta<M>(memoryBuffer,bufferIndex);
    BITSET:: template parforeach_index<M>(
      f,
      meta,
      memoryBuffer,
      BufferIndex(bufferIndex.tid,bufferIndex.index+sizeof(size_t)));
  }

  template <class A>
  static inline size_t get_num_bytes(
    const uint32_t * const data,
    const size_t len)
  {
    const size_t num_words = len > 0 ? BITSET::word_index(data[len-1])-BITSET::word_index(data[0])+1 : 0;
    return sizeof(size_t)+sizeof(Meta)+(num_words*sizeof(uint64_t));
  }

  //constructors
  static inline void from_array(
    uint8_t* restrict buffer,    
    const uint32_t * const restrict data,
    const size_t len) 
  {
    Meta* meta = new(buffer+sizeof(size_t)) Meta();
    meta->cardinality = len;
    meta->start = 0;
    meta->end = 0;
    meta->type = type::UINTEGER;
    if(len == 0)
      return;
    else{
      meta->start = *data;
      meta->end = *(data+(len-1));
      meta->type = get_type();
    }

    *((size_t*)buffer) = 0;
    BITSET::from_vector(buffer+sizeof(size_t)+sizeof(Meta),data,len);
  }

  //constructors
  template <class A>
  static inline void from_array(
    uint8_t* restrict buffer,    
    const uint32_t * const data,
    const A * const restrict values,
    const size_t len,
    const size_t anno_offset) 
  {
    (void) values;
    Meta* meta = new(buffer+sizeof(size_t)) Meta();
    meta->cardinality = len;
    meta->start = 0;
    meta->end = 0;
    meta->type = type::UINTEGER;
    if(len == 0)
      return;
    else{
      meta->start = *data;
      meta->end = *(data+(len-1));
      meta->type = get_type();
    }
    *((size_t*)buffer) = anno_offset;
    BITSET::from_vector(buffer+sizeof(size_t)+sizeof(Meta),data,len);
  }
};
#endif
