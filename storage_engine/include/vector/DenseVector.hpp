#ifndef _DENSEVECTOR_H_
#define _DENSEVECTOR_H_

#include "layout/BITSET.hpp"

/*
Vectors are laid flat in memory as follows

---------------------------------------------------------
Vector | Indices (uint/bitset/block) | Annotations
---------------------------------------------------------
*/

struct DenseVector{ 
  //Find the index of a data elem.
  template <class A, class M>
  static inline uint32_t indexOf(
    const uint32_t data) 
  {
      (void) data;
      return 0;
  }

  //calls index of then calls get below.
  template <class A, class M>
  static inline A get(
    const uint32_t data,
    const Meta * const restrict meta,
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex)
  {
    return BITSET:: template get<A,M>(data,meta,memoryBuffer,bufferIndex);
  }

  template <class A, class M>
  static inline A* get_block(
    const uint32_t data,
    const Meta * const restrict meta,
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex)
  {
    return BITSET:: template get_block<A,M>(data,meta,memoryBuffer,bufferIndex);
  }

  //look up a data value
  template <class A, class M>
  static inline A get(
    const uint32_t index,
    const uint32_t data,
    const Meta * const restrict meta,
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex)
  {
    return BITSET:: template get<A,M>(index,data,meta,memoryBuffer,bufferIndex);
  }

  //set an annotation value
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
    BITSET:: template set<A,M>(
      index,
      data,
      value,
      meta,
      memoryBuffer,
      bufferIndex);
  }

  //look up a data value
  template <class A, class M>
  static inline bool contains(const uint32_t key) {
    (void) key;
    return false;
  }

  //mutable loop (returns data and index)
  template <class M, typename F>
  static inline void foreach_block(
    F f,
    const Meta * const restrict meta,
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex)
  {
    BITSET:: template foreach_block<M>(f,meta,memoryBuffer,bufferIndex);
  }

  //mutable loop (returns data and index)
  template <class A, class M, typename F>
  static inline void foreach(
    F f,
    const Meta * const restrict meta,
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex)
  {
    BITSET:: template foreach<A,M>(f,meta,memoryBuffer,bufferIndex);
  }

    //mutable loop (returns data and index)
  template <class M, typename F>
  static inline void foreach_index(
    F f,
    const Meta * const restrict meta,
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex)
  {
    BITSET:: template foreach_index<M>(f,meta,memoryBuffer,bufferIndex);
  }

    //mutable loop (returns data and index)
  template <class M, typename F>
  static inline void parforeach_index(
    F f,
    const Meta * const restrict meta,
    const M * const restrict memoryBuffer,
    const BufferIndex& restrict bufferIndex)
  {
    BITSET:: template parforeach_index<M>(
      f,
      meta,
      memoryBuffer,
      bufferIndex);
  }

  template<class A>
  static inline size_t get_num_bytes(
    const Meta * const restrict meta)
  {
    return sizeof(Meta)+(BITSET::get_num_data_words(meta)*64*sizeof(A))
      +(sizeof(uint64_t)*BITSET::get_num_data_words(meta));
  }

  template <class A>
  static inline size_t get_num_bytes(
    const uint32_t * const data,
    const size_t len)
  {
    (void) data;
    const size_t num_words = len > 0 ? BITSET::word_index(data[len-1])-BITSET::word_index(data[0])+1 : 0;
    return sizeof(Meta)+(num_words*64*sizeof(A))+(num_words*sizeof(uint64_t));
  }

  static inline size_t get_num_index_bytes(
    const Meta * const restrict meta)
  {
    const size_t num_words = BITSET::get_num_data_words(meta); 
    return (num_words*sizeof(uint64_t));
  }

  //parallel iterator
  static inline type::layout get_type() 
  {
    return type::BITSET;
  }

  //constructors
  template <class A, class M>
  static inline void from_array(
    uint8_t* restrict buffer,    
    const uint32_t * const restrict data,
    const size_t len) 
  {
    BITSET::from_vector(buffer,data,len);
  }

  //constructors
  template <class A, class M>
  static inline void from_array(
    uint8_t* restrict buffer,    
    const uint32_t * const data,
    const A * const restrict values,
    const size_t len) 
  {
    BITSET::from_vector<A>(buffer,data,values,len);
  }
};

template<>
inline size_t DenseVector::get_num_bytes<void*>(
  const uint32_t * const restrict data,
  const size_t len) 
{
  const size_t num_words = len > 0 ? data[len-1]-data[0]+1 : 0; 
  return sizeof(Meta)+(num_words*sizeof(uint64_t));
}

template<>
inline size_t DenseVector::get_num_bytes<void*>(
  const Meta * const restrict meta) 
{
  const size_t num_words = BITSET::get_num_data_words(meta); 
  return sizeof(Meta)+(num_words*sizeof(uint64_t));
}

#endif
