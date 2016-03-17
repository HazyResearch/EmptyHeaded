/******************************************************************************
*
* Author: Christopher R. Aberger
*
* TOP LEVEL CLASS FOR OUR SORTED VectorS.  PROVIDE A Vector OF GENERAL PRIMITIVE 
* TYPES THAT WE PROVIDE (PSHORT,UINT,VARIANT,BITPACKED,BITVector).  WE ALSO
* PROVIDE A HYBRID Vector IMPLEMENTATION THAT DYNAMICALLY CHOOSES THE TYPE
* FOR THE DEVELOPER. IMPLEMENTATION FOR PRIMITIVE TYPE OPS CAN BE 
* FOUND IN STATIC CLASSES IN THE LAYOUT FOLDER.
******************************************************************************/

#ifndef _VECTOR_H_
#define _VECTOR_H_

#include "utils/utils.hpp"
#include "vector/SparseVector.hpp"
#include "vector/annotations/BufferIndex.hpp"
#include "vector/Meta.hpp"
/*
Vectors are laid flat in the memory buffer as follows

---------------------------------------------------------
Meta | Indices (uint/bitset/block) | Annotations
---------------------------------------------------------
*/
template <class T, class A, class M>
struct Vector{ 
  //retrieve via placement new (actual data in buffer)
  Meta* meta;
  //Construct this on your own (just manages the memory)
  BufferIndex bufferIndex;
  M* memoryBuffer;

  Vector(
    M* memory_buffer_in,
    const BufferIndex& restrict buffer_index_in)
  {
    memoryBuffer = memory_buffer_in;
    bufferIndex = buffer_index_in;
    //placement new
    meta = new (memory_buffer_in->get_address(bufferIndex)) Meta();
  }

  Vector(
    M* memory_buffer_in)
  {
    BufferIndex buffer_index_in;
    buffer_index_in.tid = NUM_THREADS;
    buffer_index_in.index = 0;
    memoryBuffer = memory_buffer_in;
    bufferIndex = buffer_index_in;
    //placement new
    meta = new (memory_buffer_in->get_address(bufferIndex)) Meta();
  }


  //Find the index of a data elem.
  uint32_t indexOf(const uint32_t data) const;

  //calls index of then calls get below.
  A get(const uint32_t data) const;

  //look up a data value
  A get(
    const uint32_t index,
    const uint32_t data) const;

  //set an annotation value
  void set(
    const uint32_t index,
    const uint32_t data, 
    const A& value);

  //look up a data value
  bool contains(const uint32_t key) const;

  inline uint8_t* get_data() const{
    return memoryBuffer->get_address(bufferIndex)+sizeof(Meta);
  }

  inline uint8_t* get_annotation() const {
    return memoryBuffer->get_address(bufferIndex)
      +sizeof(Meta)
      +T::get_num_index_bytes(meta);
  }

  //mutable loop (returns data and index)
  template<typename F>
  inline void foreach(F f) const{
    T:: template foreach<A,M>(
      f,
      meta,
      memoryBuffer,
      bufferIndex);
  }

  //mutable loop (returns data and index)
  template<typename F>
  inline void foreach_index(F f) const{
    T:: template foreach_index<M>(
      f,
      meta,
      memoryBuffer,
      bufferIndex);
  }

  //parallel iterator
  template<typename F>
  inline void parforeach_index(F f) const{
    T:: template parforeach_index<M>(
      f,
      meta,
      memoryBuffer,
      bufferIndex);
  }

  //constructors
  static Vector<T,A,M> from_array(
    const size_t tid,
    M* memoryBuffer,
    const uint32_t * const data,
    const size_t len);

  //constructors
  static Vector<T,A,M> from_array(
    const size_t tid,
    M* memoryBuffer,
    const uint32_t * const data,
    const A * const values,
    const size_t len);
};

#endif
