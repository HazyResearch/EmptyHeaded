#ifndef _SPARSEVECTOR_H_
#define _SPARSEVECTOR_H_

#include "layout/UINTEGER.hpp"

/*
Vectors are laid flat in memory as follows

---------------------------------------------------------
Vector | Indices (uint/bitset/block) | Annotations
---------------------------------------------------------
*/

struct SparseVector{ 
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
      return UINTEGER:: template get<A,M>(data,meta,memoryBuffer,bufferIndex);
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
      return UINTEGER:: template get<A,M>(index,data,meta,memoryBuffer,bufferIndex);
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
      UINTEGER:: template set<A,M>(
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
    template <class A, class M, typename F>
    static inline void foreach(
      F f,
      const Meta * const restrict meta,
      const M * const restrict memoryBuffer,
      const BufferIndex& restrict bufferIndex)
    {
      UINTEGER:: template foreach<A,M>(f,meta,memoryBuffer,bufferIndex);
    }

      //mutable loop (returns data and index)
    template <class M, typename F>
    static inline void foreach_index(
      F f,
      const Meta * const restrict meta,
      const M * const restrict memoryBuffer,
      const BufferIndex& restrict bufferIndex)
    {
      UINTEGER:: template foreach_index<M>(f,meta,memoryBuffer,bufferIndex);
    }

      //mutable loop (returns data and index)
    template <class M, typename F>
    static inline void parforeach_index(
      F f,
      const Meta * const restrict meta,
      const M * const restrict memoryBuffer,
      const BufferIndex& restrict bufferIndex)
    {
      UINTEGER:: template parforeach_index<M>(
        f,
        meta,
        memoryBuffer,
        bufferIndex);
    }

    template<class A>
    static inline size_t get_num_bytes(
      const Meta * const restrict meta)
    {
      return meta->cardinality*(sizeof(uint32_t)*sizeof(A));
    }

    template <class A>
    static inline size_t get_num_bytes(
      const uint32_t * const data,
      const size_t len)
    {
      (void) data;
      return len*(sizeof(uint32_t)+sizeof(A));
    }

    static inline size_t get_num_index_bytes(
      const Meta * const restrict meta)
    {
      return meta->cardinality*sizeof(uint32_t);
    }

    //parallel iterator
    static inline type::layout get_type() 
    {
      return type::UINTEGER;
    }

    //constructors
    template <class A, class M>
    static inline void from_array(
      uint8_t* restrict buffer,    
      const uint32_t * const restrict data,
      const size_t len) 
    {
      UINTEGER::from_vector(buffer,data,len);
    }

    //constructors
    template <class A, class M>
    static inline void from_array(
      uint8_t* restrict buffer,    
      const uint32_t * const data,
      const A * const restrict values,
      const size_t len) 
    {
      UINTEGER::from_vector<A>(buffer,data,values,len);
    }
};

template<>
inline size_t SparseVector::get_num_bytes<void*>(
  const uint32_t * const restrict data,
  const size_t len) 
{
  (void) data;
  return len*sizeof(uint32_t);
}

template<>
inline size_t SparseVector::get_num_bytes<void*>(
  const Meta * const restrict meta) 
{
  return meta->cardinality*sizeof(uint32_t);
}

#endif
