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
    static inline const uint32_t indexOf(const uint32_t data) {
    };

    //calls index of then calls get below.
    template <class A, class M>
    static inline A get(const uint32_t data,
      Meta* meta,
      M* memoryBuffer,
      const size_t index) {
      return UINTEGER:: template get<A,M>(data,meta,memoryBuffer,index);
    };

    //look up a data value
    template <class A, class M>
    static inline A get(
      const uint32_t index,
      const uint32_t data,
      Meta* meta,
      M* memoryBuffer,
      const size_t buffer_index) {
      return UINTEGER:: template get<A,M>(index,data,meta,memoryBuffer,buffer_index);
    };

    //set an annotation value
    template <class A, class M>
    static inline void set(
      const uint32_t index,
      const uint32_t data, 
      const A value,
      Meta* meta,
      M* memoryBuffer,
      const size_t buffer_index) {
      (void) data;
      UINTEGER:: template get<A,M>(
        index,
        value,
        meta,
        memoryBuffer,
        buffer_index);
    };

    //look up a data value
    template <class A, class M>
    static inline bool contains(const uint32_t key) {

    };

    //mutable loop (returns data and index)
    template <class A, class M, typename F>
    static inline void foreach(F f,Meta* meta,M* memoryBuffer,const size_t index) {
      UINTEGER:: template foreach<A,M>(f,meta,memoryBuffer,index);
    };

      //mutable loop (returns data and index)
    template <class M, typename F>
    static inline void foreach_index(F f,Meta* meta,M* memoryBuffer,const size_t index) {
      UINTEGER:: template foreach_index<M>(f,meta,memoryBuffer,index);
    };

    //parallel iterator
    template <class A, class M, typename F>
    static inline void par_foreach(F f) {

    };

    template<class A>
    static inline size_t get_num_bytes(Meta* m) {
      return m->cardinality*(sizeof(uint32_t)*sizeof(A));
    };

    template <class A>
    static inline size_t get_num_bytes(const uint32_t const * data,const size_t len) {
      return len*(sizeof(uint32_t)+sizeof(A));
    };

    static inline size_t get_num_index_bytes(Meta* m) {
      return m->cardinality*sizeof(uint32_t);
    };

    //parallel iterator
    static inline type::layout get_type() {
      return type::UINTEGER;
    };

    //constructors
    template <class A, class M>
    static inline void from_vector(
      uint8_t* buffer,    
      const uint32_t const * data,
      const size_t len) {
      UINTEGER::from_vector(buffer,data,len);
    };

    //constructors
    template <class A, class M>
    static inline void from_vector(
      uint8_t* buffer,    
      const uint32_t const * data,
      const A const * values,
      const size_t len) {
      UINTEGER::from_vector<A>(buffer,data,values,len);
    };
};

template<>
inline size_t SparseVector::get_num_bytes<void*>(const uint32_t const * data,const size_t len) {
  return len*sizeof(uint32_t);
};

template<>
inline size_t SparseVector::get_num_bytes<void*>(Meta* m) {
  return m->cardinality*sizeof(uint32_t);
};

#endif
