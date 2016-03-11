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
    static inline A get(const uint32_t data) {
    
    };

    //look up a data value
    template <class A, class M>
    static inline A get(
      const uint32_t index,
      const uint32_t data) {

    };

    //set an annotation value
    template <class A, class M>
    static inline void set(
      const uint32_t index,
      const uint32_t data, 
      const A value) {

    };

    //look up a data value
    template <class A, class M>
    static inline bool contains(const uint32_t key) {

    };

    //mutable loop (returns data and index)
    template <class A, class M, typename F>
    static inline inline void foreach(F f) {

    };

    //parallel iterator
    template <class A, class M, typename F>
    static inline inline void par_foreach(F f) {

    };

    //constructors
    template <class A, class M>
    static inline void from_vector(
      M* memory_buffer,
      std::vector<uint32_t>* v) {
      UINTEGER::from_vector(memory_buffer,v->data(),v->size());
    };
};

#endif
