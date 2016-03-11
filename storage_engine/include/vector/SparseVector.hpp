#ifndef _SPARSEVECTOR_H_
#define _SPARSEVECTOR_H_

#include "layout/uinteger.hpp"

/*
Vectors are laid flat in memory as follows

---------------------------------------------------------
Vector | Indices (uint/bitset/block) | Annotations
---------------------------------------------------------
*/

template <class A, class M>
class SparseVector{ 
  public: 
    Meta meta;
    Buffer<M> buffer;

    SparseVector(const Meta& meta_in,
      const Buffer<M>& buffer_in){
      meta = meta_in;
      buffer = buffer_in;
    };
    
    //Find the index of a data elem.
    const uint32_t indexOf(const uint32_t data) const{
    };

    //calls index of then calls get below.
    A get(const uint32_t data) const{
    
    };

    //look up a data value
    A get(
      const uint32_t index,
      const uint32_t data) const{

    };

    //set an annotation value
    void set(
      const uint32_t index,
      const uint32_t data, 
      const A value) const{

    };

    //look up a data value
    bool contains(const uint32_t key) const{

    };

    //mutable loop (returns data and index)
    template<typename F>
    inline void foreach(F f) const{

    };

    //parallel iterator
    template<typename F>
    inline void par_foreach(F f) const{

    };

    //constructors
    void from_vector(
      M* memory_buffer,
      std::vector<uint32_t>* v) const{

    };
};

#endif
