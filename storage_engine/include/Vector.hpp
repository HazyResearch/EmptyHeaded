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

#include "vector/SparseVector.hpp"

/*
Vectors are laid flat in the memory buffer as follows

---------------------------------------------------------
Meta | Indices (uint/bitset/block) | Annotations
---------------------------------------------------------
*/
template <class T, class A, class M>
class Vector{ 
  public: 
    //retrieve via placement new (actual data in buffer)
    Meta* meta;
    //Construct this on your own (just manages the memory)
    Buffer<M> buffer;
    Vector(
      M* memory_buffer_in, size_t index){
      buffer.memory_buffer = memory_buffer_in;
      buffer.index = index;
      //placement new
      meta = new (memory_buffer_in->get_address(index)) Meta();
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
      const A value);

    //look up a data value
    bool contains(const uint32_t key) const;

    //mutable loop (returns data and index)
    template<typename F>
    inline void foreach(F f) const{

    };

    //parallel iterator
    template<typename F>
    inline void par_foreach(F f) const{

    };

    //constructors
    static Vector<T,A,M> from_array(
      M const * memoryBuffer,
      const uint32_t *array_data, 
      const size_t data_size);
};

#endif
