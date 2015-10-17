/******************************************************************************
*
* Author: Christopher R. Aberger
*
* TOP LEVEL CLASS FOR OUR SORTED SETS.  PROVIDE A SET OF GENERAL PRIMITIVE 
* TYPES THAT WE PROVIDE (PSHORT,UINT,VARIANT,BITPACKED,BITSET).  WE ALSO
* PROVIDE A HYBRID SET IMPLEMENTATION THAT DYNAMICALLY CHOOSES THE TYPE
* FOR THE DEVELOPER. IMPLEMENTATION FOR PRIMITIVE TYPE OPS CAN BE 
* FOUND IN STATIC CLASSES IN THE LAYOUT FOLDER.
******************************************************************************/

#ifndef _SET_H_
#define _SET_H_

#include "layouts/hybrid.hpp"
#include "layouts/block.hpp"

template <class T>
class Set{ 
  public: 
    uint32_t cardinality;
    uint32_t range;
    size_t number_of_bytes;
    type::layout type;

    Set(){
      number_of_bytes = 0;
      cardinality = 0;
      range = 0;
      type = type::NOT_VALID;
    };

    //All values passed in
    Set(const Set &obj):
      cardinality(obj.cardinality),
      range(obj.range),
      number_of_bytes(obj.number_of_bytes),
      type(obj.type){}    
    //All values passed in
    Set(
      uint32_t cardinality_in, 
      uint32_t range_in,
      size_t number_of_bytes_in,
      type::layout type_in):
      cardinality(cardinality_in),
      range(range_in),
      number_of_bytes(number_of_bytes_in),
      type(type_in){}

    //A set that is just a buffer zeroed out to certain size.
    Set(size_t number_of_bytes_in){
        cardinality = 0;
        range = 0;
        number_of_bytes = number_of_bytes_in;
        type = T::get_type();
      }

    inline uint8_t* get_data() const {
      return (uint8_t*)((uint8_t*)this + sizeof(Set<T>));
    }

    std::tuple<size_t,bool> find(size_t index, uint32_t key) const {
      /*std::cout << number_of_bytes << std::endl;
      std::cout << number_of_bytes << std::endl;*/
      //std::cout << "---" << std::endl;
      return T::find(index,key,this->get_data(),number_of_bytes,type);
    }

    long find(uint32_t key) const {
      /*std::cout << number_of_bytes << std::endl;
      std::cout << number_of_bytes << std::endl;*/
      //std::cout << "---" << std::endl;
      return T::find(key,this->get_data(),number_of_bytes,type);
    }


    // Applies a function to each element in the set.
    //
    // Note: We use templates here to allow the compiler to inline the
    // lambda [1].
    //
    // [1] http://stackoverflow.com/questions/21407691/c11-inline-lambda-functions-without-template
    template<typename F>
    void foreach(F f) const {
      /*std::cout << number_of_bytes << std::endl;
      std::cout << number_of_bytes << std::endl;*/
      //std::cout << "---" << std::endl;
      T::foreach(f,this->get_data(),cardinality,number_of_bytes,type);
    }

    template<class M, typename F>
    void foreach(const size_t index, M* memoryBuffer, F f) const {
      /*std::cout << number_of_bytes << std::endl;
      std::cout << number_of_bytes << std::endl;*/
      //std::cout << "---" << std::endl;
      T::foreach(
        index,
        memoryBuffer,
        f,
        cardinality,
        number_of_bytes,
        type);
    }

    template<typename F>
    void foreach_index(F f) const {
      /*std::cout << number_of_bytes << std::endl;
      std::cout << number_of_bytes << std::endl;*/
      //std::cout << "---" << std::endl;
      T::foreach_index(f,this->get_data(),cardinality,number_of_bytes,type);
    }

    // Applies a function to each element in the set until the function returns
    // true.
    //
    // Note: We use templates here to allow the compiler to inline the
    // lambda [1].
    //
    // [1] http://stackoverflow.com/questions/21407691/c11-inline-lambda-functions-without-template
    template<typename F>
    void foreach_until(F f) const {
      T::foreach_until(f,this->get_data(),cardinality,number_of_bytes,type);
    }

    template<typename F>
    size_t par_foreach(F f) const {
      return T::par_foreach(f, this->get_data(), cardinality, number_of_bytes, type);
    }

    template<typename F>
    size_t par_foreach_index(F f) const {
      return T::par_foreach_index(f, this->get_data(), cardinality, number_of_bytes, type);
    }

    template<typename F>
    size_t static_par_foreach_index(F f) const {
      return T::static_par_foreach_index(f, this->get_data(), cardinality, number_of_bytes, type);
    }

    Set<uinteger> decode(uint32_t *buffer);
    void copy_from(Set<T> src);

    //constructors
    void from_array(uint8_t *set_data, uint32_t *array_data, size_t data_size);
};

///////////////////////////////////////////////////////////////////////////////
//CREATE A SET FROM AN ARRAY OF UNSIGNED INTEGERS
///////////////////////////////////////////////////////////////////////////////
template <class T>
inline void Set<T>::from_array(uint8_t *set_data, uint32_t *array_data, size_t data_size){
  range = (data_size > 0) ? array_data[data_size-1] : 0;
  const std::tuple<size_t,type::layout> bl = T::build(set_data,array_data,data_size);

  number_of_bytes = std::get<0>(bl);
  cardinality = data_size;
  type = std::get<1>(bl);
}

#endif
