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
    uint8_t *data;
    uint32_t cardinality;
    uint32_t range;
    size_t number_of_bytes;
    type::layout type;

    Set(){
      number_of_bytes = 0;
      cardinality = 0;
      range = 0;
      data = NULL;
      type = type::NOT_VALID;
    };

    //All values passed in
    Set(const Set &obj):
      data(obj.data),
      cardinality(obj.cardinality),
      range(obj.range),
      number_of_bytes(obj.number_of_bytes),
      type(obj.type){}    
    //All values passed in
    Set(uint8_t *data_in, 
      uint32_t cardinality_in, 
      uint32_t range_in,
      size_t number_of_bytes_in,
      type::layout type_in):
      data(data_in),
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

        data = new uint8_t[number_of_bytes];
        memset(data,(uint8_t)0,number_of_bytes);
      }

    //A set that is just a buffer.
    Set(uint8_t *data_in, Set<T> in):
      data(data_in){
        cardinality = in.cardinality;
        range = in.range;
        number_of_bytes = in.number_of_bytes;
        type = in.type;
        memcpy(data_in,in.data,in.number_of_bytes);
      }
      
    //A set that is just a buffer.
    Set(uint8_t *data_in):
      data(data_in){
        cardinality = 0;
        range = 0;
        number_of_bytes = 0;
        type = T::get_type();
      }

    //Implicit Conversion Between Unlike Types
    template <class U> 
    Set<T>(Set<U> in){
      data = in.data;
      cardinality = in.cardinality;
      range = in.range;
      number_of_bytes = in.number_of_bytes;
      type = in.type;
    }

    Set(uint8_t* data_in, size_t number_of_bytes_in):
      data(data_in), number_of_bytes(number_of_bytes_in) {
        cardinality = 0;
        range = 0;
        number_of_bytes = number_of_bytes_in;
        type = T::get_type();
    }

    template <class U> 
    Set<T>(Set<U> *in){
      data = in->data;
      cardinality = in->cardinality;
      range = in->range;
      number_of_bytes = in->number_of_bytes;
      type = in->type;
    }

    std::tuple<size_t,bool> find(size_t index, uint32_t key) const {
      /*std::cout << number_of_bytes << std::endl;
      std::cout << number_of_bytes << std::endl;*/
      //std::cout << "---" << std::endl;
      return T::find(index,key,data,number_of_bytes,type);
    }

    long find(uint32_t key) const {
      /*std::cout << number_of_bytes << std::endl;
      std::cout << number_of_bytes << std::endl;*/
      //std::cout << "---" << std::endl;
      return T::find(key,data,number_of_bytes,type);
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
      T::foreach(f,data,cardinality,number_of_bytes,type);
    }

    template<typename F>
    void foreach_index(F f) const {
      /*std::cout << number_of_bytes << std::endl;
      std::cout << number_of_bytes << std::endl;*/
      //std::cout << "---" << std::endl;
      T::foreach_index(f,data,cardinality,number_of_bytes,type);
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
      T::foreach_until(f,data,cardinality,number_of_bytes,type);
    }

    template<typename F>
    size_t par_foreach(F f) const {
      return T::par_foreach(f, data, cardinality, number_of_bytes, type);
    }

    template<typename F>
    size_t par_foreach_index(F f) const {
      return T::par_foreach_index(f, data, cardinality, number_of_bytes, type);
    }

    template<typename F>
    size_t static_par_foreach_index(F f) const {
      return T::static_par_foreach_index(f, data, cardinality, number_of_bytes, type);
    }

    Set<uinteger> decode(uint32_t *buffer);
    void copy_from(Set<T> src);
    void to_binary(std::ofstream* outfile); 

    //constructors
    static Set<T>* from_binary(std::ifstream* infile, 
      allocator::memory<uint8_t> *allocator_in, 
      const size_t tid);
    static Set<T> from_array(uint8_t *set_data, uint32_t *array_data, size_t data_size);
};

///////////////////////////////////////////////////////////////////////////////
//Copy Data from one set into another
///////////////////////////////////////////////////////////////////////////////
template <class T>
inline void Set<T>::copy_from(Set<T> src){ 
  memcpy(data,src.data,src.number_of_bytes);
  cardinality = src.cardinality;
  range = src.range;
  number_of_bytes = src.number_of_bytes;
  type = src.type;
}

///////////////////////////////////////////////////////////////////////////////
//Write a set in a binary format to disk
///////////////////////////////////////////////////////////////////////////////
template <class T>
inline void Set<T>::to_binary(std::ofstream* outfile){ 
  outfile->write((char *)&cardinality, sizeof(cardinality));
  outfile->write((char *)&range, sizeof(range));
  outfile->write((char *)&number_of_bytes, sizeof(number_of_bytes));
  outfile->write((char *)&type, sizeof(type));

  for(size_t i = 0; i < number_of_bytes; i++){
    outfile->write((char *)&data[i], sizeof(uint8_t));
  }
}
///////////////////////////////////////////////////////////////////////////////
//Read a from binary format
///////////////////////////////////////////////////////////////////////////////
template<class T>
inline Set<T>* Set<T>::from_binary(std::ifstream* infile, 
  allocator::memory<uint8_t> *allocator_in, 
  const size_t tid){

  Set<T>* output_set = new (
            allocator_in->get_next(tid, sizeof(Set<T>)))
            Set<T>();
  
  infile->read((char *)&output_set->cardinality, sizeof(output_set->cardinality));
  infile->read((char *)&output_set->range, sizeof(output_set->range));
  infile->read((char *)&output_set->number_of_bytes, sizeof(output_set->number_of_bytes));
  infile->read((char *)&output_set->type, sizeof(output_set->type));
  
  output_set->data = allocator_in->get_next(tid,output_set->number_of_bytes);
  for(size_t i = 0; i < output_set->number_of_bytes; i++){
    infile->read((char *)&output_set->data[i], sizeof(uint8_t));
  }

  return output_set;
}
///////////////////////////////////////////////////////////////////////////////
//DECODE ARRAY
///////////////////////////////////////////////////////////////////////////////
template <class T>
inline Set<uinteger> Set<T>::decode(uint32_t *buffer){ 
  size_t i = 0;
  T::foreach( ([&i,&buffer] (uint32_t data){
    buffer[i++] = data;
  }),data,cardinality,number_of_bytes,type);
  return Set<uinteger>((uint8_t*)buffer,cardinality,range,cardinality*sizeof(int),type::UINTEGER);
}

///////////////////////////////////////////////////////////////////////////////
//CREATE A SET FROM AN ARRAY OF UNSIGNED INTEGERS
///////////////////////////////////////////////////////////////////////////////
template <class T>
inline Set<T> Set<T>::from_array(uint8_t *set_data, uint32_t *array_data, size_t data_size){
  const uint32_t range = (data_size > 0) ? array_data[data_size-1] : 0;
  const std::tuple<size_t,type::layout> bl = T::build(set_data,array_data,data_size);
  return Set<T>(set_data,data_size,range,std::get<0>(bl),std::get<1>(bl));
}

#endif
