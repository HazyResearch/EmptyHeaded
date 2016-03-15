#include "vector/SparseVector.hpp"
#include "Vector.hpp"

template <class T, class A, class M>
uint32_t Vector<T,A,M>::indexOf(const uint32_t data) const{
  (void) data;
  return 0;
}

//calls index of then calls get below.
template <class T, class A, class M>
A Vector<T,A,M>::get(const uint32_t data) const{
  return T:: template get<A,M>(
    data,
    meta,
    buffer.memory_buffer,
    buffer.index+sizeof(Meta));
}

//look up a data value
template <class T, class A, class M>
A Vector<T,A,M>::get(
  const uint32_t index,
  const uint32_t data) const{
  return T:: template get<A,M>(
    index,
    data,
    meta,
    buffer.memory_buffer,
    buffer.index+sizeof(Meta));
}

//set an annotation value
template <class T, class A, class M>
void Vector<T,A,M>::set(
  const uint32_t index,
  const uint32_t data, 
  const A& value) {
  (void) index;
  (void) data;
  (void) value;
  /*
  T:: template set<A,M>(
    index,
    data,
    value,
    meta,
    buffer.memory_buffer,
    buffer.index+sizeof(Meta));
  */
}

//look up a data value
template <class T, class A, class M>
bool Vector<T,A,M>::contains(const uint32_t key) const{
  (void) key;
  return false;
}

//constructors
template <class T, class A, class M>
Vector<T,A,M> Vector<T,A,M>::from_array(
  M* memoryBuffer,
  const uint32_t * const data,
  const A * const values,
  const size_t len){

  const size_t index = memoryBuffer->get_offset();
  const size_t num_bytes = T:: template get_num_bytes<A>(data,len);
  //reserve memory
  memoryBuffer->get_next(sizeof(Meta)+num_bytes);
  Meta* meta = new(memoryBuffer->get_address(index)) Meta();

  meta->cardinality = len;
  meta->start = 0;
  meta->end = 0;
  if(len > 0){
    meta->start = *data;
    meta->end = *(data+(len-1));
  }
  meta->type = T::get_type();
  T:: template from_array<A,M>(memoryBuffer->get_address(index+sizeof(Meta)),data,values,len);
  return Vector<T,A,M>(memoryBuffer,index);
}

//constructors
template <class T, class A, class M>
Vector<T,A,M> Vector<T,A,M>::from_array(
  M* memoryBuffer,
  const uint32_t * const data,
  const size_t len){

  const size_t index = memoryBuffer->get_offset();
  const size_t num_bytes = T:: template get_num_bytes<A>(data,len);
  //reserve memory
  memoryBuffer->get_next(sizeof(Meta)+num_bytes);
  Meta* meta = new(memoryBuffer->get_address(index)) Meta();

  meta->cardinality = len;
  meta->start = 0;
  meta->end = 0;
  if(len > 0){
    meta->start = *data;
    meta->end = *(data+(len-1));
  }
  meta->type = T::get_type();

  T:: template from_array<A,M>(memoryBuffer->get_address(index+sizeof(Meta)),data,len);
  return Vector<T,A,M>(memoryBuffer,index);
}


template struct Vector<SparseVector,void*,MemoryBuffer>;
template struct Vector<SparseVector,int,MemoryBuffer>;
template struct Vector<SparseVector,long,MemoryBuffer>;
template struct Vector<SparseVector,float,MemoryBuffer>;
template struct Vector<SparseVector,double,MemoryBuffer>;
template struct Vector<SparseVector,NextLevel,MemoryBuffer>;

