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
    memoryBuffer,
    bufferIndex);
}

//look up a data value
template <class T, class A, class M>
A Vector<T,A,M>::get(
  const uint32_t index,
  const uint32_t data) const{
  return T:: template get<A,M>(
    index,
    data,
    memoryBuffer,
    bufferIndex);
}

//set an annotation value
template <class T, class A, class M>
void Vector<T,A,M>::set(
  const uint32_t index,
  const uint32_t data, 
  const A& value) {
  T:: template set<A,M>(
    index,
    data,
    value,
    memoryBuffer,
    bufferIndex);
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
  const size_t tid,
  M* memoryBuffer,
  const uint32_t * const data,
  const A * const values,
  const size_t len,
  const size_t anno_offset){

  const size_t index = memoryBuffer->get_offset(tid);
  BufferIndex bufferIndex(tid,index);
  const size_t num_bytes = T:: template get_num_bytes<A>(data,len);
  //reserve memory
  memoryBuffer->get_next(tid,num_bytes);

  T:: template from_array<A>(
    memoryBuffer->get_address(bufferIndex),
    data,
    values,
    len,
    anno_offset);

  return Vector<T,A,M>(memoryBuffer,bufferIndex);
}

//constructors
template <class T, class A, class M>
Vector<T,A,M> Vector<T,A,M>::from_array(
  const size_t tid,
  M* memoryBuffer,
  const uint32_t * const data,
  const size_t len){

  const size_t index = memoryBuffer->get_offset(tid);
  BufferIndex bufferIndex(tid,index);
  const size_t num_bytes = T:: template get_num_bytes<A>(data,len);
  //reserve memory
  memoryBuffer->get_next(tid,num_bytes);

  T::from_array(
    memoryBuffer->get_address(bufferIndex),
    data,
    len);
  return Vector<T,A,M>(memoryBuffer,bufferIndex);
}

template struct Vector<BLASVector,void*,ParMemoryBuffer>;
template struct Vector<BLASVector,int,ParMemoryBuffer>;
template struct Vector<BLASVector,long,ParMemoryBuffer>;
template struct Vector<BLASVector,float,ParMemoryBuffer>;
template struct Vector<BLASVector,double,ParMemoryBuffer>;

template struct Vector<EHVector,BufferIndex,ParMemoryBuffer>;
template struct Vector<EHVector,void*,ParMemoryBuffer>;
template struct Vector<EHVector,int,ParMemoryBuffer>;
template struct Vector<EHVector,long,ParMemoryBuffer>;
template struct Vector<EHVector,float,ParMemoryBuffer>;
template struct Vector<EHVector,double,ParMemoryBuffer>;