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
  Meta* meta = new (memoryBuffer->get_address(bufferIndex)) Meta();
  return T:: template get<A,M>(
    data,
    meta,
    memoryBuffer,
    bufferIndex);
}

template <class T, class A, class M>
A* Vector<T,A,M>::get_block(const uint32_t data) const{
  Meta* meta = new (memoryBuffer->get_address(bufferIndex)) Meta();
  return T:: template get_block<A,M>(
    data,
    meta,
    memoryBuffer,
    bufferIndex);
}

//look up a data value
template <class T, class A, class M>
A Vector<T,A,M>::get(
  const uint32_t index,
  const uint32_t data) const{
  Meta* meta = new (memoryBuffer->get_address(bufferIndex)) Meta();
  return T:: template get<A,M>(
    index,
    data,
    meta,
    memoryBuffer,
    bufferIndex);
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
  Meta* meta = new (memoryBuffer->get_address(bufferIndex)) Meta();
  T:: template set<A,M>(
    index,
    data,
    value,
    meta,
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
  const size_t len){

  const size_t index = memoryBuffer->get_offset(tid);
  BufferIndex bufferIndex;
  bufferIndex.tid = tid;
  bufferIndex.index = index;
  const size_t num_bytes = T:: template get_num_bytes<A>(data,len);
  //reserve memory
  memoryBuffer->get_next(tid,num_bytes);
  Meta* meta = new(memoryBuffer->get_address(bufferIndex)) Meta();

  meta->cardinality = len;
  meta->start = 0;
  meta->end = 0;
  if(len > 0){
    meta->start = *data;
    meta->end = *(data+(len-1));
  }

  meta->type = T::get_type();
  T:: template from_array<A,M>(
    memoryBuffer->get_address(bufferIndex)+sizeof(Meta),
    data,
    values,
    len);

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
  BufferIndex bufferIndex;
  bufferIndex.tid = tid;
  bufferIndex.index = index;

  const size_t num_bytes = T:: template get_num_bytes<A>(data,len);
  //reserve memory
  memoryBuffer->get_next(tid,num_bytes);
  Meta* meta = new(memoryBuffer->get_address(bufferIndex)) Meta();

  meta->cardinality = len;
  meta->start = 0;
  meta->end = 0;
  if(len > 0){
    meta->start = *data;
    meta->end = *(data+(len-1));
  }
  meta->type = T::get_type();
  T:: template from_array<A,M>(
    memoryBuffer->get_address(bufferIndex)+sizeof(Meta),
    data,
    len);
  return Vector<T,A,M>(memoryBuffer,bufferIndex);
}


template struct Vector<SparseVector,void*,ParMemoryBuffer>;
template struct Vector<SparseVector,int,ParMemoryBuffer>;
template struct Vector<SparseVector,long,ParMemoryBuffer>;
template struct Vector<SparseVector,float,ParMemoryBuffer>;
template struct Vector<SparseVector,double,ParMemoryBuffer>;
template struct Vector<SparseVector,BufferIndex,ParMemoryBuffer>;

template struct Vector<DenseVector,void*,ParMemoryBuffer>;
template struct Vector<DenseVector,int,ParMemoryBuffer>;
template struct Vector<DenseVector,long,ParMemoryBuffer>;
template struct Vector<DenseVector,float,ParMemoryBuffer>;
template struct Vector<DenseVector,double,ParMemoryBuffer>;
template struct Vector<DenseVector,BufferIndex,ParMemoryBuffer>;