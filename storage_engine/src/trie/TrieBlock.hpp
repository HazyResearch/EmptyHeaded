#ifndef _TRIEBLOCK_H_
#define _TRIEBLOCK_H_

#include "../utils/utils.hpp"
#include "set/ops.hpp"
#include "NextLevel.hpp"

template<class T, class M>
struct TrieBlock{
  bool is_sparse;  

  TrieBlock(){}

  inline const Set<T>* get_const_set() const {
    const Set<T>* const result = (const Set<T>*)((uint8_t*)this + sizeof(TrieBlock<T,M>));
    //result->data = ((uint8_t*)result + sizeof(Set<hybrid>));
    return result;
  }

  inline Set<T>* get_set() const {
    Set<T> *result = (Set<T>*)((uint8_t*)this + sizeof(TrieBlock<T,M>));
    //result->data = ((uint8_t*)result + sizeof(Set<hybrid>));
    return result;
  }

  inline size_t nextSize() {
   const Set<hybrid>* const set = this->get_const_set();
    is_sparse = common::is_sparse(set->cardinality,set->range);
    return is_sparse ? set->cardinality:(set->range+1);
  }

  inline size_t getNextIndex(const uint32_t index, const uint32_t data) const {
   const Set<T>* const set = this->get_const_set();
    return common::is_sparse(set->cardinality,set->range) ? index:data;
  }
  
  inline size_t get_index(const size_t index, const size_t data) const {
    return is_sparse ? index:data;
  }

  inline NextLevel* getNext(const size_t index) const {
    const Set<hybrid>* const set = this->get_const_set();
    return (NextLevel*)(
      ((uint8_t*)set)+
      sizeof(Set<hybrid>) +
      (set->number_of_bytes) +
      (sizeof(NextLevel)*index) );
  }

  inline void init_next(const size_t tid, M* allocator_in){
    const size_t next_size = this->nextSize();
    allocator_in->get_next(tid, sizeof(NextLevel)*(next_size));
  }

  inline void set_next_block(
    const uint32_t index, 
    const uint32_t data, 
    int setIndex,
    const size_t setOffset){

    const Set<hybrid>* const set = this->get_const_set();
    setIndex = (set->cardinality != 0) ? setIndex : -1;
    if(!is_sparse){  //allocate has to be called first
      (void) index;
      NextLevel* next = this->getNext(data);
      next->index = setIndex;
      next->offset = setOffset;
    } else{
      (void) data;
      NextLevel* next = this->getNext(index);
      next->index = setIndex;
      next->offset = setOffset;    
    }
  }

  inline static TrieBlock<T,M>* get_block(
    const int bufferIndex,
    const size_t bufferOffset,
    M* buffer) {
      if(bufferIndex == -1)
        return NULL;
      TrieBlock<T,M>* result = (TrieBlock<T,M>*) buffer->get_address(bufferIndex,bufferOffset);
      return result;
  }

  inline TrieBlock<T,M>* get_next_block(
    const uint32_t data,
    M* buffer) const {
    TrieBlock<T,M>* result = NULL;
    const Set<hybrid> * const set = this->get_const_set();
    if(!common::is_sparse(set->cardinality,set->range)){
      NextLevel* next = this->getNext(data);
      const int bufferIndex = next->index;
      const size_t bufferOffset = next->offset;
      if(bufferIndex != -1){
        result = (TrieBlock<T,M>*) buffer->get_address(bufferIndex,bufferOffset);
      }
    } else{
      //first need to see if the data is in the set
      const long index = set->find(data);
      if(index != -1){
        NextLevel* next = this->getNext(index);
        const int bufferIndex = next->index;
        const size_t bufferOffset = next->offset;
        if(bufferIndex != -1){
          result = (TrieBlock<T,M>*) buffer->get_address(bufferIndex,bufferOffset);
        }
      }
    }
    return result;
  }
  
  inline TrieBlock<T,M>* get_next_block(
    const uint32_t index, 
    const uint32_t data,
    M* buffer) const {
    //we must 
    TrieBlock<T,M>* result = NULL;
    const uint32_t nextIndex = this->getNextIndex(index,data);
    NextLevel* next = this->getNext(nextIndex);
    const int bufferIndex = next->index;
    const size_t bufferOffset = next->offset;
    assert(bufferIndex <= (int) NUM_THREADS);
    if(bufferIndex != -1){
      result = (TrieBlock<T,M>*) buffer->get_address(bufferIndex,bufferOffset);
    }
    return result;
  }

  inline bool contains(const uint32_t data) const {
    const Set<hybrid> * const set = this->get_const_set();
    return set->find(data) != -1;
  }

  template<class A>
  inline void set_annotation(
    A annotationValue,
    const uint32_t index, 
    const uint32_t data) {

    const Set<T>* s1 = this->get_const_set();
    const uint32_t annotationIndex = this->getNextIndex(index,data);
    A* annotation = (A*)( ((uint8_t*)s1)+
        sizeof(Set<T>) +
        s1->number_of_bytes);
    annotation[annotationIndex] = annotationValue;
  }

  template<class A>
  inline A get_annotation(
    const uint32_t index, 
    const uint32_t data) const {

    const Set<T>* s1 = this->get_const_set();
    const uint32_t annotationIndex = this->getNextIndex(index,data);
    A* annotation = (A*)( ((uint8_t*)s1)+
        sizeof(Set<T>) +
        s1->number_of_bytes);
    return annotation[annotationIndex];
  }

};

#endif
