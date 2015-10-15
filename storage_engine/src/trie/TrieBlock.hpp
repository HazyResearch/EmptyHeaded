#ifndef _TRIEBLOCK_H_
#define _TRIEBLOCK_H_

#include "../utils/utils.hpp"
#include "set/ops.hpp"
#include "NextLevel.hpp"

template<class T, class M>
struct TrieBlock{
  bool is_sparse;  

  TrieBlock(){}

  inline Set<hybrid>* get_set() const {
      Set<hybrid> *result = (Set<hybrid>*)((uint8_t*)this + sizeof(TrieBlock<T,M>));
      result->data = ((uint8_t*)result + sizeof(Set<hybrid>));
      return result;
  }

  inline size_t nextSize() {
    Set<hybrid> *set = this->get_set();
    is_sparse = common::is_sparse(set->cardinality,set->range);
    return is_sparse ? set->cardinality:(set->range+1);
  }
  
  inline size_t get_index(const size_t index, const size_t data) const {
    return is_sparse ? index:data;
  }

  inline NextLevel* next(size_t index) const {
    Set<hybrid> *set = this->get_set();
    return (NextLevel*)(
      (uint8_t*)this + 
      sizeof(TrieBlock<T,M>) + 
      sizeof(Set<hybrid>) +
      set->number_of_bytes +
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

    Set<hybrid> *set = this->get_set();
    setIndex = (set->cardinality != 0) ? setIndex : -1;
    if(!is_sparse){
      (void) index;
      NextLevel* next = this->next(data);
      next->index = setIndex;
      next->offset = setOffset;
    } else{
      (void) data;
      NextLevel* next = this->next(index);
      next->index = setIndex;
      next->offset = setOffset;    }
  }

  inline static TrieBlock<T,M>* get_block(
    const int bufferIndex,
    const size_t bufferOffset,
    M* buffer) {
      TrieBlock<T,M>* result = (TrieBlock<T,M>*) buffer->get_address(bufferIndex,bufferOffset);
      return result;
  }

  inline TrieBlock<T,M>* get_next_block(
    const uint32_t data,
    M* buffer) const {

    Set<hybrid> *set = this->get_set();
    TrieBlock<T,M>* result = NULL;
    if(!is_sparse){
      NextLevel* next = this->next(data);
      const int bufferIndex = next->index;
      const size_t bufferOffset = next->offset;
      if(bufferIndex != -1){
        result = (TrieBlock<T,M>*) buffer->get_address(bufferIndex,bufferOffset);
      }
    } else{
      //first need to see if the data is in the set
      const long index = set->find(data);
      if(index != -1){
        NextLevel* next = this->next(data);
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

    TrieBlock<T,M>* result = NULL;
    const uint32_t nextIndex = this->is_sparse ? index:data;
    NextLevel* next = this->next(nextIndex);
    const int bufferIndex = next->index;
    const size_t bufferOffset = next->offset;
    if(bufferIndex != -1){
      result = (TrieBlock<T,M>*) buffer->get_address(bufferIndex,bufferOffset);
    }
    return result;
  }
};

#endif
