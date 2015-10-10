#ifndef _TRIEBLOCK_H_
#define _TRIEBLOCK_H_

#include "../utils/utils.hpp"
#include "set/ops.hpp"

struct NextLevel{
  int index; //# of threads
  size_t offset;
};

template<class T, class M>
struct TrieBlock{
  bool is_sparse;  
  Set<T> set;

  TrieBlock(Set<T> setIn){
    set = setIn;
  }
  TrieBlock(){}


  inline size_t nextSize(){
    return is_sparse ? set.cardinality:(set.range+1);
  }

  inline NextLevel* next(size_t index){
    return (NextLevel*)(
      (uint8_t*)this + 
      sizeof(TrieBlock<T,M>) + 
      set.number_of_bytes +
      sizeof(NextLevel)*index);
  }

  inline void init_next(const size_t tid, M* allocator_in){
    is_sparse = common::is_sparse(set.cardinality,set.range);
    const size_t next_size = this->nextSize(); 
    allocator_in->get_next(tid, sizeof(NextLevel)*(next_size));
  }

  inline void set_block(
    const uint32_t index, 
    const uint32_t data, 
    int setIndex,
    const size_t setOffset){

    setIndex = (set.cardinality != 0) ? setIndex : -1;
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

  /*
  inline TrieBlock<T,R>* get_block(uint32_t data) const{
    TrieBlock<T,R>* result = NULL;
    if(!is_sparse){
      result = next_level[data];
    } else{
      //something like get the index from the set then move forward.
      const long index = set.find(data);
      if(index != -1)
        result = next_level[index];
    }
    return result;
  }
  */

  TrieBlock<T,M>* get_block(
    uint32_t index, 
    uint32_t data,
    M* buffer) {

    TrieBlock<T,M>* result = NULL;
    if(!this->is_sparse){
      NextLevel* next = this->next(data);
      const int bufferIndex = next->index;
      const size_t bufferOffset = next->offset;
      result = (TrieBlock<T,M>*) buffer->get_address(bufferIndex,bufferOffset);
      result->set.data = (uint8_t*)((uint8_t*)result + sizeof(TrieBlock<T,M>));
    } else{
      NextLevel* next = this->next(index);
      const int bufferIndex = next->index;
      const size_t bufferOffset = next->offset;
      result = (TrieBlock<T,M>*)buffer->get_address(bufferIndex,bufferOffset);   
      result->set.data = (uint8_t*)((uint8_t*)result + sizeof(TrieBlock<T,M>));
    }
    return result;
  }
};

#endif
