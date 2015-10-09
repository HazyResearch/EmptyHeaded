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
  NextLevel* next;

  TrieBlock(Set<T> setIn){
    set = setIn;
  }
  TrieBlock(){}

  inline void init_pointers(const size_t tid, M* allocator_in){
    is_sparse = common::is_sparse(set.cardinality,set.range);
    if(!is_sparse){
      next = (NextLevel*)allocator_in->get_next(tid, sizeof(NextLevel)*(set.range+1));
    } else{
      next = (NextLevel*)allocator_in->get_next(tid, sizeof(NextLevel)*set.cardinality);
    }
  }

  inline void set_block(
    const uint32_t index, 
    const uint32_t data, 
    int setIndex,
    const size_t setOffset){

    setIndex = (set.cardinality != 0) ? setIndex : -1;
    if(!is_sparse){
      (void) index;
      next[data].index = setIndex;
      next[data].offset = setOffset;
    } else{
      (void) data;
      next[index].index = setIndex;
      next[index].offset = setOffset;    }
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
      const int bufferIndex = next[data].index;
      const size_t bufferOffset = next[data].offset;
      result = (TrieBlock<T,M>*) buffer->get_address(bufferIndex,bufferOffset);
      result->set.data = (uint8_t*)((uint8_t*)result + sizeof(TrieBlock<T,M>));
      result->next = (NextLevel*)((uint8_t*)result + sizeof(TrieBlock<T,M>) + result->set.number_of_bytes);

    } else{
      const int bufferIndex = next[index].index;
      const size_t bufferOffset = next[index].offset;
      result = (TrieBlock<T,M>*)buffer->get_address(bufferIndex,bufferOffset);   
      result->set.data = (uint8_t*)((uint8_t*)result + sizeof(TrieBlock<T,M>));
      result->next = (NextLevel*)((uint8_t*)result + sizeof(TrieBlock<T,M>) + result->set.number_of_bytes);
    }
    return result;
  }
};

#endif
