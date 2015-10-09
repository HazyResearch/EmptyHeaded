#ifndef _TRIEBLOCK_H_
#define _TRIEBLOCK_H_

#include "../utils/utils.hpp"
#include "set/ops.hpp"

struct NextLevel{
  size_t index;
  size_t offset;
};

template<class T>
struct TrieBlock{
  Set<T> set;
  NextLevel next;
  bool is_sparse;

  TrieBlock(Set<T> setIn){
    set = setIn;
  }
  TrieBlock(){}
  TrieBlock(bool sparse){
    is_sparse = sparse;
  }
};

#endif
