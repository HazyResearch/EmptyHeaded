/******************************************************************************
*
* Author: Christopher R. Aberger
*
* The top level datastructure. This class holds the methods to create the 
* trie from a table. The TrieBlock class holds the more interesting methods.
******************************************************************************/
#ifndef _TRIE_H_
#define _TRIE_H_

#include <stdint.h>
#include <stddef.h>
#include <vector>
#include <functional>

template<class T, class R> struct TrieBlock;
class hybrid;

typedef hybrid layout;

/*
* Very simple tree structure stores the trie. All that is needed is the 
* head and number of levels.  Methods are to build a trie from an encoded
* table.
*/
template<class R>
struct Trie{
  bool annotated;
  size_t num_levels;
  R annotation = (R)0;
  TrieBlock<layout,R>* head;

  Trie<R>(size_t num_levels_in, bool annotated_in){
    num_levels = num_levels_in;
    annotated = annotated_in;
  };

  Trie<R>(TrieBlock<layout,R>* head_in, size_t num_levels_in, bool annotated_in){
    num_levels = num_levels_in;
    head = head_in;
    annotated = annotated_in;
  };

  Trie<R>(
    std::vector<uint32_t>* max_set_sizes, 
    std::vector<std::vector<uint32_t> >* attr_in, 
    std::vector<R>* annotation);

  void foreach(const std::function<void(std::vector<uint32_t>*,R)> body);
  
  void recursive_foreach(
    TrieBlock<layout,R> *current, 
    const size_t level, 
    const size_t num_levels,
    std::vector<uint32_t>* tuple, 
    const std::function<void(std::vector<uint32_t>*,R)> body);
};

#endif