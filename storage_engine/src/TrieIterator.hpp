/******************************************************************************
*
* Author: Christopher R. Aberger
*
* The top level datastructure. This class holds the methods to create the 
* trie from a table. The TrieBlock class holds the more interesting methods.
******************************************************************************/
#ifndef _TRIEITERATOR_H_
#define _TRIEITERATOR_H_

#include <stdint.h>
#include <stddef.h>
#include <vector>
#include <functional>
#include "layout.hpp"

template<class A, class M> struct Trie;

/*
* Very simple tree structure stores the trie. All that is needed is the 
* head and number of levels.  Methods are to build a trie from an encoded
* table.
*/
template<class A, class M>
struct TrieIterator{
  Trie<A,M>* trie;
  std::vector<TrieBlock<layout,M>*> levels;
  TrieIterator<A,M>(Trie<A,M>* t_in);

  void get_next_block(const uint32_t data);

  void get_next_block(
    const size_t level, 
    const uint32_t data);

  TrieBlock<hybrid,M>* get_block(
    const size_t level);
};

template<class A, class M>
struct ParTrieIterator{
  std::vector<TrieIterator<A,M>*> iterators;
  ParTrieIterator<A,M>(Trie<A,M>* t_in);
};



#endif