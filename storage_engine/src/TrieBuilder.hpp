/******************************************************************************
*
* Author: Christopher R. Aberger
*
* The top level datastructure. This class holds the methods to create the 
* trie from a table. The TrieBlock class holds the more interesting methods.
******************************************************************************/
#ifndef _TRIEBUILDER_H_
#define _TRIEBUILDER_H_

#include <vector>
#include "layout.hpp"

template<class A, class M> struct Trie;

/*
* Very simple tree structure stores the trie. All that is needed is the 
* head and number of levels.  Methods are to build a trie from an encoded
* table.
*/
template<class A, class M>
struct TrieBuilder{
  Trie<A,M>* trie;
  std::vector<ParMemoryBuffer*> tmp_buffers;
  TrieBuilder<A,M>(Trie<A,M>* t_in);

  Set<hybrid>* build_aggregated_set(
    const size_t tid,
    const size_t level,
    TrieBlock<hybrid,M> *s1, 
    TrieBlock<hybrid,M> *s2);

  size_t count_set(
    TrieBlock<hybrid,M> *s1, 
    TrieBlock<hybrid,M> *s2);

};

#endif