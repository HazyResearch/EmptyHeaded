/******************************************************************************
*
* Author: Christopher R. Aberger
*
* The top level datastructure. This class holds the methods to create the 
* trie from a table. The TrieBlock class holds the more interesting methods.
******************************************************************************/
#ifndef _TRIEBUILDER_H_
#define _TRIEBUILDER_H_

#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include "layout.hpp"
#include "utils/MemoryBuffer.hpp"
#include "trie/NextLevel.hpp"

template<class A, class M> struct Trie;

/*
* Very simple tree structure stores the trie. All that is needed is the 
* head and number of levels.  Methods are to build a trie from an encoded
* table.
*/
template<class A, class M>
struct TrieBuilder{
  Trie<A,M>* trie;
  std::vector<MemoryBuffer*> tmp_buffers;
  std::vector<NextLevel> next;
  TrieBuilder<A,M>(Trie<A,M>* t_in);

  Set<hybrid> build_aggregated_set(
    const size_t level,
    const TrieBlock<hybrid,M> *s1, 
    const TrieBlock<hybrid,M> *s2);

  size_t count_set(
    const TrieBlock<hybrid,M> *s1, 
    const TrieBlock<hybrid,M> *s2);

  Set<hybrid> build_set(
    const TrieBlock<hybrid,M> *s1,
    const TrieBlock<hybrid,M> *s2);

  Set<hybrid> set_next_level(
    const size_t cur_level,
    const TrieBlock<hybrid,M> *s1);
};

template<class A, class M>
struct ParTrieBuilder{
  std::vector<TrieBuilder<A,M>*> builders;
  ParTrieBuilder<A,M>(Trie<A,M>* t_in);

  Set<hybrid> build_aggregated_set(
    const TrieBlock<hybrid,M> *s1) const;

  Set<hybrid> build_set(
    const TrieBlock<hybrid,M> *s1);
};

#endif
