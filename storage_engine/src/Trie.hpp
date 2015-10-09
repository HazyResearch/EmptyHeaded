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

/*
* Very simple tree structure stores the trie. All that is needed is the 
* head and number of levels.  Methods are to build a trie from an encoded
* table.
*/
template<class A>
struct Trie{
  bool annotated;
  size_t num_rows;
  size_t num_columns;
  A annotation = (A)0;
  uint8_t **data;

  Trie<A>(
    std::string path,
    std::vector<uint32_t>* max_set_sizes, 
    std::vector<std::vector<uint32_t> >* attr_in, 
    std::vector<A>* annotation);
};

#endif