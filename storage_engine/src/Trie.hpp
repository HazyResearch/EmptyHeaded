/******************************************************************************
*
* Author: Christopher R. Aberger
*
* The top level datastructure. This class holds the methods to create the 
* trie from a table. The TrieBlock class holds the more interesting methods.
******************************************************************************/
#ifndef _TRIE_H_
#define _TRIE_H_

#include <iostream>
#include <string>
#include <vector>
#include <functional>
#include "layout.hpp"

/*
* Very simple tree structure stores the trie. All that is needed is the 
* head and number of levels.  Methods are to build a trie from an encoded
* table.
*/
template<class A, class M>
struct Trie{
  bool annotated;
  size_t num_rows;
  size_t num_columns;
  M *memoryBuffers;
  std::vector<void*> encodings;
  A annotation = (A)0;

  Trie<A,M>(){}
  Trie<A,M>(bool annotated_in, size_t num_rows_in, size_t num_columns_in, M* buf_in){
    annotated = annotated_in;
    num_rows = num_rows_in;
    num_columns = num_columns_in;
    memoryBuffers = buf_in;
  };

  Trie<A,M>(std::string path, size_t num_columns_in, bool annotated_in){
    annotated = annotated_in;
    memoryBuffers = new M(path,100);
    num_columns = num_columns_in;
  };

  Trie<A,M>(
    std::string path,
    std::vector<uint32_t>* max_set_sizes, 
    std::vector<std::vector<uint32_t> >* attr_in, 
    std::vector<A>* annotation);

  ~Trie<A,M>(){};

  void foreach(const std::function<void(std::vector<uint32_t>*,A)> body);
  void save();
  TrieBlock<layout,M>* getHead();

  static Trie<A,M>* load(std::string path);
};

#endif