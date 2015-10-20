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
  //A annotation = (A)0;

  Trie<A,M>(){};

  Trie<A,M>(std::string path, size_t num_columns_in, bool annotated_in){
    annotated = annotated_in;
    memoryBuffers = new M(path,7000);
    num_columns = num_columns_in;
  };

  Trie<A,M>(
    std::string path,
    std::vector<uint32_t>* max_set_sizes, 
    std::vector<std::vector<uint32_t> >* attr_in, 
    std::vector<A>* annotation);

  ~Trie<A,M>(){
    delete memoryBuffers;
  };

  void foreach(const std::function<void(std::vector<uint32_t>*,A)> body);
  void save();
  TrieBlock<layout,M>* getHead();

  static Trie<A,M>* load(std::string path);
};

#endif