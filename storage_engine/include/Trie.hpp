/******************************************************************************
*
* Author: Christopher R. Aberger
*
* The top level datastructure. This class holds the methods to create the 
* trie from a table. The TrieBlock class holds the more interesting methods.
******************************************************************************/
#ifndef _TRIE_H_
#define _TRIE_H_

#include "utils/utils.hpp"

/*
* Very simple tree structure stores the trie. All that is needed is the 
* head and number of levels.  Methods are to build a trie from an encoded
* table.
*/
template<class T, class A, class M>
struct Trie{
  bool annotated;
  size_t num_rows;
  size_t num_columns;
  M *memoryBuffers;
  std::vector<void*> encodings;
  A annotation;

  void print();
  void foreach(const std::function<void(std::vector<uint32_t>*,A)> body);

  Trie<T,A,M>(
    std::string path,
    const std::vector<uint32_t>* restrict max_set_sizes, 
    const std::vector<std::vector<uint32_t> >* restrict attr_in, 
    const std::vector<void*>&);

  Trie<T,A,M>(const std::string path, 
    const size_t num_columns_in, 
    const bool annotated_in){
    
    annotated = annotated_in;
    memoryBuffers = new M(path,2);
    num_columns = num_columns_in;
  };

  ~Trie<T,A,M>(){
    delete memoryBuffers;
  };
};

#endif