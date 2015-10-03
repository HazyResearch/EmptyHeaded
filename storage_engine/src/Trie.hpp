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

template<class T, class R> struct TrieBlock;

/*
* Very simple tree structure stores the trie. All that is needed is the 
* head and number of levels.  Methods are to build a trie from an encoded
* table.
*/
template<class T, class R>
struct Trie{
  bool annotated;
  size_t num_levels;
  R annotation = (R)0;
  TrieBlock<T,R>* head;

  Trie<T,R>(size_t num_levels_in, bool annotated_in){
    num_levels = num_levels_in;
    annotated = annotated_in;
  };

  Trie<T,R>(TrieBlock<T,R>* head_in, size_t num_levels_in, bool annotated_in){
    num_levels = num_levels_in;
    head = head_in;
    annotated = annotated_in;
  };

  Trie<T,R>(
    std::vector<uint32_t>* max_set_sizes, 
    std::vector<std::vector<uint32_t>>* attr_in, 
    std::vector<R>* annotation);

  template<typename F>
  void foreach(const F body);
  
  template<typename F>
  void recursive_foreach(
    TrieBlock<T,R> *current, 
    const size_t level, 
    const size_t num_levels,
    std::vector<uint32_t>* tuple, 
    const F body);
};


template<class T, class R> template <typename F>
void Trie<T,R>::recursive_foreach(
  TrieBlock<T,R> *current, 
  const size_t level, 
  const size_t num_levels,
  std::vector<uint32_t>* tuple,
  const F body){

  if(level+1 == num_levels){
    current->set.foreach_index([&](uint32_t a_i, uint32_t a_d){
      tuple->push_back(a_d);
      if(annotated)
        body(tuple,current->get_data(a_i,a_d));
      else 
        body(tuple,(R)0);
      tuple->pop_back();
    });
  } else {
    current->set.foreach_index([&](uint32_t a_i, uint32_t a_d){
      //if not done recursing and we have data
      tuple->push_back(a_d);
      if(current->get_block(a_i,a_d) != NULL){
        recursive_foreach(current->get_block(a_i,a_d),level+1,num_levels,tuple,body);
      }
      tuple->pop_back(); //delete the last element
    });
  }
}

/*
* Write the trie to a binary file 
*/
template<class T, class R> template<typename F>
void Trie<T,R>::foreach(const F body){
  std::vector<uint32_t>* tuple = new std::vector<uint32_t>();
  head->set.foreach_index([&](uint32_t a_i, uint32_t a_d){
    tuple->push_back(a_d);
    if(num_levels > 1 && head->get_block(a_i,a_d) != NULL){
      recursive_foreach(head->get_block(a_i,a_d),1,num_levels,tuple,body);
    } else if(annotated) {
      body(tuple,head->get_data(a_i,a_d));
    } else{
      body(tuple,(R)0); 
    }
    tuple->pop_back(); //delete the last element
  });
}

#endif