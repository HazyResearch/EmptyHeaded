/******************************************************************************
*
* Author: Christopher R. Aberger
*
* The top level datastructure. This class holds the methods to create the 
* trie from a table. The TrieBlock class holds the more interesting methods.
******************************************************************************/
#include "TrieBuilder.hpp"
#include "Trie.hpp"
#include "utils/ParMemoryBuffer.hpp"
#include "emptyheaded.hpp"

template<class A,class M>
TrieBuilder<A,M>::TrieBuilder(Trie<A,M>* t_in){
  trie = t_in;
  tmp_buffers.resize(t_in->num_columns);
  for(size_t i = 0; i < t_in->num_columns; i++){
    tmp_buffers.at(i) = new ParMemoryBuffer(100);
  }
}

template<class A,class M>
Set<hybrid> TrieBuilder<A,M>::build_aggregated_set(
  const size_t tid,
  const size_t level,
  TrieBlock<hybrid,M> *s1, 
  TrieBlock<hybrid,M> *s2){

    const size_t alloc_size =
      std::max(s1->set.number_of_bytes,
               s2->set.number_of_bytes);
    Set<hybrid> r(tmp_buffers.at(level)->get_next(tid,alloc_size));
    tmp_buffers.at(level)->roll_back(tid,alloc_size); 
    //we can roll back because we won't try another buffer at this level and tid until
    //after this memory is consumed.
    return ops::set_intersect(
            (Set<hybrid> *)&r, (const Set<hybrid> *)&s1->set,
            (const Set<hybrid> *)&s2->set);
}

template<class A,class M>
size_t TrieBuilder<A,M>::count_set(
  TrieBlock<hybrid,M> *s1, 
  TrieBlock<hybrid,M> *s2){
  size_t result = ops::set_intersect(
            (const Set<hybrid> *)&s1->set,
            (const Set<hybrid> *)&s2->set);
  return result;
}

template struct TrieBuilder<void*,ParMemoryBuffer>;
template struct TrieBuilder<long,ParMemoryBuffer>;
template struct TrieBuilder<int,ParMemoryBuffer>;
template struct TrieBuilder<float,ParMemoryBuffer>;
template struct TrieBuilder<double,ParMemoryBuffer>;

template struct TrieBuilder<void*,ParMMapBuffer>;
template struct TrieBuilder<long,ParMMapBuffer>;
template struct TrieBuilder<int,ParMMapBuffer>;
template struct TrieBuilder<float,ParMMapBuffer>;
template struct TrieBuilder<double,ParMMapBuffer>;
