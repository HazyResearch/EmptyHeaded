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
    tmp_buffers.at(i) = new MemoryBuffer(100);
  }
}

template<class A,class M>
Set<hybrid> TrieBuilder<A,M>::build_aggregated_set(
  const size_t level,
  const TrieBlock<hybrid,M> *s1, 
  const TrieBlock<hybrid,M> *s2){

    const size_t alloc_size =
      std::max(s1->set.number_of_bytes,
               s2->set.number_of_bytes);
    Set<hybrid> r((uint8_t*) (tmp_buffers.at(level)->get_next(alloc_size)) );
    tmp_buffers.at(level)->roll_back(alloc_size); 
    //we can roll back because we won't try another buffer at this level and tid until
    //after this memory is consumed.
    return ops::set_intersect(
            (Set<hybrid> *)&r, (const Set<hybrid> *)&s1->set,
            (const Set<hybrid> *)&s2->set);
}

template<class A,class M>
size_t TrieBuilder<A,M>::count_set(
  const TrieBlock<hybrid,M> *s1, 
  const TrieBlock<hybrid,M> *s2){
  size_t result = ops::set_intersect(
            (const Set<hybrid> *)&s1->set,
            (const Set<hybrid> *)&s2->set);
  return result;
}

template<class A, class M>
ParTrieBuilder<A,M>::ParTrieBuilder(Trie<A,M> *t_in){
  builders.resize(NUM_THREADS);
  for(size_t i = 0; i < NUM_THREADS; i++){
    builders.at(i) = new TrieBuilder<A,M>(t_in);
  }
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

template struct ParTrieBuilder<void*,ParMemoryBuffer>;
template struct ParTrieBuilder<long,ParMemoryBuffer>;
template struct ParTrieBuilder<int,ParMemoryBuffer>;
template struct ParTrieBuilder<float,ParMemoryBuffer>;
template struct ParTrieBuilder<double,ParMemoryBuffer>;

template struct ParTrieBuilder<void*,ParMMapBuffer>;
template struct ParTrieBuilder<long,ParMMapBuffer>;
template struct ParTrieBuilder<int,ParMMapBuffer>;
template struct ParTrieBuilder<float,ParMMapBuffer>;
template struct ParTrieBuilder<double,ParMMapBuffer>;
