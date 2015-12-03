/******************************************************************************
*
* Author: Christopher R. Aberger
*
******************************************************************************/
#ifndef _TRANSITIVE_CLOSURE_H_
#define _TRANSITIVE_CLOSURE_H_

#include "trie/TrieBlock.hpp"

namespace tc {
  template<class T, class M, class A, typename J>
  void unweighted_single_source(
    const uint32_t start, // from encoding
    const size_t num_distinct, //from encoding
    Trie<void*,M>* input, //input graph
    Trie<A,M>* output, //output vector
    const A init,
    J join){
    
    assert(input->num_columns == 2);
    
    ///just stuff to setup the visited set
    const size_t visited_num_bytes = sizeof(TrieBlock<range_bitset,M>) +
      sizeof(Set<range_bitset>) +
      range_bitset::get_number_of_bytes(num_distinct,num_distinct) +
      (sizeof(A)*num_distinct);

    uint8_t* bs_data = output->memoryBuffers->get_next(NUM_THREADS,visited_num_bytes);
    memset(bs_data,(uint8_t)0,visited_num_bytes);

    TrieBlock<range_bitset,M>* visited = (TrieBlock<range_bitset,M>*)bs_data;

    Set<range_bitset>* vs = visited->get_set();
    vs->cardinality = num_distinct+1;
    vs->range = num_distinct;
    vs->number_of_bytes = range_bitset::get_number_of_bytes(num_distinct,num_distinct);
    vs->type = type::RANGE_BITSET;

    //setup buffers for the frontier
    uint32_t* frontier_allocator = new uint32_t[num_distinct*NUM_THREADS];
    uint32_t** frontier_buffer = new uint32_t*[NUM_THREADS];
    uint32_t* frontier_sizes = new uint32_t[NUM_THREADS*PADDING];
    for(size_t i = 0; i < NUM_THREADS; i++){
      frontier_buffer[i] = &frontier_allocator[i*num_distinct];
      frontier_sizes[i*PADDING] = 0;
    }

    //copy the data in the head set to the frontier and mark it as visited
    uint32_t* frontier = new uint32_t[num_distinct];
    uint32_t frontier_size = 1;
    ops::atomic_union(vs,start);
    //visited->set_data(0,start,0);
    frontier[0] = start;

    size_t iteration = 0;
    while(frontier_size != 0){
      std::cout  << "ITERATION: " << iteration << " FRONTIER SIZE: " << frontier_size << std::endl;
      par::for_range(0,frontier_size,100,[&](size_t tid, size_t i){
        const uint32_t f = frontier[i];
        TrieBlock<T,M>* level2 = input->getHead()->get_next_block(f,input->memoryBuffers);
        if(level2 != NULL){
          level2->get_const_set()->foreach([&](uint32_t l2){
            //union in element and return index if element does not exist in the set
            // and return -1 if it exists in the set (no work to do in the union)
            if(ops::atomic_union(vs,l2)){
              visited->template set_annotation<A>(join(visited->template get_annotation<A>(0,f)),0,l2); //index does not matter this is dense
              const size_t buffer_index = frontier_sizes[tid*PADDING];
              frontier_buffer[tid][buffer_index] = l2;
              frontier_sizes[tid*PADDING]++;
            }
          });
        }
      });
      //reconstruct frontier buffer
      frontier_size = 0;
      for(size_t t = 0 ; t < NUM_THREADS; t++){
        const size_t num_thread_elems = frontier_sizes[t*PADDING];
        memcpy(&frontier[frontier_size],frontier_buffer[t],num_thread_elems*sizeof(uint32_t));
        frontier_size += num_thread_elems;
        frontier_sizes[t*PADDING] = 0;
      }
      iteration++;
    }

    size_t numvis = range_bitset::set_indices(
      visited->get_const_set()->get_data(),
      visited->get_const_set()->cardinality,
      visited->get_const_set()->number_of_bytes,
      visited->get_const_set()->type);
    
    visited->get_set()->cardinality = numvis;
    output->num_rows = numvis;
    /*
    output->foreach([&](std::vector<uint32_t> *v, int a){
      std::cout << v->at(0) << "\t" << a << std::endl;
    });
    */
  }

  //TrieBlock<T,R> weighted_single_source(uint32_t start, Trie<T,R> Graph, function f)

  //Trie unweighted(Set<T> head, Trie<T,R> Graph)

  //weighted
}

#endif