/******************************************************************************
*
* Author: Christopher R. Aberger
*
******************************************************************************/
#ifndef _TRANSITIVE_CLOSURE_H_
#define _TRANSITIVE_CLOSURE_H_

#include "Trie.hpp"

namespace tc {

  template<class T, class R,typename J>
  TrieBlock<T,R>* unweighted_single_source(
    uint32_t start,
    Trie<T,R>* graph, 
    R init, 
    const size_t num_distinct,
    allocator::memory<uint8_t> *output_buffer,
    J join){
    
    assert(graph->num_levels == 2);
    
    ///just stuff to setup the visited set
    const size_t visited_num_bytes = range_bitset::get_number_of_bytes(num_distinct,num_distinct);
    uint8_t* bs_data = output_buffer->get_next(0,visited_num_bytes);
    memset(bs_data,(uint8_t)0,visited_num_bytes);
    Set<range_bitset> vs = new(output_buffer->get_next(0, sizeof(Set<range_bitset>))) Set<range_bitset>(bs_data,num_distinct+1,num_distinct,visited_num_bytes,type::RANGE_BITSET);
    TrieBlock<range_bitset,R>* visited = new (output_buffer->get_next(0, sizeof(TrieBlock<range_bitset, R>)))
              TrieBlock<range_bitset,R>(vs);
    visited->init_data(0,output_buffer,(R)0);

    //setup buffers for the frontier
    allocator::memory<uint32_t>* frontier_allocator = new allocator::memory<uint32_t>(num_distinct);
    uint32_t** frontier_buffer = new uint32_t*[NUM_THREADS];
    uint32_t* frontier_sizes = new uint32_t[NUM_THREADS*PADDING];
    for(size_t i = 0; i < NUM_THREADS; i++){
      frontier_buffer[i] = frontier_allocator->get_next(i,num_distinct);
      frontier_sizes[i*PADDING] = 0;
    }

    //copy the data in the head set to the frontier and mark it as visited
    uint32_t* frontier = new uint32_t[num_distinct];
    uint32_t frontier_size = 1;
    ops::atomic_union(visited->set,start);
    visited->set_data(0,start,0);
    frontier[0] = start;

    size_t iteration = 0;
    while(frontier_size != 0){
      std::cout  << "ITERATION: " << iteration << " FRONTIER SIZE: " << frontier_size << std::endl;
      par::for_range(0,frontier_size,100,[&](size_t tid, size_t i){
        const uint32_t f = frontier[i];
        TrieBlock<T,R>* level2 = graph->head->get_block(f);
        level2->set.foreach([&](uint32_t l2){
          //union in element and return index if element does not exist in the set
          // and return -1 if it exists in the set (no work to do in the union)
          if(ops::atomic_union(visited->set,l2)){
            //std::cout << "data: " << visited->get_data(f) << " " << init << std::endl;
            visited->set_data(0,l2,join(visited->get_data(f),init)); //index does not matter this is dense
            const size_t buffer_index = frontier_sizes[tid*PADDING];
            frontier_buffer[tid][buffer_index] = l2;
            frontier_sizes[tid*PADDING]++;
          }
        });
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

    range_bitset::set_indices(visited->set.data,visited->set.cardinality,visited->set.number_of_bytes,visited->set.type);
    /*
    visited->set.foreach_index([&](uint32_t i, uint32_t d){
      std::cout << "i: " << i << " d: " << d << " data: " << visited->get_data(i,d) << std::endl;
    });
    */
    //reconstruct the visited set indicies, package into a trie and return
    return (TrieBlock<T,R>*)visited;
  }

  //TrieBlock<T,R> weighted_single_source(uint32_t start, Trie<T,R> Graph, function f)

  //Trie unweighted(Set<T> head, Trie<T,R> Graph)

  //weighted
}

#endif