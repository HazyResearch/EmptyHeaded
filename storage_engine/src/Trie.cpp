/******************************************************************************
*
* Author: Christopher R. Aberger
*
* The top level datastructure. This class holds the methods to create the 
* trie from a table. The TrieBlock class holds the more interesting methods.
******************************************************************************/
#include "tbb/parallel_sort.h"
#include "tbb/task_scheduler_init.h"
#include "Trie.hpp"
#include "trie/TrieBlock.hpp"

/*
* Recursive sort function to get the relation in order for the trie.
*/
struct SortColumns{
  std::vector<std::vector<uint32_t>> *columns; 
  SortColumns(std::vector<std::vector<uint32_t>> *columns_in){
    columns = columns_in;
  }
  bool operator()(uint32_t i, uint32_t j) const {
    for(size_t c = 0; c < columns->size(); c++){
      if(columns->at(c).at(i) != columns->at(c).at(j)){
        return columns->at(c).at(i) < columns->at(c).at(j);
      }
    }
    return false;
  }
};

/*
* Produce a TrieBlock
*/
/*
template<class B>
B* build_block(const size_t tid, allocator<uint8_t> *data_allocator, 
  const size_t set_size, uint32_t *set_data_buffer){

  B *block = (B*)data_allocator->get_next(tid,sizeof(B),BYTES_PER_CACHELINE);
  const size_t set_range = (set_size > 1) ? (set_data_buffer[set_size-1]-set_data_buffer[0]) : 0;
  const size_t set_alloc_size =  layout::get_number_of_bytes(set_size,set_range);
  uint8_t* set_data_in = data_allocator->get_next(tid,set_alloc_size,BYTES_PER_REG);
  block->set = Set<layout>::from_array(set_data_in,set_data_buffer,set_size);

  assert(set_alloc_size >= block->set.number_of_bytes);
  data_allocator->roll_back(tid,set_alloc_size-block->set.number_of_bytes);
  return block;
}
*/
/*
* Recursively build the trie. Terminates when we hit the number of levels.
*/
/*
template<class B, class R>
void recursive_build(
  const size_t index, 
  const size_t start, 
  const size_t end, 
  const uint32_t data, 
  B* prev_block, 
  const size_t level, 
  const size_t num_levels, 
  const size_t tid, 
  std::vector<std::vector<uint32_t>> *attr_in,
  allocator<uint8_t> *data_allocator, 
  std::vector<size_t*> *ranges_buffer, 
  std::vector<uint32_t*> *set_data_buffer, 
  uint32_t *indicies,
  std::vector<R>* annotation){

  uint32_t *sb = set_data_buffer->at(level*NUM_THREADS+tid);
  encode_tail(start,end,sb,&attr_in->at(level),indicies);

  B *tail = build_block<B,R>(tid,data_allocator,(end-start),sb);
  prev_block->set_block(index,data,tail);

  if(level < (num_levels-1)){
    tail->init_pointers(tid,data_allocator);
    auto tup = produce_ranges(start,end,ranges_buffer->at(level*NUM_THREADS+tid),set_data_buffer->at(level*NUM_THREADS+tid),indicies,&attr_in->at(level));
    const size_t set_size = std::get<0>(tup);
    for(size_t i = 0; i < set_size; i++){
      const size_t next_start = ranges_buffer->at(level*NUM_THREADS+tid)[i];
      const size_t next_end = ranges_buffer->at(level*NUM_THREADS+tid)[i+1];
      const uint32_t next_data = set_data_buffer->at(level*NUM_THREADS+tid)[i];        
      recursive_build<B,R>(
        i,
        next_start,
        next_end,
        next_data,
        tail,
        level+1,
        num_levels,
        tid,
        attr_in,
        data_allocator,
        ranges_buffer,
        set_data_buffer,
        indicies,
        annotation);
    }
  } else if(annotation->size() != 0){
    tail->alloc_data(tid,data_allocator);
    for(size_t i = start; i < end; i++){
      uint32_t data_value = attr_in->at(level).at(indicies[i]);
      R annotationValue = annotation->at(indicies[i]);
      tail->set_data(i-start,data_value,annotationValue);
    }
  }
}
*/

template<class R>
Trie<R>::Trie(
  std::string path,
  std::vector<uint32_t>* max_set_sizes, 
  std::vector<std::vector<uint32_t>> *attr_in,
  std::vector<R>* annotation){

  /*
  allocator<uint8_t>* data_allocator = new allocator<uint8_t>(1000);
  const size_t num_levels_in = attr_in->size();
  const size_t num_rows = attr_in->at(0).size();

  //Filter rows via selection and sort for the Trie
  uint32_t *indicies = new uint32_t[num_rows];
  uint32_t *iterator = indicies;
  for(size_t i = 0; i < num_rows; i++){
    *iterator++ = i; 
  }

  const size_t num_rows_post_filter = iterator-indicies;

  tbb::task_scheduler_init init(NUM_THREADS);
  tbb::parallel_sort(indicies,iterator,SortColumns(attr_in));

  std::vector<size_t*> *ranges_buffer = new std::vector<size_t*>();
  std::vector<uint32_t*> *set_data_buffer = new std::vector<uint32_t*>();
  
  //to do should be temp buffers
  for(size_t i = 0; i < num_levels_in; i++){
    for(size_t t = 0; t < NUM_THREADS; t++){
      size_t* ranges = (size_t*)data_allocator->get_next(t,sizeof(size_t)*(max_set_sizes->at(i)+1));
      uint32_t* sd = (uint32_t*)data_allocator->get_next(t,sizeof(uint32_t)*(max_set_sizes->at(i)+1));
      ranges_buffer->push_back(ranges);
      set_data_buffer->push_back(sd); 
    }
  }

  assert(num_levels_in != 0  && num_rows_post_filter != 0);

  //Find the ranges for distinct values in the head
  auto tup = produce_ranges(0,
    num_rows_post_filter,
    ranges_buffer->at(0),
    set_data_buffer->at(0),
    indicies,
    &attr_in->at(0));
  const size_t head_size = std::get<0>(tup);
  const size_t head_range = std::get<1>(tup);

  //Build the head set.
  TrieBlock<layout>* new_head = build_block<TrieBlock<layout>>(0,data_allocator,head_size,set_data_buffer->at(0));

  size_t cur_level = 1;
  if(num_levels_in > 1){
    new_head->init_pointers(0,data_allocator);
    
    par::for_range(0,head_range,100,[&](size_t tid, size_t i){
      (void) tid;
      new_head->next_level[i] = NULL;
    });

    par::for_range(0,head_size,100,[&](size_t tid, size_t i){
      //some sort of recursion here
      const size_t start = ranges_buffer->at(0)[i];
      const size_t end = ranges_buffer->at(0)[i+1];
      const uint32_t data = set_data_buffer->at(0)[i];

      recursive_build<TrieBlock<layout,R>,R>(
        i,
        start,
        end,
        data,
        new_head,
        cur_level,
        num_levels_in,
        tid,
        attr_in,
        data_allocator,
        ranges_buffer,
        set_data_buffer,
        indicies,
        annotation);
    });
  } else if(annotation->size() > 0){
    new_head->alloc_data(0,data_allocator);
    for(size_t i = 0; i < head_size; i++){
      const uint32_t data = set_data_buffer->at(0)[i]; 
      R annotationValue = annotation->at(indicies[i]);
      new_head->set_data(i,data,annotationValue);
    }
  }
  
  //encode the set, create a block with NULL pointers to next level
  //should be a 1-1 between pointers in block and next ranges
  //also a 1-1 between blocks and numbers of next ranges

  memory=data_allocator;
  head=new_head;
  num_levels=num_levels_in;
  annotated=(annotation->size()!=0);
  */
}


template struct Trie<void*>;
template struct Trie<long>;
template struct Trie<int>;
template struct Trie<float>;
template struct Trie<double>;


