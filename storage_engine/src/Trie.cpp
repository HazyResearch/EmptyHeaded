/******************************************************************************
*
* Author: Christopher R. Aberger
*
* The top level datastructure. This class holds the methods to create the 
* trie from a table. The TrieBlock class holds the more interesting methods.
******************************************************************************/
#include "tbb/parallel_sort.h"
#include "tbb/task_scheduler_init.h"
#include "Vector.hpp"
#include "VectorOps.hpp"
#include "Trie.hpp"

typedef SparseVector VectorType;

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
* Given a range of values figure out the distinct values to go in the set.
* Helper method for the constructor
*/
std::tuple<size_t,size_t> produce_ranges(
  size_t start, 
  size_t end, 
  size_t *next_ranges, 
  uint32_t *data,
  uint32_t *indicies, 
  std::vector<uint32_t> * current){

  size_t range = 0;

  size_t num_distinct = 0;
  size_t i = start;
  while(true){
    const size_t start_range = i;
    const uint32_t cur = current->at(indicies[i]);
    uint32_t prev = cur;

    next_ranges[num_distinct] = start_range;
    data[num_distinct] = cur;

    ++num_distinct;
    range = cur;

    while(cur == prev){
      if((i+1) >= end)
        goto FINISH;
      prev = current->at(indicies[++i]);
    }
  }
  FINISH:
  next_ranges[num_distinct] = end;
  return std::tuple<size_t,size_t>(num_distinct,range+1);
}

/*
* Produce a Vector
*/
template<class A,class M>
Vector<VectorType,A,MemoryBuffer> build_vector(
  const size_t thread_id,
  M *buffer, 
  const uint32_t const * set_data_buffer,
  const A const * annotation,
  const size_t set_size){

  const size_t offset = buffer->get_offset(thread_id);
  return Vector<VectorType,A,MemoryBuffer>::from_array(
    buffer->elements.at(thread_id),
    set_data_buffer,
    annotation,
    set_size);
}

/*
* Produce a Vector
*/
template<class A,class M>
Vector<VectorType,A,MemoryBuffer> build_vector(
  const size_t thread_id,
  M *buffer, 
  const uint32_t const * set_data_buffer,
  const size_t set_size){

  const size_t offset = buffer->get_offset(thread_id);
  return Vector<VectorType,A,MemoryBuffer>::from_array(
    buffer->elements.at(thread_id),
    set_data_buffer,
    set_size);
}

size_t encode_tail(
  const size_t start, 
  const size_t end,
  uint32_t * data,
  const std::vector<uint32_t>& current, 
  uint32_t *indicies){
  long prev_data = -1;
  size_t data_size = 0;
  for(size_t i = start; i < end; i++){
    if(prev_data != (long)current.at(indicies[i])){
      *data++ = current.at(indicies[i]);
      prev_data = (long)current.at(indicies[i]);
      data_size++;
    }
  }
  return data_size;
}

/*
* Recursively build the trie. Terminates when we hit the number of levels.
*/
template<class A, class M>
size_t recursive_build(
  const size_t tid, 
  const size_t start, 
  const size_t end, 
  const uint32_t data, 
  const size_t level, 
  const size_t num_levels, 
  std::vector<std::vector<uint32_t>> *attr_in,
  M *data_allocator, 
  std::vector<size_t*> *ranges_buffer, 
  std::vector<uint32_t*> *set_data_buffer, 
  uint32_t *indicies,
  std::vector<A>* annotation){

  uint32_t *sb = set_data_buffer->at(tid*num_levels+level);
  const size_t set_size = encode_tail(start,end,sb,attr_in->at(level),indicies);
  if(level < (num_levels-1)){
    Vector<VectorType,NextLevel,MemoryBuffer> head = build_vector<NextLevel,M>(
        tid,
        data_allocator,
        sb,
        set_size); 

    auto tup = produce_ranges(
      start,
      end,
      ranges_buffer->at(tid*num_levels+level),
      set_data_buffer->at(tid*num_levels+level),
      indicies,
      &attr_in->at(level));
    const size_t set_size = std::get<0>(tup);
    for(size_t i = 0; i < set_size; i++){
      const size_t next_start = ranges_buffer->at(tid*num_levels+level)[i];
      const size_t next_end = ranges_buffer->at(tid*num_levels+level)[i+1];
      const uint32_t next_data = set_data_buffer->at(tid*num_levels+level)[i]; 
      const size_t next_index = recursive_build<A,M>(
        tid,
        next_start,
        next_end,
        next_data,
        level+1,
        num_levels,
        attr_in,
        data_allocator,
        ranges_buffer,
        set_data_buffer,
        indicies,
        annotation);
      NextLevel nl;
      nl.tid = tid;
      nl.index = next_index;
      head.set(i,next_data,nl); 
    }
    return head.buffer.index;
  } else if(annotation->size() != 0) {
    Vector<VectorType,A,MemoryBuffer> head = build_vector<A,M>(
        tid,
        data_allocator,
        sb,
        annotation->data(),
        set_size); 
    return head.buffer.index;
  } else {
    Vector<VectorType,A,MemoryBuffer> head = build_vector<A,M>(
        tid,
        data_allocator,
        sb,
        set_size); 
    return head.buffer.index;
  }
}

//builds the trie from an encoded relation
template<class A, class M>
Trie<A,M>::Trie(
  std::string path,
  std::vector<uint32_t>* max_set_sizes, 
  std::vector<std::vector<uint32_t>>* attr_in,
  std::vector<A>* annotations){

  annotation = (A)0;
  annotated = annotations->size() > 0;
  num_rows = attr_in->at(0).size();
  num_columns = attr_in->size();

  memoryBuffers = new M(path,2);  
  assert(num_columns != 0  && num_rows != 0);

  //Setup indices buffer
  uint32_t *indicies = new uint32_t[num_rows];
  uint32_t *iterator = indicies;
  for(size_t i = 0; i < num_rows; i++){
    *iterator++ = i; 
  }

  //sort the relation
  tbb::task_scheduler_init init(NUM_THREADS);
  tbb::parallel_sort(indicies,iterator,SortColumns(attr_in));

  //DEBUG
  /*
  std::cout << "TRIEE BUULd" << std::endl;
  std::cout << num_columns << std::endl;
  for(size_t i = 0; i < num_rows; i++){
    for(size_t j = 0; j < num_columns; j++){
      std::cout << attr_in->at(j).at(indicies[i]) << "\t";
    }
    std::cout << std::endl;
  }
  */
  
  //set up temporary buffers needed for the build
  std::vector<size_t*> *ranges_buffer = new std::vector<size_t*>();
  std::vector<uint32_t*> *set_data_buffer = new std::vector<uint32_t*>();

  size_t alloc_size = 0;
  for(size_t i = 0; i < num_columns; i++){
    alloc_size += max_set_sizes->at(i)+1;
  }
  size_t* tmp_st = new size_t[alloc_size*NUM_THREADS];
  uint32_t* tmp_i = new uint32_t[alloc_size*NUM_THREADS];

  size_t index = 0;
  for(size_t t = 0; t < NUM_THREADS; t++){
    for(size_t i = 0; i < num_columns; i++){
      ranges_buffer->push_back(&tmp_st[index]);
      set_data_buffer->push_back(&tmp_i[index]); 
      index += max_set_sizes->at(i)+1;
    }
  }

  //Find the ranges for distinct values in the head
  auto tup = produce_ranges(0,
    num_rows,
    ranges_buffer->at(0),
    set_data_buffer->at(0),
    indicies,
    &attr_in->at(0));
  const size_t head_size = std::get<0>(tup);
  
  std::cout << "BUILDING" << std::endl;

  if(num_columns > 1){
    Vector<VectorType,NextLevel,MemoryBuffer> head = build_vector<NextLevel,M>(
        NUM_THREADS,
        memoryBuffers,
        set_data_buffer->at(0),
        head_size); 
    //change this to returning a set
    //par for over the set
    //reset new_head because a realloc could of occured
    std::cout << head_size << std::endl;
    par::for_range(0,head_size,100,[&](const size_t tid, const size_t i){
      const size_t start = ranges_buffer->at(0)[i];
      const size_t end = ranges_buffer->at(0)[i+1];
      const uint32_t data = set_data_buffer->at(0)[i];
      const size_t next_index = recursive_build<A,M>(
        tid,
        start,
        end,
        data,
        1,
        num_columns,
        attr_in,
        memoryBuffers,
        ranges_buffer,
        set_data_buffer,
        indicies,
        annotations);
      NextLevel nl;
      nl.tid = tid;
      nl.index = next_index;
      head.set(i,data,nl);
    });
  } else if(annotations->size() > 0){
      build_vector<A,M>(
        NUM_THREADS,
        memoryBuffers,
        set_data_buffer->at(0),
        annotations->data(),
        head_size);
  } else {
      build_vector<A,M>(
        NUM_THREADS,
        memoryBuffers,
        set_data_buffer->at(0),
        head_size);
  }
  /*
  //Build the head set.


  size_t cur_level = 1;
  if(num_columns > 1){
    TrieBlock<layout,M>* new_head = 
      (TrieBlock<layout,M>*)memoryBuffers->head->get_address(head_offset);
    new_head->init_next(NUM_THREADS,memoryBuffers);

    //encode the set, create a block with NULL pointers to next level
    //should be a 1-1 between pointers in block and next ranges
    //also a 1-1 between blocks and numbers of next ranges

    //reset new_head because a realloc could of occured
    new_head = (TrieBlock<layout,M>*)memoryBuffers->head->get_address(head_offset);
    const size_t loop_size = new_head->nextSize();
    par::for_range(0,loop_size,100,[&](size_t tid, size_t i){
      (void) tid;
      new_head->getNext(i)->index = -1;
    });
    //reset new_head because a realloc could of occured
    par::for_range(0,head_size,100,[&](size_t tid, size_t i){
      const size_t start = ranges_buffer->at(0)[i];
      const size_t end = ranges_buffer->at(0)[i+1];
      const uint32_t data = set_data_buffer->at(0)[i];
      recursive_build<TrieBlock<layout,M>,M,A>(
        i,
        start,
        end,
        data,
        head_offset,
        cur_level,
        num_columns,
        tid,
        attr_in,
        memoryBuffers,
        ranges_buffer,
        set_data_buffer,
        indicies,
        annotations);
    });
  } else if(annotations->size() > 0){
    TrieBlock<layout,M>* new_head = (TrieBlock<layout,M>*)memoryBuffers->head->get_address(head_offset);
    //perform allocation for annotation (0 = tid)
    //nextSize should gets the size of pointers or annotations (should be renamed?)
    memoryBuffers->head->get_next(sizeof(A)*(new_head->nextSize()));
    //could of realloced, get the head again
    new_head = (TrieBlock<layout,M>*)memoryBuffers->head->get_address(head_offset);

    for(size_t i = 0; i < head_size; i++){
      const uint32_t data = set_data_buffer->at(0)[i]; 
      A annotationValue = annotations->at(indicies[i]);
      new_head->template set_annotation<A>(annotationValue,i,data);
    }
  }
  */

  delete[] tmp_st;
  delete[] tmp_i;
  delete ranges_buffer;
  delete set_data_buffer;
}


template struct Trie<void*,ParMemoryBuffer>;
template struct Trie<long,ParMemoryBuffer>;
template struct Trie<int,ParMemoryBuffer>;
template struct Trie<float,ParMemoryBuffer>;
template struct Trie<double,ParMemoryBuffer>;