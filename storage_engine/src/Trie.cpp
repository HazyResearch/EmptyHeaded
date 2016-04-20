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

template<class T,class A,class M>
void Trie<T,A,M>::print(){
  foreach([&](std::vector<uint32_t> *v, A anno){
    for(size_t i = 0 ; i < v->size(); i++){
      std::cout << v->at(i) << "\t";
    }
    std::cout << anno << std::endl;
  });
}

template<class T, class A, class M>
void unpack_last(  
  const bool annotated,
  std::vector<uint32_t>* tuple,
  const Vector<T,A,M>& head,
  const std::function<void(std::vector<uint32_t>*,A)> body){
 if(annotated){
    head.foreach([&](const uint32_t a_i, const uint32_t a_d, const A& anno){
      (void) a_i;
      tuple->push_back(a_d);
      body(tuple,anno);
      tuple->pop_back(); //delete the last element
    });
  } else{
    head.foreach_index([&](const uint32_t a_i, const uint32_t a_d){
      (void) a_i;
      tuple->push_back(a_d);
      body(tuple,(A)0);
      tuple->pop_back(); //delete the last element 
    });
  }
}

template<class T, class A, class M>
void recursive_foreach(
  const bool annotated,
  std::vector<uint32_t>* tuple,
  const Vector<EHVector,BufferIndex,M>& head,
  M* memoryBuffers,
  const size_t level,
  const size_t num_levels,
  const std::function<void(std::vector<uint32_t>*,A)> body){

  assert(level < num_levels);
  if(level == (num_levels-1)){
    head.foreach([&](const uint32_t a_i, const uint32_t a_d, const BufferIndex& nl){
      (void) a_i;
      tuple->push_back(a_d);
      Vector<T,A,M> next(memoryBuffers,nl);
      unpack_last<T,A,M>(annotated,tuple,next,body);
      tuple->pop_back(); //delete the last element
    });
  } else {
    head.foreach([&](const uint32_t a_i, const uint32_t a_d, const BufferIndex& nl){
      (void) a_i;
      tuple->push_back(a_d);
      Vector<EHVector,BufferIndex,M> next(memoryBuffers,nl);
      recursive_foreach<T,A,M>(annotated,tuple,next,memoryBuffers,level+1,num_levels,body);
      tuple->pop_back(); //delete the last element
    });
  }
}

template<class T, class A,class M>
void Trie<T,A,M>::foreach(const std::function<void(std::vector<uint32_t>*,A)> body){
  std::vector<uint32_t>* tuple = new std::vector<uint32_t>();  
  if(num_columns > 1){
    BufferIndex bi;
    bi.tid = NUM_THREADS;
    bi.index = 0;
    Vector<EHVector,BufferIndex,M> head(memoryBuffers,bi);
    recursive_foreach<T,A,M>(annotated,tuple,head,memoryBuffers,1,num_columns,body);
  } else if(num_columns == 1){
    BufferIndex bi;
    bi.tid = NUM_THREADS;
    bi.index = 0;
    Vector<T,A,M> head(memoryBuffers,bi);
    unpack_last<T,A,M>(annotated,tuple,head,body);
  } else if(annotated){
    body(tuple,(A)annotation); 
  } 
}

/*
* Recursive sort function to get the relation in order for the trie.
*/
struct SortColumns{
  std::vector<std::vector<uint32_t>> *columns; 
  SortColumns(std::vector<std::vector<uint32_t>> *columns_in){
    columns = columns_in;
  }
  bool operator()(const uint32_t i, const uint32_t j) const {
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
template<class T, class A,class M>
Vector<T,A,M> build_vector(
  const size_t thread_id,
  M *buffer, 
  const uint32_t * const set_data_buffer,
  const A * const annotation,
  const size_t set_size,
  const size_t anno_offset){

  Vector<T,A,M> v = Vector<T,A,M>::from_array(
    thread_id,
    buffer,
    set_data_buffer,
    annotation,
    set_size,
    anno_offset);

  /*
  std::cout << "set size: " << set_size << std::endl;
  v.foreach_index([&](uint32_t i, uint32_t d){
    std::cout << d << std::endl;
  });
  std::cout << std::endl;
  */

  return v;
}

/*
* Produce a Vector
*/
template<class T, class A,class M>
Vector<T,A,M> build_vector(
  const size_t thread_id,
  M *buffer, 
  const uint32_t * const set_data_buffer,
  const size_t set_size){

  Vector<T,A,M> v = Vector<T,A,M>::from_array(
    thread_id,
    buffer,
    set_data_buffer,
    set_size);

  return v;
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

template<class A>
void encode_annotation(
  const size_t start, 
  const size_t end,
  A * annotation_output,
  const A * const annotation_input, 
  uint32_t *indicies){
  for(size_t i = start; i < end; i++){
    annotation_output[i-start] = annotation_input[indicies[i]];
  }
}

/*
* Recursively build the trie. Terminates when we hit the number of levels.
*/
template<class T, class A, class M>
size_t recursive_build(
  const size_t tid, 
  const size_t start, 
  const size_t end, 
  const size_t level, 
  const size_t num_levels, 
  std::vector<std::vector<uint32_t>> *attr_in,
  M *data_allocator, 
  std::vector<size_t*> *ranges_buffer, 
  std::vector<uint32_t*> *set_data_buffer, 
  uint32_t *indicies,
  const std::vector<void*>& annotation,
  A * annotation_buffer,
  const size_t anno_offset,
  std::vector<size_t>* dense_annotation_offsets){

  uint32_t *sb = set_data_buffer->at(tid*num_levels+level);
  const size_t set_size = encode_tail(start,end,sb,attr_in->at(level),indicies);
  if(level < (num_levels-1)){
    Vector<EHVector,BufferIndex,M> head = build_vector<EHVector,BufferIndex,M>(
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
      const size_t next_index = recursive_build<T,A,M>(
        tid,
        next_start,
        next_end,
        level+1,
        num_levels,
        attr_in,
        data_allocator,
        ranges_buffer,
        set_data_buffer,
        indicies,
        annotation,
        annotation_buffer,
        dense_annotation_offsets->at(indicies[next_start]),
        dense_annotation_offsets);
      BufferIndex nl;
      nl.tid = tid;
      nl.index = next_index;
      head.set(i,next_data,nl); 
    }
    return head.bufferIndex.index;
  } else if(annotation.size() != 0) { 
    encode_annotation<A>(
      start,
      end,
      annotation_buffer,
      (const A* const)annotation.at(0),
      indicies);
    Vector<T,A,M> head = build_vector<T,A,M>(
        tid,
        data_allocator,
        sb,
        (const A* const) annotation_buffer,
        set_size,
        anno_offset); 
    return head.bufferIndex.index;
  } else {
    Vector<EHVector,A,M> head = build_vector<EHVector,A,M>(
        tid,
        data_allocator,
        sb,
        set_size); 
    return head.bufferIndex.index;
  }
}

template<class T, class A, class M>
void build_trie(
  const size_t num_columns,
  const size_t head_size,
  ParMemoryBuffer* memoryBuffers,
  std::vector<size_t*> *ranges_buffer,
  std::vector<uint32_t*> *set_data_buffer,
  A** annotation_buffers,
  const std::vector<void*>& annotations,
  std::vector<std::vector<uint32_t>>* attr_in,
  uint32_t *indicies,
  std::vector<size_t> *dense_annotation_offsets
){
  if(num_columns > 1){
    Vector<EHVector,BufferIndex,M> head = build_vector<EHVector,BufferIndex,M>(
        NUM_THREADS,
        memoryBuffers,
        set_data_buffer->at(0),
        head_size);
    //change this to returning a set
    //par for over the set
    //reset new_head because a realloc could of occured
    par::for_range(0,head_size,100,[&](const size_t tid, const size_t i){
      const size_t start = ranges_buffer->at(0)[i];
      const size_t end = ranges_buffer->at(0)[i+1];
      const uint32_t data = set_data_buffer->at(0)[i];
      const size_t next_index = recursive_build<T,A,M>(
        tid,
        start,
        end,
        1,
        num_columns,
        attr_in,
        memoryBuffers,
        ranges_buffer,
        set_data_buffer,
        indicies,
        annotations,
        annotation_buffers[tid],
        dense_annotation_offsets->at(indicies[start]),
        dense_annotation_offsets);
      BufferIndex nl;
      nl.tid = tid;
      nl.index = next_index;
      head.set(i,data,nl);
      Vector<T,BufferIndex,M> v(memoryBuffers,nl);
    });
  } else if(annotations.size() > 0){
      encode_annotation<A>(
        0,
        head_size,
        annotation_buffers[0],
        (const A* const)annotations.at(0),
        indicies);
      build_vector<T,A,M>(
        NUM_THREADS,
        memoryBuffers,
        set_data_buffer->at(0),
        (const A* const)annotation_buffers[0],
        head_size,
        0);
  } else {
      build_vector<EHVector,A,M>(
        NUM_THREADS,
        memoryBuffers,
        set_data_buffer->at(0),
        head_size);
  }
}

//builds the trie from an encoded relation
template<class T,class A, class M>
Trie<T,A,M>::Trie(
  const std::vector<size_t> blocked_dimensions,
  std::string path,
  const std::vector<uint32_t>* restrict start_max_set_sizes, 
  const std::vector<std::vector<uint32_t>>* restrict start_attr,
  const std::vector<void*>& annotations){

  //pull out basic fields
  const size_t num_rows_in = start_attr->at(0).size();
  const size_t num_columns_in = start_attr->size(); 
  annotated = annotations.size() > 0;
  assert(num_columns_in != 0  && num_rows_in != 0);
  memoryBuffers = new M(path,2);  

  //////////////////////////////////////////////////////////////////////////////
  //some statistics
  size_t full_range = (num_columns_in == 1) ? ((start_max_set_sizes->at(0) + 64 - 1) / 64) * 64: start_max_set_sizes->at(0);
  dimensions.push_back(full_range);
  for(size_t i = 1; i < num_columns_in; i++){
    size_t dimension = 0; 
    if(i == num_columns_in-1)
      dimension = ((start_max_set_sizes->at(i) + 64 - 1) / 64) * 64;
    else
      dimension = start_max_set_sizes->at(i);
    full_range *= dimension;
    dimensions.push_back(dimension);
  }
  const float density = ((float)num_rows_in/(float)full_range);
  std::cout << "DENSITY: " << density << std::endl;
  
  if(annotated && T::is_blas()){
    const size_t num_anno_bytes = sizeof(A)*full_range;
    memoryBuffers->anno->get_next(num_anno_bytes);
    A* anno = (A*)memoryBuffers->anno->get_address(0);
    memset(anno,0,num_anno_bytes);
    for(size_t i = 0; i < num_rows_in; i++){
      size_t index = 0;
      for(size_t j = 1; j < num_columns_in; j++){
        const size_t dense_block_size = (j == num_columns_in-1) ?
         ((start_max_set_sizes->at(j) + 64 - 1) / 64) * 64 : start_max_set_sizes->at(j);
        index += std::pow(dense_block_size,j)*start_attr->at(j-1).at(i);
      }
      index += start_attr->at(num_columns_in-1).at(i);
      anno[index] = ((const A* const)annotations.at(0))[i];
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  //creates cache blocks for our tries
  std::vector<uint32_t>* max_set_sizes = new std::vector<uint32_t>();
  for(size_t i = 0; i < num_columns_in; i++){
    auto it = std::find(blocked_dimensions.begin(),blocked_dimensions.end(),i);
    if(it != blocked_dimensions.end()){
      max_set_sizes->insert(
        max_set_sizes->end(),
        start_max_set_sizes->at(i));
    }
  }
  max_set_sizes->insert(
    max_set_sizes->end(),
    start_max_set_sizes->begin(),
    start_max_set_sizes->end());

  std::vector<std::vector<uint32_t>>* blocks = new std::vector<std::vector<uint32_t>>();
  std::vector<size_t>* dense_annotation_offsets = new std::vector<size_t>();
  bool first = true;
  for(size_t i = 0; i < num_rows_in; i++){
    size_t anno_offset = 0;
    size_t blocks_offset = 0;
    for(size_t j = 0; j < num_columns_in; j++){
      auto it = std::find(blocked_dimensions.begin(),blocked_dimensions.end(),j);
      if(it != blocked_dimensions.end()){
        if(first)
          blocks->push_back(std::vector<uint32_t>());
        blocks->at(blocks_offset++).push_back((start_attr->at(j).at(i))/BLOCK_SIZE);
      }

      if(j != 0){
        const size_t dense_block_size = (j == num_columns_in-1) ?
         ((start_max_set_sizes->at(j) + 64 - 1) / 64) * 64 : start_max_set_sizes->at(j);
        anno_offset += std::pow(dense_block_size,j)*start_attr->at(j-1).at(i);
      } else
        anno_offset += start_attr->at(num_columns_in-1).at(i);
    }
    first = false;
    dense_annotation_offsets->push_back(anno_offset);
  }

  std::vector<std::vector<uint32_t>>* attr_in = new std::vector<std::vector<uint32_t>>();
  attr_in->insert(attr_in->begin(),blocks->begin(),blocks->end());
  attr_in->insert(attr_in->end(),start_attr->begin(),start_attr->end());
  
  //////////////////////////////////////////////////////////////////////////////////

  annotation = (A)0;
  num_rows = attr_in->at(0).size();
  num_columns = attr_in->size();

  assert(num_columns != 0 && num_rows != 0);

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
  for(size_t i = 0; i < num_rows; i++){
    //std::cout << indicies[i] << std::endl;
    for(size_t j = 0; j < num_columns; j++){
      std::cout << attr_in->at(j).at(indicies[i]) << "\t";    
    }
    std::cout << std::endl;
  }

  //set up temporary buffers needed for the build
  std::vector<size_t*> *ranges_buffer = new std::vector<size_t*>();
  std::vector<uint32_t*> *set_data_buffer = new std::vector<uint32_t*>();
  A** annotation_buffers = new A*[NUM_THREADS];

  size_t alloc_size = 0;
  for(size_t i = 0; i < num_columns; i++){
    alloc_size += (max_set_sizes->at(i)+1);
  }
  size_t* tmp_st = new size_t[alloc_size*NUM_THREADS];
  uint32_t* tmp_i = new uint32_t[alloc_size*NUM_THREADS];

  size_t index = 0;
  for(size_t t = 0; t < NUM_THREADS; t++){
    annotation_buffers[t] = new A[max_set_sizes->at(num_columns-1)];
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
  
  //make this into a templated method,
  //if we are competely dense the structure is a bunch of Vector<void*>

  build_trie<T,A,M>(
    num_columns,
    head_size,
    memoryBuffers,
    ranges_buffer,
    set_data_buffer,
    annotation_buffers,
    annotations,
    attr_in,
    indicies,
    dense_annotation_offsets);

  delete[] tmp_st;
  delete[] tmp_i;
  delete ranges_buffer;
  delete set_data_buffer;
  delete[] annotation_buffers;
}

template struct Trie<EHVector,void*,ParMemoryBuffer>;
template struct Trie<EHVector,long,ParMemoryBuffer>;
template struct Trie<EHVector,int,ParMemoryBuffer>;
template struct Trie<EHVector,float,ParMemoryBuffer>;
template struct Trie<EHVector,double,ParMemoryBuffer>;

template struct Trie<BLASVector,void*,ParMemoryBuffer>;
template struct Trie<BLASVector,long,ParMemoryBuffer>;
template struct Trie<BLASVector,int,ParMemoryBuffer>;
template struct Trie<BLASVector,float,ParMemoryBuffer>;
template struct Trie<BLASVector,double,ParMemoryBuffer>;
