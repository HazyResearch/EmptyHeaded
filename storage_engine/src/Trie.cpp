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
#include "utils/ParMMapBuffer.hpp"
#include "utils/ParMemoryBuffer.hpp"
#include "Annotation.hpp"

template<class A,class M>
void Trie<A,M>::save(){
  std::ofstream *writefile = new std::ofstream();
  std::string file = memoryBuffers->path+M::folder+std::string("trieinfo.bin");
  writefile->open(file, std::ios::binary | std::ios::out);
  writefile->write((char *)&annotated, sizeof(annotated));
  writefile->write((char *)&num_rows, sizeof(num_rows));
  writefile->write((char *)&num_columns, sizeof(num_columns));
  writefile->write((char *)&memoryBuffers->num_buffers, sizeof(memoryBuffers->num_buffers));
  for(size_t i = 0; i < NUM_THREADS; i++){
    const size_t t_size = memoryBuffers->get_size(i);
    writefile->write((char *)&t_size, sizeof(t_size));
  }

  writefile->close();

  memoryBuffers->save();
}

template<class A,class M>
Trie<A,M>* Trie<A,M>::load(std::string path){
  Trie<A,M>* ret = new Trie<A,M>();

  std::ifstream *infile = new std::ifstream();
  std::string file = path+M::folder+std::string("trieinfo.bin");
  infile->open(file, std::ios::binary | std::ios::in);
  infile->read((char *)&ret->annotated, sizeof(ret->annotated));
  infile->read((char *)&ret->num_rows, sizeof(ret->num_rows));
  infile->read((char *)&ret->num_columns, sizeof(ret->num_columns));
  
  size_t w_n_threads;
  infile->read((char *)&w_n_threads, sizeof(w_n_threads));
  std::vector<size_t>* buf_sizes = new std::vector<size_t>();
  for(size_t i = 0 ; i < w_n_threads; i++){
    size_t b_size;
    infile->read((char *)&b_size, sizeof(b_size));
    buf_sizes->push_back(b_size);
  }
  infile->close();

  //init memory buffers
  ret->memoryBuffers = M::load(path,buf_sizes,w_n_threads);

  return ret;
}


template<class A,class M>
void recursive_foreach(
  const bool annotated,
  M* memoryBuffers,
  TrieBlock<layout,M> *current, 
  const size_t level, 
  const size_t num_levels,
  std::vector<uint32_t>* tuple,
  const std::function<void(std::vector<uint32_t>*,A)> body){

  if(level+1 == num_levels){
    current->set.foreach_index([&](uint32_t a_i, uint32_t a_d){
      tuple->push_back(a_d);
      (void) a_i;
      if(annotated)
        assert(false);
        //body(tuple,current->get_data(a_i,a_d));
      else 
        body(tuple,(A)0);
      tuple->pop_back();
    });
  } else {
    current->set.foreach_index([&](uint32_t a_i, uint32_t a_d){
      //if not done recursing and we have data
      tuple->push_back(a_d);
      if(current->get_next_block(a_i,a_d,memoryBuffers) != NULL){
        recursive_foreach<A,M>(
          annotated,
          memoryBuffers,
          current->get_next_block(a_i,a_d,memoryBuffers),
          level+1,
          num_levels,
          tuple,
          body);
      }
      tuple->pop_back(); //delete the last element
    });
  }
}

template<class A,class M>
TrieBlock<layout,M>* Trie<A,M>::getHead(){
  TrieBlock<layout,M>* head = (TrieBlock<layout,M>*)memoryBuffers->get_address(0,0);
  head->set.data = (uint8_t*)((uint8_t*)head + sizeof(TrieBlock<layout,M>));
  return head; 
}

/*
* Write the trie to a binary file 
*/
template<class A,class M>
void Trie<A,M>::foreach(const std::function<void(std::vector<uint32_t>*,A)> body){
  std::vector<uint32_t>* tuple = new std::vector<uint32_t>();
  TrieBlock<layout,M>* head = this->getHead();

  head->set.foreach_index([&](uint32_t a_i, uint32_t a_d){
    tuple->push_back(a_d);
    TrieBlock<layout,M>* next = head->get_next_block(a_i,a_d,memoryBuffers);
    if(num_columns > 1 && next != NULL){
      recursive_foreach<A,M>(
        annotated,
        memoryBuffers,
        next,
        1,
        num_columns,
        tuple,
        body);
    } else if(annotated) {
      assert(false);
      //body(tuple,head->get_data(a_i,a_d));
    } else{
      body(tuple,(A)0); 
    }
    tuple->pop_back(); //delete the last element
  });
}


/*
/////////////////////////////////////////////////////
Constructor code from here down
/////////////////////////////////////////////////////
*/


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
* Produce a TrieBlock
*/
template<class B, class A>
size_t build_block(
  const size_t tid,  
  A *data_allocator, 
  const size_t set_size, 
  uint32_t *set_data_buffer){

  const uint8_t * const start_block = data_allocator->get_next(tid,sizeof(B));
  const size_t offset = start_block-data_allocator->get_address(tid);

  const size_t set_range = (set_size > 1) ? (set_data_buffer[set_size-1]-set_data_buffer[0]) : 0;
  const size_t set_alloc_size =  layout::get_number_of_bytes(set_size,set_range);
  uint8_t* set_data_in = data_allocator->get_next(tid,set_alloc_size);

  B* block = TrieBlock<layout,A>::get_block(tid,offset,data_allocator);
  block->set = Set<layout>::from_array(set_data_in,set_data_buffer,set_size);

  assert(set_alloc_size >= block->set.number_of_bytes);
  data_allocator->roll_back(tid,set_alloc_size-block->set.number_of_bytes);

  return offset;
}

void encode_tail(size_t start, size_t end, uint32_t *data, std::vector<uint32_t> *current, uint32_t *indicies){
  for(size_t i = start; i < end; i++){
    *data++ = current->at(indicies[i]);
  }
}
/*
* Recursively build the trie. Terminates when we hit the number of levels.
*/
template<class B, class M, class A>
void recursive_build(
  const size_t index, 
  const size_t start, 
  const size_t end, 
  const uint32_t data, 
  const size_t offset, 
  const size_t level, 
  const size_t num_levels, 
  const size_t tid, 
  std::vector<std::vector<uint32_t>> *attr_in,
  M *data_allocator, 
  std::vector<size_t*> *ranges_buffer, 
  std::vector<uint32_t*> *set_data_buffer, 
  uint32_t *indicies,
  std::vector<A>* annotation){

  uint32_t *sb = set_data_buffer->at(level*NUM_THREADS+tid);
  encode_tail(start,end,sb,&attr_in->at(level),indicies);

  const size_t next_offset = build_block<B,M>(tid,data_allocator,(end-start),sb);
  const size_t tid_prev = level == 1 ? 0:tid;
  B* prev_block = B::get_block(tid_prev,offset,data_allocator);

  prev_block->set_block(index,data,tid,next_offset);

  if(level < (num_levels-1)){
    B* tail = (B*)data_allocator->get_address(tid,next_offset);
    tail->init_next(tid,data_allocator);
    auto tup = produce_ranges(start,end,ranges_buffer->at(level*NUM_THREADS+tid),set_data_buffer->at(level*NUM_THREADS+tid),indicies,&attr_in->at(level));
    const size_t set_size = std::get<0>(tup);
    for(size_t i = 0; i < set_size; i++){
      const size_t next_start = ranges_buffer->at(level*NUM_THREADS+tid)[i];
      const size_t next_end = ranges_buffer->at(level*NUM_THREADS+tid)[i+1];
      const uint32_t next_data = set_data_buffer->at(level*NUM_THREADS+tid)[i];        
      recursive_build<B,M,A>(
        i,
        next_start,
        next_end,
        next_data,
        next_offset,
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
    /*
    tail->alloc_data(tid,data_allocator);
    for(size_t i = start; i < end; i++){
      uint32_t data_value = attr_in->at(level).at(indicies[i]);
      R annotationValue = annotation->at(indicies[i]);
      tail->set_data(i-start,data_value,annotationValue);
    }
    */
  }
}

//builds the trie from an encoded relation
template<class A, class M>
Trie<A,M>::Trie(
  std::string path,
  std::vector<uint32_t>* max_set_sizes, 
  std::vector<std::vector<uint32_t>> *attr_in,
  std::vector<A>* annotation){

  annotated = annotation->size() > 0;
  num_rows = attr_in->at(0).size();
  num_columns = attr_in->size();
  //fixme: add estimate

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

  //set up temporary buffers needed for the build
  //fixme: add estimate
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
  
  //Build the head set.
  const size_t head_offset = 
    build_block<TrieBlock<layout,M>,M>(
      0,
      memoryBuffers,
      head_size,
      set_data_buffer->at(0));

  size_t cur_level = 1;
  if(num_columns > 1){
    TrieBlock<layout,M>* new_head = (TrieBlock<layout,M>*)memoryBuffers->get_address(0,head_offset);
    new_head->init_next(0,memoryBuffers);

    //encode the set, create a block with NULL pointers to next level
    //should be a 1-1 between pointers in block and next ranges
    //also a 1-1 between blocks and numbers of next ranges

    //reset new_head because a realloc could of occured
    new_head = TrieBlock<layout,M>::get_block(0,head_offset,memoryBuffers);
    const size_t loop_size = new_head->nextSize();
    par::for_range(0,loop_size,100,[&](size_t tid, size_t i){
      (void) tid;
      new_head->next(i)->index = -1;
    });

    //reset new_head because a realloc could of occured
    par::for_range(0,head_size,100,[&](size_t tid, size_t i){
      //some sort of recursion here
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
        annotation);
    });
  } else if(annotation->size() > 0){
    /*
    new_head->alloc_data(0,data_allocator);
    for(size_t i = 0; i < head_size; i++){
      const uint32_t data = set_data_buffer->at(0)[i]; 
      R annotationValue = annotation->at(indicies[i]);
      new_head->set_data(i,data,annotationValue);
    }
    */
  }

  for(size_t i = 0; i < num_columns; i++){
    //delete ranges_buffer->at(i);
    //delete set_data_buffer->at(i);
  }
  delete ranges_buffer;
  delete set_data_buffer;
}


template struct Trie<void*,ParMemoryBuffer>;
template struct Trie<long,ParMemoryBuffer>;
template struct Trie<int,ParMemoryBuffer>;
template struct Trie<float,ParMemoryBuffer>;
template struct Trie<double,ParMemoryBuffer>;

template struct Trie<void*,ParMMapBuffer>;
template struct Trie<long,ParMMapBuffer>;
template struct Trie<int,ParMMapBuffer>;
template struct Trie<float,ParMMapBuffer>;
template struct Trie<double,ParMMapBuffer>;