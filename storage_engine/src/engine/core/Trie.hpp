/******************************************************************************
*
* Author: Christopher R. Aberger
*
* The top level datastructure. This class holds the methods to create the 
* trie from a table. The TrieBlock class holds the more interesting methods.
******************************************************************************/
#ifndef _TRIE_H_
#define _TRIE_H_

#include "TrieBlock.hpp"
#include "Encoding.hpp"
#include "tbb/parallel_sort.h"
#include "tbb/task_scheduler_init.h"

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

  template<typename F>
  static Trie<T,R>* build(
    allocator::memory<uint8_t>* data_allocator,
    std::vector<uint32_t>* max_set_sizes, 
    std::vector<std::vector<uint32_t>>* attr_in, 
    std::vector<R>* annotation, 
    F f);

  template<typename F>
  void foreach(const F body);
  
  template<typename F>
  void recursive_foreach(
    TrieBlock<T,R> *current, 
    const size_t level, 
    const size_t num_levels,
    std::vector<uint32_t>* tuple, 
    const F body);
  
  void to_binary(const std::string path);

  static Trie<T,R>* from_binary(std::string path, bool annotated_in);
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

template<class T, class R>
void recursive_visit(
  const size_t tid,
  TrieBlock<T,R> *current, 
  const size_t level, 
  const size_t num_levels,
  const uint32_t prev_index,
  const uint32_t prev_data, 
  std::vector<std::vector<std::ofstream*>>* writefiles,
  bool annotated){

  current->to_binary(writefiles->at(level).at(tid),prev_index,prev_data,annotated && level+1 == num_levels);
  current->set.foreach_index([&](uint32_t a_i, uint32_t a_d){
    //if not done recursing and we have data
    if(level+1 != num_levels && current->get_block(a_i,a_d) != NULL){
      recursive_visit(
        tid,
        current->get_block(a_i,a_d),
        level+1,
        num_levels,
        a_i,
        a_d,
        writefiles,
        annotated);
    }
  });
}

template<class T, class R>
void recursive_build_binary(
  const size_t tid,
  TrieBlock<T,R> *prev, 
  const size_t level, 
  const size_t num_levels,
  std::vector<std::vector<std::ifstream*>>* infiles,
  allocator::memory<uint8_t> *allocator_in,
  bool annotated_in){

  auto tup = TrieBlock<T,R>::from_binary(infiles->at(level).at(tid),allocator_in,tid,annotated_in && level+1 == num_levels);

  TrieBlock<T,R>* current = std::get<0>(tup);
  const uint32_t index = std::get<1>(tup);
  const uint32_t data = std::get<2>(tup);
  prev->set_block(index,data,current);

  if(level+1 == num_levels)
    return;

  current->init_pointers(tid,allocator_in);
  for(size_t i = 0; i < current->set.cardinality; i++){
    recursive_build_binary<T,R>(tid,current,level+1,num_levels,infiles,allocator_in,annotated_in);
  }
}

/*
* Write the trie to a binary file 
*/
template<class T, class R> template<typename F>
void Trie<T,R>::foreach(const F body){
  const Set<T> A = head->set;
  std::vector<uint32_t>* tuple = new std::vector<uint32_t>();
  A.foreach_index([&](uint32_t a_i, uint32_t a_d){
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

/*
* Write the trie to a binary file 
*/
template<class T, class R>
void Trie<T,R>::to_binary(const std::string path){
  //write the number of levels out first
  std::ofstream *writefile = new std::ofstream();
  std::string file = path+std::string("levels.bin");
  writefile->open(file, std::ios::binary | std::ios::out);
  writefile->write((char *)&num_levels, sizeof(num_levels));
  writefile->close();

  //open files for writing
  std::vector<std::vector<std::ofstream*>> writefiles;
  for(size_t l = 0; l < num_levels; l++){
    std::vector<std::ofstream*> myv;
    for(size_t i = 0; i < NUM_THREADS; i++){
      //prepare the data files (one per level per thread)
      writefile = new std::ofstream();
      file = path+std::string("data_l")+std::to_string(l)+std::string("_t")+std::to_string(i)+std::string(".bin");
      writefile->open(file, std::ios::binary | std::ios::out);
      myv.push_back(writefile);
    }
    writefiles.push_back(myv);
  }

  //write the data
  head->to_binary(writefiles.at(0).at(0),0,0,annotated && num_levels ==1);
  //dump the set contents
  const Set<T> A = head->set;
  if(num_levels > 1){
    A.static_par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d){
      if(head->get_block(a_i,a_d) != NULL){
        recursive_visit<T,R>(
          tid,
          head->get_block(a_i,a_d),
          1,
          num_levels,
          a_i,
          a_d,
          &writefiles,
          annotated);
      }
    });
  }

  //close the files
  for(size_t l = 0; l < num_levels; l++){
    for(size_t i = 0; i < NUM_THREADS; i++){
      writefiles.at(l).at(i)->close();
    }
  }
}

template<class T, class R>
Trie<T,R>* Trie<T,R>::from_binary(const std::string path, bool annotated_in){
  size_t num_levels_in;
  //first read the number of levels
  std::ifstream *infile = new std::ifstream();
  std::string file = path+std::string("levels.bin");
  infile->open(file, std::ios::binary | std::ios::in);
  infile->read((char *)&num_levels_in, sizeof(num_levels_in));
  infile->close();

  //open files for reading
  std::vector<std::vector<std::ifstream*>> infiles;
  for(size_t l = 0; l < num_levels_in; l++){
    std::vector<std::ifstream*> myv;
    for(size_t i = 0; i < NUM_THREADS; i++){
      infile = new std::ifstream();
      file = path+std::string("data_l")+std::to_string(l)+std::string("_t")+std::to_string(i)+std::string(".bin");
      infile->open(file, std::ios::binary | std::ios::in);
      myv.push_back(infile);
    }
    infiles.push_back(myv);
  }

  allocator::memory<uint8_t> *allocator_in = new allocator::memory<uint8_t>(10000); //TODO Fix this.
  auto tup = TrieBlock<T,R>::from_binary(infiles.at(0).at(0),allocator_in,0,annotated_in && num_levels_in == 1);
  TrieBlock<T,R>* head = std::get<0>(tup);  
  head->init_pointers(0,allocator_in);
  const Set<T> A = head->set;
  //use the same parallel call so we read in properly
  if(num_levels_in > 1){
    A.static_par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d){
      (void) a_d; (void) a_i;
      recursive_build_binary<T,R>(
        tid,
        head,
        1,
        num_levels_in,
        &infiles,
        allocator_in,
        annotated_in);
    });
  }

  //close the files
  for(size_t l = 0; l < num_levels_in; l++){
    for(size_t i = 0; i < NUM_THREADS; i++){
      infiles.at(l).at(i)->close();
    }
  }

  return new Trie<T,R>(head,num_levels_in,annotated_in);
}
/*
* Given a range of values figure out the distinct values to go in the set.
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

void encode_tail(size_t start, size_t end, uint32_t *data, std::vector<uint32_t> *current, uint32_t *indicies){
  for(size_t i = start; i < end; i++){
    *data++ = current->at(indicies[i]);
  }
}

/*
* We opitionally provide the ability to materialize selections while building the trie.
*/
template<typename F>
uint32_t* perform_selection(uint32_t *iterator, size_t num_rows, F f){
  for(size_t i = 0; i < num_rows; i++){
    if(f(i)){
      *iterator++ = i; 
    }
  }
  return iterator;
}

/*
* Produce a TrieBlock
*/
template<class B, class T, class R>
B* build_block(const size_t tid, allocator::memory<uint8_t> *data_allocator, 
  const size_t set_size, uint32_t *set_data_buffer){

  B *block = (B*)data_allocator->get_next(tid,sizeof(B),BYTES_PER_CACHELINE);
  const size_t set_range = (set_size > 1) ? (set_data_buffer[set_size-1]-set_data_buffer[0]) : 0;
  const size_t set_alloc_size =  T::get_number_of_bytes(set_size,set_range);
  uint8_t* set_data_in = data_allocator->get_next(tid,set_alloc_size,BYTES_PER_REG);
  block->set = Set<T>::from_array(set_data_in,set_data_buffer,set_size);

  assert(set_alloc_size >= block->set.number_of_bytes);
  data_allocator->roll_back(tid,set_alloc_size-block->set.number_of_bytes);
  return block;
}
/*
* Recursively build the trie. Terminates when we hit the number of levels.
*/
template<class B,class T, class R>
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
  allocator::memory<uint8_t> *data_allocator, 
  std::vector<size_t*> *ranges_buffer, 
  std::vector<uint32_t*> *set_data_buffer, 
  uint32_t *indicies,
  std::vector<R>* annotation){

  uint32_t *sb = set_data_buffer->at(level*NUM_THREADS+tid);
  encode_tail(start,end,sb,&attr_in->at(level),indicies);

  B *tail = build_block<B,T,R>(tid,data_allocator,(end-start),sb);
  prev_block->set_block(index,data,tail);

  if(level < (num_levels-1)){
    tail->init_pointers(tid,data_allocator);
    auto tup = produce_ranges(start,end,ranges_buffer->at(level*NUM_THREADS+tid),set_data_buffer->at(level*NUM_THREADS+tid),indicies,&attr_in->at(level));
    const size_t set_size = std::get<0>(tup);
    for(size_t i = 0; i < set_size; i++){
      const size_t next_start = ranges_buffer->at(level*NUM_THREADS+tid)[i];
      const size_t next_end = ranges_buffer->at(level*NUM_THREADS+tid)[i+1];
      const uint32_t next_data = set_data_buffer->at(level*NUM_THREADS+tid)[i];        
      recursive_build<B,T,R>(
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

template<class T, class R> template <typename F>
inline Trie<T,R>* Trie<T,R>::build(
  allocator::memory<uint8_t>* data_allocator,
  std::vector<uint32_t>* max_set_sizes, 
  std::vector<std::vector<uint32_t>> *attr_in,
  std::vector<R>* annotation, 
  F f){
  const size_t num_levels_in = attr_in->size();
  const size_t num_rows = attr_in->at(0).size();

  //Filter rows via selection and sort for the Trie
  uint32_t *indicies = new uint32_t[num_rows];
  uint32_t *iterator = perform_selection(indicies,num_rows,f);
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
  TrieBlock<T,R>* new_head = build_block<TrieBlock<T,R>,T,R>(0,data_allocator,head_size,set_data_buffer->at(0));

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

      recursive_build<TrieBlock<T,R>,T,R>(
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

  return new Trie<T,R>(new_head,num_levels_in,annotation->size()!=0);
}
#endif