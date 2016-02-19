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

template<class A,class M>
void Trie<A,M>::save(){
  std::ofstream *writefile = new std::ofstream();
  std::string file = memoryBuffers->path+M::folder+std::string("trieinfo.bin");
  writefile->open(file, std::ios::binary | std::ios::trunc);
  writefile->write((char *)&annotated, sizeof(annotated));
  writefile->write((char *)&annotation, sizeof(annotation));
  writefile->write((char *)&num_rows, sizeof(num_rows));
  writefile->write((char *)&num_columns, sizeof(num_columns));
  writefile->write((char *)&memoryBuffers->num_buffers, sizeof(memoryBuffers->num_buffers));
  
  const size_t h_size = memoryBuffers->head->getSize();
  writefile->write((char *)&h_size, sizeof(h_size));
  for(size_t i = 0; i < memoryBuffers->num_buffers; i++){
    const size_t t_size = memoryBuffers->get_size(i);
    writefile->write((char *)&t_size, sizeof(t_size));
  }
  writefile->close();

  memoryBuffers->save();
}

template<class A,class M>
Trie<A,M>* Trie<A,M>::load(std::string path){
  bool annotated_in;
  A annotation_in;
  size_t num_rows;
  size_t num_columns;

  std::ifstream *infile = new std::ifstream();
  std::string file = path+M::folder+std::string("trieinfo.bin");
  infile->open(file, std::ios::binary | std::ios::in);
  infile->read((char *)&annotated_in, sizeof(annotated_in));
  infile->read((char *)&annotation_in, sizeof(annotation_in));
  infile->read((char *)&num_rows, sizeof(num_rows));
  infile->read((char *)&num_columns, sizeof(num_columns));

  size_t w_n_threads;
  infile->read((char *)&w_n_threads, sizeof(w_n_threads));
  std::vector<size_t> buf_sizes;
  size_t h_size;
  infile->read((char *)&h_size, sizeof(h_size));
  buf_sizes.push_back(h_size);
  
  for(size_t i = 0 ; i < w_n_threads; i++){
    size_t b_size;
    infile->read((char *)&b_size, sizeof(b_size));
    buf_sizes.push_back(b_size);
  }
  infile->close();

  //init memory buffers
  M* memoryBuffers_in = M::load(path,w_n_threads,&buf_sizes);

  return new Trie<A,M>(
    annotated_in,
    annotation_in,
    num_rows,
    num_columns,
    memoryBuffers_in);
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
    current->get_set()->foreach_index([&](const uint32_t a_i, const uint32_t a_d){
      tuple->push_back(a_d);
      (void) a_i;
      if(annotated){
        const A annotation = current->template get_annotation<A>(a_i,a_d);
        body(tuple,annotation);
      } else{
        body(tuple,(A)0);
      } 
      tuple->pop_back();
    });
  } else {
    current->get_set()->foreach_index([&](const uint32_t a_i, const uint32_t a_d){
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
  TrieBlock<layout,M>* head = (TrieBlock<layout,M>*)(memoryBuffers->get_address(NUM_THREADS,0));
  return head; 
}

/*
* Write the trie to a binary file 
*/
template<class A,class M>
void Trie<A,M>::foreach(const std::function<void(std::vector<uint32_t>*,A)> body){
  std::vector<uint32_t>* tuple = new std::vector<uint32_t>();
  TrieBlock<layout,M>* head = this->getHead();
  if(head->get_set()->cardinality > 0){
    head->get_set()->foreach_index([&](uint32_t a_i, uint32_t a_d){
      tuple->push_back(a_d);
      if(num_columns > 1){
        TrieBlock<layout,M>* next = head->get_next_block(a_i,a_d,memoryBuffers);
        if(next != NULL){
          recursive_foreach<A,M>(
            annotated,
            memoryBuffers,
            next,
            1,
            num_columns,
            tuple,
            body);
        }
      } else if(annotated) {
        const A annotationValue = head->template get_annotation<A>(a_i,a_d);
        body(tuple,annotationValue);
      } else if(num_columns == 1){
        body(tuple,(A)0); 
      }
      tuple->pop_back(); //delete the last element
    });
  } else if(annotated){
    body(tuple,(A)annotation); 
  }
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

  Set<hybrid>* myset = (Set<hybrid>*)(data_allocator->get_next(tid,sizeof(Set<layout>)));
  const size_t set_range = (set_size > 1) ? (set_data_buffer[set_size-1]-set_data_buffer[0]) : 0;
  const size_t set_alloc_size =  layout::get_number_of_bytes(set_size,set_range)*2+100;
  uint8_t* set_data_in = data_allocator->get_next(tid,set_alloc_size);
  
  TrieBlock<layout,A>* tmp = TrieBlock<layout,A>::get_block(tid,offset,data_allocator);
  myset = tmp->get_set();
  myset->from_array(set_data_in,set_data_buffer,set_size);

///some debug code for safety (should be in a debug pragma) FIXME
  size_t check_index = 0;
  myset->foreach_index([&](uint32_t index, uint32_t data){
    (void) data; (void) index;
    assert(data == set_data_buffer[index]);
    check_index++;
  });
  assert(check_index == set_size);
//end debug code

  assert(set_alloc_size >= myset->number_of_bytes);
  data_allocator->roll_back(tid,set_alloc_size-myset->number_of_bytes);

  return offset;
}

/*
* Produce a TrieBlock
*/
template<class B, class A>
size_t build_head(
  A *data_allocator, 
  const size_t set_size, 
  uint32_t *set_data_buffer){

  const uint8_t * const start_block = (uint8_t*) (data_allocator->get_next(NUM_THREADS,sizeof(B)));
  const size_t offset = start_block-((uint8_t*)data_allocator->get_address(NUM_THREADS));

  Set<layout>* myset = (Set<layout>*)(data_allocator->get_next(NUM_THREADS,sizeof(Set<layout>)));
  const size_t set_range = (set_size > 1) ? (set_data_buffer[set_size-1]-set_data_buffer[0]) : 0;
  const size_t set_alloc_size =  layout::get_number_of_bytes(set_size,set_range)+100;
  uint8_t* set_data_in = (uint8_t*)data_allocator->get_next(NUM_THREADS,set_alloc_size);
  
  TrieBlock<layout,A>* tmp = TrieBlock<layout,A>::get_block(NUM_THREADS,offset,data_allocator);
  myset = tmp->get_set();
  myset->from_array(set_data_in,set_data_buffer,set_size);

  assert(set_alloc_size >= myset->number_of_bytes);
  data_allocator->head->roll_back(set_alloc_size - myset->number_of_bytes);

  return offset;
}

size_t encode_tail(size_t start, size_t end, uint32_t *data, std::vector<uint32_t> *current, uint32_t *indicies){
  long prev_data = -1;
  size_t data_size = 0;
  for(size_t i = start; i < end; i++){
    if(prev_data != (long)current->at(indicies[i])){
      *data++ = current->at(indicies[i]);
      prev_data = (long)current->at(indicies[i]);
      data_size++;
    }
  }
  return data_size;
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
  const size_t prev_offset, 
  const size_t level, 
  const size_t num_levels, 
  const size_t tid, 
  std::vector<std::vector<uint32_t>> *attr_in,
  M *data_allocator, 
  std::vector<size_t*> *ranges_buffer, 
  std::vector<uint32_t*> *set_data_buffer, 
  uint32_t *indicies,
  std::vector<A>* annotation){

  //NUM_THREADS*NUM_COLUMNS
  uint32_t *sb = set_data_buffer->at(tid*num_levels+level);
  size_t set_size = encode_tail(start,end,sb,&attr_in->at(level),indicies);

  const size_t next_offset = build_block<B,M>(tid,data_allocator,set_size,sb);

  if(level == 1){
    //get the head
    B* prev_block = B::get_block(NUM_THREADS,prev_offset,data_allocator);
    prev_block->set_next_block(index,data,tid,next_offset);
  } else {
    B* prev_block = B::get_block(tid,prev_offset,data_allocator);
    prev_block->set_next_block(index,data,tid,next_offset);
  }

  if(level < (num_levels-1)){
    B* tail = (B*)data_allocator->get_address(tid,next_offset);
    tail->init_next(tid,data_allocator);
    auto tup = produce_ranges(start,end,ranges_buffer->at(tid*num_levels+level),set_data_buffer->at(tid*num_levels+level),indicies,&attr_in->at(level));
    const size_t set_size = std::get<0>(tup);
    for(size_t i = 0; i < set_size; i++){
      const size_t next_start = ranges_buffer->at(tid*num_levels+level)[i];
      const size_t next_end = ranges_buffer->at(tid*num_levels+level)[i+1];
      const uint32_t next_data = set_data_buffer->at(tid*num_levels+level)[i];        
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
    B* tail = (B*)data_allocator->get_address(tid,next_offset);
    //perform allocation for annotation (0 = tid)
    //nextSize should gets the size of pointers or annotations (should be renamed?)
    data_allocator->get_next(tid,sizeof(A)*(tail->nextSize()));
    //could of realloced, get the head again
    tail = (B*)data_allocator->get_address(tid,next_offset);

    for(size_t i = start; i < end; i++){
      uint32_t data_value = attr_in->at(level).at(indicies[i]);
      A annotationValue = annotation->at(indicies[i]);
      tail->template set_annotation(annotationValue,i-start,data_value);
    }
  }
}

//builds the trie from an encoded relation
template<class A, class M>
Trie<A,M>::Trie(
  std::string path,
  std::vector<uint32_t>* max_set_sizes, 
  std::vector<std::vector<uint32_t>> *attr_in,
  std::vector<A>* annotations){

  annotation = (A)0;
  annotated = annotations->size() > 0;
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

  //DEBUG
  /*
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
  
  //Build the head set.
  const size_t head_offset = 
    build_head<TrieBlock<layout,M>,M>(
      memoryBuffers,
      head_size,
      set_data_buffer->at(0));

  size_t cur_level = 1;
  if(num_columns > 1){
    TrieBlock<layout,M>* new_head = (TrieBlock<layout,M>*)memoryBuffers->head->get_address(head_offset);
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

template struct Trie<void*,ParMMapBuffer>;
template struct Trie<long,ParMMapBuffer>;
template struct Trie<int,ParMMapBuffer>;
template struct Trie<float,ParMMapBuffer>;
template struct Trie<double,ParMMapBuffer>;