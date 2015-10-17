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
  next.resize(t_in->num_columns);
  tmp_buffers.resize(t_in->num_columns);
  if(t_in->num_columns > 0){
    next.at(0).index = NUM_THREADS; //head always lives here
    next.at(0).offset = 0;
  }
  cur_level = 1;
  tmp_level = 0;
  for(size_t i = 0; i < t_in->num_columns; i++){
    tmp_buffers.at(i) = new MemoryBuffer(2);
  }
}

/*
template<class A,class M>
const Set<hybrid>* TrieBuilder<A,M>::build_set(
  const size_t tid,
  const TrieBlock<hybrid,M>* tb1,
  const TrieBlock<hybrid,M>* tb2){

  const Set<hybrid>* s1 = (const Set<hybrid>*)((uint8_t*)tb1+sizeof(TrieBlock<hybrid,M>)); 
  const Set<hybrid>* s2 = (const Set<hybrid>*)((uint8_t*)tb2+sizeof(TrieBlock<hybrid,M>));  

  const size_t alloc_size =
    std::max(s1->number_of_bytes,
             s2->number_of_bytes);

  M* data_allocator = trie->memoryBuffers;
  uint8_t* start_block = (uint8_t*)data_allocator->get_next(tid,
    sizeof(TrieBlock<hybrid,M>)+
    sizeof(Set<hybrid>)+
    alloc_size);
  const size_t offset = start_block-data_allocator->get_address(tid);

  Set<hybrid>* myset =  (Set<hybrid>*)(start_block+sizeof(TrieBlock<hybrid,M>));
  
  myset = ops::set_intersect(
            myset, 
            s1,
            s2);

  assert(alloc_size >= myset->number_of_bytes);
  data_allocator->roll_back(tid,alloc_size-myset->number_of_bytes);

  //(3) set the offset and index of the trie block in a vector
  next.at(cur_level).index = tid;
  next.at(cur_level).offset = offset;

  return myset;
  //aside: when you call set block it will just pull the values and set with (data,index) value
  //(4) allocated space for the next blocks
  //return the set
}
*/

//Build a aggregated set for two sets
template<class A,class M>
void TrieBuilder<A,M>::build_aggregated_set(
  const TrieBlock<hybrid,M> *tb1, 
  const TrieBlock<hybrid,M> *tb2){

  //fixme
  assert(tb1 != NULL && tb2 != NULL);
  if(tb1 == NULL || tb2 == NULL){
    //clear the memory and move on (will set num bytes and cardinality to 0)
    uint8_t* place = (uint8_t*)tmp_buffers.at(tmp_level)->get_next(sizeof(Set<hybrid>));
    memset(place,(uint8_t)0,sizeof(Set<hybrid>));
    return;
  }

  const Set<hybrid>* s1 = (const Set<hybrid>*)((uint8_t*)tb1+sizeof(TrieBlock<hybrid,M>));  
  const Set<hybrid>* s2 = (const Set<hybrid>*)((uint8_t*)tb2+sizeof(TrieBlock<hybrid,M>));  

  const size_t alloc_size =
    std::max(s1->number_of_bytes,
             s2->number_of_bytes);

  uint8_t* place = (uint8_t*) (tmp_buffers.at(tmp_level)->get_next(alloc_size+sizeof(Set<hybrid>)));
  Set<hybrid> *r = (Set<hybrid>*)place;
  tmp_buffers.at(tmp_level)->roll_back(alloc_size+sizeof(Set<hybrid>)); 
  //we can roll back because we won't try another buffer at this level and tid until
  //after this memory is consumed.
  ops::set_intersect(
          r, 
          s1,
          s2);
}

//perform a count on two sets
template<class A,class M>
size_t TrieBuilder<A,M>::count_set(
  const TrieBlock<hybrid,M> *tb1, 
  const TrieBlock<hybrid,M> *tb2){
  if(tb1 == NULL || tb2 == NULL)
    return 0;
  const Set<hybrid>* s1 = (const Set<hybrid>*)((uint8_t*)tb1+sizeof(TrieBlock<hybrid,M>));   
  const Set<hybrid>* s2 = (const Set<hybrid>*)((uint8_t*)tb2+sizeof(TrieBlock<hybrid,M>));
  const size_t result = ops::set_intersect(
            s1,
            s2);
  return result;
}

/*
template<class A,class M>
void TrieBuilder<A,M>::allocate_next(
  const size_t tid){
    //(1) get the trie block at the previous level
  TrieBlock<hybrid,M>* block = TrieBlock<hybrid,M>::get_block(
    next.at(cur_level).index,
    next.at(cur_level).offset,
    trie->memoryBuffers);
  block->init_next(tid,trie->memoryBuffers);
}

//sets the pointers in the previous level to point to 
//the current level
template<class A,class M>
void TrieBuilder<A,M>::set_level(
  const uint32_t index,
  const uint32_t data){
  //(1) get the trie block at the previous level

  TrieBlock<hybrid,M>* prev_block = TrieBlock<hybrid,M>::get_block(
    next.at(cur_level-1).index,
    next.at(cur_level-1).offset,
    trie->memoryBuffers);

  //(2) call set block
  prev_block->set_next_block(
    data,
    index,
    next.at(cur_level).index,
    next.at(cur_level).offset);
  //return the set
}

template<class A,class M>
void TrieBuilder<A,M>::allocate_annotation(
  const size_t tid){
  //(1) get the trie block at the previous level
  TrieBlock<hybrid,M>* block = TrieBlock<hybrid,M>::get_block(
    next.at(cur_level).index,
    next.at(cur_level).offset,
    trie->memoryBuffers);
  //annotation is always placed right after the trie block right now
  trie->memoryBuffers->get_next(tid, sizeof(A)*(block->nextSize()));
}

//sets the pointers in the previous level to point to 
//the current level
template<class A,class M>
void TrieBuilder<A,M>::set_annotation(
  const A value, 
  const uint32_t index,
  const uint32_t data){
  //(1) get the trie block at the previous level
  TrieBlock<hybrid,M>* block = TrieBlock<hybrid,M>::get_block(
    next.at(cur_level).index,
    next.at(cur_level).offset,
    trie->memoryBuffers);
  //(2) call set block
  A* annotation = (A*)(((uint8_t*)block)+(
    sizeof(TrieBlock<hybrid,M>)+
    sizeof(Set<hybrid>)+
    block->get_set()->number_of_bytes) );
  annotation[block->get_index(index,data)] = value;
  //return the set
}

//sets the pointers in the previous level to point to 
//the current level
template<class A,class M>
A TrieBuilder<A,M>::get_annotation(
  const uint32_t index,
  const uint32_t data){
  //(1) get the trie block at the previous level
  TrieBlock<hybrid,M>* block = TrieBlock<hybrid,M>::get_block(
    next.at(cur_level).index,
    next.at(cur_level).offset,
    trie->memoryBuffers);
  const Set<hybrid>* s1 = block->get_set();
  //(2) call set block
  A* annotation = (A*)( ((uint8_t*)block)+
    (sizeof(TrieBlock<hybrid,M>)+
    sizeof(Set<hybrid>) +
    s1->number_of_bytes) );
  return annotation[block->get_index(index,data)];
  //return the set
}
*/

template<class A,class M>
void TrieBuilder<A,M>::foreach_aggregate(
  std::function<void(
    const uint32_t a_d)> f) {

    uint8_t* place = (uint8_t*)tmp_buffers.at(tmp_level)->get_address(0);
    Set<hybrid> *s = (Set<hybrid>*)place;

    auto buf = tmp_buffers.at(tmp_level);
    tmp_level++;
    s->foreach(sizeof(Set<hybrid>),buf,f);
    //s->foreach(f);
    tmp_level--;
}

template<class A,class M>
void TrieBuilder<A,M>::foreach_builder(
  std::function<void(
    const uint32_t a_i,
    const uint32_t a_d)> f) {

    const uint32_t cur_index = next.at(cur_level).index;
    const size_t cur_offset = next.at(cur_level).offset;

    auto buf = trie->memoryBuffers->elements.at(cur_index);
    uint8_t* place = (uint8_t*)(buf->get_address(cur_offset)+sizeof(TrieBlock<hybrid,M>));

    Set<hybrid> *s = (Set<hybrid>*)place;

    cur_level++;
    s->foreach_index(sizeof(Set<hybrid>),buf,f);
    cur_level--;
}

//////////////////////////////////////////////////////////////////////
////////////////////////////parallel wrapper
//////////////////////////////////////////////////////////////////////
template<class A, class M>
ParTrieBuilder<A,M>::ParTrieBuilder(Trie<A,M> *t_in){
  trie = t_in;
  builders.resize(NUM_THREADS);
  for(size_t i = 0; i < NUM_THREADS; i++){
    builders.at(i) = new TrieBuilder<A,M>(t_in);
  }
}

//When we have just a single trie accessor 
//and it is aggregated just return it
template<class A,class M>
void ParTrieBuilder<A,M>::build_aggregated_set(
  const TrieBlock<hybrid,M> *s1) {
  tmp_head = s1;
}

//When we have just a single trie accessor 
//and it is aggregated just return it
template<class A,class M>
const Set<hybrid>* ParTrieBuilder<A,M>::build_set(
  const TrieBlock<hybrid,M> *tb1){
    const Set<hybrid>* s1 = tb1->get_set();
    auto data_allocator = trie->memoryBuffers->head;
    const size_t block_size = s1->number_of_bytes+
      sizeof(TrieBlock<hybrid,M>)+
      sizeof(Set<hybrid>);

    uint8_t * const start_block = (uint8_t*)data_allocator->get_next(block_size);
    memcpy((uint8_t*)start_block,(uint8_t*)tb1,block_size);

    const Set<hybrid>* result = (const Set<hybrid>*)(start_block+sizeof(TrieBlock<hybrid,M>));
    return result;

  //(1) copy the trie block to head buffer (do not copy pointers)
  //(3) set the offset and index of the trie block in a vector
  //aside: when you call set block it will just pull the values and set with (data,index) value
  //(4) allocated space for the next blocks
  //return the set
}

/*
template<class A,class M>
const Set<hybrid>* ParTrieBuilder<A,M>::allocate_next(){
  //(1) get the trie block at the previous level
  TrieBlock<hybrid,M>* block = trie->getHead();
  const size_t next_size = block->nextSize();
  const size_t alloc_size = sizeof(NextLevel)*(next_size);
  trie->memoryBuffers->get_next(NUM_THREADS,alloc_size);

  //a realloc might of occured
  block = trie->getHead();
  return (const Set<hybrid>*)(block+sizeof(TrieBlock<hybrid,M>));
}
*/
template<class A,class M>
void ParTrieBuilder<A,M>::par_foreach_aggregate(
  std::function<void(
    const size_t tid,
    const uint32_t a_d)> f) {

    const TrieBlock<hybrid,M>* block = tmp_head;
    Set<hybrid>* s = (Set<hybrid>*)((uint8_t*)block+sizeof(TrieBlock<hybrid,M>));

    s->par_foreach(f);
}

template<class A,class M>
void ParTrieBuilder<A,M>::par_foreach_builder(
  std::function<void(
    const size_t tid,
    const size_t a_i,
    const uint32_t a_d)> f) {

    const TrieBlock<hybrid,M>* block = trie->getHead();
    Set<hybrid>* s = (Set<hybrid>*)((uint8_t*)block+sizeof(TrieBlock<hybrid,M>));
    s->par_foreach_index(f);
}

/*
template<class A,class M>
void ParTrieBuilder<A,M>::allocate_annotation(){
  //(1) get the trie block at the previous level
  TrieBlock<hybrid,M>* block = trie->getHead();
  //annotation is always placed right after the trie block right now
  trie->memoryBuffers->head->get_next(sizeof(A)*(block->nextSize()));
}*/

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
