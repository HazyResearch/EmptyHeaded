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
  for(size_t i = 0; i < t_in->num_columns; i++){
    tmp_buffers.at(i) = new MemoryBuffer(100);
  }
}

template<class A,class M>
Set<hybrid>* TrieBuilder<A,M>::build_set(
  const size_t tid,
  const size_t level,
  const TrieBlock<hybrid,M>* s1,
  const TrieBlock<hybrid,M>* s2){

  M* data_allocator = trie->memoryBuffers;
  const uint8_t * const start_block = data_allocator->get_next(tid,sizeof(TrieBlock<hybrid,M>*));
  const size_t offset = start_block-data_allocator->get_address(tid);

  const size_t alloc_size =
    std::max(s1->set.number_of_bytes,
             s2->set.number_of_bytes);

  uint8_t* set_data_in = (uint8_t*)data_allocator->get_next(tid,alloc_size);
  TrieBlock<hybrid,M>* block = TrieBlock<hybrid,M>::get_block(tid,offset,data_allocator);
  Set<hybrid> cur_set = block->set;
  cur_set.data = set_data_in;
  (void) cur_set; //FIX ME (layout should just be a pointer or all relative to buffer)
  
  std::cout << "start intersect" << std::endl;
  block->set = ops::set_intersect(
            (Set<hybrid> *)(&(block->set)), 
            (const Set<hybrid> *)&s1->set,
            (const Set<hybrid> *)&s2->set);
  std::cout << "end intersect" << std::endl;


  assert(alloc_size >= block->set.number_of_bytes);
  data_allocator->roll_back(tid,alloc_size-block->set.number_of_bytes);

  //(3) set the offset and index of the trie block in a vector
  next.at(level).index = tid;
  next.at(level).offset = offset;

  return &(block->set);
  //aside: when you call set block it will just pull the values and set with (data,index) value
  //(4) allocated space for the next blocks
  //return the set
}

//Build a aggregated set for two sets
template<class A,class M>
Set<hybrid>* TrieBuilder<A,M>::build_aggregated_set(
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

//perform a count on two sets
template<class A,class M>
size_t TrieBuilder<A,M>::count_set(
  const TrieBlock<hybrid,M> *s1, 
  const TrieBlock<hybrid,M> *s2){
  size_t result = ops::set_intersect(
            (const Set<hybrid> *)&s1->set,
            (const Set<hybrid> *)&s2->set);
  return result;
}

template<class A,class M>
void TrieBuilder<A,M>::allocate_next(
  const size_t tid,
  const size_t cur_level){
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
  const uint32_t data,
  const size_t cur_level){
  //(1) get the trie block at the previous level
  TrieBlock<hybrid,M>* block = TrieBlock<hybrid,M>::get_block(
    next.at(cur_level-1).index,
    next.at(cur_level-1).offset,
    trie->memoryBuffers);
  //(2) call set block
  block->set_next_block(
    data,
    index,
    next.at(cur_level).index,
    next.at(cur_level).offset);
  //return the set
}

template<class A,class M>
void TrieBuilder<A,M>::allocate_annotation(
  const size_t tid,
  const size_t cur_level){
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
  const uint32_t data,
  const size_t cur_level){
  //(1) get the trie block at the previous level
  TrieBlock<hybrid,M>* block = TrieBlock<hybrid,M>::get_block(
    next.at(cur_level).index,
    next.at(cur_level).offset,
    trie->memoryBuffers);
  //(2) call set block
  A* annotation = (A*)(((uint8_t*)block)+(
    sizeof(TrieBlock<hybrid,M>*)+
    block->set.number_of_bytes) );
  annotation[block->get_index(index,data)] = value;
  //return the set
}

//sets the pointers in the previous level to point to 
//the current level
template<class A,class M>
A TrieBuilder<A,M>::get_annotation(
  const uint32_t index,
  const uint32_t data,
  const size_t cur_level){
  //(1) get the trie block at the previous level
  TrieBlock<hybrid,M>* block = TrieBlock<hybrid,M>::get_block(
    next.at(cur_level).index,
    next.at(cur_level).offset,
    trie->memoryBuffers);
  //(2) call set block
  A* annotation = (A*)(((uint8_t*)block)+(
    sizeof(TrieBlock<hybrid,M>*)+
    block->set.number_of_bytes) );
  return annotation[block->get_index(index,data)];
  //return the set
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
Set<hybrid> ParTrieBuilder<A,M>::build_aggregated_set(
  const TrieBlock<hybrid,M> *s1) const {
    return s1->set;
}

//When we have just a single trie accessor 
//and it is aggregated just return it
template<class A,class M>
Set<hybrid> ParTrieBuilder<A,M>::build_set(
  const TrieBlock<hybrid,M> *s1){
    auto data_allocator = trie->memoryBuffers->head;
    const size_t block_size = s1->set.number_of_bytes+sizeof(TrieBlock<hybrid,M>*);
    uint8_t * const start_block = (uint8_t*)data_allocator->get_next(block_size);

    memcpy(start_block,s1,block_size);

    TrieBlock<hybrid,M>* r = (TrieBlock<hybrid,M>*) start_block;
    return r->set;

  //(1) copy the trie block to head buffer (do not copy pointers)
  //(3) set the offset and index of the trie block in a vector
  //aside: when you call set block it will just pull the values and set with (data,index) value
  //(4) allocated space for the next blocks
  //return the set
}

template<class A,class M>
void ParTrieBuilder<A,M>::allocate_next(){
  //(1) get the trie block at the previous level
  TrieBlock<hybrid,M>* block = trie->getHead();
  block->init_next(NUM_THREADS,trie->memoryBuffers);
}

template<class A,class M>
void ParTrieBuilder<A,M>::allocate_annotation(){
  //(1) get the trie block at the previous level
  TrieBlock<hybrid,M>* block = trie->getHead();
  //annotation is always placed right after the trie block right now
  trie->memoryBuffers->head->get_next(sizeof(A)*(block->nextSize()));
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
