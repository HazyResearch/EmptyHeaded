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
TrieBuilder<A,M>::TrieBuilder(Trie<A,M>* t_in, const size_t num_attributes){
  trie = t_in;
  next.resize(t_in->num_columns);
  if(t_in->num_columns > 0){
    next.at(0).index = NUM_THREADS; //head always lives here
    next.at(0).offset = 0;
  }
  cur_level = 1;

  tmp_level = 0;
  tmp_buffers.resize(num_attributes); 
  aggregate_sets.resize(num_attributes);
  for(size_t i = 0; i < num_attributes; i++){
    tmp_buffers.at(i) = new MemoryBuffer(2);
  }
}

template<class A,class M>
size_t TrieBuilder<A,M>::build_set(
  const size_t tid,
  const TrieBlock<hybrid,M> *tb1){

    M* data_allocator = trie->memoryBuffers;
    if(tb1 == NULL){
      next.at(cur_level).index = -1;

      //clear the memory and move on (will set num bytes and cardinality to 0)
      const size_t alloc_size = sizeof(Set<hybrid>)+sizeof(TrieBlock<hybrid,M>);
      uint8_t* place = (uint8_t*)data_allocator->get_next(tid,alloc_size);
      const size_t offset = place-data_allocator->get_address(tid);
      next.at(cur_level).offset = offset;
      memset(place,(uint8_t)0,sizeof(alloc_size));
      return 0;
    }


    const Set<hybrid>* s1 = (const Set<hybrid>*)((uint8_t*)tb1+sizeof(TrieBlock<hybrid,M>)); 
    const size_t block_size = s1->number_of_bytes+
      sizeof(TrieBlock<hybrid,M>)+
      sizeof(Set<hybrid>);

    uint8_t * const start_block = (uint8_t*)data_allocator->get_next(tid,block_size);
    const size_t offset = start_block-data_allocator->get_address(tid);
    memcpy((uint8_t*)start_block,(uint8_t*)tb1,block_size);

    next.at(cur_level).index = (s1->cardinality > 0) ? tid: -1;
    next.at(cur_level).offset = offset;
    return s1->cardinality;
}

template<class A,class M>
size_t TrieBuilder<A,M>::build_set(
  const size_t tid,
  const TrieBlock<hybrid,M>* tb1,
  const TrieBlock<hybrid,M>* tb2){

  M* data_allocator = trie->memoryBuffers;
  if(tb1 == NULL || tb2 == NULL){
    next.at(cur_level).index = -1;

    //clear the memory and move on (will set num bytes and cardinality to 0)
    const size_t alloc_size = sizeof(Set<hybrid>)+sizeof(TrieBlock<hybrid,M>);
    uint8_t* place = (uint8_t*)data_allocator->get_next(tid,alloc_size);
    const size_t offset = place-data_allocator->get_address(tid);
    next.at(cur_level).offset = offset;
    memset(place,(uint8_t)0,sizeof(alloc_size));
    return 0;
  }

  const Set<hybrid>* s1 = (const Set<hybrid>*)((uint8_t*)tb1+sizeof(TrieBlock<hybrid,M>)); 
  const Set<hybrid>* s2 = (const Set<hybrid>*)((uint8_t*)tb2+sizeof(TrieBlock<hybrid,M>));  

  const size_t alloc_size =
    std::max(s1->number_of_bytes,
             s2->number_of_bytes);

  uint8_t* start_block = (uint8_t*)data_allocator->get_next(tid,
    (sizeof(TrieBlock<hybrid,M>)+
    sizeof(Set<hybrid>)+
    alloc_size));

  const size_t offset = start_block-data_allocator->get_address(tid);
  Set<hybrid>* myset =  (Set<hybrid>*)(start_block+sizeof(TrieBlock<hybrid,M>));
  myset = ops::set_intersect(
            myset, 
            s1,
            s2);

  assert(alloc_size >= myset->number_of_bytes);
  data_allocator->roll_back(tid,alloc_size - (myset->number_of_bytes) );

  //(3) set the offset and index of the trie block in a vector
  next.at(cur_level).index = (myset->cardinality > 0) ? tid: -1;
  next.at(cur_level).offset = offset;

  return myset->cardinality;
  //aside: when you call set block it will just pull the values and set with (data,index) value
  //(4) allocated space for the next blocks
  //return the set
}

template<class A,class M>
size_t TrieBuilder<A,M>::build_set(
  const size_t tid,
  std::vector<const TrieBlock<hybrid,M>*> * isets){

  M* data_allocator = trie->memoryBuffers;

  //make a pass over the sets, find the minimum set
  //find the allocation size
  const TrieBlock<hybrid,M> *head = isets->at(0);
  if(head == NULL){
    next.at(cur_level).index = -1;

    //clear the memory and move on (will set num bytes and cardinality to 0)
    const size_t alloc_size = sizeof(Set<hybrid>)+sizeof(TrieBlock<hybrid,M>);
    uint8_t* place = (uint8_t*)data_allocator->get_next(tid,alloc_size);
    const size_t offset = place-data_allocator->get_address(tid);
    next.at(cur_level).offset = offset;
    memset(place,(uint8_t)0,sizeof(alloc_size));

    return 0;
  }
  Set<hybrid>* s1 = (Set<hybrid>*)((uint8_t*)head+sizeof(TrieBlock<hybrid,M>));
  size_t min_set = s1->cardinality;
  size_t alloc_size = s1->number_of_bytes;
  size_t min_index = 0;
  for(size_t i = 1; i < isets->size(); i++){
    const TrieBlock<hybrid,M> *tmp_head_set = isets->at(i);
    if(tmp_head_set == NULL){
      next.at(cur_level).index = -1;

      //clear the memory and move on (will set num bytes and cardinality to 0)
      const size_t alloc_size = sizeof(Set<hybrid>)+sizeof(TrieBlock<hybrid,M>);
      uint8_t* place = (uint8_t*)data_allocator->get_next(tid,alloc_size);
      const size_t offset = place-data_allocator->get_address(tid);
      next.at(cur_level).offset = offset;
      memset(place,(uint8_t)0,sizeof(alloc_size));

      return 0;
    }
    Set<hybrid>* tmp_set = (Set<hybrid>*)((uint8_t*)tmp_head_set+sizeof(TrieBlock<hybrid,M>));
    alloc_size = std::max(alloc_size,tmp_set->number_of_bytes);
    min_set = std::min((size_t)min_set,(size_t)tmp_set->cardinality);
    if(min_set == tmp_set->cardinality){
      s1 = tmp_set;
      min_index = i;
    }
  }
  isets->erase(isets->begin()+min_index);

  //allocate buffers
  uint8_t* place = (uint8_t*) (data_allocator->get_next(tid,2*(alloc_size+sizeof(Set<hybrid>)+sizeof(TrieBlock<hybrid,M>))));
  const size_t offset = place-data_allocator->get_address(tid);
  Set<hybrid> *result_set = (Set<hybrid>*)(place+sizeof(TrieBlock<hybrid,M>));
  Set<hybrid> *operator_set = (Set<hybrid>*)((uint8_t*)place+alloc_size+sizeof(Set<hybrid>)+sizeof(TrieBlock<hybrid,M>));
  data_allocator->roll_back(tid,(alloc_size+sizeof(Set<hybrid>)+sizeof(TrieBlock<hybrid,M>))); 
  if(!(isets->size() % 2)){
    Set<hybrid>* tmp = result_set;
    result_set = operator_set;
    operator_set = tmp;
  }

  //intersect first two isets
  const TrieBlock<hybrid,M> *tb2 = isets->at(0);
  const Set<hybrid>* s2 = (const Set<hybrid>*)((uint8_t*)tb2+sizeof(TrieBlock<hybrid,M>));  
  result_set = ops::set_intersect(
          result_set, 
          (const Set<hybrid>*)s1,
          s2);

  //intersect remaining isets
  for(size_t i = 1; i < isets->size(); i++){
    //swap buffers
    Set<hybrid>* tmp = result_set;
    result_set = operator_set;
    operator_set = tmp;

    const TrieBlock<hybrid,M> *tbn = isets->at(i);
    const Set<hybrid>* sn = (const Set<hybrid>*)((uint8_t*)tbn+sizeof(TrieBlock<hybrid,M>));

    result_set = ops::set_intersect(
            result_set, 
            sn,
            (const Set<hybrid>*)operator_set);
  }

  data_allocator->roll_back(tid,alloc_size - (result_set->number_of_bytes) );

  next.at(cur_level).index = (result_set->cardinality > 0) ? tid: -1;
  next.at(cur_level).offset = offset;

  return result_set->cardinality;
}

//Build a aggregated set for one sets
template<class A,class M>
size_t TrieBuilder<A,M>::build_aggregated_set(
  const TrieBlock<hybrid,M> *tb1){

  //fixme
  assert(tb1 != NULL);
  if(tb1 == NULL){
    uint8_t* place = (uint8_t*)tmp_buffers.at(tmp_level)->get_next(sizeof(Set<hybrid>));
    memset(place,(uint8_t)0,sizeof(Set<hybrid>));
    aggregate_sets.at(tmp_level) = (const Set<hybrid>*)place;
    return 0;
  }

  const Set<hybrid>* s1 = (const Set<hybrid>*)((uint8_t*)tb1+sizeof(TrieBlock<hybrid,M>));
  aggregate_sets.at(tmp_level) = s1;
  return s1->cardinality;
}

//Build a aggregated set for two sets
template<class A,class M>
size_t TrieBuilder<A,M>::build_aggregated_set(
  const TrieBlock<hybrid,M> *tb1, 
  const TrieBlock<hybrid,M> *tb2){

  //fixme
  assert(tb1 != NULL && tb2 != NULL);
  if(tb1 == NULL || tb2 == NULL){
    //clear the memory and move on (will set num bytes and cardinality to 0)
    uint8_t* place = (uint8_t*)tmp_buffers.at(tmp_level)->get_next(sizeof(Set<hybrid>));
    memset(place,(uint8_t)0,sizeof(Set<hybrid>));
    aggregate_sets.at(tmp_level) = (const Set<hybrid>*)place;
    return 0;
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
  r = ops::set_intersect(
          r, 
          s1,
          s2);
  aggregate_sets.at(tmp_level) = r;
  return r->cardinality;
}

//Build a aggregated set for two sets
template<class A,class M>
size_t TrieBuilder<A,M>::build_aggregated_set(
  std::vector<const TrieBlock<hybrid,M>*> * isets){

  //make a pass over the sets, find the minimum set
  //find the allocation size
  const TrieBlock<hybrid,M> *head = isets->at(0);
  if(head == NULL){
    uint8_t* place = (uint8_t*)tmp_buffers.at(tmp_level)->get_next(sizeof(Set<hybrid>));
    memset(place,(uint8_t)0,sizeof(Set<hybrid>));
    aggregate_sets.at(tmp_level) = (const Set<hybrid>*)place;
    return 0;
  }
  Set<hybrid>* s1 = (Set<hybrid>*)((uint8_t*)head+sizeof(TrieBlock<hybrid,M>));
  size_t min_set = s1->cardinality;
  size_t alloc_size = s1->number_of_bytes;
  size_t min_index = 0;
  for(size_t i = 1; i < isets->size(); i++){
    const TrieBlock<hybrid,M> *tmp_head_set = isets->at(i);
    if(tmp_head_set == NULL){
      uint8_t* place = (uint8_t*)tmp_buffers.at(tmp_level)->get_next(sizeof(Set<hybrid>));
      memset(place,(uint8_t)0,sizeof(Set<hybrid>));
      aggregate_sets.at(tmp_level) = (const Set<hybrid>*)place;
      return 0;
    }
    Set<hybrid>* tmp_set = (Set<hybrid>*)((uint8_t*)tmp_head_set+sizeof(TrieBlock<hybrid,M>));
    alloc_size = std::max(alloc_size,tmp_set->number_of_bytes);
    min_set = std::min((size_t)min_set,(size_t)tmp_set->cardinality);
    if(min_set == tmp_set->cardinality){
      s1 = tmp_set;
      min_index = i;
    }
  }
  isets->erase(isets->begin()+min_index);

  //allocate buffers
  uint8_t* place = (uint8_t*) (tmp_buffers.at(tmp_level)->get_next(2*(alloc_size+sizeof(Set<hybrid>))));
  Set<hybrid> *result_set = (Set<hybrid>*)place;
  Set<hybrid> *operator_set = (Set<hybrid>*)((uint8_t*)place+alloc_size+sizeof(Set<hybrid>));
  tmp_buffers.at(tmp_level)->roll_back(2*(alloc_size+sizeof(Set<hybrid>))); 
  if( !(isets->size() % 2) ){
    Set<hybrid>* tmp = result_set;
    result_set = operator_set;
    operator_set = tmp;
  }

  //intersect first two isets
  const TrieBlock<hybrid,M> *tb2 = isets->at(0);
  const Set<hybrid>* s2 = (const Set<hybrid>*)((uint8_t*)tb2+sizeof(TrieBlock<hybrid,M>));  
  result_set = ops::set_intersect(
          result_set, 
          (const Set<hybrid>*)s1,
          s2);

  //intersect remaining isets
  for(size_t i = 1; i < isets->size(); i++){
    //swap buffers
    Set<hybrid>* tmp = result_set;
    result_set = operator_set;
    operator_set = tmp;

    const TrieBlock<hybrid,M> *tbn = isets->at(i);
    const Set<hybrid>* sn = (const Set<hybrid>*)((uint8_t*)tbn+sizeof(TrieBlock<hybrid,M>));

    result_set = ops::set_intersect(
            result_set, 
            sn,
            (const Set<hybrid>*)operator_set);
  }
  aggregate_sets.at(tmp_level) = result_set;
  return result_set->cardinality;
}

//perform a count on two sets
template<class A,class M>
size_t TrieBuilder<A,M>::count_set(
  const TrieBlock<hybrid,M> *tb1){
  if(tb1 == NULL)
    return 0;
  const Set<hybrid>* s1 = (const Set<hybrid>*)((uint8_t*)tb1+sizeof(TrieBlock<hybrid,M>));   
  const size_t result = s1->cardinality;
  return result;
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

template<class A,class M>
void TrieBuilder<A,M>::allocate_next(
  const size_t tid){
    //(1) get the trie block at the previous level
  TrieBlock<hybrid,M>* block = TrieBlock<hybrid,M>::get_block(
    next.at(cur_level).index,
    next.at(cur_level).offset,
    trie->memoryBuffers);
  
  if(block != NULL)
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

  assert(next.at(cur_level).index <= (int)NUM_THREADS);
  //(2) call set block
  prev_block->set_next_block(
    index,
    data,
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
  
  if(block != NULL){
    //annotation is always placed right after the trie block right now
    trie->memoryBuffers->get_next(tid, sizeof(A)*(block->nextSize()));
  }
}

//sets the pointers in the previous level to point to 
//the current level
template<class A,class M>
void TrieBuilder<A,M>::set_annotation(
  const A value, 
  const uint32_t index,
  const uint32_t data){

  TrieBlock<hybrid,M>* prev_block = TrieBlock<hybrid,M>::get_block(
    next.at(cur_level-1).index,
    next.at(cur_level-1).offset,
    trie->memoryBuffers);

  assert(next.at(cur_level-1).index <= (int)NUM_THREADS);
  A* annotation = (A*)(((uint8_t*)prev_block)+(
    sizeof(TrieBlock<hybrid,M>)+
    sizeof(Set<hybrid>)+
    prev_block->get_const_set()->number_of_bytes) );
  annotation[prev_block->get_index(index,data)] = value;
}

//////////////////////////////////////////////////////////////////////
////////////////////////////parallel wrapper
//////////////////////////////////////////////////////////////////////
template<class A, class M>
ParTrieBuilder<A,M>::ParTrieBuilder(Trie<A,M> *t_in, const size_t num_attributes){
  trie = t_in;
  builders.resize(NUM_THREADS);
  for(size_t i = 0; i < NUM_THREADS; i++){
    builders.at(i) = new TrieBuilder<A,M>(t_in,num_attributes);
  }
}

//When we have just a single trie accessor 
//and it is aggregated just return it
template<class A,class M>
size_t ParTrieBuilder<A,M>::build_aggregated_equality_selection_set(
  const uint32_t data,
  const TrieBlock<hybrid,M> *s1) {
  return s1->get_set()->find(data);
}

//When we have just a single trie accessor 
//and it is aggregated just return it
template<class A,class M>
size_t ParTrieBuilder<A,M>::build_aggregated_set(
  const TrieBlock<hybrid,M> *s1) {
  tmp_head = s1;
  return s1->get_set()->cardinality;
}

template<class A,class M>
size_t ParTrieBuilder<A,M>::build_aggregated_set(
  const TrieBlock<hybrid,M> *tb1,
  const TrieBlock<hybrid,M> *tb2) {

  //clear the memory and move on (will set num bytes and cardinality to 0)
  uint8_t* place = (uint8_t*)trie->memoryBuffers->head->get_next(sizeof(TrieBlock<hybrid,M>)+sizeof(Set<hybrid>));
  memset(place,(uint8_t)0,sizeof(TrieBlock<hybrid,M>)+sizeof(Set<hybrid>));

  //fixme
  assert(tb1 != NULL && tb2 != NULL);
  if(tb1 == NULL || tb2 == NULL){
    //clear the memory and move on (will set num bytes and cardinality to 0)
    place = (uint8_t*)trie->memoryBuffers->head->get_next(sizeof(TrieBlock<hybrid,M>)+sizeof(Set<hybrid>));
    memset(place,(uint8_t)0,sizeof(TrieBlock<hybrid,M>)+sizeof(Set<hybrid>));
    return 0;
  }

  const Set<hybrid>* s1 = (const Set<hybrid>*)((uint8_t*)tb1+sizeof(TrieBlock<hybrid,M>));  
  const Set<hybrid>* s2 = (const Set<hybrid>*)((uint8_t*)tb2+sizeof(TrieBlock<hybrid,M>));  

  const size_t alloc_size =
    std::max(s1->number_of_bytes,
             s2->number_of_bytes);

  place = (uint8_t*) (trie->memoryBuffers->head->get_next(sizeof(TrieBlock<hybrid,M>)+alloc_size+sizeof(Set<hybrid>)));
  Set<hybrid> *r = (Set<hybrid>*)(place+sizeof(TrieBlock<hybrid,M>));
  trie->memoryBuffers->head->roll_back(alloc_size+sizeof(Set<hybrid>)+sizeof(TrieBlock<hybrid,M>)); 
  
  //we can roll back because we won't try another buffer at this level and tid until
  //after this memory is consumed.
  r = ops::set_intersect(
          r, 
          s1,
          s2);  
  tmp_head = (const TrieBlock<hybrid,M>*)place;
  return r->cardinality;
}

//Build a aggregated set for two sets
template<class A,class M>
size_t ParTrieBuilder<A,M>::build_aggregated_set(
  std::vector<const TrieBlock<hybrid,M>*> * isets){

  //clear the memory and move on (will set num bytes and cardinality to 0)
  uint8_t* place = (uint8_t*)trie->memoryBuffers->head->get_next(sizeof(TrieBlock<hybrid,M>)+sizeof(Set<hybrid>));
  memset(place,(uint8_t)0,sizeof(TrieBlock<hybrid,M>)+sizeof(Set<hybrid>));

  //make a pass over the sets, find the minimum set
  //find the allocation size
  const TrieBlock<hybrid,M> *head = isets->at(0);
  if(head == NULL)
    return 0;
  Set<hybrid>* s1 = (Set<hybrid>*)((uint8_t*)head+sizeof(TrieBlock<hybrid,M>));
  size_t min_set = s1->cardinality;
  size_t alloc_size = s1->number_of_bytes;
  size_t min_index = 0;
  for(size_t i = 1; i < isets->size(); i++){
    const TrieBlock<hybrid,M> *tmp_head_set = isets->at(i);
    if(tmp_head_set == NULL)
      return 0;
    Set<hybrid>* tmp_set = (Set<hybrid>*)((uint8_t*)tmp_head_set+sizeof(TrieBlock<hybrid,M>));
    alloc_size = std::max(alloc_size,tmp_set->number_of_bytes);
    min_set = std::min((size_t)min_set,(size_t)tmp_set->cardinality);
    if(min_set == tmp_set->cardinality){
      s1 = tmp_set;
      min_index = i;
    }
  }
  isets->erase(isets->begin()+min_index);

  //allocate buffers
  uint8_t* place1 = (uint8_t*) (trie->memoryBuffers->head->get_next(2*(alloc_size+sizeof(Set<hybrid>)+sizeof(TrieBlock<hybrid,M>))));
  Set<hybrid> *result_set = (Set<hybrid>*)(place1+sizeof(TrieBlock<hybrid,M>));
  Set<hybrid> *operator_set = (Set<hybrid>*)(place1+alloc_size+sizeof(Set<hybrid>)+sizeof(TrieBlock<hybrid,M>)+sizeof(TrieBlock<hybrid,M>));
  trie->memoryBuffers->head->roll_back(2*(alloc_size+sizeof(Set<hybrid>)+sizeof(TrieBlock<hybrid,M>))); 

  //intersect first two isets
  const TrieBlock<hybrid,M> *tb2 = isets->at(0);
  const Set<hybrid>* s2 = (const Set<hybrid>*)((uint8_t*)tb2+sizeof(TrieBlock<hybrid,M>));  
  result_set = ops::set_intersect(
          result_set, 
          (const Set<hybrid>*)s1,
          s2);

  //intersect remaining isets
  for(size_t i = 1; i < isets->size(); i++){
    //swap buffers
    Set<hybrid>* tmp = result_set;
    result_set = operator_set;
    operator_set = tmp;

    const TrieBlock<hybrid,M> *tbn = isets->at(i);
    const Set<hybrid>* sn = (const Set<hybrid>*)((uint8_t*)tbn+sizeof(TrieBlock<hybrid,M>));
    result_set = ops::set_intersect(
            result_set, 
            sn,
            (const Set<hybrid>*)operator_set);
  }

  tmp_head = (const TrieBlock<hybrid,M>*)((uint8_t*)result_set-sizeof(TrieBlock<hybrid,M>));
  return result_set->cardinality;
}

//Build a aggregated set for two sets
template<class A,class M>
size_t ParTrieBuilder<A,M>::build_set(
  std::vector<const TrieBlock<hybrid,M>*> * isets){

  //make a pass over the sets, find the minimum set
  //find the allocation size
  const TrieBlock<hybrid,M> *head = isets->at(0);
  if(head == NULL){
    //clear the memory and move on (will set num bytes and cardinality to 0)
    uint8_t* place = (uint8_t*)trie->memoryBuffers->head->get_next(sizeof(TrieBlock<hybrid,M>)+sizeof(Set<hybrid>));
    memset(place,(uint8_t)0,sizeof(TrieBlock<hybrid,M>)+sizeof(Set<hybrid>));
    return 0;
  }
  Set<hybrid>* s1 = (Set<hybrid>*)((uint8_t*)head+sizeof(TrieBlock<hybrid,M>));
  size_t min_set = s1->cardinality;
  size_t alloc_size = s1->number_of_bytes;
  size_t min_index = 0;
  for(size_t i = 1; i < isets->size(); i++){
    const TrieBlock<hybrid,M> *tmp_head_set = isets->at(i);
    if(tmp_head_set == NULL){
      //clear the memory and move on (will set num bytes and cardinality to 0)
      uint8_t* place = (uint8_t*)trie->memoryBuffers->head->get_next(sizeof(TrieBlock<hybrid,M>)+sizeof(Set<hybrid>));
      memset(place,(uint8_t)0,sizeof(TrieBlock<hybrid,M>)+sizeof(Set<hybrid>));
      return 0;
    }
    Set<hybrid>* tmp_set = (Set<hybrid>*)((uint8_t*)tmp_head_set+sizeof(TrieBlock<hybrid,M>));
    alloc_size = std::max(alloc_size,tmp_set->number_of_bytes);
    min_set = std::min((size_t)min_set,(size_t)tmp_set->cardinality);
    if(min_set == tmp_set->cardinality){
      s1 = tmp_set;
      min_index = i;
    }
  }
  isets->erase(isets->begin()+min_index);

  //allocate buffers
  uint8_t* place1 = (uint8_t*) (trie->memoryBuffers->head->get_next(2*(alloc_size+sizeof(Set<hybrid>)+sizeof(TrieBlock<hybrid,M>))));
  Set<hybrid> *result_set = (Set<hybrid>*)(place1+sizeof(TrieBlock<hybrid,M>));
  Set<hybrid> *operator_set = (Set<hybrid>*)(place1+alloc_size+sizeof(Set<hybrid>)+sizeof(TrieBlock<hybrid,M>)+sizeof(TrieBlock<hybrid,M>));
  trie->memoryBuffers->head->roll_back((alloc_size+sizeof(Set<hybrid>)+sizeof(TrieBlock<hybrid,M>))); 

  if(!(isets->size() % 2)){
    Set<hybrid>* tmp = result_set;
    result_set = operator_set;
    operator_set = tmp;
  }

  //intersect first two isets
  const TrieBlock<hybrid,M> *tb2 = isets->at(0);
  const Set<hybrid>* s2 = (const Set<hybrid>*)((uint8_t*)tb2+sizeof(TrieBlock<hybrid,M>));  
  
  result_set = ops::set_intersect(
          result_set, 
          (const Set<hybrid>*)s1,
          s2);

  //intersect remaining isets
  for(size_t i = 1; i < isets->size(); i++){
    //swap buffers
    Set<hybrid>* tmp = result_set;
    result_set = operator_set;
    operator_set = tmp;

    const TrieBlock<hybrid,M> *tbn = isets->at(i);
    const Set<hybrid>* sn = (const Set<hybrid>*)((uint8_t*)tbn+sizeof(TrieBlock<hybrid,M>));
    result_set = ops::set_intersect(
            result_set, 
            sn,
            (const Set<hybrid>*)operator_set);
  }
  return result_set->cardinality;
}

template<class A,class M>
size_t ParTrieBuilder<A,M>::build_set(
  const TrieBlock<hybrid,M> *tb1){
    const Set<hybrid>* s1 = tb1->get_set();
    auto data_allocator = trie->memoryBuffers->head;
    const size_t block_size = s1->number_of_bytes+
      sizeof(TrieBlock<hybrid,M>)+
      sizeof(Set<hybrid>);

    uint8_t * const start_block = (uint8_t*)data_allocator->get_next(block_size);
    memcpy((uint8_t*)start_block,(uint8_t*)tb1,block_size);
    return s1->cardinality;
}

template<class A,class M>
size_t ParTrieBuilder<A,M>::build_set(
  const TrieBlock<hybrid,M> *tb1,
  const TrieBlock<hybrid,M> *tb2){

  auto data_allocator = trie->memoryBuffers->head;
  //fixme
  assert(tb1 != NULL && tb2 != NULL);
  if(tb1 == NULL || tb2 == NULL){
    //clear the memory and move on (will set num bytes and cardinality to 0)
    uint8_t* place = (uint8_t*)data_allocator->get_next(sizeof(TrieBlock<hybrid,M>)+sizeof(Set<hybrid>));
    memset(place,(uint8_t)0,sizeof(TrieBlock<hybrid,M>)+sizeof(Set<hybrid>));
    return 0;
  }

  const Set<hybrid>* s1 = (const Set<hybrid>*)((uint8_t*)tb1+sizeof(TrieBlock<hybrid,M>));  
  const Set<hybrid>* s2 = (const Set<hybrid>*)((uint8_t*)tb2+sizeof(TrieBlock<hybrid,M>));  

  const size_t alloc_size =
    std::max(s1->number_of_bytes,
             s2->number_of_bytes);

  uint8_t* place = (uint8_t*) (trie->memoryBuffers->head->get_next(sizeof(TrieBlock<hybrid,M>)+alloc_size+sizeof(Set<hybrid>)));
  Set<hybrid> *r = (Set<hybrid>*)(place+sizeof(TrieBlock<hybrid,M>));  
  
  r = ops::set_intersect(
          r, 
          s1,
          s2);
  trie->memoryBuffers->head->roll_back(alloc_size-r->number_of_bytes);
  return r->cardinality;
}

template<class A,class M>
void ParTrieBuilder<A,M>::allocate_next(){
  TrieBlock<hybrid,M>* block = trie->getHead();
  const size_t next_size = block->nextSize();
  const size_t alloc_size = sizeof(NextLevel)*(next_size);
  trie->memoryBuffers->get_next(NUM_THREADS,alloc_size);
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
