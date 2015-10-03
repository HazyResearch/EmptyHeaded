#include "allocator.hpp"

template<class T>
elem<T>::elem(size_t num_elems){
  const size_t malloc_size = num_elems*sizeof(T);
  ptr = (uint8_t*)malloc(malloc_size);
  if(ptr == NULL){
    std::cout << "ERROR: Malloc failed - " << malloc_size << std::endl;
    exit(1);
  }
  max_ptr = ptr+(num_elems*sizeof(T));
  cur = ptr;
}

template<class T>
T* elem<T>::get_next(size_t num){
  //std::cout << (void*)(cur+num*sizeof(T)) << " " << (void*)max_ptr << " " << (max_ptr-(cur+num*sizeof(T))) << std::endl;
  if((cur+num*sizeof(T)) < max_ptr){
    T* val = (T*)cur;
    cur += (num*sizeof(T));
    return val;
  }
  return NULL;
}

template<class T>
T* elem<T>::get_next(size_t num, size_t align){
  const size_t offset = (size_t) cur % align;
  if(offset != 0){
    cur += (align-offset);
  }
  assert( (size_t) cur % align == 0 );
  return get_next(num);
}

template<class T>
T* elem<T>::get_next(){
  if(cur+sizeof(T) < max_ptr){
    T* val = (T*)cur;
    cur += sizeof(T);
    return val;
  }
  return NULL;
}

template<class T>
void elem<T>::roll_back(size_t num){
  const size_t roll_back_amount = (num*sizeof(T));
  cur -= roll_back_amount;
}

template<class T>
void elem<T>::adjust(long num){
  cur += long(sizeof(T))*num;
}

template<class T>
void elem<T>::deallocate(){
  free(ptr);
}
  
template<class T>
allocator<T>::allocator(size_t num_elems_in){
  if(num_elems_in > max_alloc){
    num_elems_in = max_alloc;
  }
  for(size_t i = 0; i < NUM_THREADS; i++){
    std::vector<elem<T>> current;
    num_elems.push_back(num_elems_in);
    elem<T>* new_elem = new elem<T>(num_elems.at(i));
    current.push_back(*new_elem);
    elements.push_back(current);
    indicies.push_back(0);
  }
}
//debug
template<class T>
void allocator<T>::print_sizes(){
  for(size_t i = 0; i < NUM_THREADS; i++){
    std::cout << "t: " << i << " size: " << num_elems.at(i) << " i: " << indicies.at(i) << " mem:"  << std::endl;
  }
}
template<class T>
T* allocator<T>::get_memory(size_t tid){
  return (T*)elements.at(tid).at(indicies.at(tid)).cur;
}

template<class T>
T* allocator<T>::get_next(size_t tid){
  T* val = elements.at(tid).at(indicies.at(tid)).get_next();
  if(val == NULL){
    elem<T>* new_elem = new elem<T>(num_elems.at(tid));
    elements.at(tid).push_back(elem<T>(*new_elem));
    indicies.at(tid)++;
    val = elements.at(tid).at(indicies.at(tid)).get_next();
    assert(val != NULL);
  }
  return val;
}

template<class T>
T* allocator<T>::get_next(size_t tid, size_t num){
  T* val = elements.at(tid).at(indicies.at(tid)).get_next(num);
  if(val == NULL){
    num_elems.at(tid) = (num_elems.at(tid)+1)*multplier;
    while(num > num_elems.at(tid)){
      num_elems.at(tid) = (num_elems.at(tid)+1)*multplier;
    }
    if(num_elems.at(tid) > max_alloc){
      num_elems.at(tid) = max_alloc;
      //std::cout << "Too large of alloc: " << num << std::endl;
      num = max_alloc;
    }
    //std::cout << "Allocating more memory: try a larger allocation size for better performance. " << num << " " << num_elems.at(tid) << std::endl;
    assert(num <= num_elems.at(tid));
    elem<T>* new_elem = new elem<T>(num_elems.at(tid));
    elements.at(tid).push_back(*new_elem);
    indicies.at(tid)++;
    val = elements.at(tid).at(indicies.at(tid)).get_next(num);
    assert(val != NULL);
  }
  return val;
}

template<class T>
T* allocator<T>::get_next(size_t tid, size_t num, size_t align){
  T* val = elements.at(tid).at(indicies.at(tid)).get_next(num,align);
  if(val == NULL){
    num_elems.at(tid) = (num_elems.at(tid)+1)*multplier;
    while((num+align) > num_elems.at(tid)){
      num_elems.at(tid) = (num_elems.at(tid)+1)*multplier;
    }
    if(num_elems.at(tid) > max_alloc){
      num_elems.at(tid) = max_alloc;
      //std::cout << "Too large of alloc: " << num << std::endl;
      num = max_alloc;
    }
    //std::cout << "Allocating more memory: try a larger allocation size for better performance. " << num << " " << num_elems.at(tid) << std::endl;
    assert(num <= num_elems.at(tid));
    elements.at(tid).push_back(elem<T>(num_elems.at(tid)));
    indicies.at(tid)++;
    val = elements.at(tid).at(indicies.at(tid)).get_next(num,align);
    assert(val != NULL);
  }
  return val;
}

template<class T>
void allocator<T>::roll_back(size_t tid, size_t num){
  elements.at(tid).at(indicies.at(tid)).roll_back(num);
}

template<class T>
void allocator<T>::adjust(size_t tid, long num){
  elements.at(tid).at(indicies.at(tid)).adjust(num);
}

template<class T>
void allocator<T>::free(){
  for(size_t i = 0; i < NUM_THREADS; i++){
    std::vector<elem<T>> e = elements.at(i);
    for(size_t j = 0; j < e.size(); j++){
      e.at(j).deallocate();
    }
  }
}

template struct allocator<uint32_t>;
template struct allocator<uint64_t>;
template struct allocator<uint8_t>;
