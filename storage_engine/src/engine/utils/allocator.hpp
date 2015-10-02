#ifndef allocator_H
#define allocator_H

#include "common.hpp"

namespace allocator{
  template<class T>
  struct elem{
    uint8_t *ptr;
    uint8_t *max_ptr;
    uint8_t *cur;
    elem(size_t num_elems){
      const size_t malloc_size = num_elems*sizeof(T);
      ptr = (uint8_t*)malloc(malloc_size);
      if(ptr == NULL){
        std::cout << "ERROR: Malloc failed - " << malloc_size << std::endl;
        exit(1);
      }
      max_ptr = ptr+(num_elems*sizeof(T));
      cur = ptr;
    }
    inline T* get_next(size_t num){
      //std::cout << (void*)(cur+num*sizeof(T)) << " " << (void*)max_ptr << " " << (max_ptr-(cur+num*sizeof(T))) << std::endl;
      if((cur+num*sizeof(T)) < max_ptr){
        T* val = (T*)cur;
        cur += (num*sizeof(T));
        return val;
      }
      return NULL;
    }
    inline T* get_next(size_t num, size_t align){
      const size_t offset = (size_t) cur % align;
      if(offset != 0){
        cur += (align-offset);
      }
      assert( (size_t) cur % align == 0 );
      return get_next(num);
    }
    inline T* get_next(){
      if(cur+sizeof(T) < max_ptr){
        T* val = (T*)cur;
        cur += sizeof(T);
        return val;
      }
      return NULL;
    }
    inline void roll_back(size_t num){
      const size_t roll_back_amount = (num*sizeof(T));
      cur -= roll_back_amount;
    }
    inline void adjust(long num){
      cur += long(sizeof(T))*num;
    }
    inline void deallocate(){
      free(ptr);
    }
  };

  template<class T>
  struct memory{
    size_t max_alloc = (((uint64_t)MAX_MEMORY)*1073741824l)/(sizeof(T)*NUM_THREADS); //GB
    const size_t multplier = 2;
    std::vector<size_t> num_elems;
    std::vector<size_t> indicies;
    std::vector<std::vector<elem<T>>> elements;
    memory(){}
    memory(size_t num_elems_in){
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
    void print_sizes(){
      for(size_t i = 0; i < NUM_THREADS; i++){
        std::cout << "t: " << i << " size: " << num_elems.at(i) << " i: " << indicies.at(i) << " mem:"  << std::endl;
      }
    }
    inline T* get_memory(size_t tid){
      return (T*)elements.at(tid).at(indicies.at(tid)).cur;
    }
    inline T* get_next(size_t tid){
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
    inline T* get_next(size_t tid, size_t num){
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
    inline T* get_next(size_t tid, size_t num, size_t align){
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
    inline void roll_back(size_t tid, size_t num){
      elements.at(tid).at(indicies.at(tid)).roll_back(num);
    }
    inline void adjust(size_t tid, long num){
      elements.at(tid).at(indicies.at(tid)).adjust(num);
    }
    inline void free(){
      for(size_t i = 0; i < NUM_THREADS; i++){
        std::vector<elem<T>> e = elements.at(i);
        for(size_t j = 0; j < e.size(); j++){
          e.at(j).deallocate();
        }
      }
    }
  };
}
#endif
