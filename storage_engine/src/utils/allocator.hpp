#ifndef allocator_H
#define allocator_H

#include "common.hpp"

  template<class T>
  struct elem{
    uint8_t *ptr;
    uint8_t *max_ptr;
    uint8_t *cur;
    elem(size_t num_elems);

    T* get_next(size_t num);
    T* get_next(size_t num, size_t align);
    T* get_next();
    void roll_back(size_t num);
    void adjust(long num);
    void deallocate();
  };

  template<class T>
  struct allocator{
    size_t max_alloc = (((uint64_t)MAX_MEMORY)*1073741824l)/(sizeof(T)*NUM_THREADS); //GB
    const size_t multplier = 2;
    std::vector<size_t> num_elems;
    std::vector<size_t> indicies;

    std::vector<std::vector<elem<T>>> elements;
    allocator(){}
    allocator(size_t num_elems_in);
    //debug
    void print_sizes();
    T* get_memory(size_t tid);
    T* get_next(size_t tid);
    T* get_next(size_t tid, size_t num);
    T* get_next(size_t tid, size_t num, size_t align);
    void roll_back(size_t tid, size_t num);
    void adjust(size_t tid, long num);
    void free();
  };

#endif
