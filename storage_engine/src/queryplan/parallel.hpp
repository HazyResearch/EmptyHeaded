#ifndef PARALLEL_H
#define PARALLEL_H

#include "../engine/utils/common.hpp"

namespace par{
  template<class T>
  class reducer{
  public:
    size_t padding = 300;
    T *elem;
    //tbb::cache_aligned_allocator<T>
    std::function<T (T, T)> f;
    reducer(T init_in, std::function<T (T, T)> f_in){
      f = f_in;
      elem = new T[NUM_THREADS*padding];
      for(size_t i = 0; i < NUM_THREADS; i++){
        elem[i*padding] = init_in;
      }
      memset(elem,(uint8_t)0,sizeof(T)*NUM_THREADS*padding);
    }
    inline void update(size_t tid, T new_val){
      elem[tid*padding] = f(elem[tid*padding],new_val);
    }
    inline T evaluate(T init){
      for(size_t i = 0; i < NUM_THREADS; i++){
        init = f(init,elem[i*padding]);
      }
      return init;
    }
  };

  struct staticParFor {
    size_t tid;
    size_t work_start;
    size_t work_end;
    static std::function<void(size_t, size_t)> body;
    staticParFor(size_t tid_in, size_t work_start_in, size_t work_end_in){
      tid = tid_in;
      work_start = work_start_in;
      work_end = work_end_in;
    }
    void* run();
  };

  size_t for_range(const size_t from, const size_t to, std::function<void(size_t, size_t)> body);

  struct parFor {
    size_t tid;
    static std::atomic<size_t> next_work;
    static size_t block_size;
    static size_t range_len;
    static size_t offset;
    static std::function<void(size_t, size_t)> body;
    parFor(size_t tid_in){
      tid = tid_in;
    }
    void* run();
  };

  size_t for_range(const size_t from, const size_t to, const size_t block_size, std::function<void(size_t, size_t)> body);
}
#endif
