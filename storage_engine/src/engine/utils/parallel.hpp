#ifndef PARALLEL_H
#define PARALLEL_H

#include "thread_pool.hpp"

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

  template<typename F>
  size_t for_range(const size_t from, const size_t to, F body) {
    const size_t range_len = to - from;
    size_t chunk_size = (range_len+NUM_THREADS)/NUM_THREADS;
    const size_t THREADS_CHUNKS = (range_len % NUM_THREADS);
    thread_pool::init_threads();
    staticParFor::body = body;
    staticParFor **pf = new staticParFor*[NUM_THREADS];
    for(size_t k = 0; k < THREADS_CHUNKS; k++) { 
      const size_t work_start = k*chunk_size;
      const size_t work_end = work_start+chunk_size;
      pf[k] = new staticParFor(k,work_start,work_end);
      thread_pool::submitWork(k,thread_pool::general_body<staticParFor>,(void *)(pf[k]));
    }
    const size_t offset = THREADS_CHUNKS*chunk_size;
    --chunk_size;
    for(size_t k = 0; k < (NUM_THREADS-THREADS_CHUNKS); k++){
      const size_t work_start = offset+k*chunk_size;
      const size_t work_end = work_start+chunk_size;
      const size_t tid = k+THREADS_CHUNKS;
      pf[tid] = new staticParFor(tid,work_start,work_end);
      thread_pool::submitWork(tid,thread_pool::general_body<staticParFor>,(void *)(pf[tid])); 
    }
    thread_pool::join_threads();
    for(size_t k = 0; k < NUM_THREADS; k++) { 
      delete pf[k];
    }
    delete[] pf;
    return 1;
  }

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

  template<typename F>
  size_t for_range(const size_t from, const size_t to, const size_t block_size, F body) {
    const size_t range_len = to - from;
    
    const size_t actual_block_size = (block_size > range_len) ? 1:block_size; 
    thread_pool::init_threads();
    parFor::next_work = 0;
    parFor::block_size = actual_block_size;
    parFor::range_len = range_len;
    parFor::offset = from;
    parFor::body = body;
    parFor **pf = new parFor*[NUM_THREADS];
    for(size_t k = 0; k < NUM_THREADS; k++) { 
      pf[k] = new parFor(k);
      thread_pool::submitWork(k,thread_pool::general_body<parFor>,(void *)(pf[k]));
    }
    thread_pool::join_threads();
    for(size_t k = 0; k < NUM_THREADS; k++) { 
      delete pf[k];
    }
    delete[] pf;
    return 1;
  }
}
#endif
