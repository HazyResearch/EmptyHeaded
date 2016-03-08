#include "utils/thread_pool.hpp"
#include "utils/parallel.hpp"

namespace par{

  std::atomic<size_t> parFor::next_work;
  size_t parFor::block_size;
  size_t parFor::range_len;
  size_t parFor::offset;
  std::function<void(size_t, size_t)> parFor::body;

  size_t for_range(const size_t from, const size_t to, std::function<void(size_t, size_t)> body) {
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

  size_t for_range(const size_t from, const size_t to, const size_t block_size, std::function<void(size_t, size_t)> body) {
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

  void* parFor::run(){
    while(true) {
       const size_t work_start = parFor::next_work.fetch_add(parFor::block_size, std::memory_order_relaxed);
       if(work_start > parFor::range_len)
          break;

       const size_t work_end = std::min(parFor::offset + work_start + parFor::block_size, parFor::range_len);
       //std::cout << tid << " " << work_start << " " << work_end << " " << parFor::range_len << std::endl;

       for(size_t j = work_start; j < work_end; j++) {
          parFor::body(tid, parFor::offset + j);
       }
    }
    return NULL;
  }

  std::function<void(size_t, size_t)> staticParFor::body;
  void* staticParFor::run(){
     for(size_t j = work_start; j < work_end; j++) {
        staticParFor::body(tid, j);
     }
    return NULL;
  }
}
