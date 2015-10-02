#include "parallel.hpp"

namespace par{
  std::atomic<size_t> parFor::next_work;
  size_t parFor::block_size;
  size_t parFor::range_len;
  size_t parFor::offset;
  std::function<void(size_t, size_t)> parFor::body;
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