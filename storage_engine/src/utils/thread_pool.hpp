#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#ifdef __APPLE__
#include "pthread_barrier.hpp"
#endif // __APPLE__

namespace thread_pool {  
  template<class F>
  void* general_body(void *args_in);

  //init a thread barrier
  void init_threads();
  //join threads on the thread barrier
  void join_threads();

  void initializeThread(size_t threadId);

  void submitWork(size_t threadId, void *(*work) (void *), void *arg);

  void* processWork(void* threadId);
  void initializeThreadPool();
  void deleteThreadPool();
}
#endif