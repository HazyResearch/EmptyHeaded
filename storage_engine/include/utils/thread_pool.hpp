#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <thread>
#include <atomic>
#include <cstring>
#include <pthread.h>
#include <sched.h>

#ifdef __APPLE__
#include "pthread_barrier.hpp"
#endif

typedef void* (*FN_PTR)(void*);

struct thread_pool { 
  static pthread_barrier_t barrier; // barrier synchronization object 
  static pthread_t* threadPool;
  static pthread_mutex_t* locks;
  static pthread_cond_t* readyConds;
  static pthread_cond_t* doneConds;

  static FN_PTR* workPool;
  static void** argPool;

  template<class F>
  static void* general_body(void *args_in);

  //init a thread barrier
  static void init_threads();
  //join threads on the thread barrier
  static void join_threads();

  static void initializeThread(size_t threadId);

  static void submitWork(size_t threadId, void *(*work) (void *), void *arg);

  static void* processWork(void* threadId);
  static void initializeThreadPool();
  static void deleteThreadPool();
};
#endif
