#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include "debug.hpp"
#include <thread>
#include <atomic>
#include <cstring>
#include <pthread.h>
#include <sched.h>

#ifdef __APPLE__
#include "pthread_barrier.hpp"
#endif // __APPLE__

namespace thread_pool {
  ////////////////////////////////////////////////////
  //Main thread should always call (init_threads, submit work
  // with the general body, then join threads.
  ////////////////////////////////////////////////////
  static pthread_barrier_t barrier; // barrier synchronization object
  // Iterates over a range of numbers in parallel

  //general body is submitted to submit_work. Construct you op in the object
  //that is passed in as an arg.
  template<class F>
  static void* general_body(void *args_in){
    F* arg = (F*)args_in;
    arg->run();
    pthread_barrier_wait(&barrier);
    return NULL;
  }
  //init a thread barrier
  static void init_threads(){
    pthread_barrier_init (&barrier, NULL, NUM_THREADS+1);
  }
  //join threads on the thread barrier
  static void join_threads(){
    pthread_barrier_wait(&barrier);
    pthread_barrier_destroy(&barrier);
  }

  ////////////////////////////////////////////////////
  //Code to spin up threads and take work from a queue below
  //Inspired (read copied) from Delite
  //Any Parallel op should be able to be contructed on top of this 
  //thread pool
  ////////////////////////////////////////////////////

  static pthread_t* threadPool;
  static pthread_mutex_t* locks;
  static pthread_cond_t* readyConds;
  static pthread_cond_t* doneConds;

  static void** workPool;
  static void** argPool;

  static void initializeThread(size_t threadId) {
    #ifdef __linux__
    cpu_set_t cpu;
    CPU_ZERO(&cpu);
    CPU_SET(threadId, &cpu);
    sched_setaffinity(0, sizeof(cpu_set_t), &cpu);

    #ifdef __DELITE_CPP_NUMA__
      if (numa_available() >= 0) {
        int socketId = config->threadToSocket(threadId);
        if (socketId < numa_num_configured_nodes()) {
          bitmask* nodemask = numa_allocate_nodemask();
          numa_bitmask_setbit(nodemask, socketId);
          numa_set_membind(nodemask);
        }
        //VERBOSE("Binding thread %d to cpu %d, socket %d\n", threadId, threadId, socketId);
      }
    #endif //__DELITE_CPP_NUMA__
    #else
      (void) threadId;
    #endif //__linux__

  }

  static void submitWork(size_t threadId, void *(*work) (void *), void *arg) {
    pthread_mutex_lock(&locks[threadId]);
    while (argPool[threadId] != NULL) {
      pthread_cond_wait(&doneConds[threadId], &locks[threadId]);
    }
    workPool[threadId] = (void*)work;
    argPool[threadId] = arg;
    pthread_cond_signal(&readyConds[threadId]);
    pthread_mutex_unlock(&locks[threadId]);
  }

  static void* processWork(void* threadId) {
    const size_t id = (size_t)threadId;

    /////////////////////////////////////////////////
    //Per Thead Initialization code
    //VERBOSE("Initialized thread with id %d\n", id);
    pthread_mutex_init(&locks[id], NULL);
    pthread_cond_init(&readyConds[id], NULL);
    pthread_cond_init(&doneConds[id], NULL);
    workPool[id] = NULL;
    argPool[id] = NULL;
    void *(*work) (void *);
    void *arg;

    initializeThread(id);
    /////////////////////////////////////////////////

    //tell the init code that the thread is alive and rocking
    pthread_barrier_wait(&barrier);
    while(true) {
      pthread_mutex_lock(&locks[id]);
      while (argPool[id] == NULL) {
        pthread_cond_wait(&readyConds[id], &locks[id]);
      }      
      work = (void *(*)(void *))workPool[id];
      workPool[id] = NULL;
      arg = argPool[id];
      argPool[id] = NULL;
      pthread_cond_signal(&doneConds[id]);
      pthread_mutex_unlock(&locks[id]);
      work(arg);
    }
  }

  static void initializeThreadPool() {
    threadPool = new pthread_t[NUM_THREADS];
    locks = new pthread_mutex_t[NUM_THREADS];
    readyConds = new pthread_cond_t[NUM_THREADS];
    doneConds = new pthread_cond_t[NUM_THREADS];
    workPool = new void*[NUM_THREADS];
    argPool = new void*[NUM_THREADS];

    pthread_barrier_init (&barrier, NULL, NUM_THREADS+1);
    for (size_t i=0; i<NUM_THREADS; i++) {
      pthread_create(&threadPool[i], NULL, processWork, (void*)i);
    }
    pthread_barrier_wait(&barrier);
    pthread_barrier_destroy(&barrier);
  }
  static void* killThread(void *args_in){
    (void) args_in;
    pthread_exit(NULL);
  }
  static void deleteThreadPool() {
    for(size_t k = 0; k < NUM_THREADS; k++) { 
      submitWork(k,killThread,(void *)NULL);
    }
    delete[] threadPool;
    delete[] locks;
    delete[] readyConds;
    delete[] doneConds;
    delete[] workPool;
    delete[] argPool;
  }
}
#endif