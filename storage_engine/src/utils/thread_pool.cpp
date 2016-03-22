#include "utils/thread_pool.hpp"
#include "utils/parallel.hpp"

pthread_barrier_t thread_pool::barrier; // barrier synchronization object 
pthread_t* thread_pool::threadPool;
pthread_mutex_t* thread_pool::locks;
pthread_cond_t* thread_pool::readyConds;
pthread_cond_t* thread_pool::doneConds;

FN_PTR* thread_pool::workPool;
void** thread_pool::argPool;

////////////////////////////////////////////////////
//Main thread should always call (init_threads, submit work
// with the general body, then join threads.
////////////////////////////////////////////////////
// Iterates over a range of numbers in parallel
template<class F>
void* thread_pool::general_body(void *args_in){
  F* arg = (F*)args_in;
  arg->run();
  pthread_barrier_wait(&barrier);
  return NULL;
}

//init a thread barrier
void thread_pool::init_threads(){
  pthread_barrier_init (&barrier, NULL, NUM_THREADS+1);
}

//join threads on the thread barrier
void thread_pool::join_threads(){
  pthread_barrier_wait(&barrier);
  pthread_barrier_destroy(&barrier);
}

////////////////////////////////////////////////////
//Code to spin up threads and take work from a queue below
//Inspired (read copied) from Delite
//Any Parallel op should be able to be contructed on top of this 
//thread pool
////////////////////////////////////////////////////

void thread_pool::initializeThread(size_t threadId) {
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

void thread_pool::submitWork(size_t threadId, void *(*work) (void *), void *arg) {
  pthread_mutex_lock(&locks[threadId]);

  while (argPool[threadId] != NULL) {
    pthread_cond_wait(&doneConds[threadId], &locks[threadId]);
  }
  workPool[threadId] = work;
  argPool[threadId] = arg;
  pthread_cond_signal(&readyConds[threadId]);
  pthread_mutex_unlock(&locks[threadId]);
}

void* thread_pool::processWork(void* threadId) {
  const size_t id = (size_t)threadId;

  /////////////////////////////////////////////////
  //Per Thead Initialization code
  //VERBOSE("Initialized thread with id %d\n", id);
  pthread_mutex_init(&locks[id], NULL);
  pthread_cond_init(&readyConds[id], NULL);
  pthread_cond_init(&doneConds[id], NULL);
  //workPool[id] = (void *(*)(void *))NULL;
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

    work = workPool[id];
    //workPool[id] = (void *(*)(void *))NULL;
    arg = argPool[id];
    argPool[id] = NULL;
    pthread_cond_signal(&doneConds[id]);
    pthread_mutex_unlock(&locks[id]);
    work(arg);
  }
  return NULL;
}

void thread_pool::initializeThreadPool() {
  threadPool = new pthread_t[NUM_THREADS];
  locks = new pthread_mutex_t[NUM_THREADS];
  readyConds = new pthread_cond_t[NUM_THREADS];
  doneConds = new pthread_cond_t[NUM_THREADS];
  workPool = new FN_PTR[NUM_THREADS];
  argPool = new void*[NUM_THREADS];

  pthread_barrier_init (&barrier, NULL, NUM_THREADS+1);
  for (size_t i=0; i<NUM_THREADS; i++) {
    pthread_create(&threadPool[i], NULL, processWork, (void*)i);
  }
  pthread_barrier_wait(&barrier);
  pthread_barrier_destroy(&barrier);
}

void* killThread(void *args_in){
  (void) args_in;
  std::cout << "KILL" << std::endl;
  pthread_exit(NULL);
}

void thread_pool::deleteThreadPool() {
  for(size_t k = 0; k < NUM_THREADS; k++) { 
    pthread_cancel(threadPool[k]);
  }
  for(size_t k = 0; k < NUM_THREADS; k++) { 
    pthread_join(threadPool[k],NULL);
  }
  delete[] threadPool;
  delete[] locks;
  delete[] readyConds;
  delete[] doneConds;
  delete[] workPool;
  delete[] argPool;
}

template void* thread_pool::general_body<par::staticParFor>(void*);
template void* thread_pool::general_body<par::parFor>(void*);