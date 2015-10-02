#define GENERATED
#include "SortableEncodingMap.hpp"
void *run() {
  SortableEncodingMap<long>* n = new SortableEncodingMap<long>();
  n->update(1);
  n->update(2);
  n->foreach([&](long value){
    std::cout << value << std::endl;
  });
}
#ifndef GOOGLE_TEST
int main() {
  thread_pool::initializeThreadPool();
  run();
  return 0;
}
#endif
