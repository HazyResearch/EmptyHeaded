#include "Trie.hpp"
#include "Vector.hpp"

typedef std::unordered_map<std::string, void *> mymap;

void run(mymap *input_tries) {
  thread_pool::initializeThreadPool();

  ParMemoryBuffer const * mybuf = new ParMemoryBuffer(100);
  std::vector<uint32_t>* a = new std::vector<uint32_t>();
  for(size_t i = 0; i < 10; i++){
    a->push_back(i*10);
  }
  Vector<SparseVector,void*,MemoryBuffer> v1 = 
    Vector<SparseVector,void*,MemoryBuffer>::from_array(
      mybuf->head,
      a->data(),
      a->size());

  std::vector<uint32_t>* b = new std::vector<uint32_t>();
  for(size_t i = 0; i < 1000; i++){
    b->push_back(i*2);
  }

  std::vector<float>* anno = new std::vector<float>();
  for(size_t i = 0; i < 1000; i++){
    anno->push_back((float)2.0);
  }

  std::vector<std::vector<uint32_t>> *trie = new std::vector<std::vector<uint32_t>>();
  std::vector<uint32_t> *max_sizes = new std::vector<uint32_t>();
  trie->push_back(*a);
  max_sizes->push_back(10);
  trie->push_back(*a);
  max_sizes->push_back(10);
  std::vector<void*>* annotations = new std::vector<void*>();
  Trie<void*,ParMemoryBuffer> *t = new Trie<void*,ParMemoryBuffer>(
    "",
    max_sizes, 
    trie,
    annotations);

  t->foreach([&](std::vector<uint32_t> *v, void* anno){
    for(size_t i = 0 ; i < v->size(); i++){
      std::cout << v->at(i) << "\t";
    }
    std::cout << std::endl;
  });

  thread_pool::deleteThreadPool();
  /*

  //from vector
    //void* just build index/data
    //annotation build index/data and values
    //NextLevel build index/data and alloc next level (worry about later)
  Vector<SparseVector,float,MemoryBuffer> v2 = 
    Vector<SparseVector,float,MemoryBuffer>::from_array(
      mybuf->head,
      b->data(),
      anno->data(),
      b->size());

  v2.foreach([&](const uint32_t index, const uint32_t data, const float value){
    std::cout << index << " " << data << " " << value << std::endl;
  });

  //intersect
    //annotation (mat) -> retrun index/data and values
      // - run intersection, loop over fill in annotations
    //annotation (agg) -> return value
      // - run intersection, loop over compute annotation
    //void* and NextLevel (mat/agg) -> return index/data
      // - run intersection (possible alloc NextLevel)
  
  Vector<SparseVector,void*,MemoryBuffer> r = 
    ops::mat_intersect<void*,void*,float>(mybuf->head,v1,v2);
  r.foreach_index([&](const uint32_t index, const uint32_t data){
    std::cout << index << " " << data << std::endl;
  });
  */
}
