#include "Vector.hpp"
#include "VectorOps.hpp"

typedef std::unordered_map<std::string, void *> mymap;

void run(mymap *input_tries) {
  ParMemoryBuffer const * mybuf = new ParMemoryBuffer(100);
  std::vector<uint32_t>* a = new std::vector<uint32_t>();
  for(size_t i = 0; i < 10; i++){
    a->push_back(i*10);
  }
  Vector<SparseVector,void*,MemoryBuffer> v1 = 
    Vector<SparseVector,void*,MemoryBuffer>::from_vector(
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

  //from vector
    //void* just build index/data
    //annotation build index/data and values
    //NextLevel build index/data and alloc next level (worry about later)
  Vector<SparseVector,float,MemoryBuffer> v2 = 
    Vector<SparseVector,float,MemoryBuffer>::from_vector(
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
}
