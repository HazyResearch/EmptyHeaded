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
    Vector<SparseVector,void*,MemoryBuffer>::from_vector(mybuf->head,a);

  std::vector<uint32_t>* b = new std::vector<uint32_t>();
  for(size_t i = 0; i < 1000; i++){
    b->push_back(i*2);
  }
  Vector<SparseVector,void*,MemoryBuffer> v2 = 
    Vector<SparseVector,void*,MemoryBuffer>::from_vector(mybuf->head,b);


  std::cout << "HERE" << std::endl;
  Vector<SparseVector,void*,MemoryBuffer> r = ops::vintersect(mybuf->head,v1,v2);
  r.foreach([&](uint32_t index, uint32_t data){
    std::cout << index << " " << data << std::endl;
  });

}
