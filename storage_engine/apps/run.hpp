#include "Vector.hpp"
#include "VectorOps.hpp"

typedef std::unordered_map<std::string, void *> mymap;

void run(mymap *input_tries) {
  ParMemoryBuffer const * mybuf = new ParMemoryBuffer(100);
  std::vector<uint32_t>* a = new std::vector<uint32_t>();
  a->push_back(0);
  a->push_back(1);
  a->push_back(2);
  Vector<SparseVector,void*,MemoryBuffer> v = Vector<SparseVector,void*,MemoryBuffer>::from_vector(mybuf->head,a);

  v.foreach([&](uint32_t index, uint32_t data){
    std::cout << index << " " << data << std::endl;
  });

  std::cout << "HERE" << std::endl;
  Vector<SparseVector,void*,MemoryBuffer> v2 = ops::vintersect(mybuf->head,v,v);
  v2.foreach([&](uint32_t index, uint32_t data){
    std::cout << index << " " << data << std::endl;
  });

}
