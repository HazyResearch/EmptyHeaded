#include "Vector.hpp"

typedef std::unordered_map<std::string, void *> mymap;

void run(mymap *input_tries) {
  ParMemoryBuffer const * mybuf = new ParMemoryBuffer(100);
  std::vector<uint32_t>* a = new std::vector<uint32_t>();
  a->push_back(0);
  a->push_back(1);
  a->push_back(2);
  //Vector<SparseVector,void*,MemoryBuffer> v(mybuf->head,0);

  Vector<SparseVector,void*,MemoryBuffer>::from_vector(mybuf->head,a);
}
