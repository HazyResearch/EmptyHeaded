#include "Trie.hpp"
#include "Vector.hpp"
#include "VectorOps.hpp"
#include "load.hpp"

int main()
{
  thread_pool::initializeThreadPool();

  ParMemoryBuffer *p = new ParMemoryBuffer(2);

  std::vector<uint32_t> v1;
  std::vector<float> a;

  for(size_t i = 0; i < 10; i++){
    v1.push_back(i*32);
    a.push_back(0.234);
  }

  Vector<DenseVector,float,ParMemoryBuffer> v =
    Vector<DenseVector,float,ParMemoryBuffer>::from_array(
      0,
      p,
      v1.data(),
      a.data(),
      v1.size());

  v.foreach([&](const uint32_t index, const uint32_t data, const float& anno){
    std::cout << index << " " << data  << " " << anno << std::endl;
  });

  return 0;
}
