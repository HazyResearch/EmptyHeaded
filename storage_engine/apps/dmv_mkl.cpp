#include "TrieBuffer.hpp"
#include "Trie.hpp"
#include "Vector.hpp"
#include "VectorOps.hpp"
#include "load.hpp"

#include "/opt/intel/mkl/include/mkl.h"
#include "/opt/intel/mkl/include/mkl_spblas.h"

int main()
{
 thread_pool::initializeThreadPool();

  const size_t mat_size = 16;

  //auto tup = load_matrix_and_transpose("../../../matrix_benchmarking/data/simple.tsv");
  Trie<BLASVector,float,ParMemoryBuffer> *R = load_1block_dense_matrix(mat_size,mat_size);
  R->print();

  Trie<BLASVector,float,ParMemoryBuffer> *V = load_dense_vector(mat_size);
  V->print();
  //temporary buffers for aggregate intersections.
  ParMemoryBuffer **tmp_buffers = new ParMemoryBuffer*[4];
  for(size_t i = 0; i < 4; i++){
    tmp_buffers[i] = new ParMemoryBuffer(100);
  }

  //(i,j)(j)
  //(j,i)(j)
  auto query_time = timer::start_clock();

  Trie<BLASVector,float,ParMemoryBuffer>* result = 
    new Trie<BLASVector,float,ParMemoryBuffer>(
      "", 
      1, 
      true);

  TrieBuffer<void*>* tmp_block = 
    new TrieBuffer<void*>(true,result->memoryBuffers,1);

  result->dimensions.push_back(R->dimensions.at(0));
  float* result_anno = (float*)result->memoryBuffers->anno->get_next(R->dimensions.at(0)*sizeof(float));

  //R(i,j),S(j)
  //attr order j,i
  Vector<EHVector,BufferIndex,ParMemoryBuffer> R_J(
    R->memoryBuffers);

  Vector<EHVector,BufferIndex,ParMemoryBuffer> V_J(
    V->memoryBuffers);

  Vector<EHVector,void*,ParMemoryBuffer> RESULT_J = 
    ops::agg_intersect<ops::BS_BS_VOID<void*>,void*,BufferIndex,BufferIndex>(
      0,
      tmp_buffers[0],
      R_J,
      V_J);

  //setup tmp buffer
  RESULT_J.foreach_index([&](const uint32_t J_i, const uint32_t J_d){
    std::cout << "J: " << J_d << std::endl;
    Vector<EHVector,BufferIndex,ParMemoryBuffer> R_j(
      R->memoryBuffers,
      R_J.get(J_d));

    Vector<BLASVector,float,ParMemoryBuffer> V_j(
      V->memoryBuffers,
      V_J.get(J_d));

    //intersection? EHVector with a BLASVector
    R_j.foreach_index([&](const uint32_t j_i, const uint32_t j_d){
      //pull out value from vector
      Vector<EHVector,BufferIndex,ParMemoryBuffer> R_i(
        R->memoryBuffers,
        R_j.get(j_d));

      //multipy and union R_i with value from vector into tmp buffer
    });
  });

  return 0;
}
