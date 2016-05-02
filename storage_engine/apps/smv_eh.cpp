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

  auto tup = load_sparse_matrix(
    "/dfs/scratch0/caberger/systems/matrix_benchmarking/data/harbor/data.tsv"
  );
  Trie<EHVector,float,ParMemoryBuffer> *R = tup.second;

  //R->print();
  Trie<BLASVector,float,ParMemoryBuffer> *V = 
    load_dense_vector(R->dimensions.at(0));

  //V->print();
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
    new TrieBuffer<void*>(
      true,
      result->memoryBuffers,
      1,
      R->dimensions.at(0));

  result->dimensions.push_back(R->dimensions.at(0));
  float* result_anno = (float*)result->memoryBuffers->anno->get_next(R->dimensions.at(0)*sizeof(float));

  std::vector<size_t> block_offsets;
  block_offsets.push_back(0);
  tmp_block->zero(result->dimensions,block_offsets);

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

  Vector<BLASVector,float,ParMemoryBuffer> tmp_i = 
    tmp_block->at<float>(0,0);

  //setup tmp buffer
  RESULT_J.foreach_index([&](const uint32_t J_i, const uint32_t J_d){
    Vector<EHVector,BufferIndex,ParMemoryBuffer> R_j(
      R->memoryBuffers,
      R_J.get(J_d));

    Vector<BLASVector,float,ParMemoryBuffer> V_j(
      V->memoryBuffers,
      V_J.get(J_d));

    Vector<EHVector,void*,ParMemoryBuffer> RESULT_j = 
      ops::agg_intersect<ops::BS_BS_VOID<void*>,void*,float,BufferIndex>(
        0,
        tmp_buffers[1],
        V_j,
        R_j);

    const uint32_t j_d = 0;
    RESULT_j.foreach_index([&](const uint32_t j_i, const uint32_t j_d){
      //pull out value from vector
      const float mult_value = V_j.get(j_d);
      Vector<EHVector,float,ParMemoryBuffer> R_i(
        R->memoryBuffers,
        R_j.get(j_d));
      //multipy and union R_i with value from vector into tmp buffer
      ops::union_in_place<float>(mult_value,tmp_i,R_i);
    });
  });

  Vector<BLASVector,void*,ParMemoryBuffer> vector_result = 
    tmp_block->copy_vector(NUM_THREADS,result->memoryBuffers);

  timer::stop_clock("QUERY",query_time);

  std::cout << "RESULT" << std::endl;
  result->print();

  return 0;
}
