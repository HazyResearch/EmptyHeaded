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

  const size_t mat_size = 4;
  auto tup = load_dense_matrix_and_transpose(mat_size,mat_size);

  //auto tup = load_matrix_and_transpose("../../../matrix_benchmarking/data/simple.tsv");
  Trie<BLASVector,float,ParMemoryBuffer> *R = tup.first;
  Trie<BLASVector,float,ParMemoryBuffer> *S = tup.second;

  R->print();
  //temporary buffers for aggregate intersections.

  ParMemoryBuffer **tmp_buffers = new ParMemoryBuffer*[4];
  for(size_t i = 0; i < 4; i++){
    tmp_buffers[i] = new ParMemoryBuffer(100);
  }
  TrieBuffer<void*>* tmp_block = 
    new TrieBuffer<void*>(2);

  auto query_time = timer::start_clock();

  Trie<BLASVector,float,ParMemoryBuffer>* result = 
    new Trie<BLASVector,float,ParMemoryBuffer>(
      "", 
      4, 
      true);
  result->dimensions.push_back(R->dimensions.at(0));
  result->dimensions.push_back(R->dimensions.at(1));
  float* result_anno = (float*)result->memoryBuffers->anno->get_next(R->dimensions.at(0)*R->dimensions.at(1)*sizeof(float));
  /*
  for(size_t i = 0; i < R->dimensions.at(0); i++){
    for(size_t j = 0; j < R->dimensions.at(1); j++){
      //std::cout << i*R->dimensions.at(1)+j <<  " " << i+j << std::endl;
      result_anno[i*R->dimensions.at(1)+j] = i*R->dimensions.at(1)+j;
    }
  }*/

  //R(i,k),S(j,k)
  Vector<EHVector,BufferIndex,ParMemoryBuffer> R_I(
    R->memoryBuffers);
  Vector<EHVector,BufferIndex,ParMemoryBuffer> S_J(
    S->memoryBuffers);

  Vector<EHVector,BufferIndex,ParMemoryBuffer> RESULT_I = 
    Vector<EHVector,BufferIndex,ParMemoryBuffer>(
      NUM_THREADS,
      result->memoryBuffers,
      R_I);

  RESULT_I.foreach_index([&](const uint32_t I_i, const uint32_t I_d){
    Vector<EHVector,BufferIndex,ParMemoryBuffer> R_K(
      R->memoryBuffers,
      R_I.get(I_d));

    Vector<EHVector,BufferIndex,ParMemoryBuffer> RESULT_J = 
      Vector<EHVector,BufferIndex,ParMemoryBuffer>(
        0,
        result->memoryBuffers,
        S_J);

    RESULT_J.foreach_index([&](const uint32_t J_i, const uint32_t J_d){
      Vector<EHVector,BufferIndex,ParMemoryBuffer> S_K(
        S->memoryBuffers,
        S_J.get(J_d));

      Vector<EHVector,void*,ParMemoryBuffer> RESULT_K = 
        ops::agg_intersect<ops::BS_BS_VOID<void*>,void*,BufferIndex,BufferIndex>(
          0,
          tmp_buffers[0],
          R_K,
          S_K);

      std::vector<size_t> block_offsets;
      block_offsets.push_back(I_d);
      block_offsets.push_back(J_d);
      tmp_block->zero(result->dimensions,block_offsets); //zero out the memory.
      RESULT_K.foreach_index([&](const uint32_t K_i, const uint32_t K_d){
        Vector<EHVector,BufferIndex,ParMemoryBuffer> R_i(
          R->memoryBuffers,
          R_K.get(K_d));

        Vector<EHVector,BufferIndex,ParMemoryBuffer> S_j(
          S->memoryBuffers,
          S_K.get(K_d));

        Vector<BLASVector,void*,ParMemoryBuffer> RESULT_i = 
          ops::union_in_place(tmp_block->at(0,0),R_i);
        RESULT_i.foreach_index([&](const uint32_t r_i, const uint32_t r_d){ 
          ops::union_in_place(tmp_block->at(1,r_d),S_j);
        });
      });
      //std::cout << "COPY: " << I_d << " " << J_d*BLOCK_SIZE << std::endl;
      RESULT_J.set(J_i,J_d,tmp_block->copy(0,result->memoryBuffers).bufferIndex);
    });
    RESULT_I.set(I_i,I_d,RESULT_J.bufferIndex);
  });

  //call MKL.
  cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasTrans,
    R->dimensions.at(0), //num rows R
    S->dimensions.at(0), //num cols S (we have the transpose)
    R->dimensions.at(1),  //num cols R num rows S
    1.0, //scalar alpha
    (float*)R->memoryBuffers->anno->get_address(0),R->dimensions.at(1),
    (float*)S->memoryBuffers->anno->get_address(0),S->dimensions.at(1),
    0.0, result_anno, result->dimensions.at(1));

  timer::stop_clock("QUERY",query_time);

  std::cout << "RESULT" << std::endl;
  result->print();

  return 0;
}
