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

  const size_t mat_size = 128;
  auto tup = load_dense_matrix_and_transpose(mat_size,mat_size);

  //auto tup = load_matrix_and_transpose("../../../matrix_benchmarking/data/simple.tsv");
  Trie<BLASVector,float,ParMemoryBuffer> *R = tup.first;
  Trie<BLASVector,float,ParMemoryBuffer> *S = tup.second;

  //R->print();
  //temporary buffers for aggregate intersections.

  ParMemoryBuffer **tmp_buffers = new ParMemoryBuffer*[4];
  for(size_t i = 0; i < 4; i++){
    tmp_buffers[i] = new ParMemoryBuffer(100);
  }

  auto query_time = timer::start_clock();

  Trie<BLASVector,float,ParMemoryBuffer>* result = 
    new Trie<BLASVector,float,ParMemoryBuffer>(
      "", 
      4, 
      true);
  result->dimensions.push_back(R->dimensions.at(0));
  result->dimensions.push_back(R->dimensions.at(1));
  const size_t num_anno = (R->dimensions.at(0)*R->dimensions.at(1));
  float* result_anno = (float*)result->memoryBuffers->anno->get_next(num_anno*sizeof(float));
  memset(result_anno,0,num_anno*sizeof(float));

  TrieBuffer<float>* tmp_block = 
    new TrieBuffer<float>(result->memoryBuffers,2);
  tmp_block->set_anno(result->memoryBuffers);

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
      std::cout << "BLOCK: " << I_d << " " << J_d << std::endl;
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
          Vector<BLASVector,float,ParMemoryBuffer> R_k(
            R->memoryBuffers,
            R_i.get(r_d));

          Vector<BLASVector,void*,ParMemoryBuffer> tmp_j = 
            ops::union_in_place(tmp_block->at(1,r_d),S_j);
          Vector<BLASVector,float,ParMemoryBuffer> RESULT_j(
            tmp_j.memoryBuffer,
            tmp_j.bufferIndex);
              
          RESULT_j.foreach_index([&](const uint32_t j_i, const uint32_t j_d){ 
            Vector<BLASVector,float,ParMemoryBuffer> S_k(
              S->memoryBuffers,
              S_j.get(j_d));

            const float anno_value = ops::agg_intersect<ops::BS_BS_SUM<float>,float>(
              0,
              result->memoryBuffers,
              R_k, 
              S_k);
            RESULT_j.set(j_i,j_d,RESULT_j.get(j_d)+anno_value);
          });
        });
      });
      Vector<EHVector,BufferIndex,ParMemoryBuffer> tmp = tmp_block->copy(0,result->memoryBuffers);
      RESULT_J.set(J_i,J_d,tmp.bufferIndex);
    });
    RESULT_I.set(I_i,I_d,RESULT_J.bufferIndex);
  });

  timer::stop_clock("QUERY",query_time);

  std::cout << "RESULT" << std::endl;
  result->print();

  return 0;
}
