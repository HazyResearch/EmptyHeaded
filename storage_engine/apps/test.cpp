#include "TrieBuffer.hpp"
#include "Trie.hpp"
#include "Vector.hpp"
#include "VectorOps.hpp"
#include "load.hpp"

int main()
{
 thread_pool::initializeThreadPool();

  const size_t mat_size = 512;
  auto tup = load_dense_matrix_and_transpose(mat_size,mat_size);

  //auto tup = load_matrix_and_transpose("../../../matrix_benchmarking/data/simple.tsv");
  Trie<BLASVector,float,ParMemoryBuffer> *R = tup.first;
  Trie<BLASVector,float,ParMemoryBuffer> *S = tup.second;

  //R->print();
  //std::cout << "BREAK" << std::endl;
  //S->print();

  //temporary buffers for aggregate intersections.
  ParMemoryBuffer **tmp_buffers = new ParMemoryBuffer*[4];
  for(size_t i = 0; i < 4; i++){
    tmp_buffers[i] = new ParMemoryBuffer(100);
  }
  TrieBuffer<float,ParMemoryBuffer>* tmp_block = 
    new TrieBuffer<float,ParMemoryBuffer>(2,true);

  auto query_time = timer::start_clock();

  Trie<EHVector,float,ParMemoryBuffer>* result = 
    new Trie<EHVector,float,ParMemoryBuffer>(
      "", 
      4, 
      true);

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
        ops::agg_intersect<BufferIndex,BufferIndex>(
          0,
          tmp_buffers[0],
          R_K,
          S_K);

      std::vector<size_t> block_offsets;
      block_offsets.push_back(I_d*BLOCK_SIZE);
      block_offsets.push_back(J_d*BLOCK_SIZE);
      tmp_block->zero(block_offsets); //zero out the memory.
      RESULT_K.foreach_index([&](const uint32_t K_i, const uint32_t K_d){
        Vector<EHVector,BufferIndex,ParMemoryBuffer> R_i(
          R->memoryBuffers,
          R_K.get(K_d));

        Vector<EHVector,BufferIndex,ParMemoryBuffer> S_j(
          S->memoryBuffers,
          S_K.get(K_d));

        /*
        Vector<BLASVector,void*,ParMemoryBuffer> RESULT_i = 
          ops::union_in_place(tmp_block->at(0,0),R_i);
          */
        R_i.foreach_index([&](const uint32_t r_i, const uint32_t r_d){
          Vector<BLASVector,void*,ParMemoryBuffer> RESULT_j = 
            ops::union_in_place(tmp_block->at(1,r_d),S_j);
        });
      });
      RESULT_J.set(J_i,J_d,tmp_block->copy(0,result->memoryBuffers).bufferIndex);
    });
    RESULT_I.set(I_i,I_d,RESULT_J.bufferIndex);
  });

  timer::stop_clock("QUERY",query_time);

  std::cout << "RESULT" << std::endl;
  //result->print();

  return 0;
}
