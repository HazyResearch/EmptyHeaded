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
  /*
  const size_t mat_size = 128;
  auto tup = load_dense_matrix(mat_size,mat_size);
  */
  auto tup = load_sparse_matrix(
    "/dfs/scratch0/caberger/systems/matrix_benchmarking/data/harbor/data.tsv"
    //"/dfs/scratch0/caberger/systems/matrix_benchmarking/data/sparse.tsv"
  );
    
  Trie<EHVector,float,ParMemoryBuffer> *R = tup.first;
  Trie<EHVector,float,ParMemoryBuffer> *S = tup.second;
  
  
  //std::cout << "R" << std::endl;
  //R->print(); 
  //std::cout << "S" << std::endl;
  //S->print();
  
  //temporary buffers for aggregate intersections.

  ParMemoryBuffer **tmp_buffers = new ParMemoryBuffer*[4];
  for(size_t i = 0; i < 4; i++){
    tmp_buffers[i] = new ParMemoryBuffer(100);
  }

  auto query_time = timer::start_clock();

  Trie<EHVector,float,ParMemoryBuffer>* result = 
    new Trie<EHVector,float,ParMemoryBuffer>(
      "", 
      4, 
      true);
  result->dimensions.push_back(R->dimensions.at(0));
  result->dimensions.push_back(R->dimensions.at(1));

  TrieBuffer<float>** tmp_block = new TrieBuffer<float>*[NUM_THREADS];
  for(size_t i = 0; i< NUM_THREADS; i++){
    tmp_block[i] = new TrieBuffer<float>(false,result->memoryBuffers,1);
  }

  //const size_t tid = 0;
  //R(i,k),S(k,j)
  Vector<EHVector,BufferIndex,ParMemoryBuffer> R_I(
    R->memoryBuffers);
  Vector<EHVector,BufferIndex,ParMemoryBuffer> S_J(
    S->memoryBuffers);

  Vector<EHVector,BufferIndex,ParMemoryBuffer> RESULT_I = 
    Vector<EHVector,BufferIndex,ParMemoryBuffer>(
      NUM_THREADS,
      result->memoryBuffers,
      R_I);

  //const size_t tid = 0; const uint32_t I_i = 0; const uint32_t I_d = 365;
  RESULT_I.parforeach_index([&](const size_t tid, const uint32_t I_i, const uint32_t I_d){
    Vector<EHVector,BufferIndex,ParMemoryBuffer> R_i(
      R->memoryBuffers,
      R_I.get(I_d));

    Vector<EHVector,BufferIndex,ParMemoryBuffer> RESULT_J = 
      Vector<EHVector,BufferIndex,ParMemoryBuffer>(
        tid,
        result->memoryBuffers,
        S_J);

    //const uint32_t J_i = 0; const uint32_t J_d = 365;
    RESULT_J.foreach_index([&](const uint32_t J_i, const uint32_t J_d){
      //std::cout << "J: " << J_d << std::endl;
      Vector<EHVector,BufferIndex,ParMemoryBuffer> S_k(
        S->memoryBuffers,
        S_J.get(J_d));

      Vector<EHVector,BufferIndex,ParMemoryBuffer> RESULT_i = 
        Vector<EHVector,BufferIndex,ParMemoryBuffer>(
          tid,
          result->memoryBuffers,
          R_i);

      //const uint32_t i_i = 0; const uint32_t i_d = 46834;
      RESULT_i.foreach_index([&](const uint32_t i_i, const uint32_t i_d){
        //std::cout << "i: " << i_d << std::endl;
        Vector<EHVector,float,ParMemoryBuffer> R_k(
          R->memoryBuffers,
          R_i.get(i_d));

        //std::cout << "R_k: " << R_k.get_meta()->cardinality << std::endl;

        Vector<EHVector,void*,ParMemoryBuffer> RESULT_k = 
          ops::agg_intersect<ops::BS_BS_VOID<void*>,void*,float,BufferIndex>(
            tid,
            tmp_buffers[0],
            R_k,
            S_k);

        //allocate a buffer for RESULT_j
        std::vector<size_t> block_offsets;
        block_offsets.push_back(J_d);
        tmp_block[tid]->zero(result->dimensions,block_offsets); //zero out the memory.

        Vector<BLASVector,float,ParMemoryBuffer> tmp_j = 
          tmp_block[tid]->at<float>(0,0);
        
        RESULT_k.foreach_index([&](const uint32_t k_i, const uint32_t k_d){
          //grab value from R_k
          //std::cout << "k_d: " << k_d << std::endl;
          //if(k_d == 46834){
            const float mult_value = R_k.get(k_d);
            Vector<EHVector,float,ParMemoryBuffer> S_j(
              S->memoryBuffers,
              S_k.get(k_d));
            //std::cout << "S_j: " << S_j.get_meta()->cardinality << std::endl;
            //S_j.foreach_index([&](const uint32_t ii, const uint32_t dd){
            //  std::cout << dd << std::endl;
            //});
            ops::union_in_place<float>(mult_value,tmp_j,S_j);
          //}
        });
        //tmp_block[tid]->print();
        Vector<EHVector,float,ParMemoryBuffer> RESULT_j = 
          tmp_block[tid]->sparsify_vector(tid,result->memoryBuffers);
        RESULT_i.set(i_i,i_d,RESULT_j.bufferIndex);
      });
      RESULT_J.set(J_i,J_d,RESULT_i.bufferIndex);
    });
    RESULT_I.set(I_i,I_d,RESULT_J.bufferIndex);
  });
  timer::stop_clock("QUERY",query_time);

  std::cout << "RESULT" << std::endl;
  result->print();

  return 0;
}
