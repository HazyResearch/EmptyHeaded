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
  auto tup = load_dense_matrix(mat_size,mat_size);
  Trie<EHVector,float,ParMemoryBuffer> *R = tup.first;
  Trie<EHVector,float,ParMemoryBuffer> *S = tup.second;

  std::cout << "R" << std::endl;
  R->print();
  std::cout << "S" << std::endl;
  S->print();
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
  const size_t num_anno = (R->dimensions.at(0)*R->dimensions.at(1));
  float* result_anno = (float*)result->memoryBuffers->anno->get_next(num_anno*sizeof(float));
  memset(result_anno,0,num_anno*sizeof(float));

  TrieBuffer<float>* tmp_block = 
    new TrieBuffer<float>(false,result->memoryBuffers,1);

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

  RESULT_I.foreach_index([&](const uint32_t I_i, const uint32_t I_d){
    
    Vector<EHVector,BufferIndex,ParMemoryBuffer> R_i(
      R->memoryBuffers,
      R_I.get(I_d));

    Vector<EHVector,BufferIndex,ParMemoryBuffer> RESULT_J = 
      Vector<EHVector,BufferIndex,ParMemoryBuffer>(
        0,
        result->memoryBuffers,
        S_J);

    RESULT_J.foreach_index([&](const uint32_t J_i, const uint32_t J_d){
      Vector<EHVector,BufferIndex,ParMemoryBuffer> S_k(
        S->memoryBuffers,
        S_J.get(J_d));

      Vector<EHVector,BufferIndex,ParMemoryBuffer> RESULT_i = 
        Vector<EHVector,BufferIndex,ParMemoryBuffer>(
          NUM_THREADS,
          result->memoryBuffers,
          R_i);
    
      RESULT_i.foreach_index([&](const uint32_t i_i, const uint32_t i_d){
        Vector<EHVector,float,ParMemoryBuffer> R_k(
          R->memoryBuffers,
          R_i.get(i_d));

        Vector<EHVector,void*,ParMemoryBuffer> RESULT_k = 
          ops::agg_intersect<ops::BS_BS_VOID<void*>,void*,float,BufferIndex>(
            0,
            tmp_buffers[0],
            R_k,
            S_k);

        //allocate a buffer for RESULT_j
        std::vector<size_t> block_offsets;
        block_offsets.push_back(0);
        tmp_block->zero(result->dimensions,block_offsets); //zero out the memory.

        RESULT_k.foreach_index([&](const uint32_t k_i, const uint32_t k_d){
            //grab value from R_k
            const float mult_value = R_k.get(k_d);
            Vector<EHVector,float,ParMemoryBuffer> S_j(
              S->memoryBuffers,
              S_k.get(k_d));

            //UNION S_j into buffer, multiply by value from R_k 
            ///UNION BITSET, stream through S_j, 
            //UNION_ADD BS/BS -> roughly the same as intersect
            //UNION_ADD BS/UINT -> stream through uint flip bits in BITSET
            Vector<BLASVector,float,ParMemoryBuffer> tmp_j = 
              tmp_block->at<float>(0,0);
            ops::union_in_place(mult_value,tmp_j,S_j);
        });
        //sparsify
        //set result_i
        Vector<EHVector,float,ParMemoryBuffer> RESULT_j = 
          tmp_block->sparsify_vector(0,result->memoryBuffers);
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
