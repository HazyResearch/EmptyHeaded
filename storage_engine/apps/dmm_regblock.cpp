#include "TrieBuffer.hpp"
#include "Trie.hpp"
#include "Vector.hpp"
#include "VectorOps.hpp"
#include "load.hpp"

int main()
{
 thread_pool::initializeThreadPool();

  const size_t mat_size = 2048;
  auto tup = load_dense_matrix(mat_size,mat_size);

  //auto tup = load_matrix_and_transpose("../../../matrix_benchmarking/data/simple.tsv");
  Trie<float,ParMemoryBuffer> *M = tup.first;
  Trie<float,ParMemoryBuffer> *M_T = tup.second;

  //M_T->print();

  auto query_time = timer::start_clock();
  const size_t num_nprr_cols = 4;
  Trie<float,ParMemoryBuffer>* result = new Trie<float,ParMemoryBuffer>("",4,true);
  
  //temporary buffers for aggregate intersections.
  ParMemoryBuffer **tmp_buffers = new ParMemoryBuffer*[num_nprr_cols];
  for(size_t i = 0; i < num_nprr_cols; i++){
    tmp_buffers[i] = new ParMemoryBuffer(100);
  }

  //M(a,b),M_T(c,b)
  Vector<DenseVector,BufferIndex,ParMemoryBuffer> M_head(
    M->memoryBuffers);
  Vector<DenseVector,BufferIndex,ParMemoryBuffer> M_T_head(
    M_T->memoryBuffers);

  Vector<DenseVector,BufferIndex,ParMemoryBuffer> I = 
    Vector<DenseVector,BufferIndex,ParMemoryBuffer>(
      NUM_THREADS,
      result->memoryBuffers,
      M_head);

  //allocate a dense square for i and j
  //block size for i and block size * block size for j

  //R(i,k),R(k,j)

  float * anno_buffer = new float[mat_size*mat_size]; 
  memset(anno_buffer,0,mat_size*mat_size*sizeof(float));

  TrieBuffer<float,ParMemoryBuffer>* tmp_block = 
    new TrieBuffer<float,ParMemoryBuffer>(2,true);

  I.foreach_index([&](const uint32_t I_i, const uint32_t I_d){
    size_t tid = 0; 

    (void) I_i;
    BufferIndex I_nl = M_head.get(I_d);
    Vector<DenseVector,BufferIndex,ParMemoryBuffer> K_I(
      M->memoryBuffers,
      I_nl);

    Vector<DenseVector,void*,ParMemoryBuffer> K = 
      ops::agg_intersect<BufferIndex,BufferIndex>(
        tid,
        tmp_buffers[0],
        K_I,
        M_T_head);

    K.foreach_index([&](const uint32_t K_i, const uint32_t K_d){
      BufferIndex K_nl = M_T_head.get(K_d);
      Vector<DenseVector,BufferIndex,ParMemoryBuffer> J(
        M_T->memoryBuffers,
        K_nl);

      BufferIndex K_i_nl = K_I.get(K_d);
      Vector<DenseVector,BufferIndex,ParMemoryBuffer> i(
        M->memoryBuffers,
        K_i_nl);

      //R(i,k),R(k,j)
      J.foreach_index([&](const uint32_t J_i, const uint32_t J_d){      
        BufferIndex k_J_nl = J.get(J_d);
        Vector<DenseVector,BufferIndex,ParMemoryBuffer> k_J(
          M_T->memoryBuffers,
          k_J_nl);

        //allocate all of j
        i.foreach_index([&](const uint32_t i_i, const uint32_t i_d){ 
          BufferIndex k_i_nl = i.get(i_d);
          Vector<DenseVector,float,ParMemoryBuffer> k_i(
            M->memoryBuffers,
            k_i_nl);

          Vector<DenseVector,void*,ParMemoryBuffer> k = 
            ops::agg_intersect<float,BufferIndex>(
              tid,
              tmp_buffers[1],
              k_i,
              k_J);
          //R(i,k),R(k,j)
          k.foreach_index([&](const uint32_t k_i_s, const uint32_t k_d){ 
            //union in first block of j
            BufferIndex j_k_nl = k_J.get(k_d);
            Vector<DenseVector,float,ParMemoryBuffer> j(
              M->memoryBuffers,
              j_k_nl);

            const float scalar_anno = k_i.get(k_d);
            __m256 a = _mm256_set1_ps(scalar_anno);

            const float * const restrict stream_anno = (float*)j.get_annotation();

            const size_t elems_per_reg = 8;
            const size_t num_avx_per_block = std::max((int)8,(int)(BLOCK_SIZE/elems_per_reg));

            for(size_t i = 0; i < num_avx_per_block; i++){
              const size_t index = i_d*mat_size+I_d*mat_size*BLOCK_SIZE+J_d*BLOCK_SIZE+i*elems_per_reg;
              __m256 r = _mm256_loadu_ps(&anno_buffer[index]);
              const __m256 b = _mm256_loadu_ps(&stream_anno[i*elems_per_reg]);
              r = _mm256_fmadd_ps(a,b,r);
              _mm256_storeu_ps(&anno_buffer[index],r);
            }
          });
        });  
      });
    });
  });

  timer::stop_clock("QUERY",query_time);

  size_t num_output = 0;
  const size_t max_num_output = 16;
  for(size_t i = 0; i < mat_size; i++){
    for(size_t j = 0; j < mat_size; j++){
      if(num_output++ > max_num_output)
        exit(0);
      std::cout << i << " " << j <<  " " << anno_buffer[i*mat_size+j] << std::endl;
    }
  }

  /*

  Encoding<uint32_t> *enc = (Encoding<uint32_t>*)M->encodings.at(0);
  result->foreach([&](std::vector<uint32_t> *v,float anno){
    //if(anno != 0){
      for(size_t i = 0; i < v->size(); i++){
        std::cout << enc->key_to_value.at(v->at(i)) << "\t";
      }
      std::cout << anno << std::endl;
      num_output++;
    //}
    if(num_output > max_num_output)
      exit(0);
  });
  */

  return 0;
}
