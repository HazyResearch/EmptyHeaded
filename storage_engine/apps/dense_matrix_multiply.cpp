#include "Trie.hpp"
#include "Vector.hpp"
#include "VectorOps.hpp"
#include "load.hpp"

int main()
{
 thread_pool::initializeThreadPool();

  auto tup = load_dense_matrix_and_transpose(8192,8192);
  
  //auto tup = load_matrix_and_transpose("../../../matrix_benchmarking/data/simple.tsv");
  Trie<float,ParMemoryBuffer> *M = tup.first;
  Trie<float,ParMemoryBuffer> *M_T = tup.second;

  auto query_time = timer::start_clock();
  const size_t num_nprr_cols = 3;
  Trie<float,ParMemoryBuffer>* result = new Trie<float,ParMemoryBuffer>("",2,true);
  
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

  Vector<DenseVector,BufferIndex,ParMemoryBuffer> A = 
    Vector<DenseVector,BufferIndex,ParMemoryBuffer>(
      NUM_THREADS,
      result->memoryBuffers,
      M_head);

  A.parforeach_index([&](const size_t tid, const uint32_t a_i, const uint32_t a_d){
    (void) a_i;
    BufferIndex a_nl = M_head.get(a_d);
    Vector<DenseVector,float,ParMemoryBuffer> M_b(
      M->memoryBuffers,
      a_nl);
    Vector<DenseVector,float,ParMemoryBuffer> B = 
      Vector<DenseVector,float,ParMemoryBuffer>(
        tid,
        result->memoryBuffers,
        M_T_head.get_this(),
        M_T_head.get_num_index_bytes(),
        M_T_head.get_num_annotation_bytes<float>());
    A.set(a_i,a_d,B.bufferIndex);
  });

  const size_t elems_per_reg = (256/(8*sizeof(float)));
  const size_t num_blocks_per_avx = BLOCK_SIZE/elems_per_reg;

  BufferIndex bi;
  bi.tid = NUM_THREADS;
  bi.index = 0;
  Vector<DenseVector,BufferIndex,ParMemoryBuffer> I(
    result->memoryBuffers,
    BufferIndex(NUM_THREADS,0));
  const size_t j_block_index = 0;
  I.foreach_block([&](const uint32_t i_block_index){
    for(size_t i = 0; i < BLOCK_SIZE; i++){
      BufferIndex i_nll = I.get(BLOCK_SIZE*j_block_index+i);
      Vector<DenseVector,float,ParMemoryBuffer> B(
        result->memoryBuffers,
        i_nll);

      BufferIndex i_nl = M_head.get(BLOCK_SIZE*i_block_index+i);
      Vector<DenseVector,float,ParMemoryBuffer> M_b(
        M->memoryBuffers,
        i_nl);
      const float * const restrict M_b_block = 
        M_b.get_block(i_block_index);
      for(size_t j = 0; j < BLOCK_SIZE; j++){
        BufferIndex j_nl = M_T_head.get(BLOCK_SIZE*i_block_index+j);
        Vector<DenseVector,float,ParMemoryBuffer> M_T_b(
          M_T->memoryBuffers,
          j_nl);
        const float * const restrict M_T_b_block = 
          M_T_b.get_block(i_block_index);

        __m256 r = _mm256_set1_ps(0.0f);
        for(size_t a = 0; a < num_blocks_per_avx; a++){
          const __m256 m_b_1 = _mm256_loadu_ps(&M_b_block[a*elems_per_reg]);
          const __m256 m_b_2 = _mm256_loadu_ps(&M_T_b_block[a*elems_per_reg]);
          r = _mm256_fmadd_ps(m_b_1,m_b_2,r);
        }
        __m256 s = _mm256_hadd_ps(r,r);
        float anno = ((float*)&s)[0] + ((float*)&s)[1] + ((float*)&s)[4] + ((float*)&s)[5];
        anno += B.get(BLOCK_SIZE*j_block_index+j,BLOCK_SIZE*j_block_index+j);
        B.set(BLOCK_SIZE*j_block_index+j,BLOCK_SIZE*j_block_index+j,anno);
      }
    }
  });

  timer::stop_clock("QUERY",query_time);

  size_t num_output = 0;
  const size_t max_num_output = 10;
  Encoding<uint32_t> *enc = (Encoding<uint32_t>*)M->encodings.at(0);
  result->foreach([&](std::vector<uint32_t> *v,float anno){
    if(anno != 0){
      for(size_t i = 0; i < v->size(); i++){
        std::cout << enc->key_to_value.at(v->at(i)) << "\t";
      }
      std::cout << anno << std::endl;
      num_output++;
    }
    if(num_output > max_num_output)
      exit(0);
  });
  return 0;
}
