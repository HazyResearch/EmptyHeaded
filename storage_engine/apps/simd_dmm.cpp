#include "Trie.hpp"
#include "Vector.hpp"
#include "VectorOps.hpp"
#include "load.hpp"

int main()
{
 thread_pool::initializeThreadPool();

  auto tup = load_dense_matrix_and_transpose(4,4);
  
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
    BufferIndex a_nl = M_head.get(a_d);
    Vector<DenseVector,float,ParMemoryBuffer> M_b(
      M->memoryBuffers,
      a_nl);
    Vector<DenseVector,float,ParMemoryBuffer> B = 
      Vector<DenseVector,float,ParMemoryBuffer>(
        tid,
        result->memoryBuffers,
        M_T_head.get_this(),
        M_T_head.get_num_bytes(),
        M_T_head.get_num_annotation_bytes<float>());
    //block on M_b
    M_b.foreach_block([&](const uint32_t block_index){
      const float * const restrict M_b_block = M_b.get_block(block_index);
      B.foreach_index([&](const uint32_t b_i, const uint32_t b_d){
        (void) b_i;
        BufferIndex b_nl = M_T_head.get(b_d);
        Vector<DenseVector,float,ParMemoryBuffer> M_T_b(
          M_T->memoryBuffers,
          b_nl);
        const float * const restrict M_T_b_block = M_T_b.get_block(block_index);

        const size_t elems_per_reg = (256/(8*sizeof(float)));
        const size_t num_blocks_per_avx = BLOCK_SIZE/elems_per_reg;
        __m256 r = _mm256_set1_ps(0.0f);
        for(size_t i = 0; i < num_blocks_per_avx; i++){
          const __m256 m_b_1 = _mm256_loadu_ps(&M_b_block[i*elems_per_reg]);
          const __m256 m_b_2 = _mm256_loadu_ps(&M_T_b_block[i*elems_per_reg]);
          r = _mm256_fmadd_ps(m_b_1,m_b_2,r);
        }
        __m256 s = _mm256_hadd_ps(r,r);
        float anno = ((float*)&s)[0] + ((float*)&s)[1] + ((float*)&s)[4] + ((float*)&s)[5];
        anno += B.get(b_i,b_d);
        B.set(b_i,b_d,anno);
      });
    });
    A.set(a_i,a_d,B.bufferIndex);
  });

  /*
  const size_t tid = 0;
  Vector<DenseVector,float,ParMemoryBuffer> B = 
    Vector<DenseVector,float,ParMemoryBuffer>(
      tid,
      result->memoryBuffers,
      M_T_head.get_this(),
      M_T_head.get_num_index_bytes(),
      M_T_head.get_num_annotation_bytes<float>()
    );
  const size_t elems_per_reg = (256/(8*sizeof(float)));
  const size_t num_blocks_per_avx = BLOCK_SIZE/elems_per_reg;

  A.foreach_block([&](const uint32_t a_block_index){
    B.foreach_block([&](const uint32_t b_block_index){
      for(size_t a_d = 0; a_d < BLOCK_SIZE; a_d++){
        BufferIndex a_nl = M_head.get(a_d);
        Vector<DenseVector,float,ParMemoryBuffer> M_b(
          M->memoryBuffers,
          a_nl);
        const float * const restrict M_b_block = M_b.get_block(a_block_index);
        //const __m256 m_b_1 = _mm256_loadu_ps(&M_b_block[a_d*elems_per_reg]);
        for(size_t b_d = 0; b_d < BLOCK_SIZE; b_d++){
          float * const restrict B_block = B.get_block(b_block_index);
          BufferIndex b_nl = M_T_head.get(b_d);
          Vector<DenseVector,float,ParMemoryBuffer> M_T_b(
            M_T->memoryBuffers,
            b_nl);
          const float * const restrict M_T_b_block = M_T_b.get_block(b_block_index);
          //const __m256 m_b_2 = _mm256_loadu_ps(&M_T_b_block[b_d*elems_per_reg]);

          //At this point we have i and j
          __m256 r = _mm256_set1_ps(0.0f);
          for(size_t i = 0; i < num_blocks_per_avx; i++){
            const __m256 m_b_1 = _mm256_loadu_ps(&M_b_block[i*elems_per_reg]);
            const __m256 m_b_2 = _mm256_loadu_ps(&M_T_b_block[i*elems_per_reg]);
            r = _mm256_fmadd_ps(m_b_1,m_b_2,r);
          }
          __m256 s = _mm256_hadd_ps(r,r);
          float anno = ((float*)&s)[0] + ((float*)&s)[1] + ((float*)&s)[4] + ((float*)&s)[5];
          std::cout << "B_D: " << b_d << " ANNO: " << anno << std::endl;
          anno += B.get(b_d,b_d);
          B.set(b_d,b_d,anno);
        }
        A.set(a_d,a_d,B.bufferIndex);
      }
    });
  });
  */

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
