#include "Trie.hpp"
#include "Vector.hpp"
#include "VectorOps.hpp"
#include "load.hpp"

int main()
{
 thread_pool::initializeThreadPool();

  std::string ehhome = std::string(getenv ("EMPTYHEADED_HOME"));
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
  Vector<SparseVector,BufferIndex,ParMemoryBuffer> M_head(
    M->memoryBuffers);
  Vector<SparseVector,BufferIndex,ParMemoryBuffer> M_T_head(
    M_T->memoryBuffers);

  //Dumb and not necessary.
  Vector<SparseVector,BufferIndex,ParMemoryBuffer> A = 
    Vector<SparseVector,BufferIndex,ParMemoryBuffer>(
      NUM_THREADS,
      result->memoryBuffers,
      M_head);

  A.parforeach_index([&](const size_t tid, const uint32_t a_i, const uint32_t a_d){
    (void) a_i;
    /*
    BufferIndex a_nl = M_head.get(a_d);
    Vector<SparseVector,float,ParMemoryBuffer> M_b(
      M->memoryBuffers,
      a_nl);
    Vector<SparseVector,float,ParMemoryBuffer> B = 
      Vector<SparseVector,float,ParMemoryBuffer>(
        tid,
        result->memoryBuffers,
        M_T_head.get_this(),
        M_T_head.get_num_bytes());
    B.foreach_index([&](const uint32_t b_i, const uint32_t b_d){
      (void) b_i;
      BufferIndex b_nl = M_T_head.get(b_d);
      Vector<SparseVector,float,ParMemoryBuffer> M_T_b(
          M_T->memoryBuffers,
          b_nl);
        float l2_c = ops::agg_intersect(
          tid,
          tmp_buffers[2],
          M_b,
          M_T_b);
        B.set(b_i,b_d,l2_c);
    });
    A.set(a_i,a_d,B.bufferIndex);
    */
  });
  timer::stop_clock("QUERY",query_time);
  /*
  Encoding<uint32_t> *enc = (Encoding<uint32_t>*)M->encodings.at(0);
  result->foreach([&](std::vector<uint32_t> *v,float anno){
    if(anno != 0){
      for(size_t i = 0; i < v->size(); i++){
        std::cout << enc->key_to_value.at(v->at(i)) << "\t";
      }
      std::cout << anno << std::endl;
    }
  });
  */
  return 0;
}
