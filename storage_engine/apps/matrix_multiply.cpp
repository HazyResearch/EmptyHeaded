#include "Trie.hpp"
#include "Vector.hpp"
#include "VectorOps.hpp"
#include "load.hpp"

int main()
{
 thread_pool::initializeThreadPool();

  //std::string ehhome = std::string(getenv ("EMPTYHEADED_HOME"));

  auto tup = load_matrix_and_transpose("/dfs/scratch0/caberger/systems/matrix_benchmarking/data/simple.tsv");
  Trie<float,ParMemoryBuffer> *M = tup.first;
  Trie<float,ParMemoryBuffer> *M_T = tup.second;

  M->print();

  std::cout << std::endl;

  M_T->print();

  auto query_time = timer::start_clock();
  const size_t num_nprr_cols = 3;
  Trie<float,ParMemoryBuffer>* result = new Trie<float,ParMemoryBuffer>("",2,true);
  
  //temporary buffers for aggregate intersections.
  MemoryBuffer **tmp_buffers = new MemoryBuffer*[num_nprr_cols];
  for(size_t i = 0; i < num_nprr_cols; i++){
    tmp_buffers[i] = new MemoryBuffer(100);
  }

  //M(a,b),M_T(c,b)
  Vector<SparseVector,NextLevel,MemoryBuffer> M_head(
    M->memoryBuffers->at(NUM_THREADS),
    0);
  Vector<SparseVector,NextLevel,MemoryBuffer> M_T_head(
    M_T->memoryBuffers->at(NUM_THREADS),
    0);

  //Dumb and not necessary.
  Vector<SparseVector,NextLevel,MemoryBuffer> A = 
    ops::mat_intersect<NextLevel,NextLevel,NextLevel>(
      result->memoryBuffers->head,
      M_head,
      M_head);
    A.foreach_index([&](const uint32_t a_i, const uint32_t a_d){
      const size_t tid = 0;
      NextLevel a_nl = M_head.get(a_d);
      Vector<SparseVector,float,MemoryBuffer> M_b(
        M->memoryBuffers->at(a_nl.tid),
        a_nl.index);
      Vector<SparseVector,float,MemoryBuffer> B = 
        ops::mat_intersect<float,NextLevel,NextLevel>(
          result->memoryBuffers->at(tid),
          M_T_head,
          M_T_head);
      B.foreach_index([&](const uint32_t b_i, const uint32_t b_d){
        NextLevel b_nl = M_T_head.get(b_d);
        Vector<SparseVector,float,MemoryBuffer> M_T_b(
          M_T->memoryBuffers->at(b_nl.tid),
          b_nl.index);
          float l2_c = ops::agg_intersect(
            tmp_buffers[2],
            M_b,
            M_T_b);
          B.set(b_i,b_d,l2_c);
      });
      NextLevel nl;
      nl.tid = tid;
      nl.index = B.buffer.index;
      A.set(a_i,a_d,nl);
    });
  timer::stop_clock("QUERY",query_time);

  Encoding<uint32_t> *enc = (Encoding<uint32_t>*)M->encodings.at(0);
  result->foreach([&](std::vector<uint32_t> *v,float anno){
    for(size_t i = 0; i < v->size(); i++){
      std::cout << enc->key_to_value.at(v->at(i)) << "\t";
    }
    std::cout << anno << std::endl;
  });

  //result->print();
  return 0;
}
