#include "Trie.hpp"
#include "Vector.hpp"
#include "VectorOps.hpp"
#include "load.hpp"

int main()
{
 thread_pool::initializeThreadPool();

  std::string ehhome = std::string(getenv ("EMPTYHEADED_HOME"));
  auto tup = load_matrix_and_transpose(ehhome+"/test/matrix/data/harbor.tsv");
  
  //auto tup = load_matrix_and_transpose("/dfs/scratch0/caberger/systems/matrix_benchmarking/data/simple.tsv");
  Trie<float,ParMemoryBuffer> *M = tup.first;

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
    M->memoryBuffers);

  
  //Dumb and not necessary.
  Vector<SparseVector,BufferIndex,ParMemoryBuffer> A = M_head;
  
  //Allocate some C buffer
  float *buf = (float*)tmp_buffers[0]->get_next(0,sizeof(float)*A.get_meta()->cardinality);
  memset((void*)buf,0,sizeof(float)*A.get_meta()->cardinality);
  A.foreach_index([&](const uint32_t a_i, const uint32_t a_d){
    const size_t tid = 0;
    BufferIndex a_nl = M_head.get(a_i,a_d);
    Vector<SparseVector,float,ParMemoryBuffer> M_b(
      M->memoryBuffers,
      a_nl);
    Vector<SparseVector,float,ParMemoryBuffer> B = M_b;//M_T_head;

    memset((void*)buf,0,sizeof(float)*A.get_meta()->cardinality);

    B.foreach([&](const uint32_t b_i, const uint32_t b_d, const float& b_anno){
      BufferIndex b_nl = M_T_head.get(b_d);
      Vector<SparseVector,float,ParMemoryBuffer> M_T_c(
        M->memoryBuffers,
        b_nl);
      M_T_c.foreach([&](const uint32_t c_i, const uint32_t c_d, const float& c_anno){
        buf[c_d] += (b_anno * c_anno);
      });

    });
   
    //sparsify vector?
    size_t card = 0;
    for(size_t i =0; i < A.get_meta()->cardinality; i++){
        if(buf[i] != 0.0){
          std::cout << a_d << " " << i << " " << buf[i] << std::endl;
          card++;
        }
    }
    A.set(a_i,a_d,BufferIndex(tid,B.bufferIndex.index));
  });
  timer::stop_clock("QUERY",query_time);

  //result->print();
  return 0;
}
