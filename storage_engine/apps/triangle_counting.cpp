#include "Trie.hpp"
#include "Vector.hpp"
#include "VectorOps.hpp"
#include "load.hpp"

int main()
{
 thread_pool::initializeThreadPool();

  std::string ehhome = std::string(getenv ("EMPTYHEADED_HOME"));

  Trie<void*,ParMemoryBuffer> *graph = load_graph(ehhome+"/test/graph/data/facebook_pruned.tsv");

  auto query_time = timer::start_clock();
  const size_t num_result_cols = 3;
  Trie<void*,ParMemoryBuffer>* result = new Trie<void*,ParMemoryBuffer>("",num_result_cols,false);
  
  //temporary buffers for aggregate intersections.
  ParMemoryBuffer **tmp_buffers = new ParMemoryBuffer*[num_result_cols];
  for(size_t i = 0; i < num_result_cols; i++){
    tmp_buffers[i] = new ParMemoryBuffer(100);
  }
  par::reducer<size_t> count(0,[&](size_t a, size_t b){return a+b;});
  Vector<SparseVector,BufferIndex,ParMemoryBuffer> graph_head(
    graph->memoryBuffers);
  Vector<SparseVector,void*,ParMemoryBuffer> A = 
    ops::agg_intersect<BufferIndex,BufferIndex>(
      NUM_THREADS,
      tmp_buffers[0],
      graph_head,
      graph_head);
    A.parforeach_index([&](const size_t tid, const uint32_t a_i, const uint32_t a_d){
      BufferIndex a_nl = graph_head.get(a_d);
      Vector<SparseVector,void*,ParMemoryBuffer> l2_a(
        graph->memoryBuffers,
        a_nl);
      Vector<SparseVector,void*,ParMemoryBuffer> B = 
        ops::agg_intersect<void*,BufferIndex>(
          tid,
          tmp_buffers[1],
          l2_a,
          graph_head);
      B.foreach_index([&](const uint32_t b_i, const uint32_t b_d){
        BufferIndex b_nl = graph_head.get(b_d);
        Vector<SparseVector,void*,ParMemoryBuffer> l2_b(
          graph->memoryBuffers,
          b_nl);
          Vector<SparseVector,void*,ParMemoryBuffer> l2_c = ops::agg_intersect<void*,void*>(
            tid,
            tmp_buffers[2],
            l2_a,
            l2_b);
          count.update(tid,l2_c.get_meta()->cardinality);
      });
    });

  timer::stop_clock("QUERY",query_time);
  std::cout << "COUNT: " << count.evaluate(0)  << std::endl;
  return 0;
}
