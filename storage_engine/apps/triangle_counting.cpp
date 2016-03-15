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
  MemoryBuffer **tmp_buffers = new MemoryBuffer*[num_result_cols];
  for(size_t i = 0; i < num_result_cols; i++){
    tmp_buffers[i] = new MemoryBuffer(100);
  }
  size_t count = 0;
  Vector<SparseVector,NextLevel,MemoryBuffer> graph_head(
    graph->memoryBuffers->at(NUM_THREADS),
    0);
  Vector<SparseVector,void*,MemoryBuffer> A = 
    ops::agg_intersect<NextLevel,NextLevel>(
      tmp_buffers[0],
      graph_head,
      graph_head);
    A.foreach_index([&](const uint32_t a_i, const uint32_t a_d){
      NextLevel a_nl = graph_head.get(a_d);
      Vector<SparseVector,void*,MemoryBuffer> l2_a(
        graph->memoryBuffers->at(a_nl.tid),
        a_nl.index);
      Vector<SparseVector,void*,MemoryBuffer> B = 
        ops::agg_intersect<void*,NextLevel>(
          tmp_buffers[1],
          l2_a,
          graph_head);
      B.foreach_index([&](const uint32_t b_i, const uint32_t b_d){
        NextLevel b_nl = graph_head.get(b_d);
        Vector<SparseVector,void*,MemoryBuffer> l2_b(
          graph->memoryBuffers->at(b_nl.tid),
          b_nl.index);
          Vector<SparseVector,void*,MemoryBuffer> l2_c = ops::agg_intersect<void*,void*>(
            tmp_buffers[2],
            l2_a,
            l2_b);
          count += l2_c.meta->cardinality;
      });
    });

  timer::stop_clock("QUERY",query_time);
  std::cout << "COUNT: " << count  << std::endl;
  return 0;
}
