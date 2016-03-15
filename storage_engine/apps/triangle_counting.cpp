#include "Trie.hpp"
#include "Vector.hpp"
#include "VectorOps.hpp"
#include "load.hpp"

int main()
{
 thread_pool::initializeThreadPool();

  std::string ehhome = std::string(getenv ("EMPTYHEADED_HOME"));

  Trie<void*,ParMemoryBuffer> *graph = load_graph(ehhome+"/test/graph/data/facebook_pruned.tsv");
  //graph->print();

  /*
  Trie<float,ParMemoryBuffer> *mat = load_matrix(ehhome+"mat.tsv");
  mat->print();

  Trie<float,ParMemoryBuffer> *vec = load_vector(ehhome+"vector.tsv");
  vec->print();
  */


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
  /*
  ParMemoryBuffer const * mybuf = new ParMemoryBuffer(100);
  std::vector<uint32_t>* a = new std::vector<uint32_t>();
  for(size_t i = 0; i < 10; i++){
    a->push_back(i*10);
  }
  Vector<SparseVector,void*,MemoryBuffer> v1 = 
    Vector<SparseVector,void*,MemoryBuffer>::from_array(
      mybuf->head,
      a->data(),
      a->size());

  std::vector<uint32_t>* b = new std::vector<uint32_t>();
  for(size_t i = 0; i < 1000; i++){
    b->push_back(i*2);
  }

  std::vector<float>* anno = new std::vector<float>();
  for(size_t i = 0; i < 1000; i++){
    anno->push_back((float)2.0);
  }

  std::vector<std::vector<uint32_t>> *trie = new std::vector<std::vector<uint32_t>>();
  std::vector<uint32_t> *max_sizes = new std::vector<uint32_t>();
  trie->push_back(*a);
  max_sizes->push_back(10);
  trie->push_back(*a);
  max_sizes->push_back(10);
  std::vector<void*>* annotations = new std::vector<void*>();
  Trie<void*,ParMemoryBuffer> *t = new Trie<void*,ParMemoryBuffer>(
    "",
    max_sizes, 
    trie,
    annotations);

  t->foreach([&](std::vector<uint32_t> *v, void* anno){
    for(size_t i = 0 ; i < v->size(); i++){
      std::cout << v->at(i) << "\t";
    }
    std::cout << std::endl;
  });

  thread_pool::deleteThreadPool();
  /*

  //from vector
    //void* just build index/data
    //annotation build index/data and values
    //NextLevel build index/data and alloc next level (worry about later)
  Vector<SparseVector,float,MemoryBuffer> v2 = 
    Vector<SparseVector,float,MemoryBuffer>::from_array(
      mybuf->head,
      b->data(),
      anno->data(),
      b->size());

  v2.foreach([&](const uint32_t index, const uint32_t data, const float value){
    std::cout << index << " " << data << " " << value << std::endl;
  });

  //intersect
    //annotation (mat) -> retrun index/data and values
      // - run intersection, loop over fill in annotations
    //annotation (agg) -> return value
      // - run intersection, loop over compute annotation
    //void* and NextLevel (mat/agg) -> return index/data
      // - run intersection (possible alloc NextLevel)
  
  Vector<SparseVector,void*,MemoryBuffer> r = 
    ops::mat_intersect<void*,void*,float>(mybuf->head,v1,v2);
  r.foreach_index([&](const uint32_t index, const uint32_t data){
    std::cout << index << " " << data << std::endl;
  });
  */
  return 0;
}
