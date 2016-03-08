
#include "utils/thread_pool.hpp"
#include "Trie.hpp"
#include "TrieBuilder.hpp"
#include "TrieIterator.hpp"
#include "utils/timer.hpp"
#include "Encoding.hpp"

typedef std::unordered_map<std::string, void *> mymap;

void run(mymap *input_tries) {
  thread_pool::initializeThreadPool();
  Trie<float, ParMemoryBuffer> *Trie_InvDegree_0 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_InvDegree_0 = Trie<float, ParMemoryBuffer>::load(
        "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/graph/"
        "data/db_duplicated/relations/InvDegree/InvDegree_0");
    timer::stop_clock("LOADING Trie InvDegree_0", start_time);
  }
  Trie_InvDegree_0->foreach([&](std::vector<uint32_t>* d, float a){
    std::cout << d->at(0) << " " << a << std::endl;
  });
  Trie<void *, ParMemoryBuffer> *Trie_Edge_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_Edge_0_1 = Trie<void *, ParMemoryBuffer>::load(
        "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/graph/"
        "data/db_duplicated/relations/Edge/Edge_0_1");
    timer::stop_clock("LOADING Trie Edge_0_1", start_time);
  }

  auto e_loading_uint32_t = timer::start_clock();
  Encoding<uint32_t> *Encoding_uint32_t = Encoding<uint32_t>::from_binary(
      "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/graph/data/"
      "db_duplicated/encodings/uint32_t/");
  (void)Encoding_uint32_t;
  timer::stop_clock("LOADING ENCODINGS uint32_t", e_loading_uint32_t);
  par::reducer<size_t> num_rows_reducer(
      0, [](size_t a, size_t b) { return a + b; });
  //
  // query plan
  //
  auto query_timer = timer::start_clock();
  Trie<long, ParMemoryBuffer> *Trie_N_ = new Trie<long, ParMemoryBuffer>(
      "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/graph/data/"
      "db_duplicated/relations/N",
      0, true);
  long N;
  {
    auto bag_timer = timer::start_clock();
    num_rows_reducer.clear();
    ParTrieBuilder<long, ParMemoryBuffer> Builders(Trie_N_, 1);
    ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_x_y(Trie_Edge_0_1);
    const size_t count_x =
        Builders.build_aggregated_set(Iterators_Edge_x_y.head);
    par::reducer<long> annotation_x(0, [&](long a, long b) { return a + b; });
    num_rows_reducer.update(0, count_x);
    const long intermediate_x = (long)1 * count_x;
    annotation_x.update(0, intermediate_x);
    Builders.trie->annotation = annotation_x.evaluate(0);
    N = Builders.trie->annotation;
    Builders.trie->num_rows = num_rows_reducer.evaluate(0);
    std::cout << "NUM ROWS: " << Builders.trie->num_rows
              << " ANNOTATION: " << Builders.trie->annotation << std::endl;
    timer::stop_clock("BAG N TIME", bag_timer);
    Trie_N_->memoryBuffers = Builders.trie->memoryBuffers;
    Trie_N_->num_rows = 0;
    Trie_N_->encodings = Builders.trie->encodings;
  }
  Trie<float, ParMemoryBuffer> *Trie_PageRank_basecase_0 =
      new Trie<float, ParMemoryBuffer>(
          "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/graph/"
          "data/db_duplicated/relations/PageRank_basecase",
          1, true);
  {
    auto bag_timer = timer::start_clock();
    num_rows_reducer.clear();
    ParTrieBuilder<float, ParMemoryBuffer> Builders(Trie_PageRank_basecase_0,
                                                    2);
    Builders.trie->encodings.push_back((void *)Encoding_uint32_t);
    ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_x_z(Trie_Edge_0_1);
    const size_t count_x = Builders.build_set(Iterators_Edge_x_z.head);
    Builders.allocate_annotation();
    Builders.par_foreach_builder([&](const size_t tid, const uint32_t x_i,
                                     const uint32_t x_d) {
      TrieBuilder<float, ParMemoryBuffer> *Builder = Builders.builders.at(tid);
      TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_x_z =
          Iterators_Edge_x_z.iterators.at(tid);
      Iterator_Edge_x_z->get_next_block(0, x_d);
      const size_t count_z =
          Builder->build_aggregated_set(Iterator_Edge_x_z->get_block(1));
      num_rows_reducer.update(tid, 1);
      float annotation_z = (float)0; // const not head
      annotation_z = (1.0 / N);
      Builder->set_annotation(annotation_z, x_i, x_d);
    });
    Builders.trie->num_rows = num_rows_reducer.evaluate(0);
    std::cout << "NUM ROWS: " << Builders.trie->num_rows
              << " ANNOTATION: " << Builders.trie->annotation << std::endl;
    timer::stop_clock("BAG PageRank_basecase TIME", bag_timer);
    Trie_PageRank_basecase_0->memoryBuffers = Builders.trie->memoryBuffers;
    Trie_PageRank_basecase_0->num_rows = Builders.trie->num_rows;
    Trie_PageRank_basecase_0->encodings = Builders.trie->encodings;
  }
  Trie<float, ParMemoryBuffer> *Trie_PageRank_0 =
      new Trie<float, ParMemoryBuffer>("/Users/caberger/Documents/Research/"
                                       "code/EmptyHeaded/examples/graph/data/"
                                       "db_duplicated/relations/PageRank",
                                       1, true);
  {
    size_t num_iterations = 0;
    while (num_iterations < 2) {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<float, ParMemoryBuffer> Builders(Trie_PageRank_0, 2);
      Builders.trie->encodings.push_back((void *)Encoding_uint32_t);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_x_z(
          Trie_Edge_0_1);
      ParTrieIterator<float, ParMemoryBuffer> Iterators_PageRank_basecase_z(
          Trie_PageRank_basecase_0);
      ParTrieIterator<float, ParMemoryBuffer> Iterators_InvDegree_z(
          Trie_InvDegree_0);
      const size_t count_x = Builders.build_set(Iterators_Edge_x_z.head);
      Builders.allocate_annotation();
      Builders.par_foreach_builder([&](const size_t tid, const uint32_t x_i,
                                       const uint32_t x_d) {
        TrieBuilder<float, ParMemoryBuffer> *Builder =
            Builders.builders.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_x_z =
            Iterators_Edge_x_z.iterators.at(tid);
        TrieIterator<float, ParMemoryBuffer> *Iterator_PageRank_basecase_z =
            Iterators_PageRank_basecase_z.iterators.at(tid);
        TrieIterator<float, ParMemoryBuffer> *Iterator_InvDegree_z =
            Iterators_InvDegree_z.iterators.at(tid);
        Iterator_Edge_x_z->get_next_block(0, x_d);
        std::vector<const TrieBlock<hybrid, ParMemoryBuffer> *> z_sets;
        z_sets.push_back(Iterator_PageRank_basecase_z->get_block(0));
        z_sets.push_back(Iterator_Edge_x_z->get_block(1));
        z_sets.push_back(Iterator_InvDegree_z->get_block(0));
        const size_t count_z = Builder->build_aggregated_set(&z_sets);
        num_rows_reducer.update(tid, 1);
        float annotation_z = (float)0;
        Builder->foreach_aggregate([&](const uint32_t z_d) {
          std::cout << "a: " << z_d << " " << Iterator_InvDegree_z->get_annotation(0, z_d) << " " << Iterator_PageRank_basecase_z->get_annotation(0, z_d) << std::endl;
          const float intermediate_z =
              (float)1 * Iterator_PageRank_basecase_z->get_annotation(0, z_d) *
              1.0 * Iterator_InvDegree_z->get_annotation(0, z_d);
          annotation_z += intermediate_z;
        });
        annotation_z = 0.15 + 0.85 * annotation_z;
        Builder->set_annotation(annotation_z, x_i, x_d);
        std::cout << "SETTING: " << x_i << " " << x_d << " " << annotation_z << std::endl;
      });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG PageRank TIME", bag_timer);
      Trie_PageRank_0->memoryBuffers = Builders.trie->memoryBuffers;
      Trie_PageRank_0->num_rows = Builders.trie->num_rows;
      Trie_PageRank_0->encodings = Builders.trie->encodings;
      Trie_PageRank_0 = Builders.trie;
      num_iterations++;
    }
  }
  input_tries->insert(std::make_pair("N_", Trie_N_));

  input_tries->insert(std::make_pair("PageRank_0", Trie_PageRank_0));

  timer::stop_clock("QUERY TIME", query_timer);

  thread_pool::deleteThreadPool();
}
