
#include "Query.hpp"
#include "utils/thread_pool.hpp"
#include "utils/parallel.hpp"
#include "Trie.hpp"
#include "TrieBuilder.hpp"
#include "TrieIterator.hpp"
#include "utils/timer.hpp"
#include "utils/ParMemoryBuffer.hpp"
#include "Encoding.hpp"

void Query_0::run_0() {
  thread_pool::initializeThreadPool();

  Trie<long, ParMemoryBuffer> *Trie_SFlique_ = new Trie<long, ParMemoryBuffer>(
      "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/graph/data/"
      "facebook/db/relations/SFlique/SFlique_",
      0, true);
  long SFlique;
  Trie<void *, ParMemoryBuffer> *Trie_Edge_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_Edge_0_1 = Trie<void *, ParMemoryBuffer>::load(
        "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/graph/"
        "data/facebook/db/relations/Edge/Edge_0_1");
    timer::stop_clock("LOADING Trie Edge_0_1", start_time);
  }
  Trie<void *, ParMemoryBuffer> *Trie_Edge_1_0 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_Edge_1_0 = Trie<void *, ParMemoryBuffer>::load(
        "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/graph/"
        "data/facebook/db/relations/Edge/Edge_1_0");
    timer::stop_clock("LOADING Trie Edge_1_0", start_time);
  }

  auto e_loading_node = timer::start_clock();
  Encoding<long> *Encoding_node = Encoding<long>::from_binary(
      "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/graph/data/"
      "facebook/db/encodings/node/");
  (void)Encoding_node;
  timer::stop_clock("LOADING ENCODINGS node", e_loading_node);
  par::reducer<size_t> num_rows_reducer(
      0, [](size_t a, size_t b) { return a + b; });
  //
  // query plan
  //
  {
    auto query_timer = timer::start_clock();
    Trie<long, ParMemoryBuffer> *Trie_bag_1_a_b_c_d_0 =
        new Trie<long, ParMemoryBuffer>("/Users/caberger/Documents/Research/"
                                        "code/EmptyHeaded/examples/graph/data/"
                                        "facebook/db/relations/bag_1_a_b_c_d",
                                        1, true);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<long, ParMemoryBuffer> Builders(Trie_bag_1_a_b_c_d_0, 4);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_a_d(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_c_d(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_a_c(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_b_c(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_a_b(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_b_d(
          Trie_Edge_0_1);
      std::vector<const TrieBlock<hybrid, ParMemoryBuffer> *> a_sets;
      a_sets.push_back(Iterators_Edge_a_d.head);
      a_sets.push_back(Iterators_Edge_a_c.head);
      a_sets.push_back(Iterators_Edge_a_b.head);
      const size_t count_a = Builders.build_set(&a_sets);
      Builders.allocate_annotation();
      Builders.par_foreach_builder([&](const size_t tid, const uint32_t a_i,
                                       const uint32_t a_d) {
        TrieBuilder<long, ParMemoryBuffer> *Builder = Builders.builders.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_a_d =
            Iterators_Edge_a_d.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_c_d =
            Iterators_Edge_c_d.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_a_c =
            Iterators_Edge_a_c.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_b_c =
            Iterators_Edge_b_c.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_a_b =
            Iterators_Edge_a_b.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_b_d =
            Iterators_Edge_b_d.iterators.at(tid);
        Iterator_Edge_a_d->get_next_block(0, a_d);
        Iterator_Edge_a_c->get_next_block(0, a_d);
        Iterator_Edge_a_b->get_next_block(0, a_d);
        std::vector<const TrieBlock<hybrid, ParMemoryBuffer> *> b_sets;
        b_sets.push_back(Iterator_Edge_b_c->get_block(0));
        b_sets.push_back(Iterator_Edge_a_b->get_block(1));
        b_sets.push_back(Iterator_Edge_b_d->get_block(0));
        const size_t count_b = Builder->build_aggregated_set(&b_sets);
        long annotation_b = (long)0;
        Builder->foreach_aggregate([&](const uint32_t b_d) {
          Iterator_Edge_b_c->get_next_block(0, b_d);
          Iterator_Edge_b_d->get_next_block(0, b_d);
          const long intermediate_b = (long)1 * 1;
          std::vector<const TrieBlock<hybrid, ParMemoryBuffer> *> c_sets;
          c_sets.push_back(Iterator_Edge_c_d->get_block(0));
          c_sets.push_back(Iterator_Edge_a_c->get_block(1));
          c_sets.push_back(Iterator_Edge_b_c->get_block(1));
          const size_t count_c = Builder->build_aggregated_set(&c_sets);
          long annotation_c = (long)0;
          Builder->foreach_aggregate([&](const uint32_t c_d) {
            Iterator_Edge_c_d->get_next_block(0, c_d);
            const long intermediate_c = (long)1 * 1 * 1 * intermediate_b;
            std::vector<const TrieBlock<hybrid, ParMemoryBuffer> *> d_sets;
            d_sets.push_back(Iterator_Edge_a_d->get_block(1));
            d_sets.push_back(Iterator_Edge_c_d->get_block(1));
            d_sets.push_back(Iterator_Edge_b_d->get_block(1));
            const size_t count_d = Builder->build_aggregated_set(&d_sets);
            num_rows_reducer.update(tid, 1);
            long annotation_d = (long)0;
            const long intermediate_d =
                (long)1 * 1 * 1 * 1 * intermediate_c * count_d;
            annotation_d += intermediate_d;
            annotation_c += annotation_d;
          });
          annotation_b += annotation_c;
        });
        Builder->set_annotation(annotation_b, a_i, a_d);
      });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_a_b_c_d TIME", bag_timer);
    }
    Trie<long, ParMemoryBuffer> *Trie_bag_0_e_a_ = Trie_SFlique_;
    long bag_0_e_a;
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<long, ParMemoryBuffer> Builders(Trie_bag_0_e_a_, 2);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_e_a(
          Trie_Edge_1_0);
      ParTrieIterator<long, ParMemoryBuffer> Iterators_bag_1_a_b_c_d_a(
          Trie_bag_1_a_b_c_d_0);
      const uint32_t selection_e_0 = Encoding_node->value_to_key.at(0);
      Iterators_Edge_e_a.get_next_block(selection_e_0);
      const long intermediate_e = (long)1;
      const size_t count_a = Builders.build_aggregated_set(
          Iterators_Edge_e_a.head, Iterators_bag_1_a_b_c_d_a.head);
      par::reducer<long> annotation_a(0, [&](long a, long b) { return a + b; });
      Builders.par_foreach_aggregate([&](const size_t tid, const uint32_t a_d) {
        TrieBuilder<long, ParMemoryBuffer> *Builder = Builders.builders.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_e_a =
            Iterators_Edge_e_a.iterators.at(tid);
        TrieIterator<long, ParMemoryBuffer> *Iterator_bag_1_a_b_c_d_a =
            Iterators_bag_1_a_b_c_d_a.iterators.at(tid);
        const long intermediate_a =
            (long)1 * 1 * Iterator_bag_1_a_b_c_d_a->get_annotation(0, a_d) *
            intermediate_e;
        annotation_a.update(tid, intermediate_a);
      });
      Builders.trie->annotation = annotation_a.evaluate(0);
      SFlique = Builders.trie->annotation;
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_0_e_a TIME", bag_timer);
      Trie_SFlique_->memoryBuffers = Builders.trie->memoryBuffers;
      Trie_SFlique_->num_rows = Builders.trie->num_rows;
      Trie_SFlique_->encodings = Builders.trie->encodings;
    }
    result_0 = (void *)Trie_SFlique_;
    std::cout << "NUMBER OF ROWS: " << Trie_SFlique_->num_rows << std::endl;
    timer::stop_clock("QUERY TIME", query_timer);
  }
  thread_pool::deleteThreadPool();
}
