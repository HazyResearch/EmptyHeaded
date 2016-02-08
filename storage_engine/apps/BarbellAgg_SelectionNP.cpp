
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

  long selection_value = 6;
  std::string db_path = "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/graph/data/facebook/db";

  Trie<void *, ParMemoryBuffer> *Trie_SBarbell_0_3_1_2_4_5 =
      new Trie<void *, ParMemoryBuffer>(
          db_path+"/relations/SBarbell/SBarbell_0_3_1_2_4_5",
          6, false);
  Trie<void *, ParMemoryBuffer> *Trie_Edge_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_Edge_0_1 = Trie<void *, ParMemoryBuffer>::load(
        db_path+"/relations/Edge/Edge_0_1");
    timer::stop_clock("LOADING Trie Edge_0_1", start_time);
  }
  Trie<void *, ParMemoryBuffer> *Trie_Edge_1_0 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_Edge_1_0 = Trie<void *, ParMemoryBuffer>::load(
        db_path+"/relations/Edge/Edge_1_0");
    timer::stop_clock("LOADING Trie Edge_1_0", start_time);
  }

  auto e_loading_node = timer::start_clock();
  Encoding<long> *Encoding_node = Encoding<long>::from_binary(
      db_path+"/encodings/node/");
  (void)Encoding_node;
  timer::stop_clock("LOADING ENCODINGS node", e_loading_node);
  par::reducer<size_t> num_rows_reducer(
      0, [](size_t a, size_t b) { return a + b; });
  //
  // query plan
  //
  {
    auto query_timer = timer::start_clock();
    Trie<long, ParMemoryBuffer> *Trie_bag_1_a_b_c_0 =
        new Trie<long, ParMemoryBuffer>(
            db_path+"/relations/bag_1_a_b_c",
            1, true);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<long, ParMemoryBuffer> Builders(Trie_bag_1_a_b_c_0, 3);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_a_c(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_b_c(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_a_b(
          Trie_Edge_0_1);
      const size_t count_a =
          Builders.build_set(Iterators_Edge_a_c.head, Iterators_Edge_a_b.head);
      Builders.allocate_annotation();
      Builders.par_foreach_builder([&](const size_t tid, const uint32_t a_i,
                                       const uint32_t a_d) {
        TrieBuilder<long, ParMemoryBuffer> *Builder = Builders.builders.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_a_c =
            Iterators_Edge_a_c.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_b_c =
            Iterators_Edge_b_c.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_a_b =
            Iterators_Edge_a_b.iterators.at(tid);
        Iterator_Edge_a_c->get_next_block(0, a_d);
        Iterator_Edge_a_b->get_next_block(0, a_d);
        const size_t count_b = Builder->build_aggregated_set(
            Iterator_Edge_b_c->get_block(0), Iterator_Edge_a_b->get_block(1));
        long annotation_b = (long)0;
        Builder->foreach_aggregate([&](const uint32_t b_d) {
          Iterator_Edge_b_c->get_next_block(0, b_d);
          const long intermediate_b = (long)1 * 1;
          const size_t count_c = Builder->build_aggregated_set(
              Iterator_Edge_a_c->get_block(1), Iterator_Edge_b_c->get_block(1));
          num_rows_reducer.update(tid, 1);
          long annotation_c = (long)0;
          const long intermediate_c =
              (long)1 * 1 * 1 * intermediate_b * count_c;
          annotation_c += intermediate_c;
          annotation_b += annotation_c;
        });
        Builder->set_annotation(annotation_b, a_i, a_d);
      });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_a_b_c TIME", bag_timer);
    }

    Trie<long, ParMemoryBuffer> *Trie_bag_0_d_a_0 =
        new Trie<long, ParMemoryBuffer>(
            db_path+"/relations/bag_0_d_a",
            1, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<long, ParMemoryBuffer> Builders(Trie_bag_0_d_a_0, 2);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_d_a(
          Trie_Edge_1_0);
      ParTrieIterator<long, ParMemoryBuffer> Iterators_bag_1_a_b_c_a(
          Trie_bag_1_a_b_c_0);
      const uint32_t selection_d_0 = Encoding_node->value_to_key.at(selection_value);
      Iterators_Edge_d_a.get_next_block(selection_d_0);
      const size_t count_d = Builders.build_aggregated_set(
          Iterators_Edge_d_a.head,Iterators_bag_1_a_b_c_a.head);
      par::reducer<long> annotation_a(0, [&](long a, long b) { return a + b; });
      Builders.par_foreach_aggregate([&](const size_t tid, const uint32_t a_d) {
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_d_a =
            Iterators_Edge_d_a.iterators.at(tid);
        TrieIterator<long, ParMemoryBuffer> *Iterator_bag_1_a_b_c_a =
            Iterators_bag_1_a_b_c_a.iterators.at(tid);
        const long annotation = Iterator_bag_1_a_b_c_a->get_annotation(0, a_d);
        annotation_a.update(tid, annotation);
      });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      Builders.trie->annotation = annotation_a.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_0_d_a TIME", bag_timer);
    }

    Trie<long, ParMemoryBuffer> *Trie_bag_0_d_x_0 =
        new Trie<long, ParMemoryBuffer>(
            db_path+"/relations/bag_0_d_a",
            1, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<long, ParMemoryBuffer> Builders(Trie_bag_0_d_x_0, 2);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_d_a(
          Trie_Edge_1_0);
      ParTrieIterator<long, ParMemoryBuffer> Iterators_bag_1_a_b_c_a(
          Trie_bag_1_a_b_c_0);
      const uint32_t selection_d_0 = Encoding_node->value_to_key.at(selection_value);
      Iterators_Edge_d_a.get_next_block(selection_d_0);
      const size_t count_d = Builders.build_aggregated_set(
          Iterators_Edge_d_a.head,Iterators_bag_1_a_b_c_a.head);
      par::reducer<long> annotation_a(0, [&](long a, long b) { return a + b; });
      Builders.par_foreach_aggregate([&](const size_t tid, const uint32_t a_d) {
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_d_a =
            Iterators_Edge_d_a.iterators.at(tid);
        TrieIterator<long, ParMemoryBuffer> *Iterator_bag_1_a_b_c_a =
            Iterators_bag_1_a_b_c_a.iterators.at(tid);
        const long annotation = Iterator_bag_1_a_b_c_a->get_annotation(0, a_d);
        annotation_a.update(tid, annotation);
      });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      Builders.trie->annotation = annotation_a.evaluate(0)*Trie_bag_0_d_a_0->annotation;
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_0_d_x TIME", bag_timer);
    }

    timer::stop_clock("QUERY TIME", query_timer);
  }
  thread_pool::deleteThreadPool();
}
