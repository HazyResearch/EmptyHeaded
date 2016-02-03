
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

  Trie<void *, ParMemoryBuffer> *Trie_SBarbell_0_3_1_2_4_5 =
      new Trie<void *, ParMemoryBuffer>(
          "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/graph/"
          "data/facebook/db_pruned/relations/SBarbell/SBarbell_0_3_1_2_4_5",
          6, false);
  Trie<void *, ParMemoryBuffer> *Trie_Edge_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_Edge_0_1 = Trie<void *, ParMemoryBuffer>::load(
        "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/graph/"
        "data/facebook/db_pruned/relations/Edge/Edge_0_1");
    timer::stop_clock("LOADING Trie Edge_0_1", start_time);
  }
  Trie<void *, ParMemoryBuffer> *Trie_Edge_1_0 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_Edge_1_0 = Trie<void *, ParMemoryBuffer>::load(
        "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/graph/"
        "data/facebook/db_pruned/relations/Edge/Edge_1_0");
    timer::stop_clock("LOADING Trie Edge_1_0", start_time);
  }

  auto e_loading_node = timer::start_clock();
  Encoding<long> *Encoding_node = Encoding<long>::from_binary(
      "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/graph/data/"
      "facebook/db_pruned/encodings/node/");
  (void)Encoding_node;
  timer::stop_clock("LOADING ENCODINGS node", e_loading_node);
  par::reducer<size_t> num_rows_reducer(
      0, [](size_t a, size_t b) { return a + b; });
  //
  // query plan
  //
  {
    auto query_timer = timer::start_clock();
    Trie<void *, ParMemoryBuffer> *Trie_bag_1_a_b_c_0_1_2 =
        new Trie<void *, ParMemoryBuffer>(
            "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/"
            "graph/data/facebook/db_pruned/relations/bag_1_a_b_c",
            3, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_1_a_b_c_0_1_2,
                                                       3);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_a_b(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_b_c(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_a_c(
          Trie_Edge_0_1);
      const size_t count_a =
          Builders.build_set(Iterators_Edge_a_b.head, Iterators_Edge_a_c.head);
      Builders.allocate_next();
      Builders.par_foreach_builder([&](const size_t tid, const uint32_t a_i,
                                       const uint32_t a_d) {
        TrieBuilder<void *, ParMemoryBuffer> *Builder =
            Builders.builders.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_a_b =
            Iterators_Edge_a_b.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_b_c =
            Iterators_Edge_b_c.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_a_c =
            Iterators_Edge_a_c.iterators.at(tid);
        Iterator_Edge_a_b->get_next_block(0, a_d);
        Iterator_Edge_a_c->get_next_block(0, a_d);
        const size_t count_b =
            Builder->build_set(tid, Iterator_Edge_a_b->get_block(1),
                               Iterator_Edge_b_c->get_block(0));
        Builder->allocate_next(tid);
        Builder->foreach_builder([&](const uint32_t b_i, const uint32_t b_d) {
          Iterator_Edge_b_c->get_next_block(0, b_d);
          const size_t count_c =
              Builder->build_set(tid, Iterator_Edge_b_c->get_block(1),
                                 Iterator_Edge_a_c->get_block(1));
          num_rows_reducer.update(tid, count_c);
          Builder->set_level(b_i, b_d);
        });
        Builder->set_level(a_i, a_d);
      });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_a_b_c TIME", bag_timer);
    }
    Trie<void *, ParMemoryBuffer> *Trie_bag_2_x_y_z_0_1_2 =
        Trie_bag_1_a_b_c_0_1_2;
    Trie<void *, ParMemoryBuffer> *Trie_bag_0_d_a_0 =
        new Trie<void *, ParMemoryBuffer>(
            "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/"
            "graph/data/facebook/db_pruned/relations/bag_0_d_a",
            1, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_0_d_a_0, 2);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_d_a(
          Trie_Edge_1_0);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_a_b_c_a_b_c(
          Trie_bag_1_a_b_c_0_1_2);
      const uint32_t selection_d_0 = Encoding_node->value_to_key.at(0);
      const size_t count_d = Builders.build_set(
          Iterators_Edge_d_a.head);
      num_rows_reducer.update(0, count_d);
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_0_d_a TIME", bag_timer);
    }
    Trie<void *, ParMemoryBuffer> *Trie_bag_1_d_x_0_1 =
        Trie_bag_0_d_a_0;
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_0_d_a(
          Trie_bag_0_d_a_0);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_a_b_c(
          Trie_bag_1_a_b_c_0_1_2);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_d_x(
          Trie_bag_1_d_x_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_2_x_y_z(
          Trie_bag_2_x_y_z_0_1_2);
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(
          Trie_SBarbell_0_3_1_2_4_5, 7);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.build_set(Iterators_bag_0_d_a.head);
      Builders.allocate_next();
      Builders.par_foreach_builder([&](const size_t tid, const uint32_t a_i,
                                       const uint32_t a_d) {
        TrieBuilder<void *, ParMemoryBuffer> *Builder =
            Builders.builders.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_0_d_a =
            Iterators_bag_0_d_a.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_a_b_c =
            Iterators_bag_1_a_b_c.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_d_x =
            Iterators_bag_1_d_x.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_2_x_y_z =
            Iterators_bag_2_x_y_z.iterators.at(tid);
        Iterator_bag_1_a_b_c->get_next_block(0, a_d);
        Builder->build_set(tid, Iterator_bag_1_a_b_c->get_block(1));
        Builder->allocate_next(tid);
        Builder->foreach_builder([&](const uint32_t b_i, const uint32_t b_d) {
          Iterator_bag_1_a_b_c->get_next_block(1, b_i, b_d);
          Builder->build_set(tid, Iterator_bag_1_a_b_c->get_block(2));
          Builder->allocate_next(tid);
          Builder->foreach_builder([&](const uint32_t c_i, const uint32_t c_d) {
            /*
            Builder->build_set(tid, Iterator_bag_1_d_x->get_block(0));
            Builder->allocate_next(tid);
            Builder->foreach_builder(
                [&](const uint32_t d_i, const uint32_t d_d) {
                  Iterator_bag_1_d_x->get_next_block(0, d_i, d_d);
                  Builder->build_set(tid, Iterator_bag_1_d_x->get_block(1));
                  Builder->allocate_next(tid);
                  Builder->foreach_builder([&](const uint32_t x_i,
                                               const uint32_t x_d) {
                    Iterator_bag_2_x_y_z->get_next_block(0, x_d);
                    Builder->build_set(tid, Iterator_bag_2_x_y_z->get_block(1));
                    Builder->allocate_next(tid);
                    Builder->foreach_builder(
                        [&](const uint32_t y_i, const uint32_t y_d) {
                          Iterator_bag_2_x_y_z->get_next_block(1, y_i, y_d);
                          const size_t count_z = Builder->build_set(
                              tid, Iterator_bag_2_x_y_z->get_block(2));
                          num_rows_reducer.update(tid, count_z);
                          Builder->set_level(y_i, y_d);
                        });
                    Builder->set_level(x_i, x_d);
                  });
                  Builder->set_level(d_i, d_d);
                });
                */
            Builder->set_level(c_i, c_d);
          });
          Builder->set_level(b_i, b_d);
        });
        Builder->set_level(a_i, a_d);
      });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      timer::stop_clock("TOP DOWN TIME", bag_timer);
    }

    result_0 = (void *)Trie_SBarbell_0_3_1_2_4_5;
    std::cout << "NUMBER OF ROWS: " << Trie_SBarbell_0_3_1_2_4_5->num_rows
              << std::endl;
    timer::stop_clock("QUERY TIME", query_timer);
  }
  thread_pool::deleteThreadPool();
}
