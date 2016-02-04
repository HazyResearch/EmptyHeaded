
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

  Trie<void *, ParMemoryBuffer> *Trie_SBarbell_0_1_2_3_4_5 =
      new Trie<void *, ParMemoryBuffer>(
          db_path+"/relations/SBarbell/SBarbell_0_1_2_3_4_5",
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
    Trie<void *, ParMemoryBuffer> *Trie_bag_1_d_x_y_z_0_1_2_3 =
        new Trie<void *, ParMemoryBuffer>(
            db_path+"/relations/bag_1_d_x_y_z",
            3, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(
          Trie_bag_1_d_x_y_z_0_1_2_3, 3);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_x_y(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_y_z(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_x_z(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_d_x(
          Trie_Edge_0_1);
      const uint32_t selection_d_0 = Encoding_node->value_to_key.at(selection_value);
      Iterators_Edge_d_x.get_next_block(selection_d_0);

      std::vector<const TrieBlock<hybrid, ParMemoryBuffer> *> x_sets;
      x_sets.push_back(Iterators_Edge_d_x.head);
      x_sets.push_back(Iterators_Edge_x_z.head);
      x_sets.push_back(Iterators_Edge_x_y.head);
      const size_t count_x = Builders.build_set(&x_sets);
      std::cout << count_x << std::endl;
      Builders.allocate_next();
      Builders.par_foreach_builder([&](const size_t tid, const uint32_t x_i,
                                       const uint32_t x_d) {
        TrieBuilder<void *, ParMemoryBuffer> *Builder =
            Builders.builders.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_x_y =
            Iterators_Edge_x_y.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_y_z =
            Iterators_Edge_y_z.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_x_z =
            Iterators_Edge_x_z.iterators.at(tid);
          
        Iterator_Edge_x_y->get_next_block(0, x_d);
        Iterator_Edge_x_z->get_next_block(0, x_d);

        const size_t count_y = Builder->build_set(tid, 
          Iterator_Edge_x_y->get_block(1),
          Iterator_Edge_y_z->get_block(0));
        std::cout << "y: " << count_y << std::endl;
        Builder->allocate_next(tid);
        Builder->foreach_builder([&](const uint32_t y_i, const uint32_t y_d) {
          Iterator_Edge_y_z->get_next_block(0, y_d);
          const size_t count_z =
              Builder->build_set(tid, Iterator_Edge_x_z->get_block(1),
                                 Iterator_Edge_y_z->get_block(1));
          num_rows_reducer.update(tid, count_z);
          Builder->set_level(y_i, y_d);
        });
        Builder->set_level(x_i, x_d);
      });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_d_x_y_z TIME", bag_timer);
    }

    Trie<void *, ParMemoryBuffer> *Trie_bag_0_d_a_b_c_0_1_2 =
        new Trie<void *, ParMemoryBuffer>(
            db_path+"/relations/bag_0_d_a_b_c",
            3, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(
          Trie_bag_0_d_a_b_c_0_1_2, 3);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_x_y(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_y_z(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_x_z(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_d_x(
          Trie_Edge_0_1);
      const uint32_t selection_d_0 = Encoding_node->value_to_key.at(selection_value);
      Iterators_Edge_d_x.get_next_block(selection_d_0);

      std::vector<const TrieBlock<hybrid, ParMemoryBuffer> *> x_sets;
      x_sets.push_back(Iterators_Edge_d_x.head);
      x_sets.push_back(Iterators_Edge_x_z.head);
      x_sets.push_back(Iterators_Edge_x_y.head);

      const size_t count_x = Builders.build_set(&x_sets);
      Builders.allocate_next();
      Builders.par_foreach_builder([&](const size_t tid, const uint32_t x_i,
                                       const uint32_t x_d) {
        TrieBuilder<void *, ParMemoryBuffer> *Builder =
            Builders.builders.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_x_y =
            Iterators_Edge_x_y.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_y_z =
            Iterators_Edge_y_z.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_x_z =
            Iterators_Edge_x_z.iterators.at(tid);
          
        Iterator_Edge_x_y->get_next_block(0, x_d);
        Iterator_Edge_x_z->get_next_block(0, x_d);

        const size_t count_y = Builder->build_set(tid, 
          Iterator_Edge_x_y->get_block(1),
          Iterator_Edge_y_z->get_block(0));
        Builder->allocate_next(tid);
        Builder->foreach_builder([&](const uint32_t y_i, const uint32_t y_d) {
          Iterator_Edge_y_z->get_next_block(0, y_d);
          const size_t count_z =
              Builder->build_set(tid, Iterator_Edge_x_z->get_block(1),
                                 Iterator_Edge_y_z->get_block(1));
          num_rows_reducer.update(tid, count_z);
          Builder->set_level(y_i, y_d);
        });
        Builder->set_level(x_i, x_d);
      });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_d_x_y_z TIME", bag_timer);
    }
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_0_d_a_b_c(
          Trie_bag_0_d_a_b_c_0_1_2);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_d_x_y_z(
          Trie_bag_1_d_x_y_z_0_1_2_3);
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(
          Trie_SBarbell_0_1_2_3_4_5, 6);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.build_set(Iterators_bag_0_d_a_b_c.head);
      Builders.allocate_next();
      Builders.par_foreach_builder([&](const size_t tid, const uint32_t a_i,
                                       const uint32_t a_d) {
        TrieBuilder<void *, ParMemoryBuffer> *Builder =
            Builders.builders.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_0_d_a_b_c =
            Iterators_bag_0_d_a_b_c.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_d_x_y_z =
            Iterators_bag_1_d_x_y_z.iterators.at(tid);
        Iterator_bag_0_d_a_b_c->get_next_block(0, a_i, a_d);
        Builder->build_set(tid, Iterator_bag_0_d_a_b_c->get_block(1));
        Builder->allocate_next(tid);
        Builder->foreach_builder([&](const uint32_t b_i, const uint32_t b_d) {
          Iterator_bag_0_d_a_b_c->get_next_block(1, b_i, b_d);
          Builder->build_set(tid, Iterator_bag_0_d_a_b_c->get_block(2));
          Builder->allocate_next(tid);
          Builder->foreach_builder([&](const uint32_t c_i, const uint32_t c_d) {
            Builder->build_set(tid, Iterator_bag_1_d_x_y_z->get_block(0));
            Builder->allocate_next(tid);
            Builder->foreach_builder([&](const uint32_t x_i, const uint32_t x_d) {
              Iterator_bag_1_d_x_y_z->get_next_block(0, x_i, x_d);
              Builder->build_set(tid, Iterator_bag_1_d_x_y_z->get_block(1));
              Builder->allocate_next(tid);
              Builder->foreach_builder([&](const uint32_t y_i,
                                           const uint32_t y_d) {
                Iterator_bag_1_d_x_y_z->get_next_block(1, y_i, y_d);
                const size_t count_z = Builder->build_set(tid, Iterator_bag_1_d_x_y_z->get_block(2));
                num_rows_reducer.update(tid, count_z);
                Builder->set_level(y_i, y_d);
              });
              Builder->set_level(x_i, x_d);
            });
            Builder->set_level(c_i, c_d);
          });
          Builder->set_level(b_i, b_d);
        });
        Builder->set_level(a_i, a_d);
      });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      timer::stop_clock("TOP DOWN TIME", bag_timer);
    }
    result_0 = (void *)Trie_SBarbell_0_1_2_3_4_5;
    std::cout << "NUMBER OF ROWS: " << Trie_SBarbell_0_1_2_3_4_5->num_rows
              << std::endl;
    timer::stop_clock("QUERY TIME", query_timer);
  }
  thread_pool::deleteThreadPool();
}
