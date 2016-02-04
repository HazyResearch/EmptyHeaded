
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

  long selection_value = 0;
  std::string db_path = "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/graph/data/facebook/db";

  Trie<void *, ParMemoryBuffer> *Trie_SFlique_0_1_2_3 =
      new Trie<void *, ParMemoryBuffer>(
          db_path+"/relations/SFlique/SFlique_0_1_2_3",
          4, false);
  Trie<void *, ParMemoryBuffer> *Trie_Edge_1_0 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_Edge_1_0 = Trie<void *, ParMemoryBuffer>::load(
        db_path+"/relations/Edge/Edge_1_0");
    timer::stop_clock("LOADING Trie Edge_1_0", start_time);
  }
  Trie<void *, ParMemoryBuffer> *Trie_Edge_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_Edge_0_1 = Trie<void *, ParMemoryBuffer>::load(
        db_path+"/relations/Edge/Edge_0_1");
    timer::stop_clock("LOADING Trie Edge_0_1", start_time);
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
    Trie<void *, ParMemoryBuffer> *Trie_bag_1_e_a_0 =
        new Trie<void *, ParMemoryBuffer>(
            db_path+"/relations/bag_1_e_a",
            1, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_1_e_a_0, 2);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_e_a(
          Trie_Edge_1_0);
      const uint32_t selection_e_0 = Encoding_node->value_to_key.at(selection_value);
      Iterators_Edge_e_a.get_next_block(selection_e_0);
      const size_t count_a = Builders.build_set(Iterators_Edge_e_a.head);
      num_rows_reducer.update(0, count_a);
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_e_a TIME", bag_timer);
    }
    Trie<void *, ParMemoryBuffer> *Trie_bag_0_a_b_c_d_0_1_2_3 =
        Trie_SFlique_0_1_2_3;
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(
          Trie_bag_0_a_b_c_d_0_1_2_3, 4);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      Builders.trie->encodings.push_back((void *)Encoding_node);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_a_b(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_b_c(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_a_c(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_a_d(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_b_d(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_c_d(
          Trie_Edge_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_e_a_a(
          Trie_bag_1_e_a_0);
      std::vector<const TrieBlock<hybrid, ParMemoryBuffer> *> a_sets;
      a_sets.push_back(Iterators_Edge_a_b.head);
      a_sets.push_back(Iterators_Edge_a_c.head);
      a_sets.push_back(Iterators_Edge_a_d.head);
      a_sets.push_back(Iterators_bag_1_e_a_a.head);
      const size_t count_a = Builders.build_set(&a_sets);
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
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_a_d =
            Iterators_Edge_a_d.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_b_d =
            Iterators_Edge_b_d.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_c_d =
            Iterators_Edge_c_d.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_e_a_a =
            Iterators_bag_1_e_a_a.iterators.at(tid);
        Iterator_Edge_a_b->get_next_block(0, a_d);
        Iterator_Edge_a_c->get_next_block(0, a_d);
        Iterator_Edge_a_d->get_next_block(0, a_d);
        std::vector<const TrieBlock<hybrid, ParMemoryBuffer> *> b_sets;
        b_sets.push_back(Iterator_Edge_a_b->get_block(1));
        b_sets.push_back(Iterator_Edge_b_c->get_block(0));
        b_sets.push_back(Iterator_Edge_b_d->get_block(0));
        const size_t count_b = Builder->build_set(tid, &b_sets);
        Builder->allocate_next(tid);
        Builder->foreach_builder([&](const uint32_t b_i, const uint32_t b_d) {
          Iterator_Edge_b_c->get_next_block(0, b_d);
          Iterator_Edge_b_d->get_next_block(0, b_d);
          std::vector<const TrieBlock<hybrid, ParMemoryBuffer> *> c_sets;
          c_sets.push_back(Iterator_Edge_b_c->get_block(1));
          c_sets.push_back(Iterator_Edge_a_c->get_block(1));
          c_sets.push_back(Iterator_Edge_c_d->get_block(0));
          const size_t count_c = Builder->build_set(tid, &c_sets);
          Builder->allocate_next(tid);
          Builder->foreach_builder([&](const uint32_t c_i, const uint32_t c_d) {
            Iterator_Edge_c_d->get_next_block(0, c_d);
            std::vector<const TrieBlock<hybrid, ParMemoryBuffer> *> d_sets;
            d_sets.push_back(Iterator_Edge_a_d->get_block(1));
            d_sets.push_back(Iterator_Edge_b_d->get_block(1));
            d_sets.push_back(Iterator_Edge_c_d->get_block(1));
            const size_t count_d = Builder->build_set(tid, &d_sets);
            num_rows_reducer.update(tid, count_d);
            Builder->set_level(c_i, c_d);
          });
          Builder->set_level(b_i, b_d);
        });
        Builder->set_level(a_i, a_d);
      });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_0_a_b_c_d TIME", bag_timer);
      Trie_SFlique_0_1_2_3->memoryBuffers = Builders.trie->memoryBuffers;
      Trie_SFlique_0_1_2_3->num_rows = Builders.trie->num_rows;
      Trie_SFlique_0_1_2_3->encodings = Builders.trie->encodings;
    }
    result_0 = (void *)Trie_SFlique_0_1_2_3;
    std::cout << "NUMBER OF ROWS: " << Trie_SFlique_0_1_2_3->num_rows
              << std::endl;
    timer::stop_clock("QUERY TIME", query_timer);
  }
  thread_pool::deleteThreadPool();
}
