
#include "Query12_ghd.hpp"
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

  Trie<void *, ParMemoryBuffer> *Trie_lubm12_0_1 =
      new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                        "lubm10000/db_python/relations/lubm12/"
                                        "lubm12_0_1",
                                        2, false);
  Trie<void *, ParMemoryBuffer> *Trie_rdftype_1_0 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_rdftype_1_0 = Trie<void *, ParMemoryBuffer>::load(
        "/dfs/scratch0/caberger/datasets/lubm10000/db_python/relations/rdftype/"
        "rdftype_1_0");
    timer::stop_clock("LOADING Trie rdftype_1_0", start_time);
  }
  Trie<void *, ParMemoryBuffer> *Trie_subOrganizationOf_1_0 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_subOrganizationOf_1_0 = Trie<void *, ParMemoryBuffer>::load(
        "/dfs/scratch0/caberger/datasets/lubm10000/db_python/relations/"
        "subOrganizationOf/subOrganizationOf_1_0");
    timer::stop_clock("LOADING Trie subOrganizationOf_1_0", start_time);
  }
  Trie<void *, ParMemoryBuffer> *Trie_worksFor_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_worksFor_0_1 = Trie<void *, ParMemoryBuffer>::load(
        "/dfs/scratch0/caberger/datasets/lubm10000/db_python/relations/"
        "worksFor/worksFor_0_1");
    timer::stop_clock("LOADING Trie worksFor_0_1", start_time);
  }

  auto e_loading_subject = timer::start_clock();
  Encoding<std::string> *Encoding_subject = Encoding<std::string>::from_binary(
      "/dfs/scratch0/caberger/datasets/lubm10000/db_python/encodings/subject/");
  (void)Encoding_subject;
  timer::stop_clock("LOADING ENCODINGS subject", e_loading_subject);

  auto e_loading_types = timer::start_clock();
  Encoding<std::string> *Encoding_types = Encoding<std::string>::from_binary(
      "/dfs/scratch0/caberger/datasets/lubm10000/db_python/encodings/types/");
  (void)Encoding_types;
  timer::stop_clock("LOADING ENCODINGS types", e_loading_types);
  par::reducer<size_t> num_rows_reducer(
      0, [](size_t a, size_t b) { return a + b; });
  //
  // query plan
  //
  {
    auto query_timer = timer::start_clock();
    Trie<void *, ParMemoryBuffer> *Trie_bag_1_c_a_0 =
        new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                          "lubm10000/db_python/relations/"
                                          "bag_1_c_a",
                                          1, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_1_c_a_0, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_rdftype_c_a(
          Trie_rdftype_1_0);
      const uint32_t selection_c_0 = Encoding_types->value_to_key.at(
          "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor");
      Iterators_rdftype_c_a.get_next_block(selection_c_0);
      const size_t count_a = Builders.build_set(Iterators_rdftype_c_a.head);
      num_rows_reducer.update(0, count_a);
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_c_a TIME", bag_timer);
    }
    Trie<void *, ParMemoryBuffer> *Trie_bag_1_d_b_0 =
        new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                          "lubm10000/db_python/relations/"
                                          "bag_1_d_b",
                                          1, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_1_d_b_0, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_subOrganizationOf_d_b(
          Trie_subOrganizationOf_1_0);
      const uint32_t selection_d_0 =
          Encoding_subject->value_to_key.at("http://www.University0.edu");
      Iterators_subOrganizationOf_d_b.get_next_block(selection_d_0);
      const size_t count_b =
          Builders.build_set(Iterators_subOrganizationOf_d_b.head);
      num_rows_reducer.update(0, count_b);
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_d_b TIME", bag_timer);
    }
    Trie<void *, ParMemoryBuffer> *Trie_bag_1_e_b_0 =
        new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                          "lubm10000/db_python/relations/"
                                          "bag_1_e_b",
                                          1, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_1_e_b_0, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_rdftype_e_b(
          Trie_rdftype_1_0);
      const uint32_t selection_e_0 = Encoding_types->value_to_key.at(
          "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");
      Iterators_rdftype_e_b.get_next_block(selection_e_0);
      const size_t count_b = Builders.build_set(Iterators_rdftype_e_b.head);
      num_rows_reducer.update(0, count_b);
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_e_b TIME", bag_timer);
    }
    Trie<void *, ParMemoryBuffer> *Trie_bag_0_a_b_0_1 = Trie_lubm12_0_1;
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_0_a_b_0_1, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_worksFor_a_b(
          Trie_worksFor_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_c_a_a(
          Trie_bag_1_c_a_0);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_d_b_b(
          Trie_bag_1_d_b_0);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_e_b_b(
          Trie_bag_1_e_b_0);
      const size_t count_a = Builders.build_set(
                                                Iterators_bag_1_c_a_a.head);
      Builders.allocate_next();
      Builders.par_foreach_builder(
          [&](const size_t tid, const uint32_t a_i, const uint32_t a_d) {
            TrieBuilder<void *, ParMemoryBuffer> *Builder =
                Builders.builders.at(tid);
            TrieIterator<void *, ParMemoryBuffer> *Iterator_worksFor_a_b =
                Iterators_worksFor_a_b.iterators.at(tid);
            TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_c_a_a =
                Iterators_bag_1_c_a_a.iterators.at(tid);
            TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_d_b_b =
                Iterators_bag_1_d_b_b.iterators.at(tid);
            TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_e_b_b =
                Iterators_bag_1_e_b_b.iterators.at(tid);
            Iterator_worksFor_a_b->get_next_block(0, a_d);
            std::vector<const TrieBlock<hybrid, ParMemoryBuffer> *> b_sets;
            b_sets.push_back(Iterator_worksFor_a_b->get_block(1));
            b_sets.push_back(Iterator_bag_1_d_b_b->get_block(0));
            b_sets.push_back(Iterator_bag_1_e_b_b->get_block(0));
            const size_t count_b = Builder->build_set(tid, &b_sets);
            num_rows_reducer.update(tid, count_b);
            Builder->set_level(a_i, a_d);
          });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_0_a_b TIME", bag_timer);
      Trie_lubm12_0_1->memoryBuffers = Builders.trie->memoryBuffers;
      Trie_lubm12_0_1->num_rows = Builders.trie->num_rows;
      Trie_lubm12_0_1->encodings = Builders.trie->encodings;
    }
    result_0 = (void *)Trie_lubm12_0_1;
    std::cout << "NUMBER OF ROWS: " << Trie_lubm12_0_1->num_rows << std::endl;
    timer::stop_clock("QUERY TIME", query_timer);
  }
  thread_pool::deleteThreadPool();
}
