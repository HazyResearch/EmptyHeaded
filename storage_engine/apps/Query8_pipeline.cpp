#include "Query8_ghd.hpp"
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

  Trie<void *, ParMemoryBuffer> *Trie_lubm8_0_1_2 =
      new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                        "lubm10000/db_python/relations/lubm8/"
                                        "lubm8_0_1_2",
                                        3, false);
  Trie<void *, ParMemoryBuffer> *Trie_emailAddress_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_emailAddress_0_1 = Trie<void *, ParMemoryBuffer>::load(
        "/dfs/scratch0/caberger/datasets/lubm10000/db_python/relations/"
        "emailAddress/emailAddress_0_1");
    timer::stop_clock("LOADING Trie emailAddress_0_1", start_time);
  }
  Trie<void *, ParMemoryBuffer> *Trie_memberOf_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_memberOf_0_1 = Trie<void *, ParMemoryBuffer>::load(
        "/dfs/scratch0/caberger/datasets/lubm10000/db_python/relations/"
        "memberOf/memberOf_0_1");
    timer::stop_clock("LOADING Trie memberOf_0_1", start_time);
  }
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

  auto e_loading_subject = timer::start_clock();
  Encoding<std::string> *Encoding_subject = Encoding<std::string>::from_binary(
      "/dfs/scratch0/caberger/datasets/lubm10000/db_python/encodings/subject/");
  (void)Encoding_subject;
  timer::stop_clock("LOADING ENCODINGS subject", e_loading_subject);

  auto e_loading_email = timer::start_clock();
  Encoding<std::string> *Encoding_email = Encoding<std::string>::from_binary(
      "/dfs/scratch0/caberger/datasets/lubm10000/db_python/encodings/email/");
  (void)Encoding_email;
  timer::stop_clock("LOADING ENCODINGS email", e_loading_email);

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
    Trie<void *, ParMemoryBuffer> *Trie_bag_1_d_a_0 =
        new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                          "lubm10000/db_python/relations/"
                                          "bag_1_d_a",
                                          1, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_1_d_a_0, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_rdftype_d_a(
          Trie_rdftype_1_0);
      const uint32_t selection_d_0 = Encoding_types->value_to_key.at(
          "http://www.lehigh.edu/~zhp2/2004/0401/"
          "univ-bench.owl#UndergraduateStudent");
      Iterators_rdftype_d_a.get_next_block(selection_d_0);
      const size_t count_a = Builders.build_set(Iterators_rdftype_d_a.head);
      num_rows_reducer.update(0, count_a);
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_d_a TIME", bag_timer);
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
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_subOrganizationOf_e_b(
          Trie_subOrganizationOf_1_0);
      const uint32_t selection_e_0 =
          Encoding_subject->value_to_key.at("http://www.University0.edu");
      Iterators_subOrganizationOf_e_b.get_next_block(selection_e_0);
      const size_t count_b =
          Builders.build_set(Iterators_subOrganizationOf_e_b.head);
      num_rows_reducer.update(0, count_b);
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_e_b TIME", bag_timer);
    }
    Trie<void *, ParMemoryBuffer> *Trie_bag_1_f_b_0 =
        new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                          "lubm10000/db_python/relations/"
                                          "bag_1_f_b",
                                          1, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_1_f_b_0, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_rdftype_f_b(
          Trie_rdftype_1_0);
      const uint32_t selection_f_0 = Encoding_types->value_to_key.at(
          "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");
      Iterators_rdftype_f_b.get_next_block(selection_f_0);
      const size_t count_b = Builders.build_set(Iterators_rdftype_f_b.head);
      num_rows_reducer.update(0, count_b);
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_f_b TIME", bag_timer);
    }
    Trie<void *, ParMemoryBuffer> *Trie_bag_0_a_b_0_1 =
        new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                          "lubm10000/db_python/relations/"
                                          "bag_0_a_b",
                                          3, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_0_a_b_0_1, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_memberOf_a_b(
          Trie_memberOf_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_d_a_a(
          Trie_bag_1_d_a_0);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_e_b_b(
          Trie_bag_1_e_b_0);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_f_b_b(
          Trie_bag_1_f_b_0);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_emailAddress_a_c(
          Trie_emailAddress_0_1);

      std::vector<const TrieBlock<hybrid, ParMemoryBuffer> *> a_sets;
      a_sets.push_back(Iterators_memberOf_a_b.head);
      a_sets.push_back(Iterators_bag_1_d_a_a.head);
      a_sets.push_back(Iterators_emailAddress_a_c.head);
      const size_t count_a = Builders.build_set(&a_sets);
      Builders.allocate_next();
      Builders.par_foreach_builder(
          [&](const size_t tid, const uint32_t a_i, const uint32_t a_d) {
            TrieBuilder<void *, ParMemoryBuffer> *Builder =
                Builders.builders.at(tid);
            TrieIterator<void *, ParMemoryBuffer> *Iterator_memberOf_a_b =
                Iterators_memberOf_a_b.iterators.at(tid);
            TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_d_a_a =
                Iterators_bag_1_d_a_a.iterators.at(tid);
            TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_e_b_b =
                Iterators_bag_1_e_b_b.iterators.at(tid);
            TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_f_b_b =
                Iterators_bag_1_f_b_b.iterators.at(tid);
            TrieIterator<void *, ParMemoryBuffer> *Iterator_emailAddress_a_c =
              Iterators_emailAddress_a_c.iterators.at(tid);
            Iterator_memberOf_a_b->get_next_block(0, a_d);
            Iterator_emailAddress_a_c->get_next_block(0, a_d);
            std::vector<const TrieBlock<hybrid, ParMemoryBuffer> *> b_sets;
            b_sets.push_back(Iterator_memberOf_a_b->get_block(1));
            b_sets.push_back(Iterator_bag_1_e_b_b->get_block(0));
            b_sets.push_back(Iterator_bag_1_f_b_b->get_block(0));
            const size_t count_b = Builder->build_set(tid, &b_sets);
            Builder->allocate_next(tid);
            Builder->foreach_builder([&](const uint32_t b_i, const uint32_t b_d){
              const size_t count_c =
                Builder->build_set(tid, Iterator_emailAddress_a_c->get_block(1));
              num_rows_reducer.update(tid, count_c);
              Builder->set_level(b_i, b_d);
            });
            Builder->set_level(a_i, a_d);
          });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      Trie_lubm8_0_1_2 = Builders.trie;
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_0_a_b TIME", bag_timer);
    }
    /*
    Trie<void *, ParMemoryBuffer> *Trie_bag_1_a_c_0_1 =
        new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                          "lubm10000/db_python/relations/"
                                          "bag_1_a_c",
                                          2, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_1_a_c_0_1, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      Builders.trie->encodings.push_back((void *)Encoding_email);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_emailAddress_a_c(
          Trie_emailAddress_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_0_a_b(
          Trie_bag_0_a_b_0_1);
      const size_t count_a = Builders.build_set(Iterators_emailAddress_a_c.head,Iterators_bag_0_a_b.head);
      Builders.allocate_next();
      Builders.par_foreach_builder([&](const size_t tid, const uint32_t a_i,
                                       const uint32_t a_d) {
        TrieBuilder<void *, ParMemoryBuffer> *Builder =
            Builders.builders.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_emailAddress_a_c =
            Iterators_emailAddress_a_c.iterators.at(tid);
        Iterator_emailAddress_a_c->get_next_block(0, a_d);
        const size_t count_c =
            Builder->build_set(tid, Iterator_emailAddress_a_c->get_block(1));
        num_rows_reducer.update(tid, count_c);
        Builder->set_level(a_i, a_d);
      });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_a_c TIME", bag_timer);
    }
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_0_a_b(
          Trie_bag_0_a_b_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_f_b(
          Trie_bag_1_f_b_0);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_a_c(
          Trie_bag_1_a_c_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_d_a(
          Trie_bag_1_d_a_0);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_e_b(
          Trie_bag_1_e_b_0);
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_lubm8_0_1_2, 3);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      Builders.trie->encodings.push_back((void *)Encoding_email);
      Builders.build_set(Iterators_bag_0_a_b.head);
      Builders.allocate_next();
      Builders.par_foreach_builder([&](const size_t tid, const uint32_t a_i,
                                       const uint32_t a_d) {
        TrieBuilder<void *, ParMemoryBuffer> *Builder =
            Builders.builders.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_0_a_b =
            Iterators_bag_0_a_b.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_f_b =
            Iterators_bag_1_f_b.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_a_c =
            Iterators_bag_1_a_c.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_d_a =
            Iterators_bag_1_d_a.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_e_b =
            Iterators_bag_1_e_b.iterators.at(tid);
        Iterator_bag_0_a_b->get_next_block(0, a_i, a_d);
        Iterator_bag_1_a_c->get_next_block(0, a_d);
        Builder->build_set(tid, Iterator_bag_0_a_b->get_block(1));
        Builder->allocate_next(tid);
        Builder->foreach_builder([&](const uint32_t b_i, const uint32_t b_d) {
          const size_t count_c =
              Builder->build_set(tid, Iterator_bag_1_a_c->get_block(1));
          num_rows_reducer.update(tid, count_c);
          Builder->set_level(b_i, b_d);
        });
        Builder->set_level(a_i, a_d);
      });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      timer::stop_clock("TOP DOWN TIME", bag_timer);
    }
    */
    result_0 = (void *)Trie_lubm8_0_1_2;
    std::cout << "NUMBER OF ROWS: " << Trie_lubm8_0_1_2->num_rows << std::endl;
    timer::stop_clock("QUERY TIME", query_timer);
  }
  thread_pool::deleteThreadPool();
}
