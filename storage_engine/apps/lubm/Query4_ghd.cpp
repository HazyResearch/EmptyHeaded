
#include "Query4_ghd.hpp"
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

  Trie<void *, ParMemoryBuffer> *Trie_lubm4_0_1_2_3 =
      new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                        "lubm10000/db_python/relations/lubm4/"
                                        "lubm4_0_1_2_3",
                                        4, false);
  Trie<void *, ParMemoryBuffer> *Trie_emailAddress_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_emailAddress_0_1 = Trie<void *, ParMemoryBuffer>::load(
        "/dfs/scratch0/caberger/datasets/lubm10000/db_python/relations/"
        "emailAddress/emailAddress_0_1");
    timer::stop_clock("LOADING Trie emailAddress_0_1", start_time);
  }
  Trie<void *, ParMemoryBuffer> *Trie_rdftype_1_0 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_rdftype_1_0 = Trie<void *, ParMemoryBuffer>::load(
        "/dfs/scratch0/caberger/datasets/lubm10000/db_python/relations/rdftype/"
        "rdftype_1_0");
    timer::stop_clock("LOADING Trie rdftype_1_0", start_time);
  }
  Trie<void *, ParMemoryBuffer> *Trie_telephone_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_telephone_0_1 = Trie<void *, ParMemoryBuffer>::load(
        "/dfs/scratch0/caberger/datasets/lubm10000/db_python/relations/"
        "telephone/telephone_0_1");
    timer::stop_clock("LOADING Trie telephone_0_1", start_time);
  }
  Trie<void *, ParMemoryBuffer> *Trie_worksFor_1_0 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_worksFor_1_0 = Trie<void *, ParMemoryBuffer>::load(
        "/dfs/scratch0/caberger/datasets/lubm10000/db_python/relations/"
        "worksFor/worksFor_1_0");
    timer::stop_clock("LOADING Trie worksFor_1_0", start_time);
  }
  Trie<void *, ParMemoryBuffer> *Trie_name_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_name_0_1 = Trie<void *, ParMemoryBuffer>::load(
        "/dfs/scratch0/caberger/datasets/lubm10000/db_python/relations/name/"
        "name_0_1");
    timer::stop_clock("LOADING Trie name_0_1", start_time);
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

  auto e_loading_telephone = timer::start_clock();
  Encoding<std::string> *Encoding_telephone =
      Encoding<std::string>::from_binary("/dfs/scratch0/caberger/datasets/"
                                         "lubm10000/db_python/encodings/"
                                         "telephone/");
  (void)Encoding_telephone;
  timer::stop_clock("LOADING ENCODINGS telephone", e_loading_telephone);

  auto e_loading_name = timer::start_clock();
  Encoding<std::string> *Encoding_name = Encoding<std::string>::from_binary(
      "/dfs/scratch0/caberger/datasets/lubm10000/db_python/encodings/name/");
  (void)Encoding_name;
  timer::stop_clock("LOADING ENCODINGS name", e_loading_name);
  par::reducer<size_t> num_rows_reducer(
      0, [](size_t a, size_t b) { return a + b; });
  //
  // query plan
  //
  {
    auto query_timer = timer::start_clock();
    Trie<void *, ParMemoryBuffer> *Trie_bag_1_f_a_0 =
        new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                          "lubm10000/db_python/relations/"
                                          "bag_1_f_a",
                                          1, false);
    Trie<void *, ParMemoryBuffer> *Trie_bag_1_e_a_0 =
        new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                          "lubm10000/db_python/relations/"
                                          "bag_1_e_a",
                                          1, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_1_e_a_0, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_worksFor_e_a(
          Trie_worksFor_1_0);
      const uint32_t selection_e_0 = Encoding_subject->value_to_key.at(
          "http://www.Department0.University0.edu");
      Iterators_worksFor_e_a.get_next_block(selection_e_0);
      const size_t count_a = Builders.build_set(Iterators_worksFor_e_a.head);
      num_rows_reducer.update(0, count_a);
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_e_a TIME", bag_timer);
    }
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_1_f_a_0, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_rdftype_f_a(
          Trie_rdftype_1_0);
      const uint32_t selection_f_0 = Encoding_types->value_to_key.at(
          "http://www.lehigh.edu/~zhp2/2004/0401/"
          "univ-bench.owl#AssociateProfessor");
      Iterators_rdftype_f_a.get_next_block(selection_f_0);
      const size_t count_a = Builders.build_set(Iterators_rdftype_f_a.head);
      num_rows_reducer.update(0, count_a);
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_f_a TIME", bag_timer);
    }
    Trie<void *, ParMemoryBuffer> *Trie_bag_1_a_d_0_1 =
        new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                          "lubm10000/db_python/relations/"
                                          "bag_1_a_d",
                                          2, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_1_a_d_0_1, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      Builders.trie->encodings.push_back((void *)Encoding_email);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_emailAddress_a_d(
          Trie_emailAddress_0_1);

      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_f_a_a(
          Trie_bag_1_f_a_0);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_e_a_a(
          Trie_bag_1_e_a_0);

      std::vector<const TrieBlock<hybrid,ParMemoryBuffer>*> a_sets;
      a_sets.push_back((const TrieBlock<hybrid,ParMemoryBuffer>*)Iterators_emailAddress_a_d.head);
      a_sets.push_back((const TrieBlock<hybrid,ParMemoryBuffer>*)Iterators_bag_1_f_a_a.head);
      a_sets.push_back((const TrieBlock<hybrid,ParMemoryBuffer>*)Iterators_bag_1_e_a_a.head);

      const size_t count_a =
          Builders.build_set(&a_sets);
      Builders.allocate_next();
      Builders.par_foreach_builder([&](const size_t tid, const uint32_t a_i,
                                       const uint32_t a_d) {
        TrieBuilder<void *, ParMemoryBuffer> *Builder =
            Builders.builders.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_emailAddress_a_d =
            Iterators_emailAddress_a_d.iterators.at(tid);
        Iterator_emailAddress_a_d->get_next_block(0, a_d);
        const size_t count_d =
            Builder->build_set(tid, Iterator_emailAddress_a_d->get_block(1));
        num_rows_reducer.update(tid, count_d);
        Builder->set_level(a_i, a_d);
      });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_a_d TIME", bag_timer);
    }
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
      Builders.trie->encodings.push_back((void *)Encoding_telephone);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_telephone_a_c(
          Trie_telephone_0_1);

      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_a_d_a_d(
          Trie_bag_1_a_d_0_1);

      const size_t count_a = Builders.build_set(Iterators_telephone_a_c.head,Iterators_bag_1_a_d_a_d.head);
      Builders.allocate_next();
      Builders.par_foreach_builder(
          [&](const size_t tid, const uint32_t a_i, const uint32_t a_d) {
            TrieBuilder<void *, ParMemoryBuffer> *Builder =
                Builders.builders.at(tid);
            TrieIterator<void *, ParMemoryBuffer> *Iterator_telephone_a_c =
                Iterators_telephone_a_c.iterators.at(tid);
            Iterator_telephone_a_c->get_next_block(0, a_d);
            const size_t count_c =
                Builder->build_set(tid, Iterator_telephone_a_c->get_block(1));
            num_rows_reducer.update(tid, count_c);
            Builder->set_level(a_i, a_d);
          });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_a_c TIME", bag_timer);
    }
    Trie<void *, ParMemoryBuffer> *Trie_bag_0_a_b_0_1 =
        new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                          "lubm10000/db_python/relations/"
                                          "bag_0_a_b",
                                          2, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_0_a_b_0_1, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      Builders.trie->encodings.push_back((void *)Encoding_name);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_name_a_b(
          Trie_name_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_a_c_a_c(
          Trie_bag_1_a_c_0_1);
      const size_t count_a = Builders.build_set(Iterators_name_a_b.head,Iterators_bag_1_a_c_a_c.head);
      Builders.allocate_next();
      Builders.par_foreach_builder(
          [&](const size_t tid, const uint32_t a_i, const uint32_t a_d) {
            TrieBuilder<void *, ParMemoryBuffer> *Builder =
                Builders.builders.at(tid);
            TrieIterator<void *, ParMemoryBuffer> *Iterator_name_a_b =
                Iterators_name_a_b.iterators.at(tid);
            TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_a_c_a_c =
                Iterators_bag_1_a_c_a_c.iterators.at(tid);
            Iterator_name_a_b->get_next_block(0, a_d);
            Iterator_bag_1_a_c_a_c->get_next_block(0, a_d);
            const size_t count_b =
                Builder->build_set(tid, Iterator_name_a_b->get_block(1));
            num_rows_reducer.update(tid, count_b);
            Builder->set_level(a_i, a_d);
          });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_0_a_b TIME", bag_timer);
    }
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_0_a_b(
          Trie_bag_0_a_b_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_a_d(
          Trie_bag_1_a_d_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_a_c(
          Trie_bag_1_a_c_0_1);
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_lubm4_0_1_2_3, 4);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      Builders.trie->encodings.push_back((void *)Encoding_name);
      Builders.trie->encodings.push_back((void *)Encoding_email);
      Builders.trie->encodings.push_back((void *)Encoding_telephone);
      Builders.build_set(Iterators_bag_0_a_b.head);
      Builders.allocate_next();
      Builders.par_foreach_builder([&](const size_t tid, const uint32_t a_i,
                                       const uint32_t a_d) {
        TrieBuilder<void *, ParMemoryBuffer> *Builder =
            Builders.builders.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_0_a_b =
            Iterators_bag_0_a_b.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_a_d =
            Iterators_bag_1_a_d.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_a_c =
            Iterators_bag_1_a_c.iterators.at(tid);
        Iterator_bag_0_a_b->get_next_block(0, a_i, a_d);
        Iterator_bag_1_a_d->get_next_block(0, a_d);
        Iterator_bag_1_a_c->get_next_block(0, a_d);
        Builder->build_set(tid, Iterator_bag_0_a_b->get_block(1));
        Builder->allocate_next(tid);
        Builder->foreach_builder([&](const uint32_t b_i, const uint32_t b_d) {
          Builder->build_set(tid, Iterator_bag_1_a_d->get_block(1));
          Builder->allocate_next(tid);
          Builder->foreach_builder([&](const uint32_t d_i, const uint32_t d_d) {
            const size_t count_c =
                Builder->build_set(tid, Iterator_bag_1_a_c->get_block(1));
            num_rows_reducer.update(tid, count_c);
            Builder->set_level(d_i, d_d);
          });
          Builder->set_level(b_i, b_d);
        });
        Builder->set_level(a_i, a_d);
      });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      timer::stop_clock("TOP DOWN TIME", bag_timer);
    }
    result_0 = (void *)Trie_lubm4_0_1_2_3;
    std::cout << "NUMBER OF ROWS: " << Trie_lubm4_0_1_2_3->num_rows
              << std::endl;
    timer::stop_clock("QUERY TIME", query_timer);
  }
  thread_pool::deleteThreadPool();
}
