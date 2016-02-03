
#include "LUBM.hpp"
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

  Trie<void *, ParMemoryBuffer> *Trie_lubm7_0_1 =
      new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                        "lubm10000/db_python/relations/lubm7/"
                                        "lubm7_0_1",
                                        2, false);
  Trie<void *, ParMemoryBuffer> *Trie_rdftype_1_0 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_rdftype_1_0 = Trie<void *, ParMemoryBuffer>::load(
        "/dfs/scratch0/caberger/datasets/lubm10000/db_python/relations/rdftype/"
        "rdftype_0_1");
    timer::stop_clock("LOADING Trie rdftype_1_0", start_time);
  }
  Trie<void *, ParMemoryBuffer> *Trie_teacherOf_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_teacherOf_0_1 = Trie<void *, ParMemoryBuffer>::load(
        "/dfs/scratch0/caberger/datasets/lubm10000/db_python/relations/"
        "teacherOf/teacherOf_1_0");
    timer::stop_clock("LOADING Trie teacherOf_0_1", start_time);
  }
  Trie<void *, ParMemoryBuffer> *Trie_takesCourse_1_0 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_takesCourse_1_0 = Trie<void *, ParMemoryBuffer>::load(
        "/dfs/scratch0/caberger/datasets/lubm10000/db_python/relations/"
        "takesCourse/takesCourse_1_0");
    timer::stop_clock("LOADING Trie takesCourse_1_0", start_time);
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
    Trie<void *, ParMemoryBuffer> *Trie_bag_1_a_b_0 =
        new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                          "lubm10000/db_python/relations/"
                                          "bag_1_a_b",
                                          1, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_1_a_b_0, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_teacherOf_a_b(
          Trie_teacherOf_0_1);
      const uint32_t selection_a_0 = Encoding_subject->value_to_key.at(
          "http://www.Department0.University0.edu/AssociateProfessor0");

      //////////////////////////////////////////////
      //copy me for bad attr selection
      auto iterator = Iterators_teacherOf_a_b;
      auto selection = selection_a_0;
      auto data_allocator = Builders.trie->memoryBuffers->head;
      size_t alloc_size = iterator.head->get_const_set()->cardinality * sizeof(uint32_t);
      uint8_t* place = (uint8_t*) (data_allocator->get_next(sizeof(TrieBlock<hybrid,ParMemoryBuffer>)+alloc_size+sizeof(Set<hybrid>)));
      Set<hybrid> *r = (Set<hybrid>*)(place+sizeof(TrieBlock<hybrid,ParMemoryBuffer>));  
      uint32_t* integer_data = (uint32_t*)(((uint8_t*)r) + sizeof(Set<hybrid>));
      std::atomic<size_t> array_index(0);
      iterator.head->get_const_set()->par_foreach_index([&](const size_t tid, const uint32_t index, const uint32_t data){
        TrieIterator<void *, ParMemoryBuffer> *it1 = iterator.iterators.at(tid);
        it1->get_next_block(0, data);
        const TrieBlock<hybrid,ParMemoryBuffer>*l2 = it1->levels.at(1);
        if(l2->get_const_set()->find(selection) != -1){
          integer_data[array_index.fetch_add(1)] = data;
          num_rows_reducer.update(tid,1);
        }
      });
      r->number_of_bytes = (array_index*sizeof(uint32_t));
      r->cardinality = array_index;
      r->type = type::UINTEGER;
      //////////////////////////////////////////////

      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_a_b TIME", bag_timer);
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
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_rdftype_d_b(
          Trie_rdftype_1_0);
      const uint32_t selection_d_0 = Encoding_types->value_to_key.at(
          "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");

      //////////////////////////////////////////////
      //copy me for bad attr selection
      auto iterator = Iterators_rdftype_d_b;
      auto selection = selection_d_0;
      auto data_allocator = Builders.trie->memoryBuffers->head;
      size_t alloc_size = iterator.head->get_const_set()->cardinality * sizeof(uint32_t);
      uint8_t* place = (uint8_t*) (data_allocator->get_next(sizeof(TrieBlock<hybrid,ParMemoryBuffer>)+alloc_size+sizeof(Set<hybrid>)));
      Set<hybrid> *r = (Set<hybrid>*)(place+sizeof(TrieBlock<hybrid,ParMemoryBuffer>));  
      uint32_t* integer_data = (uint32_t*)(((uint8_t*)r) + sizeof(Set<hybrid>));
      std::atomic<size_t> array_index(0);
      iterator.head->get_const_set()->par_foreach_index([&](const size_t tid, const uint32_t index, const uint32_t data){
        TrieIterator<void *, ParMemoryBuffer> *it1 = iterator.iterators.at(tid);
        it1->get_next_block(0, data);
        const TrieBlock<hybrid,ParMemoryBuffer>*l2 = it1->levels.at(1);
        if(l2->get_const_set()->find(selection) != -1){
          integer_data[array_index.fetch_add(1)] = data;
          num_rows_reducer.update(tid,1);
        }
      });
      r->number_of_bytes = (array_index*sizeof(uint32_t));
      r->cardinality = array_index;
      r->type = type::UINTEGER;
      //////////////////////////////////////////////

      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_d_b TIME", bag_timer);
    }
    Trie<void *, ParMemoryBuffer> *Trie_bag_1_e_c_0 =
        new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                          "lubm10000/db_python/relations/"
                                          "bag_1_e_c",
                                          1, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_1_e_c_0, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_rdftype_e_c(
          Trie_rdftype_1_0);
      const uint32_t selection_e_0 = Encoding_types->value_to_key.at(
          "http://www.lehigh.edu/~zhp2/2004/0401/"
          "univ-bench.owl#UndergraduateStudent");

      //////////////////////////////////////////////
      //copy me for bad attr selection
      auto iterator = Iterators_rdftype_e_c;
      auto selection = selection_e_0;
      auto data_allocator = Builders.trie->memoryBuffers->head;
      size_t alloc_size = iterator.head->get_const_set()->cardinality * sizeof(uint32_t);
      uint8_t* place = (uint8_t*) (data_allocator->get_next(sizeof(TrieBlock<hybrid,ParMemoryBuffer>)+alloc_size+sizeof(Set<hybrid>)));
      Set<hybrid> *r = (Set<hybrid>*)(place+sizeof(TrieBlock<hybrid,ParMemoryBuffer>));  
      uint32_t* integer_data = (uint32_t*)(((uint8_t*)r) + sizeof(Set<hybrid>));
      std::atomic<size_t> array_index(0);
      iterator.head->get_const_set()->par_foreach_index([&](const size_t tid, const uint32_t index, const uint32_t data){
        TrieIterator<void *, ParMemoryBuffer> *it1 = iterator.iterators.at(tid);
        it1->get_next_block(0, data);
        const TrieBlock<hybrid,ParMemoryBuffer>*l2 = it1->levels.at(1);
        if(l2->get_const_set()->find(selection) != -1){
          integer_data[array_index.fetch_add(1)] = data;
          num_rows_reducer.update(tid,1);
        }
      });
      r->number_of_bytes = (array_index*sizeof(uint32_t));
      r->cardinality = array_index;
      r->type = type::UINTEGER;
      //////////////////////////////////////////////


      Iterators_rdftype_e_c.get_next_block(selection_e_0);
      const size_t count_c = Builders.build_set(Iterators_rdftype_e_c.head);
      num_rows_reducer.update(0, count_c);
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_e_c TIME", bag_timer);
    }
    Trie<void *, ParMemoryBuffer> *Trie_bag_0_b_c_0_1 = Trie_lubm7_0_1;
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_0_b_c_0_1, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_takesCourse_b_c(
          Trie_takesCourse_1_0);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_a_b_b(
          Trie_bag_1_a_b_0);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_d_b_b(
          Trie_bag_1_d_b_0);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_e_c_c(
          Trie_bag_1_e_c_0);
      std::vector<const TrieBlock<hybrid, ParMemoryBuffer> *> b_sets;
      b_sets.push_back(Iterators_takesCourse_b_c.head);
      b_sets.push_back(Iterators_bag_1_a_b_b.head);
      b_sets.push_back(Iterators_bag_1_d_b_b.head);
      const size_t count_b = Builders.build_set(&b_sets);
      Builders.allocate_next();
      Builders.par_foreach_builder(
          [&](const size_t tid, const uint32_t b_i, const uint32_t b_d) {
            TrieBuilder<void *, ParMemoryBuffer> *Builder =
                Builders.builders.at(tid);
            TrieIterator<void *, ParMemoryBuffer> *Iterator_takesCourse_b_c =
                Iterators_takesCourse_b_c.iterators.at(tid);
            TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_a_b_b =
                Iterators_bag_1_a_b_b.iterators.at(tid);
            TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_d_b_b =
                Iterators_bag_1_d_b_b.iterators.at(tid);
            TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_e_c_c =
                Iterators_bag_1_e_c_c.iterators.at(tid);
            Iterator_takesCourse_b_c->get_next_block(0, b_d);
            const size_t count_c =
                Builder->build_set(tid, Iterator_takesCourse_b_c->get_block(1),
                                   Iterator_bag_1_e_c_c->get_block(0));
            num_rows_reducer.update(tid, count_c);
            Builder->set_level(b_i, b_d);
          });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_0_b_c TIME", bag_timer);
      Trie_lubm7_0_1->memoryBuffers = Builders.trie->memoryBuffers;
      Trie_lubm7_0_1->num_rows = Builders.trie->num_rows;
      Trie_lubm7_0_1->encodings = Builders.trie->encodings;
    }
    result_0 = (void *)Trie_lubm7_0_1;
    std::cout << "NUMBER OF ROWS: " << Trie_lubm7_0_1->num_rows << std::endl;
    timer::stop_clock("QUERY TIME", query_timer);
  }
  thread_pool::deleteThreadPool();
}
