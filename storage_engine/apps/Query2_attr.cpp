
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

  Trie<void *, ParMemoryBuffer> *Trie_lubm2_0_1_2 =
      new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                        "lubm10000/db_python/relations/lubm2/"
                                        "lubm2_0_1_2",
                                        3, false);
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
    timer::stop_clock("LOADING Trie rdftype_0_1", start_time);
  }
  Trie<void *, ParMemoryBuffer> *Trie_subOrganizationOf_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_subOrganizationOf_0_1 = Trie<void *, ParMemoryBuffer>::load(
        "/dfs/scratch0/caberger/datasets/lubm10000/db_python/relations/"
        "subOrganizationOf/subOrganizationOf_0_1");
    timer::stop_clock("LOADING Trie subOrganizationOf_0_1", start_time);
  }
  Trie<void *, ParMemoryBuffer> *Trie_undergraduateDegreeFrom_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_undergraduateDegreeFrom_0_1 = Trie<void *, ParMemoryBuffer>::load(
        "/dfs/scratch0/caberger/datasets/lubm10000/db_python/relations/"
        "undergraduateDegreeFrom/undergraduateDegreeFrom_0_1");
    timer::stop_clock("LOADING Trie undergraduateDegreeFrom_0_1", start_time);
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
    Trie<void *, ParMemoryBuffer> *Trie_bag_1_z_c_0 =
        new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                          "lubm10000/db_python/relations/"
                                          "bag_1_z_c",
                                          1, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_1_z_c_0, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_rdftype_z_c(
          Trie_rdftype_1_0);
      const uint32_t selection_z_0 = Encoding_types->value_to_key.at(
          "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University");

      //////////////////////////////////////////////
      //copy me for bad attr selection
      auto iterator = Iterators_rdftype_z_c;
      auto selection = selection_z_0;
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

      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_1_z_c TIME", bag_timer);
    }
    Trie<void *, ParMemoryBuffer> *Trie_bag_1_x_a_0 =
        new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                          "lubm10000/db_python/relations/"
                                          "bag_1_x_a",
                                          1, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_1_x_a_0, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_rdftype_x_a(
          Trie_rdftype_1_0);
      const uint32_t selection_x_0 = Encoding_types->value_to_key.at(
          "http://www.lehigh.edu/~zhp2/2004/0401/"
          "univ-bench.owl#GraduateStudent");

      //////////////////////////////////////////////
      //copy me for bad attr selection
      auto iterator = Iterators_rdftype_x_a;
      auto selection = selection_x_0;
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
      timer::stop_clock("BAG bag_1_x_a TIME", bag_timer);
    }
    Trie<void *, ParMemoryBuffer> *Trie_bag_1_y_b_0 =
        new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                          "lubm10000/db_python/relations/"
                                          "bag_1_y_b",
                                          1, false);
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_1_y_b_0, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_rdftype_y_b(
          Trie_rdftype_1_0);
      const uint32_t selection_y_0 = Encoding_types->value_to_key.at(
          "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");

      //////////////////////////////////////////////
      //copy me for bad attr selection
      auto iterator = Iterators_rdftype_y_b;
      auto selection = selection_y_0;
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
      timer::stop_clock("BAG bag_1_y_b TIME", bag_timer);
    }
    Trie<void *, ParMemoryBuffer> *Trie_bag_0_a_b_c_0_1_2 = Trie_lubm2_0_1_2;
    {
      auto bag_timer = timer::start_clock();
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_0_a_b_c_0_1_2,
                                                       3);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_memberOf_a_b(
          Trie_memberOf_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_subOrganizationOf_b_c(
          Trie_subOrganizationOf_0_1);
      ParTrieIterator<void *, ParMemoryBuffer>
          Iterators_undergraduateDegreeFrom_a_c(
              Trie_undergraduateDegreeFrom_0_1);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_z_c_c(
          Trie_bag_1_z_c_0);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_x_a_a(
          Trie_bag_1_x_a_0);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_bag_1_y_b_b(
          Trie_bag_1_y_b_0);
      std::vector<const TrieBlock<hybrid, ParMemoryBuffer> *> a_sets;
      a_sets.push_back(Iterators_memberOf_a_b.head);
      a_sets.push_back(Iterators_undergraduateDegreeFrom_a_c.head);
      a_sets.push_back(Iterators_bag_1_x_a_a.head);
      const size_t count_a = Builders.build_set(&a_sets);
      Builders.allocate_next();
      Builders.par_foreach_builder([&](const size_t tid, const uint32_t a_i,
                                       const uint32_t a_d) {
        TrieBuilder<void *, ParMemoryBuffer> *Builder =
            Builders.builders.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_memberOf_a_b =
            Iterators_memberOf_a_b.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_subOrganizationOf_b_c =
            Iterators_subOrganizationOf_b_c.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *
            Iterator_undergraduateDegreeFrom_a_c =
                Iterators_undergraduateDegreeFrom_a_c.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_z_c_c =
            Iterators_bag_1_z_c_c.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_x_a_a =
            Iterators_bag_1_x_a_a.iterators.at(tid);
        TrieIterator<void *, ParMemoryBuffer> *Iterator_bag_1_y_b_b =
            Iterators_bag_1_y_b_b.iterators.at(tid);
        Iterator_memberOf_a_b->get_next_block(0, a_d);
        Iterator_undergraduateDegreeFrom_a_c->get_next_block(0, a_d);
        std::vector<const TrieBlock<hybrid, ParMemoryBuffer> *> b_sets;
        b_sets.push_back(Iterator_memberOf_a_b->get_block(1));
        b_sets.push_back(Iterator_subOrganizationOf_b_c->get_block(0));
        b_sets.push_back(Iterator_bag_1_y_b_b->get_block(0));
        const size_t count_b = Builder->build_set(tid, &b_sets);
        Builder->allocate_next(tid);
        Builder->foreach_builder([&](const uint32_t b_i, const uint32_t b_d) {
          Iterator_subOrganizationOf_b_c->get_next_block(0, b_d);
          std::vector<const TrieBlock<hybrid, ParMemoryBuffer> *> c_sets;
          c_sets.push_back(Iterator_subOrganizationOf_b_c->get_block(1));
          c_sets.push_back(Iterator_undergraduateDegreeFrom_a_c->get_block(1));
          c_sets.push_back(Iterator_bag_1_z_c_c->get_block(0));
          const size_t count_c = Builder->build_set(tid, &c_sets);
          num_rows_reducer.update(tid, count_c);
          Builder->set_level(b_i, b_d);
        });
        Builder->set_level(a_i, a_d);
      });
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_0_a_b_c TIME", bag_timer);
      Trie_lubm2_0_1_2->memoryBuffers = Builders.trie->memoryBuffers;
      Trie_lubm2_0_1_2->num_rows = Builders.trie->num_rows;
      Trie_lubm2_0_1_2->encodings = Builders.trie->encodings;
    }
    result_0 = (void *)Trie_lubm2_0_1_2;
    std::cout << "NUMBER OF ROWS: " << Trie_lubm2_0_1_2->num_rows << std::endl;
    timer::stop_clock("QUERY TIME", query_timer);
  }
  thread_pool::deleteThreadPool();
}
