
#include "Query14.hpp"
#include "utils/thread_pool.hpp"
#include "utils/parallel.hpp"
#include "Trie.hpp"
#include "TrieBuilder.hpp"
#include "TrieIterator.hpp"
#include "utils/timer.hpp"
#include "utils/ParMemoryBuffer.hpp"
#include "Encoding.hpp"

void Query14::run14() {
  thread_pool::initializeThreadPool();

  Trie<void *, ParMemoryBuffer> *Trie_lubm14_0 =
      new Trie<void *, ParMemoryBuffer>("/dfs/scratch0/caberger/datasets/"
                                        "lubm10000/db_python/relations/lubm14/"
                                        "lubm14_0",
                                        1, false);
  Trie<void *, ParMemoryBuffer> *Trie_rdftype_1_0 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_rdftype_1_0 = Trie<void *, ParMemoryBuffer>::load(
        "/dfs/scratch0/caberger/datasets/lubm10000/db_python/relations/rdftype/"
        "rdftype_0_1");
    timer::stop_clock("LOADING Trie rdftype_0_1", start_time);
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
    Trie<void *, ParMemoryBuffer> *Trie_bag_0_b_a_0 = Trie_lubm14_0;
    {
      num_rows_reducer.clear();
      ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_bag_0_b_a_0, 2);
      Builders.trie->encodings.push_back((void *)Encoding_subject);
      ParTrieIterator<void *, ParMemoryBuffer> Iterators_rdftype_b_a(
          Trie_rdftype_1_0);
      const uint32_t selection_b_0 = Encoding_types->value_to_key.at(
          "http://www.lehigh.edu/~zhp2/2004/0401/"
          "univ-bench.owl#UndergraduateStudent");


      Iterators_rdftype_b_a.head->get_const_set()->par_foreach_index([&](const size_t tid, const uint32_t index, const uint32_t data){
        TrieIterator<void *, ParMemoryBuffer> *it1 = Iterators_rdftype_b_a.iterators.at(tid);
        it1->get_next_block(0, data);
        const TrieBlock<hybrid,ParMemoryBuffer>*l2 = it1->levels.at(1);
        if(l2->get_const_set()->find(selection_b_0) != -1){
          num_rows_reducer.update(tid,1);
        }
      });
      std::cout << "NUM ROWS: " << num_rows_reducer.evaluate(0) << std::endl;
      /*
      Iterators_rdftype_b_a.get_next_block(selection_b_0);
      const size_t count_a = Builders.build_set(Iterators_rdftype_b_a.head);
      num_rows_reducer.update(0, count_a);
      Builders.trie->num_rows = num_rows_reducer.evaluate(0);
      std::cout << "NUM ROWS: " << Builders.trie->num_rows
                << " ANNOTATION: " << Builders.trie->annotation << std::endl;
      timer::stop_clock("BAG bag_0_b_a TIME", bag_timer);
      Trie_lubm14_0->memoryBuffers = Builders.trie->memoryBuffers;
      Trie_lubm14_0->num_rows = Builders.trie->num_rows;
      Trie_lubm14_0->encodings = Builders.trie->encodings;
      */
    }
    result14 = (void *)Trie_lubm14_0;
    std::cout << "NUMBER OF ROWS: " << Trie_lubm14_0->num_rows << std::endl;
    timer::stop_clock("QUERY TIME", query_timer);
  }
  thread_pool::deleteThreadPool();
}
