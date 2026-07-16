
#include "utils/thread_pool.hpp"
#include "Trie.hpp"
#include "TrieBuilder.hpp"
#include "TrieIterator.hpp"
#include "utils/timer.hpp"
#include "Encoding.hpp"

typedef std::unordered_map<std::string, void *> mymap;
typedef std::string string;

void run_0(mymap *input_tries) {
  thread_pool::initializeThreadPool();
  Trie<void *, ParMemoryBuffer> *Trie_Edge_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_Edge_0_1 = Trie<void *, ParMemoryBuffer>::load(
        "/work/triangle/generated/g16/database/relations/Edge/Edge_0_1");
    timer::stop_clock("LOADING Trie Edge_0_1", start_time);
  }

  auto e_loading_uint64_t = timer::start_clock();
  Encoding<uint64_t> *Encoding_uint64_t = Encoding<uint64_t>::from_binary(
      "/work/triangle/generated/g16/database/encodings/uint64_t/");
  (void)Encoding_uint64_t;
  timer::stop_clock("LOADING ENCODINGS uint64_t", e_loading_uint64_t);
  par::reducer<size_t> num_rows_reducer(
      0, [](size_t a, size_t b) { return a + b; });
  //
  // query plan
  //
  auto query_timer = timer::start_clock();
  Trie<long, ParMemoryBuffer> *Trie_TriangleCount_ =
      new Trie<long, ParMemoryBuffer>(
          "/work/triangle/generated/g16/database/relations/TriangleCount", 0,
          true);
  long TriangleCount;
  {
    auto bag_timer = timer::start_clock();
    num_rows_reducer.clear();
    ParTrieBuilder<long, ParMemoryBuffer> Builders(Trie_TriangleCount_, 3);
    ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_i_k(Trie_Edge_0_1);
    ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_j_k(Trie_Edge_0_1);
    ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_i_j(Trie_Edge_0_1);
    const size_t count_i = Builders.build_aggregated_set(
        Iterators_Edge_i_k.head, Iterators_Edge_i_j.head);
    par::reducer<long> annotation_i(0, [&](long a, long b) { return a + b; });
    Builders.par_foreach_aggregate([&](const size_t tid, const uint32_t i_d) {
      TrieBuilder<long, ParMemoryBuffer> *Builder = Builders.builders.at(tid);
      TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_i_k =
          Iterators_Edge_i_k.iterators.at(tid);
      TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_j_k =
          Iterators_Edge_j_k.iterators.at(tid);
      TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_i_j =
          Iterators_Edge_i_j.iterators.at(tid);
      Iterator_Edge_i_k->get_next_block(0, i_d);
      Iterator_Edge_i_j->get_next_block(0, i_d);
      const long intermediate_i = (long)1;
      const size_t count_j = Builder->build_aggregated_set(
          Iterator_Edge_j_k->get_block(0), Iterator_Edge_i_j->get_block(1));
      long annotation_j = (long)0;
      Builder->foreach_aggregate([&](const uint32_t j_d) {
        Iterator_Edge_j_k->get_next_block(0, j_d);
        const long intermediate_j = (long)1 * 1 * intermediate_i;
        const size_t count_k = Builder->count_set(
            Iterator_Edge_i_k->get_block(1), Iterator_Edge_j_k->get_block(1));
        num_rows_reducer.update(tid, 1);
        long annotation_k = (long)0;
        const long intermediate_k =
            (long)1 * 1 * 1 * intermediate_j * (count_k * 1);
        annotation_k += intermediate_k;
        annotation_j += annotation_k;
      });
      annotation_i.update(tid, annotation_j);
    });
    Builders.trie->annotation = annotation_i.evaluate(0);
    TriangleCount = Builders.trie->annotation;
    Builders.trie->num_rows = num_rows_reducer.evaluate(0);
    timer::stop_clock("BAG TriangleCount TIME", bag_timer);
    std::cout << "NUM ROWS: " << Builders.trie->num_rows
              << " ANNOTATION: " << Builders.trie->annotation << std::endl;
    Trie_TriangleCount_->memoryBuffers = Builders.trie->memoryBuffers;
    Trie_TriangleCount_->num_rows = 0;
    Trie_TriangleCount_->encodings = Builders.trie->encodings;
  }
  input_tries->insert(std::make_pair("TriangleCount_", Trie_TriangleCount_));

  timer::stop_clock("QUERY TIME", query_timer);

  thread_pool::deleteThreadPool();
}
