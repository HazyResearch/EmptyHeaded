
#include "utils/thread_pool.hpp"
#include "Trie.hpp"
#include "TrieBuilder.hpp"
#include "TrieIterator.hpp"
#include "utils/timer.hpp"
#include "Encoding.hpp"
#include "TransitiveClosure.hpp"

typedef std::unordered_map<std::string, void *> mymap;
typedef std::string string;

void run_0(std::string db_path,uint32_t start_node) {
  thread_pool::initializeThreadPool();
  Trie<void *, ParMemoryBuffer> *Trie_Edge_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_Edge_0_1 = Trie<void *, ParMemoryBuffer>::load(
        db_path+"/relations/Edge/Edge_0_1");
    timer::stop_clock("LOADING Trie Edge_0_1", start_time);
  }

  auto e_loading_uint32_t = timer::start_clock();
  Encoding<uint32_t> *Encoding_uint32_t = Encoding<uint32_t>::from_binary(
      db_path+"/encodings/uint32_t/");
  (void)Encoding_uint32_t;
  timer::stop_clock("LOADING ENCODINGS uint32_t", e_loading_uint32_t);
  par::reducer<size_t> num_rows_reducer(
      0, [](size_t a, size_t b) { return a + b; });
  //
  // query plan
  //
  auto query_timer = timer::start_clock();
  Trie<long, ParMemoryBuffer> *Trie_SSSP_0 = new Trie<long, ParMemoryBuffer>(
      db_path+"/relations/SSSP",
      1, true);
  tc::unweighted_single_source<hybrid, ParMemoryBuffer, long>(
      3,
      Encoding_uint32_t->value_to_key.at(start_node), // from encoding
      Encoding_uint32_t->num_distinct, // from encoding
      Trie_Edge_0_1, // input graph
      Trie_SSSP_0, // output vector
      1, [&](long a) { return 1 + a; });

  timer::stop_clock("QUERY TIME", query_timer);

  thread_pool::deleteThreadPool();
}
