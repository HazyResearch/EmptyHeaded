
#include "utils/thread_pool.hpp"
#include "utils/parallel.hpp"
#include "Trie.hpp"
#include "TrieBuilder.hpp"
#include "TrieIterator.hpp"
#include "utils/timer.hpp"
#include "utils/ParMemoryBuffer.hpp"
#include "Encoding.hpp"

void run() {
  thread_pool::initializeThreadPool();
  Trie<void *, RAM> *Trie_Edge_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_Edge_0_1 = Trie<void *, RAM>::load("db/relations/Edge/Edge_0_1");
    timer::stop_clock("LOADING Trie Edge_0_1", start_time);
  }

  auto e_loading_uint32_t = timer::start_clock();
  Encoding<uint32_t> *Encoding_uint32_t =
      Encoding<uint32_t>::from_binary("db/encodings/uint32_t/");
  (void)Encoding_uint32_t;
  timer::stop_clock("LOADING ENCODINGS uint32_t", e_loading_uint32_t);

  thread_pool::deleteThreadPool();
}
