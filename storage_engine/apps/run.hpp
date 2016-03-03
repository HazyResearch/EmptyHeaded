
#include "Trie.hpp"
#include "utils/timer.hpp"
#include "intermediate/intermediate.hpp"
#include "utils/thread_pool.hpp"

typedef std::vector<void *> myvector;
typedef std::pair<size_t, myvector> mypair;

void build(std::unordered_map<std::string, mypair> *map) {
  thread_pool::initializeThreadPool();
  { // load encoded relation
    auto load_time = timer::start_clock();
    EncodedColumnStore *EncodedColumnStore_Edge =
        EncodedColumnStore::from_binary("/Users/caberger/Documents/Research/"
                                        "code/EmptyHeaded/examples/graph/data/"
                                        "db_simple/relations/Edge/");
    timer::stop_clock("LOADING ENCODED Edge", load_time);
    ////////////////////emitReorderEncodedColumnStore////////////////////
    {
      std::vector<size_t> order_0_1 = {0, 1};
      EncodedColumnStore *Encoded_Edge_0_1 =
          new EncodedColumnStore(EncodedColumnStore_Edge, order_0_1);
      Trie<void *, ParMemoryBuffer> *Trie_Edge_0_1 = NULL;
      {
        auto start_time = timer::start_clock();
        // buildTrie
        Trie_Edge_0_1 = new Trie<void *, ParMemoryBuffer>(
            "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/"
            "graph/data/db_simple/relations/Edge/Edge_0_1",
            &Encoded_Edge_0_1->max_set_size, &Encoded_Edge_0_1->data,
            &Encoded_Edge_0_1->annotation);
        timer::stop_clock("BUILDING TRIE Edge_0_1", start_time);
      }
      ////////////////////emitWriteBinaryTrie////////////////////
      {
        auto start_time = timer::start_clock();
        Trie_Edge_0_1->save();
        timer::stop_clock("WRITING BINARY TRIE Edge_0_1", start_time);
      }
      delete Trie_Edge_0_1;
    }
    ////////////////////emitReorderEncodedColumnStore////////////////////
    {
      std::vector<size_t> order_1_0 = {1, 0};
      EncodedColumnStore *Encoded_Edge_1_0 =
          new EncodedColumnStore(EncodedColumnStore_Edge, order_1_0);
      Trie<void *, ParMemoryBuffer> *Trie_Edge_1_0 = NULL;
      {
        auto start_time = timer::start_clock();
        // buildTrie
        Trie_Edge_1_0 = new Trie<void *, ParMemoryBuffer>(
            "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/"
            "graph/data/db_simple/relations/Edge/Edge_1_0",
            &Encoded_Edge_1_0->max_set_size, &Encoded_Edge_1_0->data,
            &Encoded_Edge_1_0->annotation);
        timer::stop_clock("BUILDING TRIE Edge_1_0", start_time);
      }
      ////////////////////emitWriteBinaryTrie////////////////////
      {
        auto start_time = timer::start_clock();
        Trie_Edge_1_0->save();
        timer::stop_clock("WRITING BINARY TRIE Edge_1_0", start_time);
      }
      delete Trie_Edge_1_0;
    }
  }
  thread_pool::deleteThreadPool();
}