#define GENERATED
#include "main.hpp"
extern "C" void *run() {
  ////////////////////emitLoadEncodedRelation////////////////////
  EncodedRelation<float> *Encoded_InverseDegree = NULL;
  {
    auto start_time = debug::start_clock();
    Encoded_InverseDegree = EncodedRelation<float>::from_binary(
        "/Users/caberger/Documents/Research/code/python_fun/databases/higgs/db/relations/"
        "InverseDegree/");
    debug::stop_clock("LOADING ENCODED RELATION InverseDegree", start_time);
  }

  ////////////////////emitReorderEncodedRelation////////////////////
  EncodedRelation<float> *Encoded_InverseDegree_0 =
      new EncodedRelation<float>(&Encoded_InverseDegree->annotation);
  {
    auto start_time = debug::start_clock();
    // encodeRelation
    Encoded_InverseDegree_0->add_column(
        Encoded_InverseDegree->column(0),
        Encoded_InverseDegree->max_set_size.at(0));
    debug::stop_clock("REORDERING ENCODING InverseDegree_0", start_time);
  }

  ////////////////////emitBuildTrie////////////////////
  const size_t alloc_size_InverseDegree_0 =
      8 * Encoded_InverseDegree_0->data.size() *
      Encoded_InverseDegree_0->data.at(0).size() * sizeof(uint64_t) *
      sizeof(TrieBlock<hybrid, float>);
  allocator::memory<uint8_t> *data_allocator_InverseDegree_0 =
      new allocator::memory<uint8_t>(alloc_size_InverseDegree_0);
  Trie<hybrid, float> *Trie_InverseDegree_0 = NULL;
  {
    auto start_time = debug::start_clock();
    // buildTrie
    Trie_InverseDegree_0 = Trie<hybrid, float>::build(
        data_allocator_InverseDegree_0, &Encoded_InverseDegree_0->max_set_size,
        &Encoded_InverseDegree_0->data, &Encoded_InverseDegree_0->annotation,
        [&](size_t index) {
          (void)index;
          return true;
        });
    debug::stop_clock("BUILDING TRIE InverseDegree_0", start_time);
  }

  ////////////////////emitWriteBinaryTrie////////////////////
  {
    auto start_time = debug::start_clock();
    Trie_InverseDegree_0->to_binary("/Users/caberger/Documents/Research/code/python_fun/databases/higgs/db/relations/InverseDegree/"
                                    "InverseDegree_0/");
    debug::stop_clock("WRITING BINARY TRIE InverseDegree_0", start_time);
  }

  data_allocator_InverseDegree_0->free();
  delete data_allocator_InverseDegree_0;
  delete Trie_InverseDegree_0;
  return NULL;
}
#ifndef GOOGLE_TEST
int main() {
  thread_pool::initializeThreadPool();
  run();
}
#endif
