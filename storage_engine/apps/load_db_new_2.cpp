#define GENERATED
#include "main.hpp"
extern "C" void *run() {
  ////////////////////emitLoadEncodedRelation////////////////////
  EncodedRelation<float> *Encoded_Vector = NULL;
  {
    auto start_time = debug::start_clock();
    Encoded_Vector = EncodedRelation<float>::from_binary(
        "/Users/caberger/Documents/Research/code/python_fun/databases/higgs/db/relations/Vector/");
    debug::stop_clock("LOADING ENCODED RELATION Vector", start_time);
  }

  ////////////////////emitReorderEncodedRelation////////////////////
  EncodedRelation<float> *Encoded_Vector_0 =
      new EncodedRelation<float>(&Encoded_Vector->annotation);
  {
    auto start_time = debug::start_clock();
    // encodeRelation
    Encoded_Vector_0->add_column(Encoded_Vector->column(0),
                                 Encoded_Vector->max_set_size.at(0));
    debug::stop_clock("REORDERING ENCODING Vector_0", start_time);
  }

  ////////////////////emitBuildTrie////////////////////
  const size_t alloc_size_Vector_0 =
      8 * Encoded_Vector_0->data.size() * Encoded_Vector_0->data.at(0).size() *
      sizeof(uint64_t) * sizeof(TrieBlock<hybrid, float>);
  allocator::memory<uint8_t> *data_allocator_Vector_0 =
      new allocator::memory<uint8_t>(alloc_size_Vector_0);
  Trie<hybrid, float> *Trie_Vector_0 = NULL;
  {
    auto start_time = debug::start_clock();
    // buildTrie
    Trie_Vector_0 = Trie<hybrid, float>::build(
        data_allocator_Vector_0, &Encoded_Vector_0->max_set_size,
        &Encoded_Vector_0->data, &Encoded_Vector_0->annotation,
        [&](size_t index) {
          (void)index;
          return true;
        });
    debug::stop_clock("BUILDING TRIE Vector_0", start_time);
  }

  ////////////////////emitWriteBinaryTrie////////////////////
  {
    auto start_time = debug::start_clock();
    Trie_Vector_0->to_binary("/Users/caberger/Documents/Research/code/python_fun/databases/higgs/db/"
                             "relations/Vector/Vector_0/");
    debug::stop_clock("WRITING BINARY TRIE Vector_0", start_time);
  }

  data_allocator_Vector_0->free();
  delete data_allocator_Vector_0;
  delete Trie_Vector_0;
  return NULL;
}
#ifndef GOOGLE_TEST
int main() {
  thread_pool::initializeThreadPool();
  run();
}
#endif
