
#include <iostream>
#include <vector>
#include <unordered_map>
#include "utils/thread_pool.hpp"
#include "intermediate/intermediate.hpp"
#include "Encoding.hpp"
#include "Trie.hpp"
#include "utils/timer.hpp"
#include "utils/io.hpp"

typedef std::vector<void *> myvector;
typedef std::pair<size_t, myvector> mypair;
typedef std::string string;

void loadAndEncode(std::string tsv_path,
  std::string dbpath) {
  thread_pool::initializeThreadPool();

  SortableEncodingMap<uint32_t> EncodingMap_uint32_t;
  Encoding<uint32_t> Encoding_uint32_t;

  std::cout << tsv_path << std::endl;
  std::cout << dbpath << std::endl;

  std::vector<void *> ColumnStore_Edge;
  size_t NumRows_Edge = 0;
  {
    auto start_time = timer::start_clock();
    tsv_reader f_reader(tsv_path);
    char *next = f_reader.tsv_get_first();
    std::vector<uint32_t> *v_0 = new std::vector<uint32_t>();

    std::vector<uint32_t> *v_1 = new std::vector<uint32_t>();

    while (next != NULL) {

      const uint32_t value_0 = utils::from_string<uint32_t>(next);
      v_0->push_back(value_0);
      EncodingMap_uint32_t.update(value_0);
      next = f_reader.tsv_get_next();
      const uint32_t value_1 = utils::from_string<uint32_t>(next);
      v_1->push_back(value_1);
      EncodingMap_uint32_t.update(value_1);
      next = f_reader.tsv_get_next();
      NumRows_Edge++;
    }
    ColumnStore_Edge.push_back((void *)v_0->data());
    ColumnStore_Edge.push_back((void *)v_1->data());
    timer::stop_clock("READING Edge", start_time);
  }
  {
    auto start_time = timer::start_clock();
    Encoding_uint32_t.build(EncodingMap_uint32_t.get_sorted());
    timer::stop_clock("BUILDING ENCODING uint32_t", start_time);
  }
  {
    auto start_time = timer::start_clock();
    Encoding_uint32_t.to_binary(dbpath+"/encodings/uint32_t/");
    timer::stop_clock("WRITING ENCODING uint32_t", start_time);
  }
  {
    auto start_time = timer::start_clock();
    EncodedColumnStore EncodedColumnStore_Edge(NumRows_Edge, 2, 0);
    EncodedColumnStore_Edge.add_column(
        Encoding_uint32_t.encode_column((uint32_t *)ColumnStore_Edge.at(0),
                                        NumRows_Edge),
        Encoding_uint32_t.num_distinct);
    EncodedColumnStore_Edge.add_column(
        Encoding_uint32_t.encode_column((uint32_t *)ColumnStore_Edge.at(1),
                                        NumRows_Edge),
        Encoding_uint32_t.num_distinct);
    EncodedColumnStore_Edge.to_binary(dbpath+"/relations/Edge/");
    timer::stop_clock("ENCODING Edge", start_time);
  }
  { // load encoded relation
    auto load_time = timer::start_clock();
    EncodedColumnStore *EncodedColumnStore_Edge =
        EncodedColumnStore::from_binary(dbpath+"/relations/Edge/");
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
            dbpath+"/relations/Edge/Edge_0_1",
            &Encoded_Edge_0_1->max_set_size, &Encoded_Edge_0_1->data,
            (std::vector<void *> *)&Encoded_Edge_0_1->annotation);
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
            dbpath+"/relations/Edge/Edge_1_0",
            &Encoded_Edge_1_0->max_set_size, &Encoded_Edge_1_0->data,
            (std::vector<void *> *)&Encoded_Edge_1_0->annotation);
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