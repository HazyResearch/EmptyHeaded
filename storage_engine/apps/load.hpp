#include "intermediate/intermediate.hpp"
#include "Encoding.hpp"
#include "Trie.hpp"

Trie<float, ParMemoryBuffer>* load_vector(std::string file){
  SortableEncodingMap<uint32_t> EncodingMap_uint32_t;
  Encoding<uint32_t>* Encoding_uint32_t = new Encoding<uint32_t>();

  std::vector<void *> ColumnStore_graph;
  size_t NumRows_graph = 0;
  {
    auto start_time = timer::start_clock();
    tsv_reader f_reader(file);
    char *next = f_reader.tsv_get_first();
    std::vector<uint32_t> *v_0 = new std::vector<uint32_t>();
    std::vector<float> *a_0 = new std::vector<float>();

    while (next != NULL) {
      const uint32_t value_0 = utils::from_string<uint32_t>(next);
      v_0->push_back(value_0);
      EncodingMap_uint32_t.update(value_0);
      next = f_reader.tsv_get_next();
      const float anno_0 = utils::from_string<float>(next);
      a_0->push_back(anno_0);
      next = f_reader.tsv_get_next();
      NumRows_graph++;
    }
    ColumnStore_graph.push_back((void *)v_0->data());
    ColumnStore_graph.push_back((void *)a_0->data());
    timer::stop_clock("READING VECTOR", start_time);
  }
  {
    auto start_time = timer::start_clock();
    Encoding_uint32_t->build(EncodingMap_uint32_t.get_sorted());
    timer::stop_clock("BUILDING ENCODING uint32_t", start_time);
  }
  /*
  {
    auto start_time = timer::start_clock();
    Encoding_uint32_t->to_binary("/Users/caberger/Documents/Research/code/"
                                "EmptyHeaded/test/graph/databases/db_simple/"
                                "encodings/uint32_t/");
    timer::stop_clock("WRITING ENCODING uint32_t", start_time);
  }
  */
  EncodedColumnStore EncodedColumnStore_graph(NumRows_graph, 1, 1);
  {
    auto start_time = timer::start_clock();
    EncodedColumnStore_graph.add_column(
        Encoding_uint32_t->encode_column((uint32_t *)ColumnStore_graph.at(0),
                                        NumRows_graph),
        Encoding_uint32_t->num_distinct);
    EncodedColumnStore_graph.add_annotation(
      sizeof(float),
      (void*)ColumnStore_graph.at(1));
    /*
    EncodedColumnStore_graph.to_binary("/Users/caberger/Documents/Research/"
                                       "code/EmptyHeaded/test/graph/databases/"
                                       "db_simple/relations/graph/");
    */
    timer::stop_clock("ENCODING graph", start_time);
  }
  Trie<float, ParMemoryBuffer> *Trie_graph_0_1 = NULL;
  {
    std::vector<size_t> order_0_1 = {0};
    {
      auto start_time = timer::start_clock();
      // buildTrie
      Trie_graph_0_1 = new Trie<float, ParMemoryBuffer>(
          "",
          &EncodedColumnStore_graph.max_set_size, 
          &EncodedColumnStore_graph.data,
          EncodedColumnStore_graph.annotation);
      timer::stop_clock("BUILDING TRIE graph_0_1", start_time);
    }
    Trie_graph_0_1->encodings.push_back((void*)Encoding_uint32_t);
    ////////////////////emitWriteBinaryTrie////////////////////
    /*
    {
      auto start_time = timer::start_clock();
      Trie_graph_0_1->save();
      timer::stop_clock("WRITING BINARY TRIE graph_0_1", start_time);
    } */ 
  }
  return Trie_graph_0_1;
} 

std::pair<Trie<float, ParMemoryBuffer>*,Trie<float, ParMemoryBuffer>*> load_matrix_and_transpose(std::string file){
  SortableEncodingMap<uint32_t> EncodingMap_uint32_t;
  Encoding<uint32_t>* Encoding_uint32_t = new Encoding<uint32_t>();

  std::vector<void *> ColumnStore_graph;
  size_t NumRows_graph = 0;
  {
    auto start_time = timer::start_clock();
    tsv_reader f_reader(file);
    char *next = f_reader.tsv_get_first();
    std::vector<uint32_t> *v_0 = new std::vector<uint32_t>();
    std::vector<uint32_t> *v_1 = new std::vector<uint32_t>();
    std::vector<float> *a_0 = new std::vector<float>();

    while (next != NULL) {
      const uint32_t value_0 = utils::from_string<uint32_t>(next);
      v_0->push_back(value_0);
      EncodingMap_uint32_t.update(value_0);
      next = f_reader.tsv_get_next();
      const uint32_t value_1 = utils::from_string<uint32_t>(next);
      v_1->push_back(value_1);
      EncodingMap_uint32_t.update(value_1);
      next = f_reader.tsv_get_next();
      const float anno_0 = utils::from_string<float>(next);
      a_0->push_back(anno_0);
      next = f_reader.tsv_get_next();
      NumRows_graph++;
    }
    ColumnStore_graph.push_back((void *)v_0->data());
    ColumnStore_graph.push_back((void *)v_1->data());
    ColumnStore_graph.push_back((void *)a_0->data());
    timer::stop_clock("READING MATRIX", start_time);
  }
  {
    auto start_time = timer::start_clock();
    Encoding_uint32_t->build(EncodingMap_uint32_t.get_sorted());
    timer::stop_clock("BUILDING ENCODING uint32_t", start_time);
  }
  /*
  {
    auto start_time = timer::start_clock();
    Encoding_uint32_t->to_binary("/Users/caberger/Documents/Research/code/"
                                "EmptyHeaded/test/graph/databases/db_simple/"
                                "encodings/uint32_t/");
    timer::stop_clock("WRITING ENCODING uint32_t", start_time);
  }
  */
  EncodedColumnStore EncodedColumnStore_graph(NumRows_graph, 2, 1);
  {
    auto start_time = timer::start_clock();
    EncodedColumnStore_graph.add_column(
        Encoding_uint32_t->encode_column((uint32_t *)ColumnStore_graph.at(1),
                                        NumRows_graph),
        Encoding_uint32_t->num_distinct);
    EncodedColumnStore_graph.add_column(
        Encoding_uint32_t->encode_column((uint32_t *)ColumnStore_graph.at(0),
                                        NumRows_graph),
        Encoding_uint32_t->num_distinct);
    EncodedColumnStore_graph.add_annotation(
      sizeof(float),
      (void*)ColumnStore_graph.at(2));
    /*
    EncodedColumnStore_graph.to_binary("/Users/caberger/Documents/Research/"
                                       "code/EmptyHeaded/test/graph/databases/"
                                       "db_simple/relations/graph/");
    */
    timer::stop_clock("ENCODING graph", start_time);
  }
  Trie<float, ParMemoryBuffer> *Trie_graph_0_1 = NULL;
  {
    std::vector<size_t> order_0_1 = {0, 1};
    {
      auto start_time = timer::start_clock();
      // buildTrie
      Trie_graph_0_1 = new Trie<float, ParMemoryBuffer>(
          "",
          &EncodedColumnStore_graph.max_set_size, 
          &EncodedColumnStore_graph.data,
          EncodedColumnStore_graph.annotation);
      timer::stop_clock("BUILDING TRIE graph_1_0", start_time);
    }
    Trie_graph_0_1->encodings.push_back((void*)Encoding_uint32_t);
    Trie_graph_0_1->encodings.push_back((void*)Encoding_uint32_t);
    ////////////////////emitWriteBinaryTrie////////////////////
  }

  Trie<float, ParMemoryBuffer> *Trie_graph_1_0 = NULL;
  {
    std::vector<size_t> order_1_0 = {1, 0};
    EncodedColumnStore EncodedColumnStore_graph_1_0(&EncodedColumnStore_graph, order_1_0);
    {
      auto start_time = timer::start_clock();
      // buildTrie
      Trie_graph_1_0 = new Trie<float, ParMemoryBuffer>(
          "",
          &EncodedColumnStore_graph_1_0.max_set_size, 
          &EncodedColumnStore_graph_1_0.data,
          EncodedColumnStore_graph_1_0.annotation);
      timer::stop_clock("BUILDING TRIE graph_1_0", start_time);
    }
    Trie_graph_1_0->encodings.push_back((void*)Encoding_uint32_t);
    Trie_graph_1_0->encodings.push_back((void*)Encoding_uint32_t);
    ////////////////////emitWriteBinaryTrie////////////////////
  }
  return std::make_pair(Trie_graph_0_1,Trie_graph_1_0);
} 

Trie<void *, ParMemoryBuffer>* load_graph(std::string file){
  SortableEncodingMap<uint32_t> EncodingMap_uint32_t;
  Encoding<uint32_t>* Encoding_uint32_t = new Encoding<uint32_t>();

  std::vector<void *> ColumnStore_graph;
  size_t NumRows_graph = 0;
  {
    auto start_time = timer::start_clock();
    tsv_reader f_reader(file);
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
      NumRows_graph++;
    }
    ColumnStore_graph.push_back((void *)v_0->data());
    ColumnStore_graph.push_back((void *)v_1->data());
    timer::stop_clock("READING graph", start_time);
  }
  {
    auto start_time = timer::start_clock();
    Encoding_uint32_t->build(EncodingMap_uint32_t.get_sorted());
    timer::stop_clock("BUILDING ENCODING uint32_t", start_time);
  }
  /*
  {
    auto start_time = timer::start_clock();
    Encoding_uint32_t->to_binary("/Users/caberger/Documents/Research/code/"
                                "EmptyHeaded/test/graph/databases/db_simple/"
                                "encodings/uint32_t/");
    timer::stop_clock("WRITING ENCODING uint32_t", start_time);
  }
  */
  EncodedColumnStore EncodedColumnStore_graph(NumRows_graph, 2, 0);
  {
    auto start_time = timer::start_clock();
    EncodedColumnStore_graph.add_column(
        Encoding_uint32_t->encode_column((uint32_t *)ColumnStore_graph.at(0),
                                        NumRows_graph),
        Encoding_uint32_t->num_distinct);
    EncodedColumnStore_graph.add_column(
        Encoding_uint32_t->encode_column((uint32_t *)ColumnStore_graph.at(1),
                                        NumRows_graph),
        Encoding_uint32_t->num_distinct);
    /*
    EncodedColumnStore_graph.to_binary("/Users/caberger/Documents/Research/"
                                       "code/EmptyHeaded/test/graph/databases/"
                                       "db_simple/relations/graph/");
    */
    timer::stop_clock("ENCODING graph", start_time);
  }
  Trie<void *, ParMemoryBuffer> *Trie_graph_0_1 = NULL;
  {
    std::vector<size_t> order_0_1 = {0, 1};
    {
      auto start_time = timer::start_clock();
      // buildTrie
      Trie_graph_0_1 = new Trie<void *, ParMemoryBuffer>(
          "",
          &EncodedColumnStore_graph.max_set_size, 
          &EncodedColumnStore_graph.data,
          EncodedColumnStore_graph.annotation);
      timer::stop_clock("BUILDING TRIE graph_0_1", start_time);
    }
    Trie_graph_0_1->encodings.push_back((void*)Encoding_uint32_t);
    Trie_graph_0_1->encodings.push_back((void*)Encoding_uint32_t);

    ////////////////////emitWriteBinaryTrie////////////////////
    /*
    {
      auto start_time = timer::start_clock();
      Trie_graph_0_1->save();
      timer::stop_clock("WRITING BINARY TRIE graph_0_1", start_time);
    } */ 
  }
  return Trie_graph_0_1;
} 