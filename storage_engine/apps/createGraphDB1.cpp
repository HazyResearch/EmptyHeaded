#define EXECUTABLE
#include "main.hpp"

struct createGraphDB1: public application {
  ////////////////////emitInitCreateDB////////////////////
  // init ColumnStores
  void run(){
    ColumnStore<long> *Vector = new ColumnStore<long>();
    std::vector<float> *annotation_Vector = new std::vector<float>();
    ColumnStore<long, long> *R = new ColumnStore<long, long>();
    std::vector<void *> *annotation_R = new std::vector<void *>();
    ColumnStore<long> *InverseDegree = new ColumnStore<long>();
    std::vector<float> *annotation_InverseDegree = new std::vector<float>();
    // init encodings
    SortableEncodingMap<long> *node_encodingMap = new SortableEncodingMap<long>();
    Encoding<long> *Encoding_node = new Encoding<long>();

    ////////////////////emitLoadColumnStore////////////////////
    {
      auto start_time = debug::start_clock();
      tsv_reader f_reader(
          "/Users/caberger/Documents/Research/data/databases/simple/simple.tsv");
      char *next = f_reader.tsv_get_first();
      while (next != NULL) {
        node_encodingMap->update(R->append_from_string<0>(next));
        next = f_reader.tsv_get_next();
        node_encodingMap->update(R->append_from_string<1>(next));
        next = f_reader.tsv_get_next();
        R->num_rows++;
      }
      debug::stop_clock("READING ColumnStore R", start_time);
    }

    /*
    ////////////////////emitLoadColumnStore////////////////////
    {
      auto start_time = debug::start_clock();
      tsv_reader f_reader(
          "/Users/caberger/Documents/Research/code/databases/higgs/vector_degree.txt");
      char *next = f_reader.tsv_get_first();
      while (next != NULL) {
        node_encodingMap->update(Vector->append_from_string<0>(next));
        next = f_reader.tsv_get_next();
        annotation_Vector->push_back(utils::from_string<float>(next));
        next = f_reader.tsv_get_next();
        Vector->num_rows++;
      }
      debug::stop_clock("READING ColumnStore Vector", start_time);
    }
    ////////////////////emitLoadColumnStore////////////////////
    {
      auto start_time = debug::start_clock();
      tsv_reader f_reader("/Users/caberger/Documents/Research/code/databases/higgs/vector_inverse_degree.txt");
      char *next = f_reader.tsv_get_first();
      while (next != NULL) {
        node_encodingMap->update(InverseDegree->append_from_string<0>(next));
        next = f_reader.tsv_get_next();
        annotation_InverseDegree->push_back(utils::from_string<float>(next));
        next = f_reader.tsv_get_next();
        InverseDegree->num_rows++;
      }
      debug::stop_clock("READING ColumnStore InverseDegree", start_time);
    }
    */

    ////////////////////emitBuildEncodings////////////////////
    {
      auto start_time = debug::start_clock();
      Encoding_node->build(node_encodingMap->get_sorted());
      delete node_encodingMap;
      debug::stop_clock("BUILDING ENCODINGS", start_time);
    }

    ////////////////////emitWriteBinaryEncoding////////////////////
    {
      auto start_time = debug::start_clock();
      Encoding_node->to_binary(
          "/Users/caberger/Documents/Research/data/databases/simple/db/encodings/node/");
      debug::stop_clock("WRITING ENCODING node", start_time);
    }

    ////////////////////emitEncodeColumnStore////////////////////
    EncodedColumnStore<void *> *Encoded_R =
        new EncodedColumnStore<void *>(annotation_R);
    {
      auto start_time = debug::start_clock();
      // encodeColumnStore
      Encoded_R->add_column(Encoding_node->encode_column(&R->get<0>()),
                            Encoding_node->num_distinct);
      Encoded_R->add_column(Encoding_node->encode_column(&R->get<1>()),
                            Encoding_node->num_distinct);
      Encoded_R->to_binary(
          "/Users/caberger/Documents/Research/data/databases/simple/db/relations/R/");
      debug::stop_clock("ENCODING R", start_time);
    }

    /*
    ////////////////////emitEncodeColumnStore////////////////////
    EncodedColumnStore<float> *Encoded_Vector =
        new EncodedColumnStore<float>(annotation_Vector);
    {
      auto start_time = debug::start_clock();
      // encodeColumnStore
      Encoded_Vector->add_column(Encoding_node->encode_column(&Vector->get<0>()),
                                 Encoding_node->num_distinct);
      Encoded_Vector->to_binary(
          "/Users/caberger/Documents/Research/code/databases/higgs/db/relations/Vector/");
      debug::stop_clock("ENCODING Vector", start_time);
    }

    ////////////////////emitEncodeColumnStore////////////////////
    EncodedColumnStore<float> *Encoded_InverseDegree =
        new EncodedColumnStore<float>(annotation_InverseDegree);
    {
      auto start_time = debug::start_clock();
      // encodeColumnStore
      Encoded_InverseDegree->add_column(
          Encoding_node->encode_column(&InverseDegree->get<0>()),
          Encoding_node->num_distinct);
      Encoded_InverseDegree->to_binary("/Users/caberger/Documents/Research/code/databases/higgs/db/relations/InverseDegree/");
      debug::stop_clock("ENCODING InverseDegree", start_time);
    }
    */
  }
};

application* init_app(){
  return new createGraphDB1(); 
}