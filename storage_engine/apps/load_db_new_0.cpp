#define GENERATED
#include "main.hpp"
extern "C" void *run() {
  ////////////////////emitInitCreateDB////////////////////
  // init relations
  Relation<long> *Vector = new Relation<long>();
  std::vector<float> *annotation_Vector = new std::vector<float>();
  Relation<long, long> *R = new Relation<long, long>();
  std::vector<void *> *annotation_R = new std::vector<void *>();
  Relation<long> *InverseDegree = new Relation<long>();
  std::vector<float> *annotation_InverseDegree = new std::vector<float>();
  // init encodings
  SortableEncodingMap<long> *node_encodingMap = new SortableEncodingMap<long>();
  Encoding<long> *Encoding_node = new Encoding<long>();

  ////////////////////emitLoadRelation////////////////////
  {
    auto start_time = debug::start_clock();
    tsv_reader f_reader(
        "/Users/caberger/Documents/Research/code/python_fun/databases/higgs/duplicate_pruned.tsv");
    char *next = f_reader.tsv_get_first();
    while (next != NULL) {
      node_encodingMap->update(R->append_from_string<0>(next));
      next = f_reader.tsv_get_next();
      node_encodingMap->update(R->append_from_string<1>(next));
      next = f_reader.tsv_get_next();
      R->num_rows++;
    }
    debug::stop_clock("READING RELATION R", start_time);
  }

  ////////////////////emitLoadRelation////////////////////
  {
    auto start_time = debug::start_clock();
    tsv_reader f_reader(
        "/Users/caberger/Documents/Research/code/python_fun/databases/higgs/vector_degree.txt");
    char *next = f_reader.tsv_get_first();
    while (next != NULL) {
      node_encodingMap->update(Vector->append_from_string<0>(next));
      next = f_reader.tsv_get_next();
      annotation_Vector->push_back(utils::from_string<float>(next));
      next = f_reader.tsv_get_next();
      Vector->num_rows++;
    }
    debug::stop_clock("READING RELATION Vector", start_time);
  }

  ////////////////////emitLoadRelation////////////////////
  {
    auto start_time = debug::start_clock();
    tsv_reader f_reader("/Users/caberger/Documents/Research/code/python_fun/databases/higgs/vector_inverse_degree.txt");
    char *next = f_reader.tsv_get_first();
    while (next != NULL) {
      node_encodingMap->update(InverseDegree->append_from_string<0>(next));
      next = f_reader.tsv_get_next();
      annotation_InverseDegree->push_back(utils::from_string<float>(next));
      next = f_reader.tsv_get_next();
      InverseDegree->num_rows++;
    }
    debug::stop_clock("READING RELATION InverseDegree", start_time);
  }

  ////////////////////emitBuildEncodings////////////////////
  {
    auto start_time = debug::start_clock();
    Encoding_node->build(node_encodingMap);
    delete node_encodingMap;
    debug::stop_clock("BUILDING ENCODINGS", start_time);
  }

  ////////////////////emitWriteBinaryEncoding////////////////////
  {
    auto start_time = debug::start_clock();
    Encoding_node->to_binary(
        "/Users/caberger/Documents/Research/code/python_fun/databases/higgs/db/encodings/node/");
    debug::stop_clock("WRITING ENCODING node", start_time);
  }

  ////////////////////emitEncodeRelation////////////////////
  EncodedRelation<void *> *Encoded_R =
      new EncodedRelation<void *>(annotation_R);
  {
    auto start_time = debug::start_clock();
    // encodeRelation
    Encoded_R->add_column(Encoding_node->encode_column(&R->get<0>()),
                          Encoding_node->num_distinct);
    Encoded_R->add_column(Encoding_node->encode_column(&R->get<1>()),
                          Encoding_node->num_distinct);
    Encoded_R->to_binary(
        "/Users/caberger/Documents/Research/code/python_fun/databases/higgs/db/relations/R/");
    debug::stop_clock("ENCODING R", start_time);
  }

  ////////////////////emitEncodeRelation////////////////////
  EncodedRelation<float> *Encoded_Vector =
      new EncodedRelation<float>(annotation_Vector);
  {
    auto start_time = debug::start_clock();
    // encodeRelation
    Encoded_Vector->add_column(Encoding_node->encode_column(&Vector->get<0>()),
                               Encoding_node->num_distinct);
    Encoded_Vector->to_binary(
        "/Users/caberger/Documents/Research/code/python_fun/databases/higgs/db/relations/Vector/");
    debug::stop_clock("ENCODING Vector", start_time);
  }

  ////////////////////emitEncodeRelation////////////////////
  EncodedRelation<float> *Encoded_InverseDegree =
      new EncodedRelation<float>(annotation_InverseDegree);
  {
    auto start_time = debug::start_clock();
    // encodeRelation
    Encoded_InverseDegree->add_column(
        Encoding_node->encode_column(&InverseDegree->get<0>()),
        Encoding_node->num_distinct);
    Encoded_InverseDegree->to_binary("/Users/caberger/Documents/Research/code/python_fun/databases/higgs/db/relations/InverseDegree/");
    debug::stop_clock("ENCODING InverseDegree", start_time);
  }

  return NULL;
}
#ifndef GOOGLE_TEST
int main() {
  thread_pool::initializeThreadPool();
  run();
}
#endif
