/******************************************************************************
*
* Author: Christopher R. Aberger
*
* The encoding class performs the dictionary encoding and stores both
* a hash map from values to keys and an array from keys to values. Values
* here are the original values that appeared in the relations. Keys are the 
* distinct 32 bit integers assigned to each value.
******************************************************************************/

#ifndef _ENCODING_H_
#define _ENCODING_H_

#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <vector>
#include <set>

template <class T>
struct Encoding{
  uint32_t num_distinct;
  std::vector<T> key_to_value;
  std::unordered_map<T,uint32_t> value_to_key;

  Encoding(){
    num_distinct = 0;
  }
  ~Encoding(){}

  //Given a column add its values to the encoding.
  void build(std::set<T>* encodingMap);
  void build(std::vector<T>* encodingMap);

  std::vector<uint32_t>* encode_column(std::vector<T>* encodingMap);

  void to_binary(const std::string path);
  static Encoding<T>* from_binary(const std::string path);
};

#endif
