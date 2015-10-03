#ifndef _QUERY_H_
#define _QUERY_H_

#include <vector>
#include <stdint.h>
#include <tuple>
#include "Trie.hpp"
#include "Encoding.hpp"

//template types are the types of the attributes, followed by the type of the annotation
template<class T,class R,class A>
struct Query {
  uint64_t num_rows; // = 0;
  Trie<long>* result;
  std::tuple<Encoding<T>,Encoding<R>> encodings;

	Query();
	void run();

  std::tuple<std::vector<T>,std::vector<R>,std::vector<A>> 
  fetch_result(); //TO DO implement something that iterates over the trie an returns the tuples
};


#endif