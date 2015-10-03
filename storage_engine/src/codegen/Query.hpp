#ifndef _QUERY_H_
#define _QUERY_H_

#include <stdint.h>

struct GHDResult;

struct Query {
  uint64_t num_rows; // = 0;
  GHDResult* result;
  
	Query();
	void run();
  //void result(); //TO DO implement something that iterates over the trie an returns the tuples
};

#endif