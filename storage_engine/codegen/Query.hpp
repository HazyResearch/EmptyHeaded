#ifndef _QUERY_H_
#define _QUERY_H_

#include <vector>
#include <stdint.h>
#include <tuple>
#include "Trie.hpp"
#include "Encoding.hpp"

#ifdef EXECUTABLE
#include "main.hpp"
#else
struct application{};
#endif

struct ParMMapBuffer;
struct ParMemoryBuffer;

//template types are the types of the attributes, followed by the type of the annotation
struct Query : public application {
  uint64_t num_rows; // = 0;
  void* result;
  void* encodings;

	Query();
	void run();

  void fetch_result(); //TO DO implement something that iterates over the trie an returns the tuples
};

#ifdef EXECUTABLE
application* init_app(){
  return new Query(); 
}
#endif

#endif