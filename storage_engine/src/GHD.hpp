#ifndef _GHD_H_
#define _GHD_H_

#include <utility>
#include <stdint.h>

struct GHD {
	GHD();
	
  std::pair<size_t,void*> run(); //returns <num_rows,Trie>
};

#endif