#ifndef _META_H_
#define _META_H_

#include "utils/utils.hpp"

struct Meta {
  //some basic meta data
  uint32_t cardinality;
  uint32_t start;
  uint32_t end;
  type::layout type;
  Meta(){};
};

template<class M>
struct Buffer {
  size_t index;
  M* memory_buffer;
};

#endif