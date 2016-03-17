#ifndef _META_H_
#define _META_H_

#include "utils/common.hpp"

struct Meta {
  //some basic meta data
  uint32_t cardinality;
  uint32_t start;
  uint32_t end;
  type::layout type;
  Meta(){};
};

#endif