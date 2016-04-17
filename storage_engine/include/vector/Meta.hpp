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
  inline double get_density() const{
    return (double)cardinality/(end-start);
  }
};

#endif