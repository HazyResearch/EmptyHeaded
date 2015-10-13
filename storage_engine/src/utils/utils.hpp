/******************************************************************************
*
* Author: Christopher R. Aberger
*
* Some common utility functions.
******************************************************************************/

#ifndef UTILS_H
#define UTILS_H

#include "timer.hpp"
#include "io.hpp"
#include "parallel.hpp"
#include "thread_pool.hpp"
#include "debug.hpp"
#include "common.hpp"
#include "ParMMapBuffer.hpp"
#include "ParMemoryBuffer.hpp"

namespace utils {
  //takes strings and casts them to proper type. To ease code generation
  template<class T>
  T from_string(const char *string_element);
  template<>
  inline uint64_t from_string(const char *string_element){
    uint64_t element;
    sscanf(string_element,"%lu",&element);
    return element;
  }
  template<>
  inline long from_string(const char *string_element){
    long element;
    sscanf(string_element,"%ld",&element);
    return element;
  }
  template<>
  inline uint32_t from_string(const char *string_element){
    uint32_t element;
    sscanf(string_element,"%u",&element);
    return element;
  }
  template<>
  inline float from_string(const char *string_element){
    float element;
    sscanf(string_element,"%f",&element);
    return element;
  }
  template<>
  inline std::string from_string(const char *string_element){
    return string_element;
  }

  //Look for a key, pass in a pointer to an array and a start and end index.
  template<typename F>
  long binary_search(const uint32_t * const data, size_t first, size_t last, uint32_t search_key, F f){
   long index;
   if (first > last){
    index = -1;
   } else{
    size_t mid = (last+first)/2;
    if (search_key == data[f(mid)])
      index = mid;
    else{
      if (search_key < data[f(mid)]){
        if(mid == 0)
          index = -1;
        else
          index = binary_search(data,first,mid-1,search_key,f);
      }
      else
        index = binary_search(data,mid+1,last,search_key,f);
    }
   } // end if
   return index;
  }// end binarySearch

}

#endif
