/******************************************************************************
*
* Author: Christopher R. Aberger
*
* The encoding class performs the dictionary encoding and stores both
* a hash map from values to keys and an array from keys to values. Values
* here are the original values that appeared in the relations. Keys are the 
* distinct 32 bit integers assigned to each value.
******************************************************************************/

#ifndef _ENCODING_MAP_H_
#define _ENCODING_MAP_H_

#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include "tbb/parallel_sort.h"

///////////////////////////////////////////////////////////////////////////////
/*
  Have class so we can do arbitrary types
  of sortings for the dictionary encoding. By frequency
  or whatever you should always be able to write your own
  class to specify how we order for the dictionary encoding.
*/

//Basic encoding map where you sort by the sortable type itself.
template <class T>
struct SortableEncodingMap{
  std::set<T> values;

  SortableEncodingMap(){}
  ~SortableEncodingMap(){}

  //Have a way to add values
  inline void update(const T value){
    values.insert(value);
  }
  inline std::set<T>* get_sorted(){
    return &values;
  }

};

//Encoding map where you sort based upon the frequency of the values
template <class T>
struct FrequencyEncodingMap{
  std::map<T,uint32_t> values;

  FrequencyEncodingMap(){}
  ~FrequencyEncodingMap(){
    delete values;
  }

  //Have a way to add values
  inline void update(const T value){
    const auto it = std::find(value);
    uint32_t freq = 1;
    if(it != values.end()){
      freq = it->second + 1;
      values.erase(it);
    }
    values.insert(std::pair<T,uint32_t>(value,freq));
  }

  //sort functor (sort by value not key)
  bool myFunction(std::pair<T,uint32_t> first, std::pair<T,uint32_t> second){
    return first.second < second.second;
  }
  //Have a way to iterate over values (in the way you want)
  inline std::vector<T>* get_sorted(){
    //sort by the frequency
    
    //first create a vector with contents of the map, then sort that vector
    std::vector<std::pair<T,uint32_t> > myVec(values.begin(), values.end());
    tbb::parallel_sort(myVec.begin(),myVec.end(),&myFunction);
    return &myVec;
  }
};

#endif