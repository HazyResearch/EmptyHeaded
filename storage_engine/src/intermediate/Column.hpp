/******************************************************************************
*
* Author: Christopher R. Aberger
*
* This is just a vector so why do you have a separate class? Because I need 
* to overload the "append_from_string" method. Currently we support input 
* types of (uint64_t,uint32_t,std::string) as valid column types.
******************************************************************************/

#ifndef _COLUMN_H_
#define _COLUMN_H_

template <class T>
struct Column{
  std::vector<T> col;
  Column<T>(){}
  inline T at(size_t i) const{
    return col.at(i);
  }
  inline void set(size_t i, T val){
    col.at(i) = val;
  }
  inline void append(T elem){
    col.push_back(elem);
  }
  inline size_t size() const{
    return col.size();
  }
  inline void reserve(size_t size){
    col.reserve(size);
  }
  inline void assign(T * start, T* end){
    col.assign(start,end);
  }
  inline void append_from_string(const char *string_element){
    col.push_back(utils::from_string<T>(string_element));
  }
};

#endif