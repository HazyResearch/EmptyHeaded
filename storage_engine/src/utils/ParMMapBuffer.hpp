#ifndef _PARMMAPBUFFER_H
#define _PARMMAPBUFFER_H

#include "common.hpp"
#include "MMapBuffer.hpp"

struct ParMMapBuffer{
  static std::string folder;
  std::string path;
  std::vector<MMapBuffer> elements;

  ParMMapBuffer(
    std::string path,
    size_t num_elems);

  ParMMapBuffer(
    std::string path_in,
    std::vector<size_t>* num_elems_in);

  //debug
  size_t get_size(const size_t tid);
  uint8_t* get_head(const size_t tid);
  uint8_t* get_address(const size_t tid);
  uint8_t* get_address(const size_t tid,const size_t offset);
  uint8_t* get_next(const size_t tid, const size_t num);
  void roll_back(const size_t tid, const size_t num);
  void free();
  void save();

  ~ParMMapBuffer(){
    for(size_t i = 0; i < NUM_THREADS; i++){
      elements.at(i).free();
    }
  };

  /*
  Needed so that we have a common interface with parmemorybuffer
  */
  inline static ParMMapBuffer* load(    
    std::string path,
    std::vector<size_t>* num_elems){
    return new ParMMapBuffer(path,num_elems);
  };
};

#endif
