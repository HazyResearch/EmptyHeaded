#ifndef _PARMMAPBUFFER_H
#define _PARMMAPBUFFER_H

#include "common.hpp"
#include "MMapBuffer.hpp"

struct ParMMapBuffer{
  size_t num_buffers;
  std::string path;
  std::vector<MMapBuffer*> elements;
  MMapBuffer* head;

  static std::string folder;
  ParMMapBuffer(
    std::string path,
    size_t num_elems);

  ParMMapBuffer(
    std::string path_in,
    std::vector<size_t>* num_elems_in,
    size_t num_buffers_in);

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
    head->free();
    for(size_t i = 0; i < num_buffers; i++){
      elements.at(i)->free();
    }
  };

  /*
  Needed so that we have a common interface with parmemorybuffer
  */
  inline static ParMMapBuffer* load(    
    std::string path,
    std::vector<size_t>* num_elems,
    size_t num_buffers_in){
    return new ParMMapBuffer(path,num_elems,num_buffers_in);
  };
};

#endif
