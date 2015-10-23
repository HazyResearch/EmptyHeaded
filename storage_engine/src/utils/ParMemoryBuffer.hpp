#ifndef _PARMEMORYBUFFER_H
#define _PARMEMORYBUFFER_H

#include "MemoryBuffer.hpp"

struct ParMemoryBuffer{
  size_t num_buffers;
  static std::string folder;
  std::string path;
  std::vector<MemoryBuffer*> elements;
  MemoryBuffer *head;

  ParMemoryBuffer(std::string path,size_t num_elems);
  ParMemoryBuffer( size_t num_buffers_in,std::string path);
  ParMemoryBuffer(size_t num_elems);
  
  //debug
  size_t get_size(const size_t tid);
  uint8_t* get_address(const size_t tid);
  uint8_t* get_address(const size_t tid, const size_t offset);
  uint8_t* get_next(const size_t tid, const size_t num);
  void roll_back(const size_t tid, const size_t num);
  void save();
  uint8_t* get_head(const size_t tid);

  ~ParMemoryBuffer(){
    head->free();
    for(size_t i = 0; i < num_buffers; i++){
      elements.at(i)->free();
    }
  };

  static ParMemoryBuffer* load(
    std::string path,
    const size_t num_buffers_in,
    std::vector<size_t>* buf_sizes);
};

#endif
