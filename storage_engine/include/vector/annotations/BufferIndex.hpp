#ifndef _BUFFERINDEX_H_
#define _BUFFERINDEX_H_

struct BufferIndex{
  int tid; //# of threads
  size_t index;
  BufferIndex(const size_t tid_in, const size_t index_in){
    tid = tid_in;
    index = index_in;
  }
  BufferIndex(){}
};

#endif