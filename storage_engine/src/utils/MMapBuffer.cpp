//---------------------------------------------------------------------------
// TripleBit
// (c) 2011 Massive Data Management Group @ SCTS & CGCL. 
//     Web site: http://grid.hust.edu.cn/triplebit
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include <stdio.h>
#include <errno.h>
#include "common.hpp"
#include "MMapBuffer.hpp"

MMapBuffer::MMapBuffer(const char* _filename, size_t initSize) : filename(_filename) {
  // TODO Auto-generated constructor stub
  fd = open(filename.c_str(), O_CREAT | O_RDWR, 0666);
  if(fd < 0) {
    std::cout << "Create map file error" << std::endl;
    assert(false);
  }

  size = lseek(fd, 0, SEEK_END);
  if(size < initSize) {
    size = initSize;
    if(ftruncate(fd, initSize) != 0) {
      std::cout << "ftruncate file error" << std::endl;
      assert(false);
    }
  }
  if(lseek(fd, 0, SEEK_SET) != 0) {
    std::cout << "lseek file error" << std::endl;
    assert(false);
  }

  mmap_addr = (char volatile*)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if(mmap_addr == MAP_FAILED){
    std::cout<<"size: "<<size<<" map file to memory error"<<std::endl;
  }
  head = (char*)mmap_addr;
}

MMapBuffer::~MMapBuffer() {
  // TODO Auto-generated destructor stub
  flush();
  munmap((char*)mmap_addr, size);
  close(fd);
}

int MMapBuffer::flush()
{
  if(msync((char*)mmap_addr, size, MS_SYNC) == 0) {
    return 0;
  }

  return -1;
}

char* MMapBuffer::resize(size_t incrementSize)
{
  size_t newsize = size + incrementSize;

  //cout<<filename.c_str()<<": "<<__FUNCTION__<<" begin: "<<size<<" : "<<newsize<<endl;

  char* new_addr = NULL;
  if (munmap((char*)mmap_addr, size) != 0 ){
    std::cout << "resize-munmap error!" << std::endl;
    assert(false);
    //MessageEngine::showMessage(, MessageEngine::ERROR);
    return NULL;
  }

  if(ftruncate(fd, newsize) != 0) {
    std::cout << "resize-ftruncate file error!" << std::endl;
    assert(false);
    return NULL;
  }

  if((new_addr = (char*)mmap(NULL, newsize,PROT_READ|PROT_WRITE,MAP_FILE|MAP_SHARED, fd, 0)) == (char*)MAP_FAILED)
  {
    std::cout << "mmap buffer resize error!" << std::endl;
    assert(false);
    return NULL;
  }

  //cout<<filename.c_str()<<": "<<__FUNCTION__<<" begin: "<<size<<" : "<<newsize<<endl;
  const size_t head_offset = head-mmap_addr;
  mmap_addr = (char volatile*)new_addr;
  head = (char*)(mmap_addr+head_offset);

  ::memset((char*)mmap_addr + size, 0, incrementSize);

  //cout<<filename.c_str()<<": "<<__FUNCTION__<<" end: "<<size<<" : "<<newsize<<endl;
  size = newsize;
  return (char*)mmap_addr;
}

char* MMapBuffer::get_next(const size_t size_requested){
  const size_t head_offset = head-mmap_addr;
  const size_t size_left = size-head_offset;
  if(size_left > size_requested){
    head += size_requested;
  } else{
    size_t extra_alloc_size = size;
    while(size_requested > (size_left+extra_alloc_size)){
      extra_alloc_size *= 2;
    }
    resize(extra_alloc_size);
    head += size_requested;
  }
  return (char*)(mmap_addr+head_offset);
}

void MMapBuffer::roll_back(const size_t num){
  assert((size_t)(head-mmap_addr) >= num);
  head -= num;
}

void MMapBuffer::free()
{
  munmap((char*)mmap_addr, size);
  close(fd);
}

void MMapBuffer::discard()
{
  munmap((char*)mmap_addr, size);
  close(fd);
  unlink(filename.c_str());
}

char* MMapBuffer::getBuffer()
{
  return (char*)mmap_addr;
}

char* MMapBuffer::getBuffer(int pos)
{
  return (char*)mmap_addr + pos;
}

void MMapBuffer::memset(char value)
{
  ::memset((char*)mmap_addr, value, size);
}

MMapBuffer* MMapBuffer::create(const char* filename, size_t initSize)
{
  MMapBuffer* buffer = new MMapBuffer(filename, initSize);
  
  //why? doesn't make sense
  char ch;
  for(size_t i = 0 ; i < buffer->size; i++) {
    ch = buffer->mmap_addr[i];
  }
  (void) ch;

  return buffer;
}
