//---------------------------------------------------------------------------
// MODIFIED FROM LICENSE BELOW (WE ARE FREE TO EDIT AS LONG AS WE ACK :) )
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

#include "utils/MemoryBuffer.hpp"

#define INIT_PAGE_COUNT 1000000
unsigned MemoryBuffer::pagesize = 4096; //4KB

MemoryBuffer::MemoryBuffer(){}

MemoryBuffer::MemoryBuffer(size_t _size) :size(_size) {
  // TODO Auto-generated constructor stub
  buffer = NULL;
  buffer = (uint8_t*)malloc(size * sizeof(char));
  if(buffer == NULL) {
    std::cout << "MemoryBuffer::MemoryBuffer, malloc error!" << std::endl;
    assert(false);
  }
  ::memset(buffer, 0, size);
  currentHead = buffer;
}

void MemoryBuffer::free(){
  // TODO Auto-generated destructor stub
  //free the buffer
  if(buffer != NULL)
    ::free(buffer);
  buffer = NULL;
  size = 0;
}

MemoryBuffer::~MemoryBuffer() {
  // TODO Auto-generated destructor stub
  //free the buffer
  if(buffer != NULL)
    ::free(buffer);
  buffer = NULL;
  size = 0;
}

size_t MemoryBuffer::get_offset() const {
  return currentHead-buffer;
}

uint8_t* MemoryBuffer::get_next(const size_t size_requested){
  const size_t head_offset = currentHead-buffer;
  const size_t size_left = size-head_offset;
  if(size_left > size_requested){
    currentHead += size_requested;
  } else{
    size_t extra_alloc_size = size;
    while(size_requested > (size_left+extra_alloc_size)){
      extra_alloc_size *= 2;
    }
    resize(extra_alloc_size);
    currentHead += size_requested;
  }
  return (uint8_t*)(buffer+head_offset);
}

void MemoryBuffer::roll_back(const size_t size_requested){
  assert((size_t)(currentHead-buffer) >= size_requested);
  currentHead -= size_requested;
}

uint8_t* MemoryBuffer::resize(const size_t increaseSize)
{
  const size_t old_head_offset = currentHead-buffer;
  size_t newsize = size + increaseSize;
  buffer = (uint8_t*)realloc(buffer, newsize * sizeof(char));
  //std::cout << "REALLOC: " << (void*)buffer << std::endl;
  if(buffer == NULL) {
    std::cout << "MemoryBuffer::addBuffer, realloc error!" << std::endl;
    assert(false);
  }
  currentHead = buffer + old_head_offset;
  ::memset(currentHead, 0, increaseSize);
  size = size + increaseSize;

  return buffer;
}

uint8_t* MemoryBuffer::getBuffer()
{
  return buffer;
}

void MemoryBuffer::memset(const uint8_t value)
{
  ::memset(buffer, value, size);
}

uint8_t* MemoryBuffer::get_address(const size_t pos) const
{
  return buffer + pos;
}

void MemoryBuffer::save(std::ofstream& ofile)
{
  ofile<<size<<" ";
  int offset = currentHead - buffer;
  ofile<<offset<<" ";

  unsigned i;
  for(i = 0; i < size; i++) {
    ofile<<buffer[i];
  }
}

void MemoryBuffer::load(std::ifstream& ifile)
{
  int offset;
  ifile>>size;
  ifile>>offset;

  ifile.get();

  if( buffer != NULL) {
    ::free(buffer);
    buffer = NULL;
  }

  unsigned remainSize, writeSize, allocSize;
  remainSize = size; writeSize = 0; allocSize = 0;

  if(remainSize >= INIT_PAGE_COUNT * pagesize) {
    buffer = (uint8_t*)malloc(INIT_PAGE_COUNT * pagesize);
    writeSize = allocSize = INIT_PAGE_COUNT * pagesize;
  } else {
    buffer = (uint8_t*)malloc(remainSize);
    writeSize = allocSize = remainSize;
  }

  if(buffer == NULL) {
    std::cout << "MemoryBuffer::load, malloc error!" << std::endl;
    assert(false);
  }

  unsigned i;
  currentHead = buffer;
  while(remainSize > 0) {
    for(i = 0; i < writeSize; i++) {
      //ifile>>currentHead[i];
      currentHead[i] = ifile.get();
    }

    remainSize = remainSize - writeSize;
    if(remainSize >= INIT_PAGE_COUNT * pagesize) {
      std::cout << "REALLOC IN LOADING" << std::endl;
      buffer = (uint8_t*)realloc(buffer, allocSize + INIT_PAGE_COUNT * pagesize);
      writeSize = INIT_PAGE_COUNT * pagesize;
    } else {
      buffer = (uint8_t*)realloc(buffer, allocSize + remainSize);
      writeSize = remainSize;
    }

    if(buffer == NULL) {
      std::cout << "MemoryBuffer::load, realloc error!" << std::endl;
      assert(false);
    }
    currentHead = buffer + allocSize;
    allocSize = allocSize + writeSize;
  }

  currentHead = buffer + offset;
}
