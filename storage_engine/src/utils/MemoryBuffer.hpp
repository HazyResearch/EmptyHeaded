#ifndef _MEMORYBUFFER_H
#define _MEMORYBUFFER_H

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


#include "common.hpp"

class MemoryBuffer {
  size_t size;
  uint8_t* buffer;
  uint8_t* currentHead;
public:
  static unsigned pagesize;
public:
  friend class EntityIDBuffer;
  friend class ColumnBuffer;
  MemoryBuffer();
  MemoryBuffer(size_t size);
  virtual ~MemoryBuffer();
  uint8_t* resize(size_t increasedSize);
  uint8_t* getBuffer();
  void free();

  uint8_t* get_address(size_t pos);
  size_t getSize() { return size; }
  size_t get_length() {return size; }
  uint8_t* get_next(const size_t size_requested);
  uint8_t* get_head(){return currentHead;};
  void roll_back(const size_t size_requested);
  uint8_t* get_address() { return buffer; }
  void memset(uint8_t value);
  void save(std::ofstream& ofile);
  void load(std::ifstream& ifile);

private:
};
#endif
