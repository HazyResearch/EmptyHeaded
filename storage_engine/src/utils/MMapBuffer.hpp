#ifndef _MMAPBUFFER_H_
#define _MMAPBUFFER_H_

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

//#define VOLATILE   
class MMapBuffer {
  int fd;
  char volatile* mmap_addr;
  std::string filename;
  size_t size;
  char *head;
public:
  char* resize(size_t incrementSize);
  char* getBuffer();
  char* getBuffer(int pos);
  void discard();
  int flush();
  size_t getSize() { return size;}
  char* get_next(const size_t mem_size);
  void roll_back(const size_t mem_size);
  size_t get_length() { return size;}
  char * get_address() const { return (char*)mmap_addr; }

  virtual void memset(char value);

  MMapBuffer(const char* filename, size_t initSize);
  virtual ~MMapBuffer();

public:
  static MMapBuffer* create(const char* filename, size_t initSize);
};

#endif /* MMAPBUFFER_H_ */