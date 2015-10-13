#include "common.hpp"
#include "ParMemoryBuffer.hpp"

std::string ParMemoryBuffer::folder = "/ram/";

ParMemoryBuffer::ParMemoryBuffer(
  std::string path_in){
  
  path = path_in;
  for(size_t i = 0; i < NUM_THREADS; i++){
    MemoryBuffer* mbuffer = new MemoryBuffer();
    elements.push_back(mbuffer);
  }
}

ParMemoryBuffer::ParMemoryBuffer(
  std::string path_in,
  size_t num_elems_in){
  
  path = path_in;
  for(size_t i = 0; i < NUM_THREADS; i++){
    MemoryBuffer* mbuffer = new MemoryBuffer(num_elems_in);
    elements.push_back(mbuffer);
  }
}

ParMemoryBuffer::ParMemoryBuffer(size_t num_elems_in){
  for(size_t i = 0; i < NUM_THREADS; i++){
    MemoryBuffer* mbuffer = new MemoryBuffer(num_elems_in);
    elements.push_back(mbuffer);
  }
}

size_t ParMemoryBuffer::get_size(const size_t tid){
  return elements.at(tid)->getSize();
}

uint8_t* ParMemoryBuffer::get_head(const size_t tid){
  return (uint8_t*)elements.at(tid)->get_head();
}

uint8_t* ParMemoryBuffer::get_address(const size_t tid){
  return (uint8_t*)elements.at(tid)->getBuffer();
}

uint8_t* ParMemoryBuffer::get_address(const size_t tid, const size_t offset){
  return (uint8_t*)elements.at(tid)->getBuffer(offset);
}

uint8_t* ParMemoryBuffer::get_next(const size_t tid, const size_t num){
  return (uint8_t*)elements.at(tid)->get_next(num);
}

ParMemoryBuffer* ParMemoryBuffer::load(std::string path_in,std::vector<size_t>* num_elems_in){
  (void) num_elems_in; //actually this is stored in the file :) needed for mmap
  ParMemoryBuffer* ret = new ParMemoryBuffer(path_in);
  for(size_t i = 0; i < NUM_THREADS; i++){
    std::ifstream myfile;
    std::string dataF = ret->path + folder + "data_" + std::to_string(i) + ".bin";
    myfile.open(dataF);
    ret->elements.at(i)->load(myfile);
  }
  return ret;
}

void ParMemoryBuffer::save(){
  assert(!path.empty());
  for(size_t i = 0; i < NUM_THREADS; i++){
    std::ofstream myfile;
    std::string dataF = path + folder + "data_" + std::to_string(i) + ".bin";
    myfile.open(dataF,std::ios::trunc);
    elements.at(i)->save(myfile);
  }
}

void ParMemoryBuffer::roll_back(const size_t tid, const size_t num){
  elements.at(tid)->roll_back(num);
}