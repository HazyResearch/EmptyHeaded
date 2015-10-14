#include "common.hpp"
#include "ParMemoryBuffer.hpp"

std::string ParMemoryBuffer::folder = "/ram/";

ParMemoryBuffer::ParMemoryBuffer(
  size_t num_buffers_in,
  std::string path_in){
  
  num_buffers = num_buffers_in;
  path = path_in;
  head = new MemoryBuffer(1);
  for(size_t i = 0; i < num_buffers; i++){
    MemoryBuffer* mbuffer = new MemoryBuffer(1);
    elements.push_back(mbuffer);
  }
}

ParMemoryBuffer::ParMemoryBuffer(
  std::string path_in,
  size_t num_elems_in){
  
  num_buffers = NUM_THREADS;
  path = path_in;
  head = new MemoryBuffer(num_elems_in);
  for(size_t i = 0; i < num_buffers; i++){
    MemoryBuffer* mbuffer = new MemoryBuffer(num_elems_in);
    elements.push_back(mbuffer);
  }
}

ParMemoryBuffer::ParMemoryBuffer(size_t num_elems_in){
  num_buffers = NUM_THREADS;
  head = new MemoryBuffer(num_elems_in);
  for(size_t i = 0; i < num_buffers; i++){
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
  return (uint8_t*)elements.at(tid)->get_address(offset);
}

uint8_t* ParMemoryBuffer::get_next(const size_t tid, const size_t num){
  return (uint8_t*)elements.at(tid)->get_next(num);
}

ParMemoryBuffer* ParMemoryBuffer::load(
  std::string path_in,
  const size_t num_buffers_in,
  std::vector<size_t>* buf_sizes){

  (void) buf_sizes;
  ParMemoryBuffer* ret = new ParMemoryBuffer(num_buffers_in,path_in);

  std::ifstream myfile;
  std::string dataF = ret->path + folder + "data_head.bin";
  myfile.open(dataF);
  ret->head->load(myfile);
  myfile.close();
  for(size_t i = 0; i < num_buffers_in; i++){
    dataF = ret->path + folder + "data_" + std::to_string(i) + ".bin";
    myfile.open(dataF);
    ret->elements.at(i)->load(myfile);
    myfile.close();
  }
  return ret;
}

void ParMemoryBuffer::save(){
  assert(!path.empty());
  std::ofstream myfile;
  std::cout << path << " " << folder << std::endl;
  std::string dataF = path + folder + "data_head.bin";
  myfile.open(dataF,std::ios::trunc | std::ios::out | std::ios::binary);
  head->save(myfile);
  myfile.close();
  for(size_t i = 0; i < num_buffers; i++){
    dataF = path + folder + "data_" + std::to_string(i) + ".bin";
    myfile.open(dataF,std::ios::trunc);
    elements.at(i)->save(myfile);
    myfile.close();
  }
}

void ParMemoryBuffer::roll_back(const size_t tid, const size_t num){
  elements.at(tid)->roll_back(num);
}