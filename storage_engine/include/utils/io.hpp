#ifndef IO_H
#define IO_H

#include <cstdint>
#include <cstdlib>

#include "common.hpp"

class tsv_reader{
public:
  char *buffer;
  size_t file_size;
  tsv_reader(const std::string path){
    //open file for reading
    FILE *pFile = fopen(path.c_str(),"r");
    if (pFile==NULL) {fputs ("File error",stderr); exit (1);}
    
    // obtain file size:
    fseek(pFile,0,SEEK_END);
    file_size = ftell(pFile);
    rewind(pFile);

    // allocate memory to contain the whole file:
    buffer = (char*) malloc (sizeof(char)*file_size + 1);
    if (buffer == NULL) {fputs ("Memory error",stderr); exit (2);}

    // copy the file into the buffer:
    size_t result = fread (buffer,1,file_size,pFile);
    if (result != file_size) {fputs ("Reading error",stderr); exit (3);}
    buffer[result] = '\0';
    fclose(pFile);
  }
  ~tsv_reader() {
    std::free(buffer);
  }
  inline char* tsv_get_first(){
    return strtok(buffer," \t\n");
  }
  inline char* tsv_get_next(){
    return strtok(NULL," \t\n");
  }

};

namespace utils {
    //takes strings and casts them to proper type. To ease code generation
  template<class T>
  T from_string(const char *string_element);
  template<>
  inline uint64_t from_string(const char *string_element){
    return static_cast<uint64_t>(std::strtoull(string_element, NULL, 10));
  }
  template<>
  inline int64_t from_string(const char *string_element){
    return static_cast<int64_t>(std::strtoll(string_element, NULL, 10));
  }
  template<>
  inline uint32_t from_string(const char *string_element){
    uint32_t element;
    sscanf(string_element,"%u",&element);
    return element;
  }
  template<>
  inline float from_string(const char *string_element){
    float element;
    sscanf(string_element,"%f",&element);
    return element;
  }
  template<>
  inline double from_string(const char *string_element){
    double element;
    sscanf(string_element,"%lf",&element);
    return element;
  }
  template<>
  inline std::string from_string(const char *string_element){
    return string_element;
  }
}
#endif
