#ifndef IO_H
#define IO_H

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
    size_t file_size = ftell(pFile);
    rewind(pFile);

    // allocate memory to contain the whole file:
    buffer = (char*) malloc (sizeof(char)*file_size + 1);
    if (buffer == NULL) {fputs ("Memory error",stderr); exit (2);}

    // copy the file into the buffer:
    size_t result = fread (buffer,1,file_size,pFile);
    if (result != file_size) {fputs ("Reading error",stderr); exit (3);}
    buffer[result] = '\0';
  }
  inline char* tsv_get_first(){
    return strtok(buffer," \t\n");
  }
  inline char* tsv_get_next(){
    return strtok(NULL," \t\n");
  }

};
#endif
