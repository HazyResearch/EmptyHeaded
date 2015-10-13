/******************************************************************************
*
* Author: Christopher R. Aberger
*
* The encoding class performs the dictionary encoding and stores both
* a hash map from values to keys and an array from keys to values. Values
* here are the original values that appeared in the relations. Keys are the 
* distinct 32 bit integers assigned to each value.
******************************************************************************/

#include "tbb/parallel_sort.h"
#include "Encoding.hpp"
#include <iostream>
#include <fstream>
#include "utils/parallel.hpp"

///////////////////////////////////////////////////////////////////////////////
//Stores the maps for the dictionary encoding.

//Given a column add its values to the encoding.
template <class T>
void Encoding<T>::build(std::set<T>* values){
  for (auto it = values->begin(); it != values->end(); it++) {
    T value = *it;
    value_to_key.insert(std::pair<T,uint32_t>(value,num_distinct++));
    key_to_value.push_back(value);
  }
}

template <class T>
void Encoding<T>::build(std::vector<T>* values){
  for (auto it = values->begin(); it != values->end(); it++) {
    T value = *it;
    value_to_key.insert(std::pair<T,uint32_t>(value,num_distinct++));
    key_to_value.push_back(value);
  }
}

template<class T>
std::vector<uint32_t>* Encoding<T>::encode_column(std::vector<T>* encodingMap){
  std::vector<uint32_t>* column = new std::vector<uint32_t>();
  column->resize(encodingMap->size());
  par::for_range(0,encodingMap->size(),100,[&](size_t tid, size_t i){
    (void) tid;
    column->at(i) = value_to_key.at(encodingMap->at(i));
  });
  return column;
}

//Writing strings to a binary file is slightly tricky thus we have to use template specialization.
template<>
Encoding<std::string>* Encoding<std::string>::from_binary(const std::string path){
    std::ifstream *infile = new std::ifstream();
    std::string file = path+std::string("encoding.bin");
    infile->open(file, std::ios::binary | std::ios::in);

    Encoding<std::string>* new_encoding = new Encoding<std::string>();
    infile->read((char *)&new_encoding->num_distinct, sizeof(new_encoding->num_distinct));
    
    //reserve memory (and init memory for unordered memory)
    new_encoding->key_to_value.resize(new_encoding->num_distinct);
    new_encoding->value_to_key.reserve(new_encoding->num_distinct);      
    //could be a par for
    for(uint32_t index = 0; index < new_encoding->num_distinct; index++){
      size_t str_size;
      infile->read((char *)&str_size, sizeof(str_size));

      char *in_string = new char[str_size];
      infile->read(in_string, str_size);
      std::string value(in_string,str_size);

      new_encoding->value_to_key.insert(std::make_pair(value,index));
      new_encoding->key_to_value.at(index) = value;
    }

    infile->close();

    return new_encoding;
}

template<class T>
Encoding<T>* Encoding<T>::from_binary(const std::string path){
    std::ifstream *infile = new std::ifstream();
    std::string file = path+std::string("encoding.bin");
    infile->open(file, std::ios::binary | std::ios::in);

    Encoding<T>* new_encoding = new Encoding<T>();
    infile->read((char *)&new_encoding->num_distinct, sizeof(new_encoding->num_distinct));
    
    //reserve memory (and init memory for unordered memory)
    new_encoding->key_to_value.resize(new_encoding->num_distinct);
    new_encoding->value_to_key.reserve(new_encoding->num_distinct);      
    //could be a par for
    for(uint32_t index = 0; index < new_encoding->num_distinct; index++){
      T value;
      infile->read((char *)&value, sizeof(value));

      new_encoding->value_to_key.insert(std::make_pair(value,index));
      new_encoding->key_to_value.at(index) = value;
    }

    infile->close();

    return new_encoding;
}

template<>
void Encoding<std::string>::to_binary(const std::string path){
    std::ofstream *writefile = new std::ofstream();
    std::string file = path+std::string("encoding.bin");
    writefile->open(file, std::ios::binary | std::ios::out);
    writefile->write((char *)&num_distinct, sizeof(num_distinct));
    //could be a par for
    for(uint32_t index = 0; index < num_distinct; index++){
      const size_t str_size = key_to_value.at(index).size();
      writefile->write((char *)&str_size, sizeof(str_size));
      writefile->write(key_to_value.at(index).c_str(), str_size);
    }
    writefile->close();
}

template<class T>
void Encoding<T>::to_binary(const std::string path){
    std::ofstream *writefile = new std::ofstream();
    std::string file = path+std::string("encoding.bin");
    writefile->open(file, std::ios::binary | std::ios::out);
    writefile->write((char *)&num_distinct, sizeof(num_distinct));
    //could be a par for
    for(uint32_t index = 0; index < num_distinct; index++){
      writefile->write((char *)&key_to_value.at(index), sizeof(T));
    }
    writefile->close();
}


template struct Encoding<long>;
//template void Encoding<SortableEncodingMap<long>>;
//template void Encoding<FrequencyEncodingMap<long>>;
