/******************************************************************************
*
* Author: Christopher R. Aberger
*
* Stores a relation in a column wise fashion. Can take in any number of 
* different template arguments. Each template arguments corresponds to the
* type of the column. 
******************************************************************************/

#ifndef _ENCODED_COLUMN_STORE_H_
#define _ENCODED_COLUMN_STORE_H_

#include "utils/common.hpp"

struct EncodedColumnStore {
  size_t num_rows;
  size_t num_attributes;
  size_t num_annotations;

  std::vector<uint32_t> max_set_size;
  std::vector<std::vector<uint32_t>> data;

  std::vector<size_t> annotation_bytes;
  std::vector<void*> annotation;
  
  EncodedColumnStore(
    EncodedColumnStore * in,
    std::vector<size_t> order){
    num_rows = in->num_rows;
    num_attributes = in->num_attributes;
    num_annotations = in->num_annotations;

    //perform a reordering
    assert(order.size() == num_attributes);
    for(size_t i = 0; i < num_attributes; i++){
      max_set_size.push_back(in->max_set_size.at(order.at(i)));
      data.push_back(in->data.at((order.at(i))));
    }

    annotation_bytes = in->annotation_bytes;
    annotation = in->annotation;
  }

  EncodedColumnStore(
    size_t num_rows_in,
    size_t num_attributes_in,
    size_t num_annotations_in){
    num_rows = num_rows_in;
    num_attributes = num_attributes_in;
    num_annotations = num_annotations_in;
  }

  EncodedColumnStore(
    size_t num_rows_in,
    size_t num_attributes_in,
    size_t num_annotations_in,
    std::vector<uint32_t> max_set_size_in,
    std::vector<std::vector<uint32_t>> data_in,
    std::vector<size_t> annotation_bytes_in,
    std::vector<void*> annotation_in){
    num_rows = num_rows_in;
    num_attributes = num_attributes_in;
    num_annotations = num_annotations_in;
    max_set_size = max_set_size_in;
    data = data_in;
    annotation_bytes = annotation_bytes_in;
    annotation = annotation_in;
  }

  std::vector<uint32_t>* column(const size_t i){
    return &data.at(i);
  }

  void add_column(
    const std::vector<uint32_t> * const column_in,
    const uint32_t num_distinct){
    max_set_size.push_back(num_distinct);
    data.push_back(*column_in);
  }

  void add_annotation(
    const size_t num_bytes,
    void* annotation_in){
    annotation_bytes.push_back(num_bytes);
    annotation.push_back(annotation_in);
  }

  void to_binary(std::string path){
    std::ofstream *writefile = new std::ofstream();
    std::string file = path+std::string("encoded.bin");
    writefile->open(file, std::ios::binary | std::ios::out);

    writefile->write((char *)&num_rows, sizeof(num_rows));
    writefile->write((char *)&num_attributes, sizeof(num_attributes));
    writefile->write((char *)&num_annotations, sizeof(num_annotations));
    
    //write the data
    for(size_t i = 0; i < data.size(); i++){
      const uint32_t mss = max_set_size.at(i);
      writefile->write((char *)&mss, sizeof(mss));
      for(size_t j = 0; j < num_rows; j++){
        writefile->write((char *)&data.at(i).at(j), sizeof(data.at(i).at(j)));
      }
    }

    //write the annotation
    for(size_t i = 0; i < num_annotations; i++){
      const uint8_t* myanno = (const uint8_t*)annotation.at(i);
      const size_t num_bytes = annotation_bytes.at(i);
      writefile->write((char *)&num_bytes, num_bytes);
      for(size_t j = 0; j < num_rows; j++){
        writefile->write(
          (char *)myanno, 
          num_bytes);
        myanno += num_bytes;
      }
    }
    writefile->close();
  }
  
  static EncodedColumnStore* from_binary(std::string path){
    std::ifstream *infile = new std::ifstream();
    std::string file = path+std::string("encoded.bin");
    infile->open(file, std::ios::binary | std::ios::in);

    size_t num_rows_in;
    size_t num_attributes_in;
    size_t num_annotations_in;

    std::vector<uint32_t> max_set_size_in;
    std::vector<std::vector<uint32_t>> data_in;

    std::vector<size_t> annotation_bytes_in;
    std::vector<void*> annotation_in;

    infile->read((char *)&num_rows_in, sizeof(num_rows_in));
    infile->read((char *)&num_attributes_in, sizeof(num_attributes_in));
    infile->read((char *)&num_annotations_in, sizeof(num_annotations_in));

    max_set_size_in.resize(num_attributes_in);
    data_in.resize(num_attributes_in);

    annotation_bytes_in.resize(num_annotations_in);
    annotation_in.resize(num_annotations_in);

    for(size_t i = 0; i < num_attributes_in; i++){
      uint32_t mss;
      infile->read((char *)&mss, sizeof(mss));
      max_set_size_in.at(i) = mss;

      std::vector<uint32_t>* new_column = new std::vector<uint32_t>();
      new_column->resize(num_rows_in);
      for(size_t j = 0; j < num_rows_in; j++){
        uint32_t value;
        infile->read((char *)&value, sizeof(value));
        new_column->at(j) = value;
      }
      data_in.at(i) = *new_column;
    }

    //write the annotation
    for(size_t i = 0; i < num_annotations_in; i++){
      size_t num_bytes;
      infile->read((char *)&num_bytes, sizeof(num_bytes));

      annotation_bytes_in.at(i) = num_bytes;

      uint8_t* myanno = new uint8_t[num_bytes*num_rows_in];
      for(size_t j = 0; j < num_rows_in; j++){
        infile->read(
          (char*)myanno, 
          num_bytes);
        myanno += num_bytes;
      }
      annotation_in.at(i) = ((void*)myanno);
    }

    infile->close();
    return new EncodedColumnStore(
      num_rows_in,
      num_attributes_in,
      num_annotations_in,
      max_set_size_in,
      data_in,
      annotation_bytes_in,
      annotation_in);
  }

};


#endif