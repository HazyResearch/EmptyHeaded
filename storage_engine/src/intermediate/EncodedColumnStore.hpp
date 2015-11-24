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

template<class R>
struct EncodedColumnStore {
  std::vector<std::vector<uint32_t>> data;
  std::vector<uint32_t> max_set_size;
  std::vector<R> annotation;
  EncodedColumnStore(std::vector<R>* annotation_in){
    annotation = *annotation_in;
  }
  EncodedColumnStore(
    std::vector<std::vector<uint32_t>> data_in, 
    std::vector<uint32_t> num_distinct_in,
    std::vector<R> annotation_in){
    data = data_in;
    max_set_size = num_distinct_in;
    annotation = annotation_in;
  }

  std::vector<uint32_t>* column(size_t i){
    return &data.at(i);
  }

  void add_column(std::vector<uint32_t> *column_in,uint32_t num_distinct){
    data.push_back(*column_in);
    max_set_size.push_back(num_distinct);
  }

  void to_binary(std::string path){
    std::ofstream *writefile = new std::ofstream();
    std::string file = path+std::string("encoded.bin");
    writefile->open(file, std::ios::binary | std::ios::out);
    size_t num_columns = data.size();
    writefile->write((char *)&num_columns, sizeof(num_columns));
    for(size_t i = 0; i < data.size(); i++){
      const uint32_t mss = max_set_size.at(i);
      writefile->write((char *)&mss, sizeof(mss));
      const size_t num_rows = data.at(i).size();
      writefile->write((char *)&num_rows, sizeof(num_rows));
      for(size_t j = 0; j < data.at(i).size(); j++){
        writefile->write((char *)&data.at(i).at(j), sizeof(data.at(i).at(j)));
      }
    }
    const size_t num_rows = annotation.size();
    writefile->write((char *)&num_rows, sizeof(num_rows));
    for(size_t i = 0; i < annotation.size(); i++){
      const R aValue = annotation.at(i);
      writefile->write((char *)&aValue, sizeof(aValue));
    }
    writefile->close();
  }
  
  static EncodedColumnStore<R>* from_binary(std::string path){
    std::ifstream *infile = new std::ifstream();
    std::string file = path+std::string("encoded.bin");
    infile->open(file, std::ios::binary | std::ios::in);

    std::vector<std::vector<uint32_t>> data_in;

    size_t num_columns;
    infile->read((char *)&num_columns, sizeof(num_columns));
    data_in.resize(num_columns);

    std::vector<uint32_t> max_set_size_in;
    for(size_t i = 0; i < num_columns; i++){
      uint32_t mss;
      infile->read((char *)&mss, sizeof(mss));
      max_set_size_in.push_back(mss);

      size_t num_rows;
      infile->read((char *)&num_rows, sizeof(num_rows));
      std::vector<uint32_t>* new_column = new std::vector<uint32_t>();
      new_column->resize(num_rows);
      for(size_t j = 0; j < num_rows; j++){
        uint32_t value;
        infile->read((char *)&value, sizeof(value));
        new_column->at(j) = value;
      }
      data_in.at(i) = *new_column;
    }

    size_t num_annotation_rows;
    infile->read((char *)&num_annotation_rows, sizeof(num_annotation_rows));
    std::vector<R>* annotation_in = new std::vector<R>();
    annotation_in->resize(num_annotation_rows);
    for(size_t j = 0; j < num_annotation_rows; j++){
      R value;
      infile->read((char *)&value, sizeof(value));
      annotation_in->at(j) = value;
    }
    infile->close();
    return new EncodedColumnStore<R>(data_in,max_set_size_in,*annotation_in);
  }

};


#endif