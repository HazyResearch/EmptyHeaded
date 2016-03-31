/******************************************************************************
*
* Author: Christopher R. Aberger
*
******************************************************************************/
#ifndef _TRIEBUFFER_H_
#define _TRIEBUFFER_H_

#include "utils/utils.hpp"
#include "Vector.hpp"

template<class A, class M>
struct TrieBuffer{
  size_t num_columns;
  std::vector<std::vector<Vector<DenseVector,void*,ParMemoryBuffer>>> buffers;
  A* anno;
  size_t num_anno;

  TrieBuffer<A,M>(
    const size_t num_columns_in, 
    const bool annotated_in){
    num_columns = num_columns_in;

    M* memoryBuffers = new M("",2);

    const size_t num_words = (BLOCK_SIZE >> ADDRESS_BITS_PER_WORD)+1;
    const size_t bytes_per_level =
      sizeof(Meta)+
      num_words*sizeof(uint64_t);

    for(size_t i = 0; i < num_columns; i++){
      const size_t num = pow(BLOCK_SIZE,i);
      std::vector<Vector<DenseVector,void*,ParMemoryBuffer>> b;
      for(size_t j = 0; j < num; j++){
        Vector<DenseVector,void*,ParMemoryBuffer> head(
          NUM_THREADS,
          memoryBuffers,
          bytes_per_level
        );
        Meta* m = head.get_meta();
        m->start = j*BLOCK_SIZE;
        m->end = ((j+1)*BLOCK_SIZE)-1;
        m->cardinality = 0;
        m->type = type::BITSET;
        b.push_back(head);
      }
      buffers.push_back(b);
    }

    const size_t anno_block = pow(BLOCK_SIZE,num_columns);
    anno = new A[anno_block];
    num_anno = anno_block;
  };

  inline Vector<DenseVector,BufferIndex,ParMemoryBuffer> copy(
    const size_t tid,
    ParMemoryBuffer* memoryBuffer){

    Vector<DenseVector,BufferIndex,ParMemoryBuffer> head(
      tid,
      memoryBuffer,
      buffers.at(0).at(0).get_this(),
      buffers.at(0).at(0).get_num_index_bytes(),
      buffers.at(0).at(0). template get_num_annotation_bytes<BufferIndex>()
    );

    head.foreach_index([&](const uint32_t index, const uint32_t data){
      Vector<DenseVector,float,ParMemoryBuffer> cur(
        tid,
        memoryBuffer,
        buffers.at(1).at(data%BLOCK_SIZE).get_this(),
        buffers.at(1).at(data%BLOCK_SIZE).get_num_index_bytes(),
        get_anno(data),
        buffers.at(1).at(data%BLOCK_SIZE). template get_num_annotation_bytes<A>()
      );
      head.set(index,data,cur.bufferIndex);
    });
    return head;
  };

  inline void zero(const std::vector<size_t>& offset){
    for(size_t i = 0; i < num_columns; i++){
      const size_t num = pow(BLOCK_SIZE,i);
      const size_t num_words = (BLOCK_SIZE >> ADDRESS_BITS_PER_WORD)+1;
      for(size_t j = 0; j < num; j++){
        Vector<DenseVector,void*,ParMemoryBuffer> cur = 
          buffers.at(i).at(j);
        uint8_t *data = cur.get_data();
        memset(data,0,num_words*sizeof(uint64_t));
        Meta* m = cur.get_meta();
        m->start = offset.at(i);
        m->end = offset.at(i)+BLOCK_SIZE-1;
        m->cardinality = 0;
        m->type = type::BITSET;
      }
    }
    memset(anno,0,num_anno*sizeof(A));
  };

  inline void print(const size_t I, const size_t J){
    for(size_t i = 0; i < num_anno; i+=BLOCK_SIZE){
      A* start = anno+i;
      for(size_t j = 0; j < BLOCK_SIZE; j++){
        std::cout << I*BLOCK_SIZE+(i/BLOCK_SIZE) << " " << J*BLOCK_SIZE+j << " " << start[j] << std::endl;
      }
    }
  };

  inline Vector<DenseVector,void*,ParMemoryBuffer> at(
    const size_t column, 
    const size_t index){

    return buffers.at(column).at(index%BLOCK_SIZE);
  };

  inline A* get_anno(
    const size_t column){
    return anno+column*BLOCK_SIZE;
  };
};

#endif