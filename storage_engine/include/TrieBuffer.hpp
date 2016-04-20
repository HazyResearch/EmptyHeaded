/******************************************************************************
*
* Author: Christopher R. Aberger
*
******************************************************************************/
#ifndef _TRIEBUFFER_H_
#define _TRIEBUFFER_H_

#include "utils/utils.hpp"
#include "Vector.hpp"

/*
The union actually helps us take care of block sizes that
do not fit into the BLOCK_SIZE*BLOCK_SIZE chunk we might be computing
over. We will just compute and copy the part that we need.
*/
  
template<class A>
std::tuple<A*,size_t> tb_alloc_anno(
  ParMemoryBuffer* memoryBuffers,
  const size_t b_size, 
  const size_t num_columns)
{
  const size_t anno_block = pow(b_size,num_columns);
  A* anno = (A*)memoryBuffers->anno->get_next(anno_block*sizeof(A));
  return std::tuple<A*,size_t>(anno,anno_block);
}

template<>
std::tuple<void**,size_t> tb_alloc_anno(
  ParMemoryBuffer* memoryBuffers,
  const size_t b_size, 
  const size_t num_columns)
{
  (void) b_size; (void) num_columns;
  return std::tuple<void**,size_t>((void**)NULL,0);
}

template<class A>
struct TrieBuffer{
  size_t num_columns;
  std::vector<std::vector<Vector<BLASVector,void*,ParMemoryBuffer>>> buffers;
  A* anno;
  size_t num_anno;

  TrieBuffer<A>(
    const size_t num_columns_in){
    num_columns = num_columns_in;

    ParMemoryBuffer* memoryBuffers = new ParMemoryBuffer("",2);

    const size_t num_words = (BLOCK_SIZE >> ADDRESS_BITS_PER_WORD)+1;
    const size_t bytes_per_level =
      sizeof(size_t)+
      sizeof(Meta)+
      num_words*sizeof(uint64_t);

    for(size_t i = 0; i < num_columns; i++){
      const size_t num = pow(BLOCK_SIZE,i);
      std::vector<Vector<BLASVector,void*,ParMemoryBuffer>> b;
      for(size_t j = 0; j < num; j++){
        Vector<BLASVector,void*,ParMemoryBuffer> head(
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

    auto tup = tb_alloc_anno<A>(memoryBuffers,BLOCK_SIZE,num_columns);
    anno = std::get<0>(tup);
    num_anno = std::get<1>(tup);
  };

  //need to have a copy for void* 
  //copy into 1-level buffer copy into EHVector
  inline Vector<EHVector,BufferIndex,ParMemoryBuffer> copy(
    const size_t tid,
    ParMemoryBuffer* memoryBuffer){

    Vector<EHVector,BufferIndex,ParMemoryBuffer> head(
      tid,
      memoryBuffer,
      (uint8_t*)buffers.at(0).at(0).get_meta(),
      buffers.at(0).at(0).get_num_index_bytes()-sizeof(size_t),
      buffers.at(0).at(0). template get_num_annotation_bytes<BufferIndex>()
    );

    head.foreach_index([&](const uint32_t index, const uint32_t data){
      const size_t anno_offset = *(size_t*)buffers.at(1).at(data%BLOCK_SIZE).get_this();
      Vector<BLASVector,A,ParMemoryBuffer> cur(
        tid,
        memoryBuffer,
        (uint8_t*)buffers.at(1).at(data%BLOCK_SIZE).get_this(),
        buffers.at(1).at(data%BLOCK_SIZE).get_num_index_bytes(),
        anno+anno_offset,
        buffers.at(1).at(data%BLOCK_SIZE).get_num_annotation_bytes<A>()
      );
      head.set(index,data,cur.bufferIndex);
    });
    return head;
  };

  inline void zero(
    const std::vector<size_t>& dimensions,
    const std::vector<size_t>& offset){
    for(size_t i = 0; i < num_columns; i++){
      const size_t num = pow(BLOCK_SIZE,i);
      const size_t num_words = (BLOCK_SIZE >> ADDRESS_BITS_PER_WORD)+1;
      size_t anno_offset2 = 0;
      for(size_t j = i; j > 0; j--){
        anno_offset2 += dimensions.at(j)*BLOCK_SIZE*offset.at(j-1);
      }
      for(size_t j = 0; j < num; j++){
        Vector<BLASVector,void*,ParMemoryBuffer> cur = 
          buffers.at(i).at(j);
        uint8_t *data = cur.get_index_data();
        memset(data,0,num_words*sizeof(uint64_t));
        Meta* m = cur.get_meta();
        m->start = (offset.at(i)*BLOCK_SIZE);
        m->end = (offset.at(i)*BLOCK_SIZE)+BLOCK_SIZE-1;
        m->cardinality = 0;
        m->type = type::BITSET;
        const size_t anno_offset = anno_offset2+(offset.at(i)*BLOCK_SIZE)+(j*dimensions.at(i));
        *(size_t*)cur.get_this() = anno_offset;
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

  inline Vector<BLASVector,void*,ParMemoryBuffer> at(
    const size_t column, 
    const size_t index){

    return buffers.at(column).at(index%BLOCK_SIZE);
  };

  inline A* get_anno(
    const size_t column){
    return anno+column*BLOCK_SIZE;
  };
}; 

//need to have a copy for void*
template<> 
inline Vector<EHVector,BufferIndex,ParMemoryBuffer> TrieBuffer<void*>::copy(
  const size_t tid,
  ParMemoryBuffer* memoryBuffer){
  Vector<EHVector,BufferIndex,ParMemoryBuffer> head(
    tid,
    memoryBuffer,
    (uint8_t*)buffers.at(0).at(0).get_meta(),
    buffers.at(0).at(0).get_num_index_bytes()-sizeof(size_t),
    buffers.at(0).at(0). template get_num_annotation_bytes<BufferIndex>()
  );

  head.foreach_index([&](const uint32_t index, const uint32_t data){
    Vector<BLASVector,void*,ParMemoryBuffer> cur(
      tid,
      memoryBuffer,
      (uint8_t*)buffers.at(1).at(data%BLOCK_SIZE).get_this(),
      buffers.at(1).at(data%BLOCK_SIZE).get_num_index_bytes(),
      0
    );
    head.set(index,data,cur.bufferIndex);
  });
  return head;
}

#endif