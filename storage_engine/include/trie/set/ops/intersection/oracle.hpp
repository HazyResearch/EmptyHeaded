#ifndef _ORACLE_H_
#define _ORACLE_H_

namespace oracle{

  template<class T, class R>
  size_t bring_into_cache(const Set<T> *A_in, const Set<R> *B_in){
    uint32_t count = 0;
    //std::cout << "A: " << std::endl;
    count += A_in->cardinality;
    count += A_in->number_of_bytes;
    A_in->foreach([&](uint32_t data){
      //std::cout << data << std::endl;
      count += data;
    });
    count += B_in->cardinality;
    count += B_in->number_of_bytes;
    //std::cout << "B: " << std::endl;
    B_in->foreach([&](uint32_t data){
      //std::cout << data << std::endl;
      count += data;
    });
    return count;
  }

  template<class T, class R>
  std::pair<double,size_t> run_intersection(Set<T> *C_in, const Set<T> *A_in, const Set<R> *B_in){
    bring_into_cache<T,R>(A_in,B_in);
    auto sc = debug::start_clock();
    ops::set_intersect(C_in,A_in,B_in);
    auto timer = debug::stop_clock(sc);
    return std::make_pair(timer,C_in->cardinality);
  }

  std::tuple<double,double,size_t> set_intersect(Set<uinteger> *C_in, const Set<uinteger> *A_in, const Set<uinteger> *B_in,
    uint8_t *A_buffer, uint8_t *B_buffer){

    //////////////////////////////////////////////////////////
    //uint/uint
    auto uint_uint = run_intersection<uinteger,uinteger>(C_in,A_in,B_in);
    //////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////
    //rb/uint
    Set<range_bitset> rb_a = Set<range_bitset>::from_array(A_buffer,(uint32_t*)A_in->data,A_in->cardinality);
    auto rb_uint = run_intersection<uinteger,range_bitset>(C_in,B_in,&rb_a);
    //////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////
    //bb/uint
    Set<block_bitset> bb_a = Set<block_bitset>::from_array(A_buffer,(uint32_t*)A_in->data,A_in->cardinality);
    auto bb_uint = run_intersection<uinteger,block_bitset>(C_in,B_in,&bb_a);
    //////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////
    //uint/rb
    Set<range_bitset> rb_b = Set<range_bitset>::from_array(B_buffer,(uint32_t*)B_in->data,B_in->cardinality);
    auto uint_rb = run_intersection<uinteger,range_bitset>(C_in,A_in,&rb_b);
    //////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////
    //uint/bb
    Set<block_bitset> bb_b = Set<block_bitset>::from_array(B_buffer,(uint32_t*)B_in->data,B_in->cardinality);
    auto uint_bb = run_intersection<uinteger,block_bitset>(C_in,A_in,&bb_b);
    //////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////
    //rb/bb
    rb_a = Set<range_bitset>::from_array(A_buffer,(uint32_t*)A_in->data,A_in->cardinality);
    bb_b = Set<block_bitset>::from_array(B_buffer,(uint32_t*)B_in->data,B_in->cardinality);
    auto rb_bb = run_intersection<block_bitset,range_bitset>((Set<block_bitset>*)C_in,&bb_b,&rb_a);
    //////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////
    //rb/bb
    bb_a = Set<block_bitset>::from_array(A_buffer,(uint32_t*)A_in->data,A_in->cardinality);
    rb_b = Set<range_bitset>::from_array(B_buffer,(uint32_t*)B_in->data,B_in->cardinality);
    auto bb_rb = run_intersection<block_bitset,range_bitset>((Set<block_bitset>*)C_in,&bb_a,&rb_b);
    //////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////
    //bb/bb
    bb_a = Set<block_bitset>::from_array(A_buffer,(uint32_t*)A_in->data,A_in->cardinality);
    bb_b = Set<block_bitset>::from_array(B_buffer,(uint32_t*)B_in->data,B_in->cardinality);
    auto bb_bb = run_intersection<block_bitset,block_bitset>((Set<block_bitset>*)C_in,&bb_a,&bb_b);
    //////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////
    //bb/bb
    rb_a = Set<range_bitset>::from_array(A_buffer,(uint32_t*)A_in->data,A_in->cardinality);
    rb_b = Set<range_bitset>::from_array(B_buffer,(uint32_t*)B_in->data,B_in->cardinality);
    auto rb_rb = run_intersection<range_bitset,range_bitset>((Set<range_bitset>*)C_in,&rb_a,&rb_b);
    //////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////
    //b/b
    Set<block> b_a = Set<block>::from_array(A_buffer,(uint32_t*)A_in->data,A_in->cardinality);
    Set<block> b_b = Set<block>::from_array(B_buffer,(uint32_t*)B_in->data,B_in->cardinality);
    auto bl_bl = run_intersection<block,block>((Set<block>*)C_in,&b_b,&b_a);
    //////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////
    //hybrid/hybrid
    Set<hybrid> o_a = Set<hybrid>::from_array(A_buffer,(uint32_t*)A_in->data,A_in->cardinality);
    Set<hybrid> o_b = Set<hybrid>::from_array(B_buffer,(uint32_t*)B_in->data,B_in->cardinality);
    auto optimizer = run_intersection<hybrid,hybrid>((Set<hybrid>*)C_in,&o_a,&o_b);
    //////////////////////////////////////////////////////////

    //uint_uint,rb_uint,bb_uint,uint_rb,uint_bb,rb_bb,bb_rb,bb_bb,rb_rb,bl_bl
    std::vector<double> times;
    times.push_back(uint_uint.first);
    times.push_back(rb_uint.first);
    times.push_back(bb_uint.first);
    times.push_back(uint_rb.first);
    times.push_back(uint_bb.first);
    times.push_back(rb_bb.first);
    times.push_back(bb_rb.first);
    times.push_back(bb_bb.first);
    times.push_back(rb_rb.first);
    times.push_back(bl_bl.first);

    assert(uint_uint.second == rb_uint.second);
    assert(uint_uint.second == bb_uint.second);
    assert(uint_uint.second == uint_rb.second);
    assert(uint_uint.second == uint_bb.second);
    assert(uint_uint.second == rb_bb.second);
    assert(uint_uint.second == bb_rb.second); 
    assert(uint_uint.second == bb_bb.second);
    assert(uint_uint.second == rb_rb.second);
    assert(uint_uint.second == bl_bl.second);
    assert(uint_uint.second == optimizer.second);

    double min = *std::min_element(times.begin(),times.end());

    return std::make_tuple(min,optimizer.first,uint_uint.second);

  }
}

#endif
