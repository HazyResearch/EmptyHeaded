#include "emptyheaded.hpp"

int main (int argc, char* argv[]) {

  typedef block_bitset return_type;
  typedef range_bitset type1;
  typedef block_bitset type2;

  const size_t a_size = 3000;
  uint32_t *a = new uint32_t[a_size];
  for(size_t i = 0; i < a_size; i++){
    a[i] = i;
  }

  uint32_t b[10] = {0,50,78,89,90,91,96,613,650,768};

  uint8_t *a_data = new uint8_t[a_size*10*sizeof(uint64_t)];
  uint8_t *b_data = new uint8_t[10*10*sizeof(uint64_t)];

  Set<type1> as = Set<type1>::from_array(a_data,a,a_size);
  Set<type2> bs = Set<type2>::from_array(b_data,b,10);

  size_t count = ops::set_intersect(&as,&bs,[&](uint32_t data, uint32_t a_index, uint32_t b_index){
    std::cout << "Data: " << data << " a_i: " << a_index << " b_i: " << b_index << std::endl;
    return 1; 
  });
  std::cout << count << std::endl;

  uint8_t *c_data = new uint8_t[10*10*sizeof(uint64_t)];
  Set<return_type> cs(c_data);

  cs = ops::set_intersect(&cs,&as,&bs,[&](uint32_t data, uint32_t a_index, uint32_t b_index){
    std::cout << "Data: " << data << " a_i: " << a_index << " b_i: " << b_index << std::endl;
    return 1; 
  })->cardinality;
  std::cout << count << std::endl;

  std::cout << "OUTPUT SET" << std::endl;
  cs.foreach([&](uint32_t data){
    std::cout << "Data: " << data << std::endl;
  });

  return 0;
}