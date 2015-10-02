#ifndef _SSE_MASKS_H_
#define _SSE_MASKS_H_

namespace masks{
  inline void print_sse_register(__m128i reg){
    std::cout << "reg[0]: " << _mm_extract_epi32(reg,0) << std::endl;
    std::cout << "reg[1]: " << _mm_extract_epi32(reg,1) << std::endl;
    std::cout << "reg[2]: " << _mm_extract_epi32(reg,2) << std::endl;
    std::cout << "reg[3]: " << _mm_extract_epi32(reg,3) << std::endl << std::endl;   
  }
  
  static uint64_t find_mask[64];
  static inline int init_masks(){
    find_mask[0] = 0;
    for(size_t i = 1; i < 64; i++){
      find_mask[i] = ((uint64_t)1 << (i-(uint64_t)1)) | find_mask[i-1];
    }
    return 0;
  }
  //force init_masks to run before main
  static int m = init_masks();
  static const __m128i shuffle_mask32[16] = {        
    _mm_set_epi8(15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0), //0
    _mm_set_epi8(15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0), //1
    _mm_set_epi8(15,14,13,12,11,10,9,8,7,6,5,4,7,6,5,4), //2
    _mm_set_epi8(15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0), //3
    _mm_set_epi8(15,14,13,12,11,10,9,8,7,6,5,4,11,10,9,8), //4
    _mm_set_epi8(15,14,13,12,11,10,9,8,11,10,9,8,3,2,1,0), //5
    _mm_set_epi8(15,14,13,12,11,10,9,8,11,10,9,8,7,6,5,4), //6
    _mm_set_epi8(15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0), //7
    _mm_set_epi8(15,14,13,12,11,10,9,8,7,6,5,4,15,14,13,12), //8
    _mm_set_epi8(15,14,13,12,11,10,9,8,15,14,13,12,3,2,1,0), //9
    _mm_set_epi8(15,14,13,12,11,10,9,8,15,14,13,12,7,6,5,4), //10
    _mm_set_epi8(15,14,13,12,15,14,13,12,7,6,5,4,3,2,1,0), //11
    _mm_set_epi8(15,14,13,12,11,10,9,8,15,14,13,12,11,10,9,8), //12
    _mm_set_epi8(15,14,13,12,15,14,13,12,11,10,9,8,3,2,1,0), //13
    _mm_set_epi8(15,14,13,12,15,14,13,12,11,10,9,8,7,6,5,4), //14
    _mm_set_epi8(15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0), //15
  }; 
  
  static __m128i shuffle_mask16[256]; // precomputed dictionary

  static inline int getBitSD(uint32_t value, uint32_t position) {
    return ( ( value & (1 << position) ) >> position);
  }
  static inline void prepare_shuffling_dictionary16() {
    //Number of bits that can possibly be set are the lower 8
    for(uint32_t i = 0; i < 256; i++) { // 2^8 possibilities we need to store masks for
      uint32_t counter = 0;
      unsigned char permutation[16];
      memset(permutation, 0xFF, sizeof(permutation));
      for(unsigned char b = 0; b < 8; b++) { //Check each possible bit that can be set 1-by-1
        if(getBitSD(i, b)) {
          permutation[counter++] = 2*b; //tell us byte offset to std::get from comparison vector
          permutation[counter++] = 2*b + 1; //tells us byte offset to std::get from comparison vector
        }
      }
      __m128i mask = _mm_loadu_si128((const __m128i*)permutation);
      shuffle_mask16[i] = mask;
    }
  }
}
#endif
