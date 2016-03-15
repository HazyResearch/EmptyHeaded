#ifndef _INTERSECT_UINTEGER_H_
#define _INTERSECT_UINTEGER_H_

namespace ops{
  //Code adopted from Daniel Lemire.
  //https://github.com/lemire/SIMDCompressionAndIntersection/blob/master/src/intersection.cpp
  /**
   * This is often called galloping or exponential search.
   *
   * Used by frogintersectioncardinality below
   *
   * Based on binary search...
   * Find the smallest integer larger than pos such
   * that array[pos]>= min.
   * If none can be found, return array.length.
   * From code by O. Kaser.
   */
  static size_t __frogadvanceUntil(
    const uint32_t * const array, 
    const size_t pos,
    const size_t length, 
    const size_t min) {
      size_t lower = pos + 1;

      // special handling for a possibly common sequential case
      if ((lower >= length) or (array[lower] >= min)) {
          return lower;
      }

      size_t spansize = 1; // could set larger
      // bootstrap an upper limit

      while ((lower + spansize < length) and (array[lower + spansize] < min))
          spansize *= 2;
      size_t upper = (lower + spansize < length) ? lower + spansize : length - 1;

      if (array[upper] < min) {// means array has no item >= min
          return length;
      }

      // we know that the next-smallest span was too small
      lower += (spansize / 2);

      // else begin binary search
      size_t mid = 0;
      while (lower + 1 != upper) {
          mid = (lower + upper) / 2;
          if (array[mid] == min) {
              return mid;
          } else if (array[mid] < min)
              lower = mid;
          else
              upper = mid;
      }
      return upper;

  }
  
  inline void scalar_gallop(
      uint32_t *out, 
      const uint32_t * const smallset, 
      const size_t smalllength, 
      const uint32_t * const largeset,
      const size_t largelength){

      size_t count = 0;
      if(largelength < smalllength){
        return scalar_gallop(
          out,
          largeset,
          largelength,
          smallset,
          smalllength);
      }
      if (0 == smalllength){
        return;
      }

      size_t k1 = 0, k2 = 0;
      while (true) {
          if (largeset[k1] < smallset[k2]) {
              k1 = __frogadvanceUntil(largeset, k1, largelength, smallset[k2]);
              if (k1 == largelength)
                  break;
          }
  midpoint:
          if (smallset[k2] < largeset[k1]) {
              ++k2;
              if (k2 == smalllength)
                  break;
          } else {
              *out++  = smallset[k2];
              ++count;
              ++k2;
              if (k2 == smalllength)
                  break;
              k1 = __frogadvanceUntil(largeset, k1, largelength, smallset[k2]);
              if (k1 == largelength)
                  break;
              goto midpoint;
          }
      }
  }

  /**
   * Fast scalar scheme designed by N. Kurz.
   */
  inline size_t scalar(
    uint32_t *out, 
    const uint32_t *A, 
    const size_t lenA,
    const uint32_t *B, 
    const size_t lenB) {
      if (lenA == 0 || lenB == 0)
          return 0;

      const uint32_t *endA = A + lenA;
      const uint32_t *endB = B + lenB;
      size_t count = 0;

      while (1) {
          while (*A < *B) {
  SKIP_FIRST_COMPARE:
              if (++A == endA){
                return count;
              }
          }
          while (*A > *B) {
              if (++B == endB){
                return count;
              }
          }
          if (*A == *B) {
              *out++ = *A;
              ++count;
              if (++A == endA || ++B == endB){
                return count;
              }
          } else {
              goto SKIP_FIRST_COMPARE;
          }
      }
      return count; // NOTREACHED
  }

  inline size_t set_intersect_shuffle(
    Meta* meta,
    uint32_t * C, 
    const uint32_t * const A,
    const size_t s_a,
    const uint32_t * const B,
    const size_t s_b){

    size_t count = 0;
    size_t i_a = 0, i_b = 0;

    *C = 0;
    //assert( ((size_t) A % 16) == 0 );
    //assert( ((size_t) B % 16) == 0 );

    // trim lengths to be a multiple of 4
    #if VECTORIZE == 1
    size_t st_a = (s_a / 4) * 4;
    size_t st_b = (s_b / 4) * 4;

    while(i_a < st_a && i_b < st_b) {
      //[ load segments of four 32-bit elements
      __m128i v_a = _mm_loadu_si128((__m128i*)&A[i_a]);
      __m128i v_b = _mm_loadu_si128((__m128i*)&B[i_b]);
      //]
      //[ compute mask of common elements
      const uint32_t right_cyclic_shift = _MM_SHUFFLE(0,3,2,1);

      __m128i cmp_mask1 = _mm_cmpeq_epi32(v_a, v_b);    // pairwise comparison
      v_b = _mm_shuffle_epi32(v_b, right_cyclic_shift);       // shuffling

      __m128i cmp_mask2 = _mm_cmpeq_epi32(v_a, v_b);    // again...
      const uint32_t left_cyclic_shift = _MM_SHUFFLE(2,1,0,3);
      __m128i cmp_mask2b = _mm_shuffle_epi32(cmp_mask2, left_cyclic_shift);
      v_b = _mm_shuffle_epi32(v_b, right_cyclic_shift);

      __m128i cmp_mask3 = _mm_cmpeq_epi32(v_a, v_b);    // and again...
      const uint32_t left_cyclic_shift2 = _MM_SHUFFLE(1,0,3,2);
      __m128i cmp_mask3b = _mm_shuffle_epi32(cmp_mask3, left_cyclic_shift2);
      v_b = _mm_shuffle_epi32(v_b, right_cyclic_shift);

      __m128i cmp_mask4 = _mm_cmpeq_epi32(v_a, v_b);    // and again.
      const uint32_t left_cyclic_shift3 = _MM_SHUFFLE(0,3,2,1);
      __m128i cmp_mask4b = _mm_shuffle_epi32(cmp_mask4, left_cyclic_shift3);
      __m128i cmp_mask = _mm_or_si128(
              _mm_or_si128(cmp_mask1, cmp_mask2),
              _mm_or_si128(cmp_mask3, cmp_mask4)
      ); // OR-ing of comparison masks
      __m128i cmp_mask_b = _mm_or_si128(
              _mm_or_si128(cmp_mask1, cmp_mask2b),
              _mm_or_si128(cmp_mask3b, cmp_mask4b)
      ); // OR-ing of comparison masks

      // convert the 128-bit mask to the 4-bit mask
      const uint32_t mask = _mm_movemask_ps((__m128)cmp_mask);
      const uint32_t mask_b = _mm_movemask_ps((__m128)cmp_mask_b);
      //]

      __m128i a_offset = _mm_set1_epi32(i_a);
      __m128i b_offset = _mm_set1_epi32(i_b);
      
      a_offset = _mm_add_epi32(_mm_shuffle_epi8(_mm_set_epi32(3,2,1,0),masks::shuffle_mask32[mask]),a_offset);
      b_offset = _mm_add_epi32(_mm_shuffle_epi8(_mm_set_epi32(3,2,1,0),masks::shuffle_mask32[mask_b]),b_offset);

      const __m128i p = _mm_shuffle_epi8(v_a, masks::shuffle_mask32[mask]);
      const size_t num_hit =_mm_popcnt_u32(mask);
      _mm_storeu_si128((__m128i*)C, p);
      C += num_hit;
      count += num_hit;

      //advance pointers
      const uint32_t a_max = A[i_a+3];
      const uint32_t b_max = B[i_b+3];

      i_a += (a_max <= b_max) * 4;
      i_b += (a_max >= b_max) * 4;
    }
    #endif

    // intersect the tail using scalar intersection
    count += scalar(C,&A[i_a],s_a-i_a,&B[i_b],s_b-i_b);
    C += count;

    meta->cardinality = count;
    meta->start = *(C-count);
    meta->end = *(C-1);
    meta->type = type::UINTEGER;
    return count;  
  }

  /**
   * This is the SIMD galloping function. This intersection function works well
   * when lenRare and lenFreq have vastly different values.
   *
   * It assumes that lenRare <= lenFreq.
   *
   * Note that this is not symmetric: flipping the rare and freq pointers
   * as well as lenRare and lenFreq could lead to significant performance
   * differences.
   *
   * The matchOut pointer can safely be equal to the rare pointer.
   *
   * This function DOES NOT use assembly. It only relies on intrinsics.
   */
  inline size_t set_intersect_galloping(
      Meta * meta,
      uint32_t * out,
      const uint32_t *rare,
      const size_t lenRare,
      const uint32_t *freq,
      const size_t lenFreq
    ){
      size_t count = 0;

      *out = 0;
      assert(lenRare <= lenFreq);
      typedef __m128i vec;
      const uint32_t veclen = sizeof(vec) / sizeof(uint32_t);
      const size_t vecmax = veclen - 1;
      const size_t freqspace = 32 * veclen;
      const size_t rarespace = 1;

      const uint32_t *stopFreq = freq + lenFreq - freqspace;
      const uint32_t *stopRare = rare + lenRare - rarespace;
      if (freq > stopFreq) {
        const size_t final_count = scalar(out,freq,lenFreq,rare,lenRare);
        
        meta->cardinality = final_count;
        meta->start = *out;
        meta->end = *(out+final_count-1);
        meta->type = type::UINTEGER;
        return final_count;
      }
      for (; rare < stopRare; ++rare) {
          const uint32_t matchRare = *rare;//nextRare;
          const vec Match = _mm_set1_epi32(matchRare);

          if (freq[veclen * 31 + vecmax] < matchRare) { // if no match possible
              uint32_t offset = 1;
              if (freq + veclen  * 32 > stopFreq) {
                  freq += veclen * 32;
                  goto FINISH_SCALAR;
              }
              while (freq[veclen * offset * 32 + veclen * 31 + vecmax]
                     < matchRare) { // if no match possible
                  if (freq + veclen * (2 * offset) * 32 <= stopFreq) {
                      offset *= 2;
                  } else if (freq + veclen * (offset + 1) * 32 <= stopFreq) {
                      offset = static_cast<uint32_t>((stopFreq - freq) / (veclen * 32));
                      //offset += 1;
                      if (freq[veclen * offset * 32 + veclen * 31 + vecmax]
                          < matchRare) {
                          freq += veclen * offset * 32;
                          goto FINISH_SCALAR;
                      } else {
                          break;
                      }
                  } else {
                      freq += veclen * offset * 32;
                      goto FINISH_SCALAR;
                  }
              }
              uint32_t lower = offset / 2;
              while (lower + 1 != offset) {
                  const uint32_t mid = (lower + offset) / 2;
                  if (freq[veclen * mid * 32 + veclen * 31 + vecmax]
                      < matchRare)
                      lower = mid;
                  else
                      offset = mid;
              }
              freq += veclen * offset * 32;
          }
          vec Q0, Q1, Q2, Q3;
          if (freq[veclen * 15 + vecmax] >= matchRare) {
              if (freq[veclen * 7 + vecmax] < matchRare) {
                  //there are 16 SSE registers in AVX architecture, we use 12 here + 1 + 1 + 1
                  //if the compiler is good this code should just use 15 registers (could spill though)
                  //these are all vectors we are comparing can get index from that (+8 = +8 vectors)
                 //Match, r0-r7, Q0-Q3, lr Match (8+4+2)
                  const size_t offset = 8;
                  __m128i lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset);
                  const __m128i r0 = _mm_cmpeq_epi32(lr, Match);
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 1);
                  const __m128i r1 = _mm_cmpeq_epi32(lr, Match);
                  Q0 = _mm_or_si128(r0,r1);
                  
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 2);
                  const __m128i r2 = _mm_cmpeq_epi32(lr, Match);
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 3);
                  const __m128i r3 = _mm_cmpeq_epi32(lr, Match);
                  Q1 = _mm_or_si128(r2,r3);

                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 4);
                  const __m128i r4 = _mm_cmpeq_epi32(lr, Match);
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 5);
                  const __m128i r5 = _mm_cmpeq_epi32(lr, Match);
                  Q2 = _mm_or_si128(r4,r5);

                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 6);
                  const __m128i r6 = _mm_cmpeq_epi32(lr, Match);
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 7);
                  const __m128i r7 = _mm_cmpeq_epi32(lr, Match);
                  Q3 = _mm_or_si128(r6,r7);

                  lr = _mm_or_si128(Q0, Q1);
                  const vec F0 = _mm_or_si128(lr, _mm_or_si128(Q2, Q3));
        
                  if (_mm_testz_si128(F0, F0) == 0) {
                    *out++ = matchRare; 
                    ++count;
                  } 
              } else {
                  //there are 16 SSE registers in AVX architecture, we use 12 here + 1 + 1 + 1
                  //if the compiler is good this code should just use 15 registers (could spill though)
                  //these are all vectors we are comparing can get index from that (+8 = +8 vectors)
                 //Match, r0-r7, Q0-Q3, lr Match (8+4+2)
                  const size_t offset = 0;
                  __m128i lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset);
                  const __m128i r0 = _mm_cmpeq_epi32(lr, Match);
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 1);
                  const __m128i r1 = _mm_cmpeq_epi32(lr, Match);
                  Q0 = _mm_or_si128(r0,r1);
                  
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 2);
                  const __m128i r2 = _mm_cmpeq_epi32(lr, Match);
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 3);
                  const __m128i r3 = _mm_cmpeq_epi32(lr, Match);
                  Q1 = _mm_or_si128(r2,r3);

                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 4);
                  const __m128i r4 = _mm_cmpeq_epi32(lr, Match);
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 5);
                  const __m128i r5 = _mm_cmpeq_epi32(lr, Match);
                  Q2 = _mm_or_si128(r4,r5);

                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 6);
                  const __m128i r6 = _mm_cmpeq_epi32(lr, Match);
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 7);
                  const __m128i r7 = _mm_cmpeq_epi32(lr, Match);
                  Q3 = _mm_or_si128(r6,r7);

                  lr = _mm_or_si128(Q0, Q1);
                  const vec F0 = _mm_or_si128(lr, _mm_or_si128(Q2, Q3));
        
                  if (_mm_testz_si128(F0, F0) == 0) {
                    *out++ = matchRare; 
                    ++count;
                  }  
              }
          } else {
              if (freq[veclen * 23 + vecmax] < matchRare) {
                  //there are 16 SSE registers in AVX architecture, we use 12 here + 1 + 1 + 1
                  //if the compiler is good this code should just use 15 registers (could spill though)
                  //these are all vectors we are comparing can get index from that (+8 = +8 vectors)
                 //Match, r0-r7, Q0-Q3, lr Match (8+4+2)
                  const size_t offset = 24;
                  __m128i lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset);
                  const __m128i r0 = _mm_cmpeq_epi32(lr, Match);
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 1);
                  const __m128i r1 = _mm_cmpeq_epi32(lr, Match);
                  Q0 = _mm_or_si128(r0,r1);
                  
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 2);
                  const __m128i r2 = _mm_cmpeq_epi32(lr, Match);
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 3);
                  const __m128i r3 = _mm_cmpeq_epi32(lr, Match);
                  Q1 = _mm_or_si128(r2,r3);

                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 4);
                  const __m128i r4 = _mm_cmpeq_epi32(lr, Match);
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 5);
                  const __m128i r5 = _mm_cmpeq_epi32(lr, Match);
                  Q2 = _mm_or_si128(r4,r5);

                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 6);
                  const __m128i r6 = _mm_cmpeq_epi32(lr, Match);
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 7);
                  const __m128i r7 = _mm_cmpeq_epi32(lr, Match);
                  Q3 = _mm_or_si128(r6,r7);

                  lr = _mm_or_si128(Q0, Q1);
                  const vec F0 = _mm_or_si128(lr, _mm_or_si128(Q2, Q3));
        
                  if (_mm_testz_si128(F0, F0) == 0) {
                    *out++ = matchRare; 
                    ++count;
                  } 
              } else {
                  //there are 16 SSE registers in AVX architecture, we use 12 here + 1 + 1 + 1
                  //if the compiler is good this code should just use 15 registers (could spill though)
                  //these are all vectors we are comparing can get index from that (+8 = +8 vectors)
                 //Match, r0-r7, Q0-Q3, lr Match (8+4+2)
                  const size_t offset = 16;
                  __m128i lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset);
                  const __m128i r0 = _mm_cmpeq_epi32(lr, Match);
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 1);
                  const __m128i r1 = _mm_cmpeq_epi32(lr, Match);
                  Q0 = _mm_or_si128(r0,r1);
                  
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 2);
                  const __m128i r2 = _mm_cmpeq_epi32(lr, Match);
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 3);
                  const __m128i r3 = _mm_cmpeq_epi32(lr, Match);
                  Q1 = _mm_or_si128(r2,r3);

                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 4);
                  const __m128i r4 = _mm_cmpeq_epi32(lr, Match);
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 5);
                  const __m128i r5 = _mm_cmpeq_epi32(lr, Match);
                  Q2 = _mm_or_si128(r4,r5);

                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 6);
                  const __m128i r6 = _mm_cmpeq_epi32(lr, Match);
                  lr = _mm_loadu_si128(reinterpret_cast<const vec *>(freq) + offset + 7);
                  const __m128i r7 = _mm_cmpeq_epi32(lr, Match);
                  Q3 = _mm_or_si128(r6,r7);

                  lr = _mm_or_si128(Q0, Q1);
                  const vec F0 = _mm_or_si128(lr, _mm_or_si128(Q2, Q3));
        
                  if (_mm_testz_si128(F0, F0) == 0) {
                    *out++ = matchRare; 
                    ++count;
                  } 
              }

          }
      }

  FINISH_SCALAR: 
    const size_t final_count = count + scalar(
      out,
      rare, 
      stopRare + rarespace - rare, 
      freq,
      stopFreq + freqspace - freq);
    meta->cardinality = final_count;
    meta->start = *out;
    meta->end = *(out+final_count-1);
    meta->type = type::UINTEGER;
    return final_count;
  }

}
#endif