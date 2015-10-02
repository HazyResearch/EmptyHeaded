#ifndef _UINTEGER_INTERSECTION_H_
#define _UINTEGER_INTERSECTION_H_

#define GALLOP_SIZE 16
#define VEC_T __m128i
#define COMPILER_LIKELY(x)     __builtin_expect((x),1)
#define COMPILER_RARELY(x)     __builtin_expect((x),0)
/**
 * The following macros (VEC_OR, VEC_ADD_PTEST,VEC_CMP_EQUAL,VEC_SET_ALL_TO_INT,VEC_LOAD_OFFSET,
 * ASM_LEA_ADD_BYTES are only used in the v1 procedure below.
 */
#define VEC_OR(dest, other)                                             \
    __asm volatile("por %1, %0" : "+x" (dest) : "x" (other) )

// // decltype is C++ and typeof is C
#define VEC_ADD_PTEST(var, add, xmm)      {                             \
        decltype(var) _new = var + add;                                   \
        __asm volatile("ptest %2, %2\n\t"                           \
                           "cmovnz %1, %0\n\t"                          \
                           : /* writes */ "+r" (var)                    \
                           : /* reads */  "r" (_new), "x" (xmm)         \
                           : /* clobbers */ "cc");                      \
    }


#define VEC_CMP_EQUAL(dest, other)                                      \
    __asm volatile("pcmpeqd %1, %0" : "+x" (dest) : "x" (other))

#define VEC_SET_ALL_TO_INT(reg, int32)                                  \
    __asm volatile("movd %1, %0; pshufd $0, %0, %0"                 \
                       : "=x" (reg) : "g" (int32) )

#define VEC_LOAD_OFFSET(xmm, ptr, bytes)                    \
    __asm volatile("movdqu %c2(%1), %0" : "=x" (xmm) :  \
                   "r" (ptr), "i" (bytes))

#define ASM_LEA_ADD_BYTES(ptr, bytes)                            \
    __asm volatile("lea %c1(%0), %0\n\t" :                       \
                   /* reads/writes %0 */  "+r" (ptr) :           \
                   /* reads */ "i" (bytes));

namespace ops{
  struct no_check{
    static inline size_t check_registers(const __m128i Q0, const __m128i Q1){
      (void) Q0; (void) Q1;
      return 0;
    }
    static inline size_t check_registers(const __m128i Q0, const __m128i Q1, 
      const __m128i Q2, const __m128i Q3, 
      const __m128i r0, const __m128i r1, const __m128i r2, const __m128i r3, 
      const __m128i r4, const __m128i r5, const __m128i r6, const __m128i r7){
      (void) Q0; (void) Q1; (void) Q2; (void) Q3; 
      (void) r0; (void) r1; (void) r2; (void) r3; 
      (void) r4; (void) r5; (void) r6; (void) r7; 
      return 0;
    }
  };
  struct unpack_aggregate:no_check{
    static inline uint32_t range(uint32_t *C, size_t cardinality){
      (void) C; (void) cardinality;
      return 0;
    }
    static inline uint32_t* advanceC(uint32_t *C, size_t amount){
      (void) C; (void) amount;
      return NULL;
    }
    template <typename F>
    static inline size_t scalar(const uint32_t x, uint32_t *w, F f, const uint32_t i_a, const uint32_t i_b){
      (void) x; (void) w; (void) f; (void) i_a; (void) i_b;
      return 1;
    }
    template<typename F>
    static inline size_t unpack(const __m128i x, uint32_t *w, 
      const size_t num, F f, const __m128i i_a, const __m128i i_b){
      (void) i_a; (void) i_b;
      (void) x; (void) num; (void) f; (void) w;

      return num;
    }
  };
  struct unpack_materialize:no_check{
    static inline uint32_t range(uint32_t *C, size_t cardinality){
      return (cardinality > 0) ? C[cardinality-1]-C[0] : 0;
    }
    static inline uint32_t* advanceC(uint32_t *C, size_t amount){
      return C+amount;
    }
    template <typename F>
    static inline size_t scalar(const uint32_t x, uint32_t *w, F f, const uint32_t i_a, const uint32_t i_b){
      (void) i_a; (void) i_b; (void) f;
      *w = x;
      return 1;
    }
    template<typename F>
    static inline size_t unpack(const __m128i x, uint32_t *w, 
      const size_t num, F f, const __m128i i_a, const __m128i i_b){
      (void) i_a; (void) i_b; (void) f;
      _mm_storeu_si128((__m128i*)w, x);
      return num;
    }
  };
  struct check{
    static inline size_t check_registers(const __m128i Q0, const __m128i Q1, 
      const __m128i Q2, const __m128i Q3, 
      const __m128i r0, const __m128i r1, const __m128i r2, const __m128i r3, 
      const __m128i r4, const __m128i r5, const __m128i r6, const __m128i r7){

      (void) Q3;
      const size_t mask_array[16] = {0,0,1,0,2,0,0,0,3,0,0,0,0,0,0,0};

      if (_mm_testz_si128(Q0, Q0) == 0) {
        if(_mm_testz_si128(r0, r0) == 0){
          const uint32_t mask = _mm_movemask_ps((__m128)r0);
          return mask_array[mask]+4*0;
        } else{
          const uint32_t mask = _mm_movemask_ps((__m128)r1);
          return mask_array[mask]+4*1;
        }
      } else if(_mm_testz_si128(Q1, Q1) == 0){
        if(_mm_testz_si128(r2, r2) == 0){
          const uint32_t mask = _mm_movemask_ps((__m128)r2);
          return mask_array[mask]+4*2;
        } else{
          const uint32_t mask = _mm_movemask_ps((__m128)r3);
          return mask_array[mask]+4*3;
        }
      } else if(_mm_testz_si128(Q2, Q2) == 0){
        if(_mm_testz_si128(r4, r4) == 0){
          const uint32_t mask = _mm_movemask_ps((__m128)r4);
          return mask_array[mask]+4*4;
        } else{
          const uint32_t mask = _mm_movemask_ps((__m128)r5);
          return mask_array[mask]+4*5;
        }
      } else{
        if(_mm_testz_si128(r6, r6) == 0){
          const uint32_t mask = _mm_movemask_ps((__m128)r6);
          return mask_array[mask]+4*6;
        } else{
          const uint32_t mask = _mm_movemask_ps((__m128)r7);
          return mask_array[mask]+4*7;
        }
      }

      //not reached
      return 0;
    }
    static inline size_t check_registers(const __m128i Q0, const __m128i Q1){
      const size_t mask_array[16] = {0,0,1,0,2,0,0,0,3,0,0,0,0,0,0,0};
      if(_mm_testz_si128(Q0, Q0) == 0){
        const uint32_t mask = _mm_movemask_ps((__m128)Q0);
        return mask_array[mask];
      } else{
        const uint32_t mask = _mm_movemask_ps((__m128)Q1);
        return mask_array[mask]+4;
      }
      return 0;
    }
  };
  struct unpack_uinteger_materialize:check{
    static inline uint32_t range(uint32_t *C, size_t cardinality){
      return (cardinality > 0) ? C[cardinality-1]-C[0] : 0;
    }
    static inline uint32_t* advanceC(uint32_t *C, size_t amount){
      return C+amount;
    }
    template <typename F>
    static inline size_t scalar(const uint32_t x, uint32_t *w, F f, const uint32_t i_a, const uint32_t i_b){
      *w = x;
      return f(x,i_a,i_b);
    }
    template<typename F>
    static inline size_t unpack(const __m128i x, uint32_t *w, 
      const size_t num, F f, const __m128i i_a, const __m128i i_b){
      size_t filter = 0;
      //I can't write a for loop because that won't compile with gcc
      if(num == 0){
        *(w+0) = _mm_extract_epi32(x,0);
        filter += f(_mm_extract_epi32(x,0),_mm_extract_epi32(i_a,0),_mm_extract_epi32(i_b,0)); 
      } else if(num == 1){
        *(w+0) = _mm_extract_epi32(x,0);
        filter += f(_mm_extract_epi32(x,0),_mm_extract_epi32(i_a,0),_mm_extract_epi32(i_b,0)); 

        *(w+1) = _mm_extract_epi32(x,1);
        filter += f(_mm_extract_epi32(x,1),_mm_extract_epi32(i_a,1),_mm_extract_epi32(i_b,1)); 
      } else if(num == 2){
        *(w+0) = _mm_extract_epi32(x,0);
        filter += f(_mm_extract_epi32(x,0),_mm_extract_epi32(i_a,0),_mm_extract_epi32(i_b,0)); 

        *(w+1) = _mm_extract_epi32(x,1);
        filter += f(_mm_extract_epi32(x,1),_mm_extract_epi32(i_a,1),_mm_extract_epi32(i_b,1));

        *(w+2) = _mm_extract_epi32(x,2);
        filter += f(_mm_extract_epi32(x,2),_mm_extract_epi32(i_a,2),_mm_extract_epi32(i_b,2)); 
      } else {
        *(w+0) = _mm_extract_epi32(x,0);
        filter += f(_mm_extract_epi32(x,0),_mm_extract_epi32(i_a,0),_mm_extract_epi32(i_b,0)); 

        *(w+1) = _mm_extract_epi32(x,1);
        filter += f(_mm_extract_epi32(x,1),_mm_extract_epi32(i_a,1),_mm_extract_epi32(i_b,1));

        *(w+2) = _mm_extract_epi32(x,2);
        filter += f(_mm_extract_epi32(x,2),_mm_extract_epi32(i_a,2),_mm_extract_epi32(i_b,2)); 

        *(w+3) = _mm_extract_epi32(x,3);
        filter += f(_mm_extract_epi32(x,3),_mm_extract_epi32(i_a,3),_mm_extract_epi32(i_b,3)); 
      } 
      return filter;
    }
  };
  struct unpack_uinteger_aggregate:check{
    static inline uint32_t range(uint32_t *C, size_t cardinality){
      (void) C; (void) cardinality;
      return 0;
    }
    static inline uint32_t* advanceC(uint32_t *C, size_t amount){
      (void) C; (void) amount;
      return NULL;
    }
    template <typename F>
    static inline size_t scalar(const uint32_t x, uint32_t *w, F f, const uint32_t i_a, const uint32_t i_b){
      (void) w;
      return f(x,i_a,i_b);
    }
    template<typename F>
    static inline size_t unpack(const __m128i x, uint32_t *w, 
      const size_t num, F f, const __m128i i_a, const __m128i i_b){
      (void) w;
      size_t filter = 0;

      if(num == 0){
        filter += f(_mm_extract_epi32(x,0),_mm_extract_epi32(i_a,0),_mm_extract_epi32(i_b,0)); 
      } else if(num == 1){
        filter += f(_mm_extract_epi32(x,0),_mm_extract_epi32(i_a,0),_mm_extract_epi32(i_b,0)); 
        filter += f(_mm_extract_epi32(x,1),_mm_extract_epi32(i_a,1),_mm_extract_epi32(i_b,1)); 
      } else if(num == 2){
        filter += f(_mm_extract_epi32(x,0),_mm_extract_epi32(i_a,0),_mm_extract_epi32(i_b,0)); 
        filter += f(_mm_extract_epi32(x,1),_mm_extract_epi32(i_a,1),_mm_extract_epi32(i_b,1)); 
        filter += f(_mm_extract_epi32(x,2),_mm_extract_epi32(i_a,2),_mm_extract_epi32(i_b,2)); 
      } else {
        filter += f(_mm_extract_epi32(x,0),_mm_extract_epi32(i_a,0),_mm_extract_epi32(i_b,0)); 
        filter += f(_mm_extract_epi32(x,1),_mm_extract_epi32(i_a,1),_mm_extract_epi32(i_b,1)); 
        filter += f(_mm_extract_epi32(x,2),_mm_extract_epi32(i_a,2),_mm_extract_epi32(i_b,2)); 
        filter += f(_mm_extract_epi32(x,3),_mm_extract_epi32(i_a,3),_mm_extract_epi32(i_b,3)); 
      }   
      return filter;
    }
  };
  
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
  static size_t __frogadvanceUntil(const uint32_t *array, const size_t pos,
                                   const size_t length, const size_t min) {
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
  
  template<class N,typename F>
  inline Set<uinteger>* scalar_gallop(Set<uinteger> *C_in, const Set<uinteger> *A_in, const Set<uinteger> *B_in, F f){
      const uint32_t *smallset = (uint32_t*)A_in->data;
      const size_t smalllength = A_in->cardinality;
      const uint32_t *largeset = (uint32_t*)B_in->data;
      const size_t largelength = B_in->cardinality;
      uint32_t *out = (uint32_t*)C_in->data;
      size_t count = 0;
      if(A_in->cardinality < B_in->cardinality){
        return scalar_gallop<N>(C_in,B_in,A_in);
      }
      if (0 == smalllength){
        C_in->cardinality = 0;
        C_in->number_of_bytes = 0;
        C_in->type = type::UINTEGER;
        return C_in;
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
              const size_t hit_amount = N::scalar(smallset[k2],out,f,k2,k1);
              out = N::advanceC(out,hit_amount);
              count += hit_amount;
              ++k2;
              if (k2 == smalllength)
                  break;
              k1 = __frogadvanceUntil(largeset, k1, largelength, smallset[k2]);
              if (k1 == largelength)
                  break;
              goto midpoint;
          }
      }
      C_in->cardinality = count;
      C_in->range = N::range((uint32_t*)C_in->data,C_in->cardinality);
      C_in->number_of_bytes = C_in->cardinality*sizeof(uint32_t);
      return C_in;
  }

  /**
   * Fast scalar scheme designed by N. Kurz.
   */
  template<class N, typename F>
  inline size_t scalar(const uint32_t *A, const size_t lenA,
                const uint32_t *B, const size_t lenB, uint32_t *out, 
                F f, const size_t offsetA, const size_t offsetB) {
      if (lenA == 0 || lenB == 0)
          return 0;

      const uint32_t *startA = A;
      const uint32_t *startB = B;
      const uint32_t *endA = A + lenA;
      const uint32_t *endB = B + lenB;
      size_t count = 0;

      while (1) {
          while (*A < *B) {
  SKIP_FIRST_COMPARE:
              if (++A == endA)
                  return count;
          }
          while (*A > *B) {
              if (++B == endB)
                  return count;
          }
          if (*A == *B) {
              const size_t hit_amount = N::scalar(*A,out,f,((A+offsetA)-startA),((B+offsetB)-startB));
              out = N::advanceC(out,hit_amount);
              count += hit_amount;
              if (++A == endA || ++B == endB)
                  return count;
          } else {
              goto SKIP_FIRST_COMPARE;
          }
      }

      return count; // NOTREACHED
  }


  /**
   * Intersections scheme designed by N. Kurz that works very
   * well when intersecting an array with another where the density
   * differential is small (between 2 to 10).
   *
   * It assumes that lenRare <= lenFreq.
   *
   * Note that this is not symmetric: flipping the rare and freq pointers
   * as well as lenRare and lenFreq could lead to significant performance
   * differences.
   *
   * The matchOut pointer can safely be equal to the rare pointer.
   *
   * This function  use inline assembly.
   */
  template<class N,typename F>
  inline Set<uinteger>* set_intersect_v1(Set<uinteger> *C_in, const Set<uinteger> *A_in, const Set<uinteger> *B_in, F f){
      const uint32_t *rare = (uint32_t*)A_in->data;
      size_t lenRare = A_in->cardinality;
      const uint32_t *freq = (uint32_t*)B_in->data;
      size_t lenFreq = B_in->cardinality;
      uint32_t *matchOut = (uint32_t*)C_in->data;

      size_t count = 0;
      const uint32_t *startFreq = freq;
      const uint32_t *startRare = rare;

      if (lenFreq == 0 || lenRare == 0){
        C_in->cardinality = 0;
        C_in->number_of_bytes = (0)*sizeof(uint32_t);
        C_in->type= type::UINTEGER;
        return C_in;
      }

      const uint64_t kFreqSpace = 2 * 4 * (0 + 1) - 1;
      const uint64_t kRareSpace = 0;

      const uint32_t *stopFreq = &freq[lenFreq] - kFreqSpace;
      const uint32_t *stopRare = &rare[lenRare] - kRareSpace;

      VEC_T Rare;

      VEC_T F0, F1;

      if (COMPILER_RARELY((rare >= stopRare) || (freq >= stopFreq))) goto FINISH_SCALAR;

      uint32_t valRare;
      valRare = rare[0];
      Rare = _mm_set1_epi32(valRare);

      uint64_t maxFreq;
      maxFreq = freq[2 * 4 - 1];
      F0 = _mm_lddqu_si128(reinterpret_cast<const __m128i *>(freq));
      F1 = _mm_lddqu_si128(reinterpret_cast<const __m128i *>(freq + 4));

      if (COMPILER_RARELY(maxFreq < valRare)) goto ADVANCE_FREQ;

  ADVANCE_RARE:
      do {
        const uint32_t old_rare = valRare;
        valRare = rare[1]; // for next iteration
        rare += 1;
        if (COMPILER_RARELY(rare >= stopRare)) {
          rare -= 1;
          goto FINISH_SCALAR;
        }
        F0 = _mm_cmpeq_epi32(F0, Rare);
        F1 = _mm_cmpeq_epi32(F1, Rare);
        Rare = _mm_set1_epi32(valRare);
        F0 = _mm_or_si128(F0, F1);
    #ifdef __SSE4_1__
        if(_mm_testz_si128(F0,F0) == 0){
          const size_t freqOffset = N::check_registers(F0,F1);
          const size_t hit_amount = N::scalar(old_rare,matchOut,f,(rare-startRare),((freq+freqOffset)-startFreq));
          matchOut = N::advanceC(matchOut,hit_amount);
          count += hit_amount;
        }
    #else
        if (_mm_movemask_epi8(F0)){
          const size_t freqOffset = N::check_registers(F0,F1);
          const size_t hit_amount = N::scalar(old_rare,matchOut,f,(rare-startRare),((freq+freqOffset)-startFreq));
          matchOut = N::advanceC(matchOut,hit_amount);
          count += hit_amount;
        }
    #endif
        F0 = _mm_lddqu_si128(reinterpret_cast<const __m128i *>(freq));
        F1 = _mm_lddqu_si128(reinterpret_cast<const __m128i *>(freq + 4));
      } while (maxFreq >= valRare);

      uint64_t maxProbe;

  ADVANCE_FREQ:
      do {
          const uint64_t kProbe = (0 + 1) * 2 * 4;
          const uint32_t *probeFreq = freq + kProbe;
          maxProbe = freq[(0 + 2) * 2 * 4 - 1];

          if (COMPILER_RARELY(probeFreq >= stopFreq)) {
              goto FINISH_SCALAR;
          }

          freq = probeFreq;

      } while (maxProbe < valRare);

      maxFreq = maxProbe;

      VEC_LOAD_OFFSET(F0, freq, 0 * sizeof(VEC_T)) ;
      VEC_LOAD_OFFSET(F1, freq, 1 * sizeof(VEC_T));

      goto ADVANCE_RARE;

  FINISH_SCALAR:

      lenFreq = stopFreq + kFreqSpace - freq;
      lenRare = stopRare + kRareSpace - rare;

    size_t tail = scalar<N>(freq, lenFreq, rare, lenRare, matchOut,f,rare-startRare,freq-startFreq);

    C_in->cardinality = count + tail;
    C_in->range = N::range((uint32_t*)C_in->data,C_in->cardinality);
    C_in->number_of_bytes = (count+tail)*sizeof(uint32_t);
    C_in->type= type::UINTEGER;

    return C_in;  
  }
  /**
   * This intersection function is similar to v1, but is faster when
   * the difference between lenRare and lenFreq is large, but not too large.
   * It assumes that lenRare <= lenFreq.
   *
   * Note that this is not symmetric: flipping the rare and freq pointers
   * as well as lenRare and lenFreq could lead to significant performance
   * differences.
   *
   * The matchOut pointer can safely be equal to the rare pointer.
   *
   * This function DOES NOT use inline assembly instructions. Just intrinsics.
   */
  template<class N,typename F>
  inline Set<uinteger>* set_intersect_v3(Set<uinteger> *C_in, const Set<uinteger> *A_in, const Set<uinteger> *B_in, F f){
      const uint32_t *rare = (uint32_t*)A_in->data;
      const size_t lenRare = A_in->cardinality;
      const uint32_t *freq = (uint32_t*)B_in->data;
      const size_t lenFreq = B_in->cardinality;
      uint32_t *out = (uint32_t*)C_in->data;

      size_t count = 0;
      const uint32_t *startRare = rare;
      const uint32_t *startFreq = freq;

      if (lenFreq == 0 || lenRare == 0){
        C_in->cardinality = 0;
        C_in->number_of_bytes = (0)*sizeof(uint32_t);
        C_in->type= type::UINTEGER;
        return C_in;
      }
      assert(lenRare <= lenFreq);
      typedef __m128i vec;
      const uint32_t veclen = sizeof(vec) / sizeof(uint32_t);
      const size_t vecmax = veclen - 1;
      const size_t freqspace = 32 * veclen;
      const size_t rarespace = 1;

      const uint32_t *stopFreq = freq + lenFreq - freqspace;
      const uint32_t *stopRare = rare + lenRare - rarespace;
      if (freq > stopFreq) {
        const size_t final_count = scalar<N>(freq, lenFreq, rare, lenRare, out,f,freq-startFreq,rare-startRare);
        C_in->cardinality = final_count;
        C_in->number_of_bytes = (final_count)*sizeof(uint32_t);
        C_in->type= type::UINTEGER;
        return C_in;
      }
      while (freq[veclen * 31 + vecmax] < *rare) {
          freq += veclen * 32;
          if (freq > stopFreq)
              goto FINISH_SCALAR;
      }
      for (; rare < stopRare; ++rare) {
          const uint32_t matchRare = *rare;//nextRare;
          const vec Match = _mm_set1_epi32(matchRare);
          while (freq[veclen * 31 + vecmax] < matchRare) { // if no match possible
              freq += veclen * 32; // advance 32 vectors
              if (freq > stopFreq)
                  goto FINISH_SCALAR;
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
                    const size_t freqOffset = offset*4 + N::check_registers(Q0,Q1,Q2,Q3,r0,r1,r2,r3,r4,r5,r6,r7);
                    const size_t hit_amount = N::scalar(matchRare,out,f,(rare-startRare),((freq+freqOffset)-startFreq));
                    out = N::advanceC(out,hit_amount);
                    count += hit_amount;
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
                    const size_t freqOffset = offset*4 + N::check_registers(Q0,Q1,Q2,Q3,r0,r1,r2,r3,r4,r5,r6,r7);
                    const size_t hit_amount = N::scalar(matchRare,out,f,(rare-startRare),((freq+freqOffset)-startFreq));
                    out = N::advanceC(out,hit_amount);
                    count += hit_amount;
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
                    const size_t freqOffset = offset*4 + N::check_registers(Q0,Q1,Q2,Q3,r0,r1,r2,r3,r4,r5,r6,r7);
                    const size_t hit_amount = N::scalar(matchRare,out,f,(rare-startRare),((freq+freqOffset)-startFreq));
                    out = N::advanceC(out,hit_amount);
                    count += hit_amount;
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
                    const size_t freqOffset = offset*4 + N::check_registers(Q0,Q1,Q2,Q3,r0,r1,r2,r3,r4,r5,r6,r7);
                    const size_t hit_amount = N::scalar(matchRare,out,f,(rare-startRare),((freq+freqOffset)-startFreq));
                    out = N::advanceC(out,hit_amount);
                    count += hit_amount;
                  }  
              }

          }
      }

  FINISH_SCALAR: 
    const size_t final_count = count + scalar<N>(rare, stopRare + rarespace - rare, freq,stopFreq + freqspace - freq, out, f, rare-startRare, freq-startFreq);
    C_in->cardinality = final_count;
    C_in->range = N::range((uint32_t*)C_in->data,C_in->cardinality);
    C_in->number_of_bytes = (final_count)*sizeof(uint32_t);
    C_in->type= type::UINTEGER;
    return C_in;
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
  template<class N,typename F>
  inline Set<uinteger>* set_intersect_galloping(Set<uinteger> *C_in, const Set<uinteger> *A_in, const Set<uinteger> *B_in, F f){
      const uint32_t *rare = (uint32_t*)A_in->data;
      const size_t lenRare = A_in->cardinality;
      const uint32_t *freq = (uint32_t*)B_in->data;
      const size_t lenFreq = B_in->cardinality;
      uint32_t *out = (uint32_t*)C_in->data;

      const uint32_t * const startRare = rare;
      const uint32_t * const startFreq = freq;
      size_t count = 0;

      if (lenFreq == 0 || lenRare == 0){
        C_in->cardinality = 0;
        C_in->number_of_bytes = (0)*sizeof(uint32_t);
        C_in->type= type::UINTEGER;
        return C_in;
      }
      assert(lenRare <= lenFreq);
      typedef __m128i vec;
      const uint32_t veclen = sizeof(vec) / sizeof(uint32_t);
      const size_t vecmax = veclen - 1;
      const size_t freqspace = 32 * veclen;
      const size_t rarespace = 1;

      const uint32_t *stopFreq = freq + lenFreq - freqspace;
      const uint32_t *stopRare = rare + lenRare - rarespace;
      if (freq > stopFreq) {
        const size_t final_count = scalar<N>(freq, lenFreq, rare, lenRare, out, f, freq-startFreq, rare-startRare);
        C_in->cardinality = final_count;
        C_in->number_of_bytes = (final_count)*sizeof(uint32_t);
        C_in->type= type::UINTEGER;
        return C_in;
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
                    const size_t freqOffset = offset*4 + N::check_registers(Q0,Q1,Q2,Q3,r0,r1,r2,r3,r4,r5,r6,r7);
                    const size_t hit_amount = N::scalar(matchRare,out,f,(rare-startRare),((freq+freqOffset)-startFreq));
                    out = N::advanceC(out,hit_amount);
                    count += hit_amount;
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
                    const size_t freqOffset = offset*4 + N::check_registers(Q0,Q1,Q2,Q3,r0,r1,r2,r3,r4,r5,r6,r7);
                    const size_t hit_amount = N::scalar(matchRare,out,f,(rare-startRare),((freq+freqOffset)-startFreq));
                    out = N::advanceC(out,hit_amount);
                    count += hit_amount;
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
                    const size_t freqOffset = offset*4 + N::check_registers(Q0,Q1,Q2,Q3,r0,r1,r2,r3,r4,r5,r6,r7);
                    const size_t hit_amount = N::scalar(matchRare,out,f,(rare-startRare),((freq+freqOffset)-startFreq));
                    out = N::advanceC(out,hit_amount);
                    count += hit_amount;
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
                    const size_t freqOffset = offset*4 + N::check_registers(Q0,Q1,Q2,Q3,r0,r1,r2,r3,r4,r5,r6,r7);
                    const size_t hit_amount = N::scalar(matchRare,out,f,(rare-startRare),((freq+freqOffset)-startFreq));
                    out = N::advanceC(out,hit_amount);
                    count += hit_amount;
                  } 
              }

          }
      }

  FINISH_SCALAR: 
    const size_t final_count = count + scalar<N>(rare, stopRare + rarespace - rare, freq,stopFreq + freqspace - freq, out, f, rare-startRare, freq-startFreq);
    C_in->cardinality = final_count;
    C_in->range = N::range((uint32_t*)C_in->data,C_in->cardinality);
    C_in->number_of_bytes = (final_count)*sizeof(uint32_t);
    C_in->type= type::UINTEGER;
    return C_in;
  }

  template<class N,typename F>
  inline Set<uinteger>* set_intersect_ibm(Set<uinteger> *C_in, const Set<uinteger> *A_in, const Set<uinteger> *B_in, F f){
    uint32_t * C = (uint32_t*) C_in->data; 
    const uint32_t * const A = (uint32_t*) A_in->data;
    const uint32_t * const B = (uint32_t*) B_in->data;
    const size_t s_a = A_in->cardinality;
    const size_t s_b = B_in->cardinality;

    const size_t st_a = (s_a / SHORTS_PER_REG) * SHORTS_PER_REG;
    const size_t st_b = (s_b / SHORTS_PER_REG) * SHORTS_PER_REG;

    size_t count = 0;
    size_t i_a = 0, i_b = 0;

    while(i_a < st_a && i_b < st_b){
      //Pull in 4 uint 32's
      const __m128i v_a_1_32 = _mm_loadu_si128((__m128i*)&A[i_a]);
      const __m128i v_a_2_32 = _mm_loadu_si128((__m128i*)&A[i_a+(SHORTS_PER_REG/2)]);


      //shuffle to std::get lower 16 bits only in one register
      const __m128i v_a_l1 = _mm_shuffle_epi8(v_a_1_32,_mm_set_epi8(uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x0D),uint8_t(0x0C),uint8_t(0x09),uint8_t(0x08),uint8_t(0x05),uint8_t(0x04),uint8_t(0x01),uint8_t(0x0)));
      const __m128i v_a_l2 = _mm_shuffle_epi8(v_a_2_32,_mm_set_epi8(uint8_t(0x0D),uint8_t(0x0C),uint8_t(0x09),uint8_t(0x08),uint8_t(0x05),uint8_t(0x04),uint8_t(0x01),uint8_t(0x0),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80)));
      const __m128i v_a_l = _mm_or_si128(v_a_l1,v_a_l2);

      const __m128i v_b_1_32 = _mm_loadu_si128((__m128i*)&B[i_b]);
      const __m128i v_b_2_32 = _mm_loadu_si128((__m128i*)&B[i_b+(SHORTS_PER_REG/2)]);

      //type::_mm128i_print(v_b_1_32);
      //type::_mm128i_print(v_b_2_32);

      const __m128i v_b_l1 = _mm_shuffle_epi8(v_b_1_32,_mm_set_epi8(uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x0D),uint8_t(0x0C),uint8_t(0x09),uint8_t(0x08),uint8_t(0x05),uint8_t(0x04),uint8_t(0x01),uint8_t(0x0)));
      const __m128i v_b_l2 = _mm_shuffle_epi8(v_b_2_32,_mm_set_epi8(uint8_t(0x0D),uint8_t(0x0C),uint8_t(0x09),uint8_t(0x08),uint8_t(0x05),uint8_t(0x04),uint8_t(0x01),uint8_t(0x0),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80)));
      const __m128i v_b_l = _mm_or_si128(v_b_l1,v_b_l2);
      
     // __m128i res_v = _mm_cmpistrm(v_b, v_a,
     //         _SIDD_UWORD_OPS|_SIDD_CMP_EQUAL_ANY|_SIDD_BIT_MASK);
      const __m128i res_vl = _mm_cmpestrm(v_b_l, SHORTS_PER_REG, v_a_l, SHORTS_PER_REG,
              _SIDD_UWORD_OPS|_SIDD_CMP_EQUAL_ANY|_SIDD_BIT_MASK);
      const uint32_t result_l = _mm_extract_epi32(res_vl, 0);

     // std::cout << "LOWER " << hex  << result_l << dec << std::endl;

      if(result_l != 0){
        //shuffle to std::get upper 16 bits only in one register
        const __m128i v_a_u1 = _mm_shuffle_epi8(v_a_1_32,_mm_set_epi8(uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x0F),uint8_t(0x0E),uint8_t(0x0B),uint8_t(0x0A),uint8_t(0x07),uint8_t(0x06),uint8_t(0x03),uint8_t(0x2)));
        const __m128i v_a_u2 = _mm_shuffle_epi8(v_a_2_32,_mm_set_epi8(uint8_t(0x0F),uint8_t(0x0E),uint8_t(0x0B),uint8_t(0x0A),uint8_t(0x07),uint8_t(0x06),uint8_t(0x03),uint8_t(0x2),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80)));
        const __m128i v_a_u = _mm_or_si128(v_a_u1,v_a_u2);

        const __m128i v_b_1_32 = _mm_loadu_si128((__m128i*)&B[i_b]);
        const __m128i v_b_2_32 = _mm_loadu_si128((__m128i*)&B[i_b+(SHORTS_PER_REG/2)]);

        const __m128i v_b_u1 = _mm_shuffle_epi8(v_b_1_32,_mm_set_epi8(uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x0F),uint8_t(0x0E),uint8_t(0x0B),uint8_t(0x0A),uint8_t(0x07),uint8_t(0x06),uint8_t(0x03),uint8_t(0x2)));
        const __m128i v_b_u2 = _mm_shuffle_epi8(v_b_2_32,_mm_set_epi8(uint8_t(0x0F),uint8_t(0x0E),uint8_t(0x0B),uint8_t(0x0A),uint8_t(0x07),uint8_t(0x06),uint8_t(0x03),uint8_t(0x2),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80),uint8_t(0x80)));
        const __m128i v_b_u = _mm_or_si128(v_b_u1,v_b_u2);

        const __m128i res_vu = _mm_cmpestrm(v_b_u, SHORTS_PER_REG, v_a_u, SHORTS_PER_REG,
                _SIDD_UWORD_OPS|_SIDD_CMP_EQUAL_ANY|_SIDD_BIT_MASK);
        const uint32_t result_u = _mm_extract_epi32(res_vu, 0);

        //std::cout << "UPPER " << hex  << result_l << dec << std::endl;

        const uint32_t w_bitmask = result_u & result_l;
        //std::cout << count << " BITMASK: " << hex  << w_bitmask << dec << std::endl;
        if(w_bitmask != 0){
          const size_t start_index = _mm_popcnt_u32((~w_bitmask)&(w_bitmask-1));
          const size_t A_pos = start_index+i_a;
          const size_t A_end = 8-start_index;
          const size_t B_pos = i_b;
          const size_t B_end = 8;
          const size_t adv_amount = scalar<N>(&A[A_pos],A_end,&B[B_pos],B_end,C,f,A_pos,B_pos);
          count += adv_amount;
          C = N::advanceC(C,adv_amount);
        }
      } 
      if(A[i_a+7] > B[i_b+7]){
        goto advanceB;
      } else if (A[i_a+7] < B[i_b+7]){
        goto advanceA;
      } else{
        goto advanceAB;
      }
      advanceA:
        i_a += 8;
        continue;
      advanceB:
        i_b += 8;
        continue;
      advanceAB:
        i_a += 8;
        i_b += 8;
        continue;
    }

    // intersect the tail using scalar intersection
    count += scalar<N>(&A[i_a],s_a-i_a,&B[i_b],s_b-i_b,C,f,i_a,i_b);

    C_in->cardinality = count;
    C_in->range = N::range((uint32_t*)C_in->data,C_in->cardinality);
    C_in->number_of_bytes = count*sizeof(uint32_t);
    C_in->type= type::UINTEGER;

    return C_in;
  }

  template<class N,typename F>
  inline Set<uinteger>* set_intersect_shuffle(Set<uinteger> *C_in, const Set<uinteger> *A_in, const Set<uinteger> *B_in, F f){
    uint32_t * C = (uint32_t*) C_in->data; 
    const uint32_t * const A = (uint32_t*) A_in->data;
    const uint32_t * const B = (uint32_t*) B_in->data;
    const size_t s_a = A_in->cardinality;
    const size_t s_b = B_in->cardinality;

    size_t count = 0;
    size_t i_a = 0, i_b = 0;

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
      const size_t num_hit = N::unpack(p,C,_mm_popcnt_u32(mask),f,a_offset,b_offset);
      C = N::advanceC(C,num_hit); //to avoid NULL pointer exception on aggregate
      count += num_hit;

      //advance pointers
      const uint32_t a_max = A[i_a+3];
      const uint32_t b_max = B[i_b+3];

      i_a += (a_max <= b_max) * 4;
      i_b += (a_max >= b_max) * 4;
    }
    #endif

    // intersect the tail using scalar intersection
    count += scalar<N>(&A[i_a],s_a-i_a,&B[i_b],s_b-i_b,C,f,i_a,i_b);
    
    C_in->cardinality = count;
    C_in->range = N::range((uint32_t*)C_in->data,C_in->cardinality);
    C_in->number_of_bytes = count*sizeof(uint32_t);
    C_in->type= type::UINTEGER;

    return C_in;  
  }

  template<class N, typename F>
  inline Set<uinteger>* run_intersection(Set<uinteger> *C_in, const Set<uinteger> *rare, const Set<uinteger> *freq, F f){
    #ifndef NO_ALGORITHM 
        const auto min = std::min(rare->cardinality,freq->cardinality);
        const auto max = std::max(rare->cardinality,freq->cardinality);
        if(min != 0 && ((double)max/min)  > 32.0){
        #if VECTORIZE == 1
          return set_intersect_galloping<N>(C_in, rare, freq, f);
        #else 
          return scalar_gallop<N>(C_in,rare,freq,f);
        #endif
        } else
    #endif
      //return set_intersect_ibm<N>(C_in, rare, freq, f);
      //return set_intersect_v3<N>(C_in, rare, freq, f);
      //return set_intersect_v1<N>(C_in, rare, freq, f);
      //return set_intersect_galloping<N>(C_in, rare, freq, f);
      return set_intersect_shuffle<N>(C_in, rare, freq, f);
  }

  template<typename F>
  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in, const Set<uinteger> *A_in, const Set<uinteger> *B_in, F f) {
    if(A_in->cardinality > B_in->cardinality){
      auto f_in = [&](uint32_t data, uint32_t a_i, uint32_t b_i){return f(data,b_i,a_i);};
      return run_intersection<unpack_uinteger_materialize>(C_in,B_in,A_in,f_in);
    } 
    return run_intersection<unpack_uinteger_materialize>(C_in,A_in,B_in,f);
  }

  template<typename F>
  inline size_t set_intersect(const Set<uinteger> *A_in, const Set<uinteger> *B_in, F f) {
    Set<uinteger> C_in;
    if(A_in->cardinality > B_in->cardinality){
      auto f_in = [&](uint32_t data, uint32_t a_i, uint32_t b_i){return f(data,b_i,a_i);};
      return run_intersection<unpack_uinteger_aggregate>(&C_in,B_in,A_in,f_in)->cardinality;
    } 
    return run_intersection<unpack_uinteger_aggregate>(&C_in,A_in,B_in,f)->cardinality;
  }
  
  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in, const Set<uinteger> *A_in, const Set<uinteger> *B_in) {
    const Set<uinteger> *rare = (A_in->cardinality > B_in->cardinality) ? B_in:A_in;
    const Set<uinteger> *freq = (A_in->cardinality > B_in->cardinality) ? A_in:B_in;
    auto f = [&](uint32_t data, const uint32_t i_a, const uint32_t i_b){(void) data; (void) i_a; (void) i_b; return;};
    return run_intersection<unpack_materialize>(C_in,rare,freq,f);
  }

  inline size_t set_intersect(const Set<uinteger> *A_in, const Set<uinteger> *B_in) {
    const Set<uinteger> *rare = (A_in->cardinality > B_in->cardinality) ? B_in:A_in;
    const Set<uinteger> *freq = (A_in->cardinality > B_in->cardinality) ? A_in:B_in;
    auto f = [&](uint32_t data, const uint32_t i_a, const uint32_t i_b){(void) data; (void) i_a; (void) i_b; return;};
    Set<uinteger> C_in;
    return run_intersection<unpack_aggregate>(&C_in,rare,freq,f)->cardinality;
  }
}
#endif
