#include <stdio.h>
#include <stdalign.h>
#include <stdint.h>
#include <time.h>
#include <immintrin.h>

#define SIZE 1024
#define ROUNDS 2097152

 int64_t test_compress_intri(int64_t *A, int64_t *B)
{
  __mmask8 mask;
  __m512i vin;
  __m512i const thr = _mm512_set1_epi64(255);
  int64_t j = 0;
  #pragma unroll 8
  for (int i = 0; i < SIZE; i += 8)
  {
    vin = _mm512_load_epi64(&A[i]);
    mask = _mm512_cmpgt_epi64_mask(vin, thr);
    _mm512_mask_compressstoreu_epi64(&B[j], mask, vin);
    j += __builtin_popcount(_mm512_mask2int(mask));
  }
  return j;
}