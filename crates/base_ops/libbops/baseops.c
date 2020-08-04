/*
 *   Copyright (c) 2020 TensorBase, and its contributors
 *   All rights reserved.

 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "baseops.h"

#if INTERFACE
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include <string.h>
#include <immintrin.h>
// #define NDEBUG

#define CONTRACT_RT(exp, desc) assert(((void)desc, exp))
#define CONTRACT_CT(exp, mesg) _Static_assert((exp, mesg))

#ifndef NDEBUG
#pragma message("[base/storage/binform] Contract enabled!")
#define INLINE_RELEASE
#endif
#ifdef NDEBUG
#pragma message("[base/storage/binform] Contract disabled!")
#define INLINE_RELEASE inline __attribute__((always_inline))
#endif

#define SIMDCSV_BUFFERSIZE 4 // it seems to be about the sweetspot.
struct ParsedCSV
{
    uint32_t n_indexes;
    uint32_t *indexes;
};

struct simd_input
{
    // #ifdef __AVX2__
    __m256i lo;
    __m256i hi;
    // #else
    // #error "It's called SIMDcsv for a reason, bro"
    // #endif
};

#endif

// static INLINE_RELEASE simd_input fill_input(const uint8_t *ptr)
// {
//     struct simd_input in;
//     in.lo = _mm256_loadu_si256((const __m256i *)(ptr + 0));
//     in.hi = _mm256_loadu_si256((const __m256i *)(ptr + 32));
//     return in;
// }
static inline bool add_overflow(uint64_t value1, uint64_t value2, uint64_t *result)
{
    return __builtin_uaddll_overflow(value1, value2, (unsigned long long *)result);
}
static inline bool mul_overflow(uint64_t value1, uint64_t value2, uint64_t *result)
{
    return __builtin_umulll_overflow(value1, value2, (unsigned long long *)result);
}

/* result might be undefined when input_num is zero */
static inline int trailingzeroes(uint64_t input_num)
{
    return __builtin_ctzll(input_num);
}

/* result might be undefined when input_num is zero */
static inline int leadingzeroes(uint64_t input_num)
{
    return __builtin_clzll(input_num);
}

/* result might be undefined when input_num is zero */
static inline int hamming(uint64_t input_num)
{
    return __builtin_popcountll(input_num);
}

// a straightforward comparison of a mask against input. 5 uops; would be
// cheaper in AVX512.
static INLINE_RELEASE uint64_t cmp_mask_against_input(simd_input *in, uint8_t m)
{
    const __m256i mask = _mm256_set1_epi8(m);
    __m256i cmp_res_0 = _mm256_cmpeq_epi8(in->lo, mask);
    uint64_t res_0 = (uint32_t)(_mm256_movemask_epi8(cmp_res_0));
    __m256i cmp_res_1 = _mm256_cmpeq_epi8(in->hi, mask);
    uint64_t res_1 = _mm256_movemask_epi8(cmp_res_1);
    return res_0 | (res_1 << 32);
}

// return the quote mask (which is a half-open mask that covers the first
// quote in a quote pair and everything in the quote pair)
// We also update the prev_iter_inside_quote value to
// tell the next iteration whether we finished the final iteration inside a
// quote pair; if so, this  inverts our behavior of  whether we're inside
// quotes for the next iteration.

static INLINE_RELEASE uint64_t find_quote_mask(simd_input *in, uint64_t *prev_iter_inside_quote)
{
    uint64_t quote_bits = cmp_mask_against_input(in, '"');

    uint64_t quote_mask = _mm_cvtsi128_si64(_mm_clmulepi64_si128(
        _mm_set_epi64x(0ULL, quote_bits), _mm_set1_epi8(0xFF), 0));
    quote_mask ^= (*prev_iter_inside_quote);

    // right shift of a signed value expected to be well-defined and standard
    // compliant as of C++20,
    // John Regher from Utah U. says this is fine code
    *prev_iter_inside_quote =
        (uint64_t)(((int64_t)quote_mask) >> 63);
    return quote_mask;
}

// flatten out values in 'bits' assuming that they are are to have values of idx
// plus their position in the bitvector, and store these indexes at
// base_ptr[base] incrementing base as we go
// will potentially store extra values beyond end of valid bits, so base_ptr
// needs to be large enough to handle this
static INLINE_RELEASE void flatten_bits(uint32_t *base_ptr, uint32_t *base,
                                uint32_t idx, uint64_t bits) {
  if (bits != 0u) {
    uint32_t cnt = hamming(bits);
    uint32_t next_base = (*base) + cnt;
    base_ptr[(*base) + 0] = (uint32_t)(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[(*base) + 1] = (uint32_t)(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[(*base) + 2] = (uint32_t)(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[(*base) + 3] = (uint32_t)(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[(*base) + 4] = (uint32_t)(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[(*base) + 5] = (uint32_t)(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[(*base) + 6] = (uint32_t)(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[(*base) + 7] = (uint32_t)(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    if (cnt > 8) {
      base_ptr[(*base) + 8] = (uint32_t)(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[(*base) + 9] = (uint32_t)(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[(*base) + 10] = (uint32_t)(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[(*base) + 11] = (uint32_t)(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[(*base) + 12] = (uint32_t)(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[(*base) + 13] = (uint32_t)(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[(*base) + 14] = (uint32_t)(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[(*base) + 15] = (uint32_t)(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
    }
    if (cnt > 16) {
      (*base) += 16;
      do {
        base_ptr[(*base)] = (idx) + trailingzeroes(bits);
        bits = bits & (bits - 1);
        (*base)++;
      } while (bits != 0);
    }
    (*base) = next_base;
  }
}

bool find_indexes(const uint8_t *buf, size_t len, ParsedCSV *pcsv, bool enable_crlf)
{
    // does the previous iteration end inside a double-quote pair?
    uint64_t prev_iter_inside_quote = 0ULL; // either all zeros or all ones
    uint64_t prev_iter_cr_end = 0ULL;       //enable_crlf
    size_t lenminus64 = len < 64 ? 0 : len - 64;
    size_t idx = 0;
    uint32_t *base_ptr = pcsv->indexes;
    uint32_t base = 0;
    // we do the index decoding in bulk for better pipelining.
    if (lenminus64 > 64 * SIMDCSV_BUFFERSIZE)
    {
        uint64_t fields[SIMDCSV_BUFFERSIZE];
        for (; idx < lenminus64 - 64 * SIMDCSV_BUFFERSIZE + 1; idx += 64 * SIMDCSV_BUFFERSIZE)
        {
            for (size_t b = 0; b < SIMDCSV_BUFFERSIZE; b++)
            {
                size_t internal_idx = 64 * b + idx;
                __builtin_prefetch(buf + internal_idx + 128);
                // simd_input in = fill_input(buf + internal_idx);
                struct simd_input in;
                const uint8_t *ptr = buf + internal_idx;
                in.lo = _mm256_loadu_si256((const __m256i *)(ptr + 0));
                in.hi = _mm256_loadu_si256((const __m256i *)(ptr + 32));
                uint64_t quote_mask = find_quote_mask(&in, &prev_iter_inside_quote);
                uint64_t sep = cmp_mask_against_input(&in, ',');
                uint64_t end;
                if (enable_crlf)
                {
                    uint64_t cr = cmp_mask_against_input(&in, 0x0d);
                    uint64_t cr_adjusted = (cr << 1) | prev_iter_cr_end;
                    uint64_t lf = cmp_mask_against_input(&in, 0x0a);
                    end = lf & cr_adjusted;
                    prev_iter_cr_end = cr >> 63;
                }
                else
                {
                    end = cmp_mask_against_input(&in, 0x0a);
                }
                fields[b] = (end | sep) & ~quote_mask;
            }
            for (size_t b = 0; b < SIMDCSV_BUFFERSIZE; b++)
            {
                size_t internal_idx = 64 * b + idx;
                flatten_bits(base_ptr, &base, internal_idx, fields[b]);
            }
        }
    }
    // tail end will be unbuffered
    for (; idx < lenminus64; idx += 64)
    {
        __builtin_prefetch(buf + idx + 128);
        // simd_input in = fill_input(buf + idx);
        struct simd_input in;
        const uint8_t *ptr = buf + idx;
        in.lo = _mm256_loadu_si256((const __m256i *)(ptr + 0));
        in.hi = _mm256_loadu_si256((const __m256i *)(ptr + 32));
        uint64_t quote_mask = find_quote_mask(&in, &prev_iter_inside_quote);
        uint64_t sep = cmp_mask_against_input(&in, ',');
        uint64_t end;
        if (enable_crlf)
        {
            uint64_t cr = cmp_mask_against_input(&in, 0x0d);
            uint64_t cr_adjusted = (cr << 1) | prev_iter_cr_end;
            uint64_t lf = cmp_mask_against_input(&in, 0x0a);
            end = lf & cr_adjusted;
            prev_iter_cr_end = cr >> 63;
        }
        else
        {
            end = cmp_mask_against_input(&in, 0x0a);
        }
        // note - a bit of a high-wire act here with quotes
        // we can't put something inside the quotes with the CR
        // then outside the quotes with LF so it's OK to "and off"
        // the quoted bits here. Some other quote convention would
        // need to be thought about carefully
        uint64_t field_sep = (end | sep) & ~quote_mask;
        flatten_bits(base_ptr, &base, idx, field_sep);
    }
    pcsv->n_indexes = base;
    return true;
}