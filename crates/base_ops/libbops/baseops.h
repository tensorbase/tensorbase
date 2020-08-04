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

/* This file was automatically generated.  Do not edit! */
#undef INTERFACE
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include <string.h>
#include <immintrin.h>
typedef struct ParsedCSV ParsedCSV;
bool find_indexes(const uint8_t *buf,size_t len,ParsedCSV *pcsv,bool enable_crlf);
typedef struct simd_input simd_input;
struct simd_input {
    // #ifdef __AVX2__
    __m256i lo;
    __m256i hi;
    // #else
    // #error "It's called SIMDcsv for a reason, bro"
    // #endif
};
struct ParsedCSV {
    uint32_t n_indexes;
    uint32_t *indexes;
};
#define SIMDCSV_BUFFERSIZE 4 // it seems to be about the sweetspot.
#if !defined(NDEBUG)
#define INLINE_RELEASE
#endif
#if defined(NDEBUG)
#define INLINE_RELEASE inline __attribute__((always_inline))
#endif
#define CONTRACT_CT(exp, mesg) _Static_assert((exp, mesg))
#define CONTRACT_RT(exp, desc) assert(((void)desc, exp))
#define INTERFACE 0
