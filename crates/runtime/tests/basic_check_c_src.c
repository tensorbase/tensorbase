/**
 * 
 polly++ -fopenmp -O3 -o bin/omp_test -mprefer-vector-width=512 -march=native ./omp_test_02.cpp
 * 
 *
 g++ -fopenmp -O2 -o omp_test_02 -mprefer-vector-width=512 -march=native ./omp_test_1.cpp
 *
 */
#include <stdio.h>
#include <stdalign.h>
#include <stdint.h>
#include <time.h>

#define SIZE 1024
#define ROUNDS 2097152

extern int64_t test_compress_intri(int64_t *, int64_t *);

// inline __attribute__((always_inline)) int64_t test_compress(int64_t *a, int64_t *b)
// {
//   // A is compressed into B
//   int64_t j = 0;
// #pragma unroll 4
//   for (int i = 0; i < SIZE; i++)
//   {
//     if (a[i] > 255)
//     {
//       b[j++] = a[i];
//     }
//   }
//   return j+b[300];
// }

int main()
{
  struct timespec tp1, tp2;
  int i;
  long t_diff;

  int64_t sum1 = 0;
  alignas(64) int64_t a[SIZE] = {0};
  alignas(64) int64_t b[SIZE] = {0};
  int64_t count = 0;
  for (int i = 0; i < SIZE; i++)
  {
    if (i % 3 == 0)
    {
      a[i] = 256;
      count++;
    }
  }
  printf("[jited]start test...\n");
  clock_gettime(CLOCK_MONOTONIC_RAW, &tp1);
  for (int i = 0; i < ROUNDS; i++)
  {
    // sum1 += test_compress(a, b);
    sum1 += test_compress_intri(a, b);
    // sum1 += test_compress_intri(a, b);
  }
  clock_gettime(CLOCK_MONOTONIC_RAW, &tp2);
  //   int64_t s = 0;
  //   for(auto& num : b)
  //     s += num;
  t_diff = ((tp2.tv_sec - tp1.tv_sec) * (1000 * 1000 * 1000) + (tp2.tv_nsec - tp1.tv_nsec)) / 1000;
  printf("[jited]%lu usec passed\n", t_diff);
  printf("[jited]sum1: %ld, count: %ld\n", sum1, count);

  return 0;
}