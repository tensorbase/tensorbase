#include <stdio.h>
#include <stdint.h>
#include <pthread.h>

void ker_scan(char **, const char *);

struct Args
{
    int32_t *part_raw_c0;
    int32_t id;
    int64_t ret;
};

void reduce(void *args)
{
    struct Args *a = (struct Args *)args;
    int32_t id = a->id;
    int32_t *part_raw_c0 = a->part_raw_c0;
    int32_t num_parts = 48;
    size_t part_len_c0 = 1464781690;
    size_t span = part_len_c0 / num_parts;
    size_t blk_len_c0 = id == num_parts - 1 ? (part_len_c0 - id * span) : span;
    int32_t *blk_c0 = part_raw_c0 + id * span;
    size_t s = 0;
    for (size_t i = 0; i < blk_len_c0; i++)
    {
        int32_t c0 = blk_c0[i];
        s += c0;
    }
    a->ret = s;
}

int64_t kernel()
{
    char *blk_raw_c0 = NULL;
    char fpath[64];
    sprintf(fpath, "/data/n3/data/%d", 0); //TEMP
    ker_scan(&blk_raw_c0, fpath);

    struct Args s[48];
    pthread_t ths[48];
    for (size_t i = 0; i < 48; i++)
    {
        s[i].id = i;
        s[i].part_raw_c0 = blk_raw_c0;
        pthread_create(&ths[i], NULL, reduce, &s[i]);
    }
    for (size_t i = 0; i < 48; i++)
    {
        pthread_join(ths[i], NULL);
    }
    int64_t sum = 0;
    for (size_t i = 0; i < 48; i++)
    {
        sum += s[i].ret;
    }

    return sum;
}

//===== debug main =====
#include <time.h>
int main()
{
    int64_t ret = kernel();

    struct timespec tp1,tp2 ;
	int i ;
	long t_diff ;
	clock_gettime(CLOCK_MONOTONIC_RAW, &tp1) ;

    ret = kernel();

    clock_gettime(CLOCK_MONOTONIC_RAW, &tp2) ;
	t_diff = ((tp2.tv_sec-tp1.tv_sec)*(1000*1000*1000) + (tp2.tv_nsec-tp1.tv_nsec)) ;
	printf("%lu nsec passed\n",t_diff) ;

    printf("ret: %ld\n", ret);
}
