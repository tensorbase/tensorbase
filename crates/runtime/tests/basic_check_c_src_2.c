#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <pthread.h>
// typedef struct
// {
//     char *path;
//     uint32_t size;

// } BlockMeta;

// void ker_meta()
// {
// }

void ker_scan(
    char **data_out, const char *fpath)
{
    struct stat fbuf;
    int status;
    status = stat(fpath, &fbuf);
    if (status < 0)
    {
        printf("status: %d\n", status);
        perror("can not stat for file");
    }
    size_t flen = fbuf.st_size;

    int fd = open(fpath, O_RDONLY);
    *data_out = mmap(0, flen, PROT_READ, MAP_PRIVATE, fd, 0);
}

// int32_t get_num_available_processors() {
//     return sysconf(_SC_NPROCESSORS_ONLN);
// }




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
    size_t part_len_c0 = 19998193;
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
int main()
{
    int64_t ret = kernel();
    printf("ret: %ld\n", ret);
}
