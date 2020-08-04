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