#ifndef TOOLS_H
#define TOOLS_H

#include <stdlib.h>
#include <string.h>
#include "common.h"

#define MSG_DATA 0
#define MSG_COUNT 1

#define BITS_PER_WORD 32
#define MASK 0x1f
#define SHIFT 5

#define BITMAP_SET(data, i)                    \
    do                                         \
    {                                          \
        data[i >> SHIFT] |= (1 << (i & MASK)); \
    } while (0)

#define BITMAP_TEST(data, i) (data[i >> SHIFT] & (1 << (i & MASK)))
#define CLR_ALL(data, len)                  \
    do                                      \
    {                                       \
        memset(data, 0, sizeof(int) * len); \
    } while (0)

#define SWAP_POINTER(a, b) \
    do                     \
    {                      \
        void *tmp = a;     \
        a = b;             \
        b = tmp;           \
    } while (0)

#define ALIGNE_SIZE 256
void *aligned_malloc(size_t size, int alignment);

void aligned_free(void *aligned);

bool is_aligned(void *data, int alignment);

typedef struct
{
    int max_row;
    int max_column;
    int *count;
    int *_underlying;
    int **data;
} buffer; //二维缓冲，行数表示数据所属的进程

struct _swarg
{
    int p_num;
    int offset_v;
    int offset_e;
    int p_id;
    int sdiv;
    int div;
    int frontier_len;
    int *visited;
    index_t *v_pos;
    index_t *e_dst;
    index_t* pred;
    index_t *frontier;
    buffer next_frontier_buf;
    buffer send_buf;
};

typedef struct
{
    index_t *f;
    index_t *nf;
    int *visited;
    int *dis;
    struct _swarg* swarg;
    buffer next_frontier_buf;
    buffer send_buf;
    buffer recv_buf;
} add_info;

int ceiling(int num, int den);

void init_buffer(buffer *buf, int r, int c);

void destroy_buffer(buffer *buf);
#endif

