#include "tools.h"

void *aligned_malloc(size_t size, int alignment)
{
    const int pointerSize = sizeof(void *);
    const int requestedSize = size + alignment - 1 + pointerSize;
    void *raw = malloc(requestedSize);
    uintptr_t start = (uintptr_t)raw + pointerSize;
    void *aligned = (void *)((start + alignment - 1) & ~(alignment - 1));
    *(void **)((uintptr_t)aligned - pointerSize) = raw;
    return aligned;
}

void aligned_free(void *aligned)
{
    void *raw = *(void **)((uintptr_t)aligned - sizeof(void *));
    free(raw);
}

bool is_aligned(void *data, int alignment)
{
    return ((uintptr_t)data & (alignment - 1)) == 0;
}

void init_buffer(buffer *buf, int r, int c)
{
    buf->max_row = r;
    buf->max_column = ceiling(c, ALIGNE_SIZE) * ALIGNE_SIZE;
    buf->_underlying = (index_t *)aligned_malloc(sizeof(index_t) * r * buf->max_column, ALIGNE_SIZE);
    buf->data = (index_t **)aligned_malloc(sizeof(index_t *) * r, ALIGNE_SIZE);
    buf->count = (index_t *)aligned_malloc(sizeof(index_t) * r, ALIGNE_SIZE);
    for (int i = 0; i < r; ++i)
    {
        buf->count[i] = 0;
        buf->data[i] = buf->_underlying + i * (buf->max_column);
    }
}

void destroy_buffer(buffer *buf)
{
    aligned_free(buf->data);
    aligned_free(buf->_underlying);
    aligned_free(buf->count);
}
int ceiling(int num, int den)
{
    return (num - 1) / den + 1;
}