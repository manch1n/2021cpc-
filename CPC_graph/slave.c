#include <slave.h>
#include <stdio.h>
#include "tools.h"

#define MAX_FRONTIER_SIZE 20 * 1024 / sizeof(index_t)      //20kb
#define MAX_NEXT_FRONTIER_SIZE 20 * 1024 / sizeof(index_t) //20kb
#define MAX_DEGREE 100                                     //400b
#define MAX_SEND_SIZE 10 * 1024 / sizeof(index_t)          //10kb
#define MAX_EDGE_SIZE MAX_SEND_SIZE / 2

__thread_local volatile unsigned long get_reply, put_reply;
__thread_local volatile int my_id;
__thread_local int pr;
__thread_local int gr;

void wait_reply(volatile unsigned long *reply, int m)
{
    while (*reply != m)
    {
    };
}

void slave_func(struct _swarg *marg)
{
    gr = 1;
    pr = 1;
    get_reply=0;
    put_reply=0;

    my_id = athread_get_id(-1);
    struct _swarg sarg;
    athread_get(PE_MODE, marg, &sarg, sizeof(sarg), &get_reply, 0, 0, 0);
    wait_reply(&get_reply, gr++);
    int p_num = sarg.p_num;
    int offset_v = sarg.offset_v;
    int *mvisited = sarg.visited;
    int p_id = sarg.p_id;
    if (my_id >= p_num)
        return;
    index_t *mv_pos = sarg.v_pos;
    index_t *me_dst = sarg.e_dst;
    index_t *mfrontier = sarg.frontier;
    int frontier_len = sarg.frontier_len;
    index_t *msend_buf = sarg.send_buf._underlying + my_id * sarg.send_buf.max_column;
    index_t *mpred = sarg.pred;
    index_t *mnext_frontier = sarg.next_frontier_buf._underlying + my_id * sarg.next_frontier_buf.max_column;
    int offset_e = sarg.offset_e;
    int sdiv=sarg.sdiv;
    int div=sarg.div;

    

    index_t frontier_buf[MAX_FRONTIER_SIZE];
    index_t nf_buf[MAX_NEXT_FRONTIER_SIZE];
    index_t vs[MAX_DEGREE];
    index_t send_buf[MAX_SEND_SIZE];

    index_t erange[2];
    index_t single_pred;

    int nf_count = 0;
    int edge_count = 0;
    int all_nf_size = 0;
    int all_edge_count = 0;

    int frontier_left = frontier_len;
    int read_offset = 0;
    while (frontier_left)
    {
        int single_read_count = 0;
        if (frontier_left >= MAX_FRONTIER_SIZE)
        {
            single_read_count = MAX_FRONTIER_SIZE;
        }
        else
        {
            single_read_count = frontier_left;
        }
        frontier_left -= single_read_count;
        if(single_read_count==0) break;
        athread_get(PE_MODE, mfrontier + read_offset, frontier_buf, sizeof(index_t) * single_read_count, &get_reply, 0, 0, 0);
        wait_reply(&get_reply, gr++);
        read_offset += single_read_count;
        for (int i = 0; i < single_read_count; ++i)
        {
            int u = frontier_buf[i];
            athread_get(PE_MODE, mv_pos + u - offset_v, erange, sizeof(index_t) * 2, &get_reply, 0, 0, 0);
            wait_reply(&get_reply, gr++);
            index_t ebegin = erange[0] - offset_e;
            index_t eend = erange[1] - offset_e;
            int elen = eend - ebegin;
            if (elen == 0)
                continue;
            athread_get(PE_MODE, me_dst + ebegin, vs, sizeof(index_t) * elen, &get_reply, 0, 0, 0);
            wait_reply(&get_reply, gr++);
            for (int e = 0; e < elen; ++e)
            {
                int v = vs[e];
                int pv = v / div;
                if (pv == p_id)
                {
                    if((v-offset_v)/sdiv!=my_id) continue;
                    athread_get(PE_MODE, mpred + v - offset_v, &single_pred, sizeof(index_t), &get_reply, 0, 0, 0);
                    wait_reply(&get_reply, gr++);
                    if (single_pred == UNREACHABLE)
                    {
                        athread_put(PE_MODE, &u, mpred + v - offset_v, sizeof(index_t), &put_reply, 0, 0);
                        wait_reply(&put_reply, pr++);
                        nf_buf[nf_count++] = v;
                        if (nf_count == MAX_NEXT_FRONTIER_SIZE)
                        {
                            athread_put(PE_MODE, nf_buf, mnext_frontier + all_nf_size, sizeof(index_t) * MAX_NEXT_FRONTIER_SIZE, &put_reply, 0, 0);
                            all_nf_size += MAX_NEXT_FRONTIER_SIZE;
                            wait_reply(&put_reply, pr++);
                            nf_count = 0;
                        }
                    }
                }
                else if (pv == my_id)
                {
                    send_buf[edge_count * 2] = u;
                    send_buf[edge_count * 2 + 1] = v;
                    edge_count++;
                    if (edge_count == MAX_EDGE_SIZE)
                    {
                        athread_put(PE_MODE, send_buf, msend_buf + all_edge_count * 2, sizeof(index_t) * MAX_EDGE_SIZE * 2, &put_reply, 0, 0);
                        wait_reply(&put_reply, pr++);
                        all_edge_count += MAX_EDGE_SIZE;
                        edge_count = 0;
                    }
                }
            }
        }
     }
    //flush
    if (nf_count)
    {
        athread_put(PE_MODE, nf_buf, mnext_frontier + all_nf_size, sizeof(index_t) * nf_count, &put_reply, 0, 0);
        wait_reply(&put_reply, pr++);
        all_nf_size += nf_count;
    }
    if (edge_count)
    {
        athread_put(PE_MODE, send_buf, msend_buf + all_edge_count * 2, sizeof(index_t) * edge_count * 2, &put_reply, 0, 0);
        wait_reply(&put_reply, pr++);
        all_edge_count += edge_count;
    }
    athread_put(PE_MODE, &all_edge_count, sarg.send_buf.count + my_id, sizeof(index_t), &put_reply, 0, 0);
    pr++;
    athread_put(PE_MODE, &all_nf_size, sarg.next_frontier_buf.count + my_id, sizeof(index_t), &put_reply, 0, 0);
    wait_reply(&put_reply, pr++);
}