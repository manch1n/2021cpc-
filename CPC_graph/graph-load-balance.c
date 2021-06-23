#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <string.h>
#include "common.h"
#include "utils.h"
#include "tools.h"
#include <athread.h>
extern void SLAVE_FUN(func)(struct _swarg *);

const char *version_name = "A reference version of edge-based load balancing";
#define PRINT_DEBUG                        \
    do                                     \
    {                                      \
        if (p_id == 0)                     \
            printf("line %d\n", __LINE__); \
    } while (0)

void preprocess(dist_graph_t *graph)
{
    int remain = graph->global_v % graph->p_num;
    int logi_v = ceiling(graph->global_v, graph->p_num);
    int less = 0;
    if (graph->local_v < logi_v)
        less = 1;
    int global_less = 0;
    MPI_Allreduce(&less, &global_less, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    if (global_less >= 2 && graph->p_id >= remain)
    {
        int send_count = graph->p_id - remain;
        int recv_count = send_count + 1;
        int recv_edge_count;
        int send_edge_count;
        index_t *tailv_pos = (index_t *)malloc(sizeof(index_t) * recv_count);
        index_t *taile_dst;
        if (graph->p_id == remain)
        {
            MPI_Recv(tailv_pos, recv_count, MPI_INT, graph->p_id + 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(&recv_edge_count, 1, MPI_INT, graph->p_id + 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            taile_dst = (index_t *)malloc(sizeof(index_t) * recv_edge_count);
            MPI_Recv(taile_dst, recv_edge_count, MPI_INT, graph->p_id + 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            graph->v_pos = realloc(graph->v_pos, sizeof(index_t) * (graph->local_v + recv_count + 1));
            graph->e_dst = realloc(graph->e_dst, sizeof(index_t) * (graph->local_e + recv_edge_count));
            memcpy(graph->v_pos + graph->local_v + 1, tailv_pos, sizeof(index_t) * recv_count);
            memcpy(graph->e_dst + graph->local_e, taile_dst, sizeof(index_t) * recv_edge_count);
            graph->local_v += recv_count;
            graph->local_e += recv_edge_count - 1;
            int ebegin = graph->v_pos[graph->local_v - 1];
            int eend = graph->v_pos[graph->local_v];
            int offsete = graph->v_pos[0];
        }
        else if (graph->p_id == (graph->p_num - 1))
        {
            MPI_Send(graph->v_pos + 1, send_count, MPI_INT, graph->p_id - 1, 0, MPI_COMM_WORLD);
            send_edge_count = graph->v_pos[send_count] - graph->v_pos[0] + 1;
            MPI_Send(&send_edge_count, 1, MPI_INT, graph->p_id - 1, 0, MPI_COMM_WORLD);
            MPI_Send(graph->e_dst, send_edge_count, MPI_INT, graph->p_id - 1, 0, MPI_COMM_WORLD);
            graph->v_pos += send_count;
            graph->e_dst += send_edge_count - 1;
            graph->local_v -= send_count;
            graph->local_e -= (send_edge_count - 1);
        }
        else
        {
            MPI_Send(graph->v_pos + 1, send_count, MPI_INT, graph->p_id - 1, 0, MPI_COMM_WORLD);
            send_edge_count = graph->v_pos[send_count] - graph->v_pos[0] + 1;
            MPI_Send(&send_edge_count, 1, MPI_INT, graph->p_id - 1, 0, MPI_COMM_WORLD);
            MPI_Send(graph->e_dst, send_edge_count, MPI_INT, graph->p_id - 1, 0, MPI_COMM_WORLD);
            MPI_Recv(tailv_pos, recv_count, MPI_INT, graph->p_id + 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(&recv_edge_count, 1, MPI_INT, graph->p_id + 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            taile_dst = (index_t *)malloc(sizeof(index_t) * recv_edge_count);
            MPI_Recv(taile_dst, recv_edge_count, MPI_INT, graph->p_id + 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            graph->v_pos = realloc(graph->v_pos, sizeof(index_t) * (graph->local_v + 1 + recv_count));
            graph->e_dst = realloc(graph->e_dst, sizeof(index_t) * (graph->local_e + recv_edge_count));
            memcpy(graph->v_pos + graph->local_v + 1, tailv_pos, sizeof(index_t) * recv_count);
            memcpy(graph->e_dst + graph->local_e, taile_dst, sizeof(index_t) * recv_edge_count);
            graph->v_pos += send_count;
            graph->e_dst += send_edge_count - 1;
            graph->local_v -= send_count;
            graph->local_e -= (send_edge_count - 1);
            graph->local_v += recv_count;
            graph->local_e += recv_edge_count - 1;
        }
        free(tailv_pos);
        free(taile_dst);
        graph->offset_v = logi_v * graph->p_id;
    }
    add_info *ai = (add_info *)malloc(sizeof(add_info));
    ai->f = (index_t *)aligned_malloc(sizeof(index_t) * graph->local_v, ALIGNE_SIZE);
    ai->nf = (index_t *)aligned_malloc(sizeof(index_t) * graph->local_v, ALIGNE_SIZE);
    ai->visited = (int *)aligned_malloc(sizeof(int) * (1 + graph->local_v / BITS_PER_WORD), ALIGNE_SIZE);
    ai->dis = (int *)aligned_malloc(sizeof(int) * graph->p_num, ALIGNE_SIZE);
    init_buffer(&(ai->send_buf), graph->p_num, graph->local_v * 4);
    init_buffer(&(ai->recv_buf), graph->p_num, graph->local_v * 4);

    struct _swarg *swarg = (struct _swarg *)aligned_malloc(sizeof(struct _swarg), ALIGNE_SIZE);
    ai->swarg = swarg;
    buffer next_frontier_buf;
    init_buffer(&next_frontier_buf, graph->p_num, graph->local_v);
    int div = ceiling(graph->global_v, graph->p_num);
    int sdiv = ceiling(graph->local_v, graph->p_num);
    ai->next_frontier_buf = next_frontier_buf;
    swarg->v_pos = graph->v_pos;
    swarg->e_dst = graph->e_dst;
    swarg->frontier = ai->f;
    swarg->frontier_len = 0;
    swarg->next_frontier_buf = next_frontier_buf;
    swarg->offset_e = graph->v_pos[0];
    swarg->offset_v = graph->offset_v;
    swarg->p_id = graph->p_id;
    swarg->p_num = graph->p_num;
    swarg->send_buf = ai->send_buf;
    swarg->visited = ai->visited;
    swarg->div = div;
    swarg->sdiv = sdiv;

    for (int p = 0; p < graph->p_num; ++p)
    {
        ai->dis[p] = p * (ai->recv_buf.max_column);
    }
    graph->additional_info = ai;
    athread_init();
}

void destroy_additional_info(void *additional_info)
{
    add_info *ai = (add_info *)additional_info;
    aligned_free(ai->f);
    aligned_free(ai->nf);
    destroy_buffer(&(ai->send_buf));
    destroy_buffer(&(ai->recv_buf));
    destroy_buffer(&(ai->next_frontier_buf));
    aligned_free(ai->dis);
    aligned_free(ai->visited);
    aligned_free(ai->swarg);
    free(ai);
}

void bfs(dist_graph_t *graph, index_t s, index_t *pred)
{
    const int p_id = graph->p_id;
    const int p_num = graph->p_num;
    const int offset_v = graph->offset_v;
    const int offset_e = graph->v_pos[0];
    const int local_v = graph->local_v;
    const int r = graph->global_v % p_num;
    const int bitmap_len = 1 + local_v / BITS_PER_WORD;

    for (int i = 0; i < local_v; ++i)
    {
        pred[i] = UNREACHABLE;
    }
    int div = ceiling(graph->global_v, graph->p_num);
    int sdiv = ceiling(graph->local_v, graph->p_num);

    add_info *ai = (add_info *)graph->additional_info;

    int *dis = ai->dis;
    index_t *frontier = ai->f; //当前要访问的节点容器
    int frontier_len = 0;
    index_t *next_frontier = ai->nf;
    int next_frontier_len = 0;
    int *visited = ai->visited;
    CLR_ALL(visited, bitmap_len);

    buffer send_buf = ai->send_buf;
    buffer recv_buf = ai->recv_buf;

    struct _swarg *swarg = ai->swarg;
    swarg->pred = pred;
    buffer next_frontier_buf = swarg->next_frontier_buf;

    int pvs = s / div; //为源节点所属进程，注意，每个节点的处理仅在其所属进程
    if (p_id == pvs)
    {
        frontier[0] = s;
        frontier_len++;
        pred[s - offset_v] = s; //插入源节点的所属进程的容器中
        BITMAP_SET(visited, s - offset_v);
    }

    bool use_slaves = false;
    MPI_Request re;
    MPI_Status sa;

    while (1)
    {
        int global_frontier_len = 0; //把所有节点的容器的长度加起来，为0则推出
        MPI_Allreduce(&frontier_len, &global_frontier_len, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
        if (global_frontier_len == 0)
            break;
        if (1)
        {
            use_slaves = false;
            for (int i = 0; i < frontier_len; ++i)
            {
                int u = frontier[i];
                int nbegin = graph->v_pos[u - offset_v]; //确定该点邻居的起始与结束的索引范围，注意要减去offset
                int nend = graph->v_pos[u - offset_v + 1];
                int pv;                             //pv表示的是该点所属进程的rank
                for (int j = nbegin; j < nend; ++j) //处理边(u->v)
                {
                    int v = graph->e_dst[j - offset_e];
                    pv = v / div;   //计算v节点的所属进程
                    if (pv == p_id) //如果在本节点则先行处理。top-down方法
                    {
                        if (BITMAP_TEST(visited, v - offset_v))
                            continue;
                        if (pred[v - offset_v] == UNREACHABLE)
                        {
                            pred[v - offset_v] = u;
                            BITMAP_SET(visited, v - offset_v);
                            next_frontier[next_frontier_len] = v;
                            next_frontier_len++;
                        }
                    }
                    else //否则增添到所属进程的发送buf中
                    {    //数据格式为 P_i【[u1,v1] [u2,v2] [u3,v3] .......】
                        send_buf.data[pv][send_buf.count[pv] * 2] = u;
                        send_buf.data[pv][send_buf.count[pv] * 2 + 1] = v;
                        send_buf.count[pv]++;
                    }
                }
            }
        }
        else
        {
            use_slaves = true;
            swarg->frontier = frontier;
            swarg->frontier_len = frontier_len;
            athread_spawn(func, swarg);
            athread_join();
        }

        for (int p = 0; p < p_num; ++p)
        {
            if (p == p_id)
                continue;
            send_buf.count[p] *= 2;     //乘2为实际int数
            if (send_buf.count[p] == 0) //如果没数据要发送也要发送1个int来驱动程序
            {
                send_buf.data[p][0] = -1;
                send_buf.count[p]++;
            }
        }

        MPI_Alltoall(send_buf.count, 1, MPI_INT, recv_buf.count, 1, MPI_INT, MPI_COMM_WORLD);

        MPI_Ialltoallv(send_buf._underlying, send_buf.count, dis, MPI_INT32_T, recv_buf._underlying, recv_buf.count, dis, MPI_INT32_T, MPI_COMM_WORLD, &re);
        if (use_slaves)
        {
            next_frontier_len = 0;
            for (int p = 0; p < p_num; ++p)
            {
                memcpy(next_frontier + next_frontier_len, next_frontier_buf.data[p], sizeof(index_t) * next_frontier_buf.count[p]);
                next_frontier_len += next_frontier_buf.count[p];
            }
        }
        MPI_Wait(&re, &sa);
        for (int p = 0; p < p_num; ++p)
        {
            if (p == p_id)
                continue;
            int nedge = recv_buf.count[p] / 2; //实际边的对数
            if (nedge == 0)
                continue;
            for (int i = 0; i < nedge; ++i)
            {
                int u = recv_buf.data[p][i * 2]; //解析出(u->v)
                int v = recv_buf.data[p][i * 2 + 1];
                if (BITMAP_TEST(visited, v - offset_v))
                    continue;
                if (pred[v - offset_v] == UNREACHABLE) //一样的top-down方法
                {
                    pred[v - offset_v] = u;

                    BITMAP_SET(visited, v - offset_v);
                    next_frontier[next_frontier_len] = v;
                    next_frontier_len++;
                }
            }
        }
        index_t *tmp = frontier; //交换容器
        frontier = next_frontier;
        next_frontier = tmp;
        frontier_len = next_frontier_len;
        next_frontier_len = 0;
        for (int p = 0; p < p_num; ++p)
        {
            send_buf.count[p] = 0;
            recv_buf.count[p] = 0;
        }
    }
}
