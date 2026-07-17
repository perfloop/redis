#ifndef PERFLOOP_IOTHREAD_HOL_SERVER_INSTRUMENT_H
#define PERFLOOP_IOTHREAD_HOL_SERVER_INSTRUMENT_H

#include <stdint.h>

#define PERFLOOP_HOL_MAX_IO_THREADS 128

typedef struct {
    uint64_t invocation_count;
    uint64_t clients_per_invocation_max;
    uint64_t commands_per_invocation_max;
    uint64_t residual_queue_depth_after_invocation_max;
    uint64_t worker_invocation_count[PERFLOOP_HOL_MAX_IO_THREADS];
    uint64_t worker_clients_per_invocation_max[PERFLOOP_HOL_MAX_IO_THREADS];
    uint64_t worker_commands_per_invocation_max[PERFLOOP_HOL_MAX_IO_THREADS];
    uint64_t worker_residual_queue_depth_after_invocation_max[PERFLOOP_HOL_MAX_IO_THREADS];
} PerfloopHolServerStats;

void perfloopHolReset(void);
void perfloopHolInvocationEnter(int io_thread_id, long long commands_before);
void perfloopHolClientProcessed(void);
void perfloopHolInvocationExit(int io_thread_id, long long commands_after,
                               uint64_t residual_queue_depth);
int perfloopHolCurrentIOThread(void);
void perfloopHolGetStats(PerfloopHolServerStats *out);

#endif
