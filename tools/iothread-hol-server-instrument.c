/*
 * Included into src/iothread.c only by the isolated HOL benchmark build.
 * The hooks delimit actual processClientsFromIOThread invocations, so their
 * client, command, and residual-queue counts do not infer a callback boundary
 * from an event-loop cycle.
 */

#include "iothread-hol-server-instrument.h"

#include <string.h>

#define PERFLOOP_HOL_MAX_INVOCATION_DEPTH 64

#if defined(__GNUC__)
#define PERFLOOP_HOL_EXPORT __attribute__((used, visibility("default")))
#else
#define PERFLOOP_HOL_EXPORT
#endif

typedef struct {
    int io_thread_id;
    uint64_t clients;
    long long commands_before;
} PerfloopHolInvocation;

static PerfloopHolServerStats perfloop_hol_stats;
static PerfloopHolInvocation perfloop_hol_invocations[PERFLOOP_HOL_MAX_INVOCATION_DEPTH];
static unsigned int perfloop_hol_invocation_depth;

static void perfloopHolUpdateMax(uint64_t *current, uint64_t value) {
    if (value > *current) *current = value;
}

PERFLOOP_HOL_EXPORT void perfloopHolReset(void) {
    /* HOL.RESET itself executes inside an instrumented invocation. Reset only
     * published counters so that invocation can still unwind normally. */
    memset(&perfloop_hol_stats, 0, sizeof(perfloop_hol_stats));
}

PERFLOOP_HOL_EXPORT void perfloopHolInvocationEnter(int io_thread_id, long long commands_before) {
    serverAssert(perfloop_hol_invocation_depth < PERFLOOP_HOL_MAX_INVOCATION_DEPTH);
    PerfloopHolInvocation *invocation =
        &perfloop_hol_invocations[perfloop_hol_invocation_depth++];
    invocation->io_thread_id = io_thread_id;
    invocation->clients = 0;
    invocation->commands_before = commands_before;
}

PERFLOOP_HOL_EXPORT void perfloopHolClientProcessed(void) {
    serverAssert(perfloop_hol_invocation_depth > 0);
    perfloop_hol_invocations[perfloop_hol_invocation_depth - 1].clients++;
}

PERFLOOP_HOL_EXPORT void perfloopHolInvocationExit(int io_thread_id, long long commands_after,
                                                    uint64_t residual_queue_depth) {
    serverAssert(perfloop_hol_invocation_depth > 0);
    PerfloopHolInvocation invocation =
        perfloop_hol_invocations[--perfloop_hol_invocation_depth];
    serverAssert(invocation.io_thread_id == io_thread_id);
    if (invocation.clients == 0) return;

    uint64_t commands = 0;
    if (commands_after >= invocation.commands_before)
        commands = (uint64_t)(commands_after - invocation.commands_before);

    perfloop_hol_stats.invocation_count++;
    perfloopHolUpdateMax(&perfloop_hol_stats.clients_per_invocation_max, invocation.clients);
    perfloopHolUpdateMax(&perfloop_hol_stats.commands_per_invocation_max, commands);
    perfloopHolUpdateMax(&perfloop_hol_stats.residual_queue_depth_after_invocation_max,
                         residual_queue_depth);

    if (io_thread_id > 0 && io_thread_id < PERFLOOP_HOL_MAX_IO_THREADS) {
        perfloop_hol_stats.worker_invocation_count[io_thread_id]++;
        perfloopHolUpdateMax(
            &perfloop_hol_stats.worker_clients_per_invocation_max[io_thread_id], invocation.clients);
        perfloopHolUpdateMax(
            &perfloop_hol_stats.worker_commands_per_invocation_max[io_thread_id], commands);
        perfloopHolUpdateMax(
            &perfloop_hol_stats.worker_residual_queue_depth_after_invocation_max[io_thread_id],
            residual_queue_depth);
    }
}

PERFLOOP_HOL_EXPORT int perfloopHolCurrentIOThread(void) {
    if (perfloop_hol_invocation_depth == 0) return -1;
    return perfloop_hol_invocations[perfloop_hol_invocation_depth - 1].io_thread_id;
}

PERFLOOP_HOL_EXPORT void perfloopHolGetStats(PerfloopHolServerStats *out) {
    *out = perfloop_hol_stats;
}
