/*
 * Redis module used only by tools/iothread-hol-bench.py.
 *
 * The command filter timestamps tagged short requests immediately before command
 * execution. The benchmark server is built with hooks directly around
 * processClientsFromIOThread, so this module can associate each tagged command
 * with the IO-worker queue that handed it to the main thread and publish true
 * invocation and residual-queue measurements.
 */

#define _GNU_SOURCE

#include "redismodule.h"
#include "iothread-hol-server-instrument.h"

#include <dlfcn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define HOL_MAX_CLIENTS 64
#define HOL_MAX_SAMPLES 262144
#define HOL_MEASURED_IO_WORKERS 2

typedef enum {
    HOL_CLIENT_NONE = 0,
    HOL_CLIENT_BULK,
    HOL_CLIENT_SHORT,
} holClientClass;

typedef struct {
    uint64_t id;
    holClientClass client_class;
    int io_worker;
} holClient;

typedef struct {
    uint64_t bulk_commands;
    uint64_t bulk_probe_commands;
    uint64_t short_commands;
} holWorkerStats;

typedef struct {
    uint64_t bulk_clients;
    uint64_t bulk_commands;
    uint64_t bulk_latency_probe_commands;
    uint64_t short_clients;
    uint64_t short_commands;
    uint64_t dropped_delay_samples;
    uint64_t unattributed_tagged_commands;
    uint64_t classification_errors;
} holStats;

typedef void (*holResetFn)(void);
typedef int (*holCurrentIOThreadFn)(void);
typedef void (*holGetStatsFn)(PerfloopHolServerStats *out);

static holResetFn server_reset;
static holCurrentIOThreadFn server_current_io_thread;
static holGetStatsFn server_get_stats;

static holClient clients[HOL_MAX_CLIENTS];
static holWorkerStats workers[PERFLOOP_HOL_MAX_IO_THREADS];
static holStats stats;
static size_t client_count;
static uint64_t short_delays[HOL_MAX_SAMPLES];
static uint64_t sort_buffer[HOL_MAX_SAMPLES];
static size_t short_delay_count;
static int collecting;

static int bytesEqual(const char *value, size_t value_len, const char *literal) {
    size_t literal_len = strlen(literal);
    return value_len == literal_len && memcmp(value, literal, literal_len) == 0;
}

static int bytesStartsWith(const char *value, size_t value_len, const char *prefix) {
    size_t prefix_len = strlen(prefix);
    return value_len >= prefix_len && memcmp(value, prefix, prefix_len) == 0;
}

static int bytesCaseEqual(const char *value, size_t value_len, const char *literal) {
    size_t literal_len = strlen(literal);

    if (value_len != literal_len) return 0;
    for (size_t i = 0; i < value_len; i++) {
        unsigned char ch = (unsigned char)value[i];
        if (ch >= 'A' && ch <= 'Z') ch = (unsigned char)(ch - 'A' + 'a');
        if (ch != (unsigned char)literal[i]) return 0;
    }
    return 1;
}

static int parseUnsigned(const char *value, size_t value_len, uint64_t *result) {
    uint64_t parsed = 0;

    if (value_len == 0) return REDISMODULE_ERR;
    for (size_t i = 0; i < value_len; i++) {
        unsigned char ch = (unsigned char)value[i];
        if (ch < '0' || ch > '9') return REDISMODULE_ERR;
        if (parsed > (UINT64_MAX - (uint64_t)(ch - '0')) / 10) return REDISMODULE_ERR;
        parsed = parsed * 10 + (uint64_t)(ch - '0');
    }
    *result = parsed;
    return REDISMODULE_OK;
}

/* Parse hol-short:<estimated-server-send-us>:<sequence>. */
static int parseShortMarker(const char *value, size_t value_len, uint64_t *sent_us) {
    static const char prefix[] = "hol-short:";
    size_t prefix_len = sizeof(prefix) - 1;
    size_t separator = prefix_len;

    if (!bytesStartsWith(value, value_len, prefix)) return REDISMODULE_ERR;
    while (separator < value_len && value[separator] != ':') separator++;
    if (separator == value_len || separator == prefix_len || separator + 1 == value_len)
        return REDISMODULE_ERR;
    return parseUnsigned(value + prefix_len, separator - prefix_len, sent_us);
}

static holClient *findClient(uint64_t id) {
    for (size_t i = 0; i < client_count; i++) {
        if (clients[i].id == id) return &clients[i];
    }
    return NULL;
}

static holClient *findOrCreateClient(uint64_t id, holClientClass client_class, int io_worker) {
    holClient *client = findClient(id);
    if (client != NULL) {
        if (client->client_class != client_class || client->io_worker != io_worker) {
            stats.classification_errors++;
            return NULL;
        }
        return client;
    }
    if (client_count == HOL_MAX_CLIENTS) {
        stats.classification_errors++;
        return NULL;
    }

    client = &clients[client_count++];
    client->id = id;
    client->client_class = client_class;
    client->io_worker = io_worker;
    if (client_class == HOL_CLIENT_BULK)
        stats.bulk_clients++;
    else
        stats.short_clients++;
    return client;
}

static void addShortDelay(uint64_t delay_us) {
    if (short_delay_count == HOL_MAX_SAMPLES) {
        stats.dropped_delay_samples++;
        return;
    }
    short_delays[short_delay_count++] = delay_us;
}

static void commandFilter(RedisModuleCommandFilterCtx *fctx) {
    RedisModuleString *command;
    RedisModuleString *argument;
    const char *command_name;
    const char *argument_value;
    size_t command_len;
    size_t argument_len;
    holClientClass client_class = HOL_CLIENT_NONE;
    int is_bulk_latency_probe = 0;
    uint64_t sent_us = 0;
    uint64_t id;
    int io_worker;
    holClient *client;

    if (!collecting || RedisModule_CommandFilterArgsCount(fctx) != 2) return;

    command = RedisModule_CommandFilterArgGet(fctx, 0);
    argument = RedisModule_CommandFilterArgGet(fctx, 1);
    command_name = RedisModule_StringPtrLen(command, &command_len);
    argument_value = RedisModule_StringPtrLen(argument, &argument_len);

    if (bytesCaseEqual(command_name, command_len, "ping") &&
        bytesEqual(argument_value, argument_len, "hol-bulk")) {
        client_class = HOL_CLIENT_BULK;
    } else if (bytesCaseEqual(command_name, command_len, "ping") &&
               bytesEqual(argument_value, argument_len, "hol-bulk-probe")) {
        is_bulk_latency_probe = 1;
    } else if (bytesCaseEqual(command_name, command_len, "echo") &&
               parseShortMarker(argument_value, argument_len, &sent_us) == REDISMODULE_OK) {
        client_class = HOL_CLIENT_SHORT;
    } else {
        return;
    }

    io_worker = server_current_io_thread();
    if (io_worker <= 0 || io_worker >= PERFLOOP_HOL_MAX_IO_THREADS) {
        stats.unattributed_tagged_commands++;
        return;
    }

    if (is_bulk_latency_probe) {
        stats.bulk_latency_probe_commands++;
        workers[io_worker].bulk_probe_commands++;
        return;
    }

    id = RedisModule_CommandFilterGetClientId(fctx);
    client = findOrCreateClient(id, client_class, io_worker);
    if (client == NULL) return;

    if (client_class == HOL_CLIENT_BULK) {
        stats.bulk_commands++;
        workers[io_worker].bulk_commands++;
        return;
    }

    stats.short_commands++;
    workers[io_worker].short_commands++;
    uint64_t now_us = RedisModule_MonotonicMicroseconds();
    if (now_us >= sent_us)
        addShortDelay(now_us - sent_us);
    else
        stats.dropped_delay_samples++;
}

static int compareU64(const void *left, const void *right) {
    const uint64_t a = *(const uint64_t *)left;
    const uint64_t b = *(const uint64_t *)right;
    return (a > b) - (a < b);
}

static uint64_t percentile(const uint64_t *values, size_t count, unsigned int percent) {
    size_t index;

    if (count == 0) return 0;
    memcpy(sort_buffer, values, count * sizeof(*values));
    qsort(sort_buffer, count, sizeof(*sort_buffer), compareU64);
    index = ((size_t)percent * count + 99) / 100;
    if (index == 0) index = 1;
    return sort_buffer[index - 1];
}

static void replyMetric(RedisModuleCtx *ctx, const char *name, uint64_t value) {
    RedisModule_ReplyWithSimpleString(ctx, name);
    RedisModule_ReplyWithLongLong(ctx, (long long)value);
}

static uint64_t schedulerWorkersObserved(const PerfloopHolServerStats *server_stats) {
    uint64_t count = 0;
    for (int worker = 1; worker < PERFLOOP_HOL_MAX_IO_THREADS; worker++) {
        if (server_stats->worker_invocation_count[worker] != 0) count++;
    }
    return count;
}

static uint64_t bulkWorkersObserved(void) {
    uint64_t count = 0;
    for (int worker = 1; worker < PERFLOOP_HOL_MAX_IO_THREADS; worker++) {
        if (workers[worker].bulk_commands != 0) count++;
    }
    return count;
}

static uint64_t leastBulkWorkerCommands(void) {
    uint64_t minimum = UINT64_MAX;
    for (int worker = 1; worker < PERFLOOP_HOL_MAX_IO_THREADS; worker++) {
        if (workers[worker].bulk_commands != 0 && workers[worker].bulk_commands < minimum)
            minimum = workers[worker].bulk_commands;
    }
    return minimum == UINT64_MAX ? 0 : minimum;
}

static int holResetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    long long pipeline;

    if (argc != 2 || RedisModule_StringToLongLong(argv[1], &pipeline) != REDISMODULE_OK || pipeline <= 0) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    memset(clients, 0, sizeof(clients));
    memset(workers, 0, sizeof(workers));
    memset(&stats, 0, sizeof(stats));
    client_count = 0;
    short_delay_count = 0;
    collecting = 0;
    server_reset();
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

static int holStartCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    if (argc != 1) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }
    collecting = 1;
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

static int holClockCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    if (argc != 1) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }
    RedisModule_ReplyWithLongLong(ctx, (long long)RedisModule_MonotonicMicroseconds());
    return REDISMODULE_OK;
}

static int holStatsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    if (argc != 1) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    PerfloopHolServerStats server_stats;
    server_get_stats(&server_stats);
    RedisModule_ReplyWithArray(ctx, 54);
    replyMetric(ctx, "short_enqueue_to_execution_p50_us", percentile(short_delays, short_delay_count, 50));
    replyMetric(ctx, "short_enqueue_to_execution_p99_us", percentile(short_delays, short_delay_count, 99));
    /* There is exactly one short stream, so these aliases state the measured
     * distribution's per-client scope without aggregating clients. */
    replyMetric(ctx, "per_client_enqueue_to_execution_p50_us", percentile(short_delays, short_delay_count, 50));
    replyMetric(ctx, "per_client_enqueue_to_execution_p99_us", percentile(short_delays, short_delay_count, 99));
    replyMetric(ctx, "short_enqueue_to_execution_samples", short_delay_count);
    replyMetric(ctx, "scheduler_invocation_count", server_stats.invocation_count);
    replyMetric(ctx, "scheduler_clients_per_invocation_max", server_stats.clients_per_invocation_max);
    replyMetric(ctx, "scheduler_commands_per_invocation_max", server_stats.commands_per_invocation_max);
    replyMetric(ctx, "scheduler_residual_queue_depth_after_invocation_max",
                server_stats.residual_queue_depth_after_invocation_max);
    replyMetric(ctx, "scheduler_io_workers_observed", schedulerWorkersObserved(&server_stats));
    replyMetric(ctx, "bulk_client_count", stats.bulk_clients);
    replyMetric(ctx, "bulk_commands_executed", stats.bulk_commands);
    replyMetric(ctx, "bulk_latency_probe_commands", stats.bulk_latency_probe_commands);
    replyMetric(ctx, "short_client_count", stats.short_clients);
    replyMetric(ctx, "short_commands_executed", stats.short_commands);
    replyMetric(ctx, "delay_samples_dropped", stats.dropped_delay_samples);
    replyMetric(ctx, "unattributed_tagged_commands", stats.unattributed_tagged_commands);
    replyMetric(ctx, "classification_errors", stats.classification_errors);
    replyMetric(ctx, "bulk_io_workers_observed", bulkWorkersObserved());
    replyMetric(ctx, "least_bulk_io_worker_commands", leastBulkWorkerCommands());
    for (int worker = 1; worker <= HOL_MEASURED_IO_WORKERS; worker++) {
        char name[64];
        snprintf(name, sizeof(name), "bulk_worker_%d_commands", worker);
        replyMetric(ctx, name, workers[worker].bulk_commands);
        snprintf(name, sizeof(name), "short_worker_%d_commands", worker);
        replyMetric(ctx, name, workers[worker].short_commands);
        snprintf(name, sizeof(name), "scheduler_worker_%d_invocations", worker);
        replyMetric(ctx, name, server_stats.worker_invocation_count[worker]);
        snprintf(name, sizeof(name), "scheduler_worker_%d_residual_queue_depth_after_invocation_max", worker);
        replyMetric(ctx, name, server_stats.worker_residual_queue_depth_after_invocation_max[worker]);
    }
    return REDISMODULE_OK;
}

static int resolveInstrumentation(RedisModuleCtx *ctx) {
    void *symbol;

    symbol = dlsym(RTLD_DEFAULT, "perfloopHolReset");
    memcpy(&server_reset, &symbol, sizeof(server_reset));
    symbol = dlsym(RTLD_DEFAULT, "perfloopHolCurrentIOThread");
    memcpy(&server_current_io_thread, &symbol, sizeof(server_current_io_thread));
    symbol = dlsym(RTLD_DEFAULT, "perfloopHolGetStats");
    memcpy(&server_get_stats, &symbol, sizeof(server_get_stats));
    if (server_reset == NULL || server_current_io_thread == NULL || server_get_stats == NULL) {
        RedisModule_Log(ctx, "warning", "HOL benchmark server hooks are unavailable");
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "holprobe", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (resolveInstrumentation(ctx) != REDISMODULE_OK)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "hol.reset", holResetCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "hol.start", holStartCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "hol.clock", holClockCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "hol.stats", holStatsCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_RegisterCommandFilter(ctx, commandFilter, REDISMODULE_CMDFILTER_NOSELF) == NULL)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;
}
