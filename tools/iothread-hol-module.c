/*
 * Redis module used only by tools/iothread-hol-bench.py.
 *
 * The command filter runs on the main thread immediately before Redis begins
 * executing a command.  The benchmark puts a server-clock timestamp in each
 * short ECHO request, so the filter can measure submission-to-execution delay
 * without treating reply arrival time as queue delay.  It also groups the
 * tagged workload commands by Redis event-loop cycle and tracks the number of
 * submitted pipeline commands that remain unexecuted at a cycle boundary.
 */

#include "redismodule.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define HOL_MAX_CLIENTS 64
#define HOL_MAX_SAMPLES 262144

typedef enum {
    HOL_CLIENT_NONE = 0,
    HOL_CLIENT_BULK,
    HOL_CLIENT_SHORT,
} holClientClass;

typedef struct {
    uint64_t id;
    uint64_t executed;
    uint64_t submitted;
    uint64_t seen_cycle;
    holClientClass client_class;
} holClient;

typedef struct {
    uint64_t cycle_count;
    uint64_t max_clients_per_cycle;
    uint64_t max_commands_per_cycle;
    uint64_t max_residual_depth;
    uint64_t bulk_clients;
    uint64_t bulk_commands;
    uint64_t bulk_latency_probe_commands;
    uint64_t short_clients;
    uint64_t short_commands;
    uint64_t dropped_delay_samples;
} holStats;

static holClient clients[HOL_MAX_CLIENTS];
static size_t client_count;
static holStats stats;
static uint64_t short_delays[HOL_MAX_SAMPLES];
static uint64_t sort_buffer[HOL_MAX_SAMPLES];
static size_t short_delay_count;
static uint64_t current_cycle;
static uint64_t cycle_clients;
static uint64_t cycle_commands;
static uint64_t bulk_pipeline = 256;
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

static holClient *findOrCreateClient(uint64_t id, holClientClass client_class) {
    holClient *client = findClient(id);
    if (client != NULL) return client;
    if (client_count == HOL_MAX_CLIENTS) return NULL;

    client = &clients[client_count++];
    memset(client, 0, sizeof(*client));
    client->id = id;
    client->client_class = client_class;
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

static uint64_t residualDepth(void) {
    uint64_t residual = 0;

    for (size_t i = 0; i < client_count; i++) {
        if (clients[i].submitted > clients[i].executed)
            residual += clients[i].submitted - clients[i].executed;
    }
    return residual;
}

static void observeCycle(void) {
    uint64_t residual;

    if (!collecting) return;
    residual = residualDepth();
    if (residual > stats.max_residual_depth)
        stats.max_residual_depth = residual;

    if (cycle_commands != 0) {
        stats.cycle_count++;
        if (cycle_clients > stats.max_clients_per_cycle)
            stats.max_clients_per_cycle = cycle_clients;
        if (cycle_commands > stats.max_commands_per_cycle)
            stats.max_commands_per_cycle = cycle_commands;
    }
}

static void finishCycle(void) {
    observeCycle();
    if (!collecting) return;

    current_cycle++;
    cycle_clients = 0;
    cycle_commands = 0;
}

static void eventLoopCallback(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(data);

    if (eid.id == REDISMODULE_EVENT_EVENTLOOP &&
        subevent == REDISMODULE_SUBEVENT_EVENTLOOP_AFTER_SLEEP) {
        /* This closes the preceding full Redis event-loop turn, including its
         * beforeSleep IO-thread handoff processing. */
        finishCycle();
    }
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
    uint64_t now_us;
    uint64_t id;
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

    if (is_bulk_latency_probe) {
        stats.bulk_latency_probe_commands++;
        return;
    }

    id = RedisModule_CommandFilterGetClientId(fctx);
    client = findOrCreateClient(id, client_class);
    if (client == NULL || client->client_class != client_class) return;

    if (client->seen_cycle != current_cycle + 1) {
        client->seen_cycle = current_cycle + 1;
        cycle_clients++;
    }
    cycle_commands++;

    if (client_class == HOL_CLIENT_BULK) {
        /* redis-benchmark emits a fixed-size pipeline then waits for all of its
         * replies. Its first tagged command therefore makes this many submitted
         * commands eligible for the residual-at-cycle-boundary accounting. */
        if (client->executed % bulk_pipeline == 0)
            client->submitted += bulk_pipeline;
        client->executed++;
        stats.bulk_commands++;
        return;
    }

    client->submitted++;
    client->executed++;
    stats.short_commands++;
    now_us = RedisModule_MonotonicMicroseconds();
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

static int holResetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    long long pipeline;

    if (argc != 2 || RedisModule_StringToLongLong(argv[1], &pipeline) != REDISMODULE_OK || pipeline <= 0) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    memset(clients, 0, sizeof(clients));
    memset(&stats, 0, sizeof(stats));
    client_count = 0;
    short_delay_count = 0;
    current_cycle = 0;
    cycle_clients = 0;
    cycle_commands = 0;
    bulk_pipeline = (uint64_t)pipeline;
    collecting = 0;
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

    /* Capture a sample even when this command runs before the next afterSleep. */
    observeCycle();
    RedisModule_ReplyWithArray(ctx, 32);
    replyMetric(ctx, "short_enqueue_to_execution_p50_us", percentile(short_delays, short_delay_count, 50));
    replyMetric(ctx, "short_enqueue_to_execution_p99_us", percentile(short_delays, short_delay_count, 99));
    /* The benchmark has exactly one registered short client, so these aliases
     * make the per-client nature of the measured series explicit. */
    replyMetric(ctx, "per_client_enqueue_to_execution_p50_us", percentile(short_delays, short_delay_count, 50));
    replyMetric(ctx, "per_client_enqueue_to_execution_p99_us", percentile(short_delays, short_delay_count, 99));
    replyMetric(ctx, "short_enqueue_to_execution_samples", short_delay_count);
    replyMetric(ctx, "main_callback_clients_per_invocation_max", stats.max_clients_per_cycle);
    replyMetric(ctx, "main_callback_commands_per_invocation_max", stats.max_commands_per_cycle);
    replyMetric(ctx, "residual_queue_depth_after_callback_max", stats.max_residual_depth);
    replyMetric(ctx, "instrumented_event_loop_cycles", stats.cycle_count);
    replyMetric(ctx, "bulk_client_count", stats.bulk_clients);
    replyMetric(ctx, "bulk_commands_executed", stats.bulk_commands);
    replyMetric(ctx, "bulk_latency_probe_commands", stats.bulk_latency_probe_commands);
    replyMetric(ctx, "short_client_count", stats.short_clients);
    replyMetric(ctx, "short_commands_executed", stats.short_commands);
    replyMetric(ctx, "delay_samples_dropped", stats.dropped_delay_samples);
    replyMetric(ctx, "bulk_pipeline_size", bulk_pipeline);
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "holprobe", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
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
    if (RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_EventLoop, eventLoopCallback) != REDISMODULE_OK)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;
}
