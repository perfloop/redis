#define _DEFAULT_SOURCE
#include "redismodule.h"

#include <unistd.h>

/* Each command below is intentionally limited to the test module. The marker
 * and stop commands are allow-busy so they can run during iothreadtest.slow's
 * event-loop yield. */
static long long yield_epoch;
static int stop_requested;

static int slowCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    if (argc != 1) {
        return RedisModule_WrongArity(ctx);
    }

    yield_epoch = 0;
    stop_requested = 0;
    while (!stop_requested) {
        yield_epoch++;
        RedisModule_Yield(ctx, REDISMODULE_YIELD_FLAG_CLIENTS, "Slow IO-thread test operation");
        if (!stop_requested)
            usleep(110000); /* Longer than the test server's 10 Hz yield interval. */
    }

    RedisModule_ReplyWithLongLong(ctx, yield_epoch);
    return REDISMODULE_OK;
}

static int markerCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    if (argc != 1) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_ReplyWithLongLong(ctx, yield_epoch);
    return REDISMODULE_OK;
}

static int stopCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    if (argc != 1) {
        return RedisModule_WrongArity(ctx);
    }
    stop_requested = 1;
    RedisModule_ReplyWithLongLong(ctx, yield_epoch);
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    if (RedisModule_Init(ctx, "iothreadtest", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "iothreadtest.slow", slowCommand, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "iothreadtest.marker", markerCommand, "allow-busy", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "iothreadtest.stop", stopCommand, "allow-busy", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;
}
