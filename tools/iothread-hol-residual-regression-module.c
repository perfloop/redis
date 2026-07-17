/* Test-only module for residual IO-thread scheduler regressions. */

#define REDISMODULE_CORE_MODULE
#include "server.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static long long holResidualValue(RedisModuleCtx *ctx, RedisModuleString *key) {
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "GET", "s", key);
    size_t length;
    const char *value;
    long long result = 0;

    if (reply == NULL) return 0;
    if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING) {
        value = RedisModule_CallReplyStringPtr(reply, &length);
        if (value != NULL && length > 0) result = strtoll(value, NULL, 10);
    }
    RedisModule_FreeCallReply(reply);
    return result;
}

/*
 * Keep the main thread inside a command while IO workers receive a fixed
 * backlog, then enter the same nested event-loop path used by long operations.
 * The reply reports command progress immediately before and after that nested
 * path, so the test can distinguish residual work from ordinary full drains.
 */
static int holResidualBlockCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    long long delay_ms, target;
    long long before, first_after, after;
    char progress[96];

    if (argc != 4 || RedisModule_StringToLongLong(argv[1], &delay_ms) != REDISMODULE_OK ||
        RedisModule_StringToLongLong(argv[3], &target) != REDISMODULE_OK ||
        delay_ms < 1 || delay_ms > 1000 || target < 1)
    {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    blockingOperationStarts();
    usleep((useconds_t)delay_ms * 1000);
    before = holResidualValue(ctx, argv[2]);
    processEventsWhileBlocked();
    first_after = holResidualValue(ctx, argv[2]);
    after = first_after;
    for (int i = 0; i < 8 && after < target; i++) {
        processEventsWhileBlocked();
        after = holResidualValue(ctx, argv[2]);
    }
    blockingOperationEnds();
    snprintf(progress, sizeof(progress), "%lld:%lld:%lld", before, first_after, after);
    return RedisModule_ReplyWithStringBuffer(ctx, progress, strlen(progress));
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "holresidual", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    return RedisModule_CreateCommand(
        ctx, "HOLRESIDUAL.BLOCK", holResidualBlockCommand, "write allow-loading", 0, 0, 0);
}
