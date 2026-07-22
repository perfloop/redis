#define _GNU_SOURCE

#include <dlfcn.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

/*
 * The proof workload needs a baseline with the predecessor's per-writev
 * quantum.  This preload shim clips the syscall while preserving writev's
 * short-write contract, and logs both the original and issued vector sizes.
 * It is test-only: Redis itself is built without a source instrumentation
 * change, and candidate evidence exposes the original size in the trace.
 */

static ssize_t (*next_writev)(int, const struct iovec *, int);
static int trace_fd = -1;
static size_t writev_cap;

static size_t parse_cap(const char *value) {
    if (value == NULL || *value == '\0') return 0;
    char *end = NULL;
    unsigned long long parsed = strtoull(value, &end, 10);
    if (end == value || *end != '\0' || parsed > SIZE_MAX) return 0;
    return (size_t)parsed;
}

__attribute__((constructor)) static void init_writev_cap_trace(void) {
    next_writev = dlsym(RTLD_NEXT, "writev");
    writev_cap = parse_cap(getenv("PERFLOOP_WRITEV_CAP"));

    const char *path = getenv("PERFLOOP_WRITEV_TRACE");
    if (path != NULL && *path != '\0') {
        trace_fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0600);
    }
}

static size_t iov_total(const struct iovec *iov, int iovcnt) {
    size_t total = 0;
    for (int i = 0; i < iovcnt; i++) {
        if (iov[i].iov_len > SIZE_MAX - total) return SIZE_MAX;
        total += iov[i].iov_len;
    }
    return total;
}

static void record_trace(size_t requested, size_t issued, ssize_t returned) {
    if (trace_fd < 0) return;
    char line[128];
    int length = snprintf(line, sizeof(line), "%zu %zu %zd\n", requested, issued, returned);
    if (length <= 0 || (size_t)length >= sizeof(line)) return;
    syscall(SYS_write, trace_fd, line, (size_t)length);
}

ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
    if (next_writev == NULL) {
        next_writev = dlsym(RTLD_NEXT, "writev");
        if (next_writev == NULL) return -1;
    }

    size_t requested = iov_total(iov, iovcnt);
    if (writev_cap == 0 || requested <= writev_cap) {
        ssize_t returned = next_writev(fd, iov, iovcnt);
        record_trace(requested, requested, returned);
        return returned;
    }

    struct iovec capped[iovcnt];
    size_t issued = 0;
    int capped_count = 0;
    for (int i = 0; i < iovcnt && issued < writev_cap; i++) {
        size_t remaining = writev_cap - issued;
        size_t length = iov[i].iov_len < remaining ? iov[i].iov_len : remaining;
        if (length == 0) continue;
        capped[capped_count] = iov[i];
        capped[capped_count].iov_len = length;
        capped_count++;
        issued += length;
    }

    ssize_t returned = next_writev(fd, capped, capped_count);
    record_trace(requested, issued, returned);
    return returned;
}
