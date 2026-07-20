#define _GNU_SOURCE

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

/*
 * This is an intentionally small, test-only LD_PRELOAD probe.  The
 * head-of-line benchmark needs the number of bytes actually accepted by one
 * writev(2), not the total bytes queued in a Redis reply.  It records the
 * largest successful writev return value in PERFLOOP_WRITEV_TRACE when the
 * server exits.
 */

static ssize_t (*real_writev_fn)(int, const struct iovec *, int);
static _Atomic size_t max_accepted_bytes;
static _Atomic uint64_t max_duration_ns;
static _Atomic size_t writev_calls;

static void resolve_writev(void) {
    if (real_writev_fn == NULL) {
        real_writev_fn = dlsym(RTLD_NEXT, "writev");
        if (real_writev_fn == NULL) _exit(127);
    }
}

static void update_max_size(_Atomic size_t *maximum, size_t value) {
    size_t previous = atomic_load_explicit(maximum, memory_order_relaxed);
    while (value > previous &&
           !atomic_compare_exchange_weak_explicit(maximum, &previous, value,
                                                   memory_order_relaxed, memory_order_relaxed)) {
    }
}

static void update_max_u64(_Atomic uint64_t *maximum, uint64_t value) {
    uint64_t previous = atomic_load_explicit(maximum, memory_order_relaxed);
    while (value > previous &&
           !atomic_compare_exchange_weak_explicit(maximum, &previous, value,
                                                   memory_order_relaxed, memory_order_relaxed)) {
    }
}

static uint64_t monotonic_ns(void) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (uint64_t)now.tv_sec * 1000000000ULL + (uint64_t)now.tv_nsec;
}

ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
    resolve_writev();
    uint64_t started = monotonic_ns();
    ssize_t result = real_writev_fn(fd, iov, iovcnt);
    uint64_t elapsed = monotonic_ns() - started;
    if (result > 0) {
        update_max_size(&max_accepted_bytes, (size_t)result);
        update_max_u64(&max_duration_ns, elapsed);
        atomic_fetch_add_explicit(&writev_calls, 1, memory_order_relaxed);
    }
    return result;
}

__attribute__((destructor)) static void emit_summary(void) {
    const char *path = getenv("PERFLOOP_WRITEV_TRACE");
    if (path == NULL || *path == '\0') return;

    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) return;

    char summary[160];
    int length = snprintf(summary, sizeof(summary),
                          "max_writev_accepted_bytes=%zu\nmax_writev_duration_ns=%llu\nwritev_calls=%zu\n",
                          atomic_load_explicit(&max_accepted_bytes, memory_order_relaxed),
                          (unsigned long long)atomic_load_explicit(&max_duration_ns, memory_order_relaxed),
                          atomic_load_explicit(&writev_calls, memory_order_relaxed));
    if (length > 0) {
        size_t written = 0;
        while (written < (size_t)length) {
            ssize_t result = write(fd, summary + written, (size_t)length - written);
            if (result < 0 && errno == EINTR) continue;
            if (result <= 0) break;
            written += (size_t)result;
        }
    }
    close(fd);
}
