#define _GNU_SOURCE

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/uio.h>
#include <unistd.h>

/*
 * Test-only writev interposer for the output-I/O-thread guard. It records the
 * largest aggregate iovec length submitted by a successful writev call, which
 * is distinct from the kernel's short-write return value.
 */

static ssize_t (*real_writev_fn)(int, const struct iovec *, int);
static pthread_once_t resolve_once = PTHREAD_ONCE_INIT;
static _Atomic size_t max_submitted_bytes;
static _Atomic size_t writev_calls;

static void resolve_writev_once(void) {
    real_writev_fn = dlsym(RTLD_NEXT, "writev");
    if (real_writev_fn == NULL) _exit(127);
}

static void update_max_size(_Atomic size_t *maximum, size_t value) {
    size_t previous = atomic_load_explicit(maximum, memory_order_relaxed);
    while (value > previous &&
           !atomic_compare_exchange_weak_explicit(maximum, &previous, value,
                                                   memory_order_relaxed, memory_order_relaxed)) {
    }
}

ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
    pthread_once(&resolve_once, resolve_writev_once);
    size_t submitted = 0;
    for (int index = 0; index < iovcnt; index++) {
        if (iov[index].iov_len > SIZE_MAX - submitted) {
            submitted = SIZE_MAX;
            break;
        }
        submitted += iov[index].iov_len;
    }

    ssize_t result = real_writev_fn(fd, iov, iovcnt);
    if (result > 0) {
        update_max_size(&max_submitted_bytes, submitted);
        atomic_fetch_add_explicit(&writev_calls, 1, memory_order_relaxed);
    }
    return result;
}

__attribute__((destructor)) static void emit_summary(void) {
    const char *path = getenv("PERFLOOP_WRITEV_TRACE");
    if (path == NULL || *path == '\0') return;

    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) return;

    char summary[112];
    int length = snprintf(summary, sizeof(summary),
                          "max_writev_submitted_bytes=%zu\nwritev_calls=%zu\n",
                          atomic_load_explicit(&max_submitted_bytes, memory_order_relaxed),
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
