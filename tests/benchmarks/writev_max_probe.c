/*
 * Records the largest successful writev() result made by a process.
 *
 * The zero-copy write fairness benchmark preloads this only into its Redis
 * child.  The probe writes only when it observes a new maximum, so it does
 * not turn every write into synchronous tracing work.
 */
#define _GNU_SOURCE

#include <dlfcn.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>

static ssize_t (*next_writev)(int fd, const struct iovec *iov, int iovcnt);
static int result_fd = -1;
static _Atomic ssize_t largest_write = 0;

__attribute__((constructor)) static void writevProbeInit(void) {
    const char *path = getenv("WRITEV_PROBE_PATH");

    next_writev = dlsym(RTLD_NEXT, "writev");
    if (path != NULL)
        result_fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0600);
}

ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
    ssize_t written = next_writev(fd, iov, iovcnt);
    ssize_t previous = atomic_load_explicit(&largest_write, memory_order_relaxed);

    while (written > previous &&
           !atomic_compare_exchange_weak_explicit(&largest_write, &previous, written,
                                                  memory_order_relaxed, memory_order_relaxed)) {
    }

    if (result_fd >= 0 && written > previous) {
        char line[64];
        int length = snprintf(line, sizeof(line), "%zd\n", written);

        if (length > 0)
            syscall(SYS_write, result_fd, line, (size_t)length);
    }
    return written;
}
