/*
 * Aggregates successful writev() return values from the Redis child.
 *
 * The fairness benchmark needs the distribution of bytes accepted by writev,
 * not only its largest result.  Updating an in-process histogram avoids a
 * tracing syscall for every write.  At orderly process shutdown the probe
 * writes the aggregate and nonempty 1 KiB buckets to WRITEV_PROBE_PATH, where
 * the harness turns them into proof metrics.
 */
#define _GNU_SOURCE

#include <dlfcn.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>

#define EVENT_WRITE_QUANTUM (64U * 1024U)
#define WRITE_BUCKET_BYTES 1024U
#define WRITE_BUCKET_COUNT 8192U

static ssize_t (*next_writev)(int fd, const struct iovec *iov, int iovcnt);
static int result_fd = -1;
static _Atomic ssize_t largest_write;
static _Atomic unsigned long long write_count;
static _Atomic unsigned long long write_sum;
static _Atomic unsigned long long over_quantum_count;
static _Atomic unsigned long long write_buckets[WRITE_BUCKET_COUNT];

static void writeTraceLine(const char *line, int length) {
    if (result_fd >= 0 && length > 0)
        syscall(SYS_write, result_fd, line, (size_t)length);
}

__attribute__((constructor)) static void writevProbeInit(void) {
    const char *path = getenv("WRITEV_PROBE_PATH");

    next_writev = dlsym(RTLD_NEXT, "writev");
    if (path != NULL)
        result_fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
}

__attribute__((destructor)) static void writevProbeFinish(void) {
    char line[96];
    unsigned long long count;
    unsigned long long maximum;
    int length;

    if (result_fd < 0)
        return;
    count = atomic_load_explicit(&write_count, memory_order_relaxed);
    maximum = (unsigned long long)atomic_load_explicit(&largest_write, memory_order_relaxed);
    length = snprintf(line, sizeof(line), "count %llu\n", count);
    writeTraceLine(line, length);
    length = snprintf(line, sizeof(line), "sum %llu\n",
                      atomic_load_explicit(&write_sum, memory_order_relaxed));
    writeTraceLine(line, length);
    length = snprintf(line, sizeof(line), "max %llu\n", maximum);
    writeTraceLine(line, length);
    length = snprintf(line, sizeof(line), "over_quantum %llu\n",
                      atomic_load_explicit(&over_quantum_count, memory_order_relaxed));
    writeTraceLine(line, length);
    for (size_t index = 0; index < WRITE_BUCKET_COUNT; index++) {
        unsigned long long bucket_count = atomic_load_explicit(&write_buckets[index], memory_order_relaxed);

        if (bucket_count == 0)
            continue;
        length = snprintf(line, sizeof(line), "bucket %zu %llu\n", index, bucket_count);
        writeTraceLine(line, length);
    }
}

ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
    ssize_t written = next_writev(fd, iov, iovcnt);

    if (written > 0) {
        ssize_t previous = atomic_load_explicit(&largest_write, memory_order_relaxed);
        size_t bucket = (size_t)written / WRITE_BUCKET_BYTES;

        if (bucket >= WRITE_BUCKET_COUNT)
            bucket = WRITE_BUCKET_COUNT - 1;
        atomic_fetch_add_explicit(&write_count, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&write_sum, (unsigned long long)written, memory_order_relaxed);
        atomic_fetch_add_explicit(&write_buckets[bucket], 1, memory_order_relaxed);
        if ((unsigned long long)written > EVENT_WRITE_QUANTUM)
            atomic_fetch_add_explicit(&over_quantum_count, 1, memory_order_relaxed);
        while (written > previous &&
               !atomic_compare_exchange_weak_explicit(&largest_write, &previous, written,
                                                      memory_order_relaxed, memory_order_relaxed)) {
        }
    }
    return written;
}
