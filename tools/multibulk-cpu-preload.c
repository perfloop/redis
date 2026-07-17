/*
 * Measure CPU time between a fully buffered Redis socket read and the next
 * socket read on the same thread. The benchmark starts Redis with one I/O
 * thread and sends one high-arity request at a time, so this interval covers
 * RESP parsing, error handling, and the small amount of event-loop work needed
 * to return that request's reply without charging client-side idle time.
 *
 * The parent creates PERFLOOP_PARSE_CPU_FILE as a 32-byte shared file. Its
 * little-endian uint64_t fields are: cpu_ns, completed_intervals, target_reads,
 * reserved. The parent resets it only while the server is idle.
 */

#define _GNU_SOURCE

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define STATS_BYTES (sizeof(uint64_t) * 4)
#define FULLY_BUFFERED_READ_BYTES 300000

typedef struct {
    _Atomic uint64_t cpu_ns;
    _Atomic uint64_t completed_intervals;
    _Atomic uint64_t target_reads;
    _Atomic uint64_t reserved;
} parse_cpu_stats;

_Static_assert(sizeof(parse_cpu_stats) == STATS_BYTES, "unexpected stats layout");

static ssize_t (*real_read_fn)(int, void *, size_t);
static parse_cpu_stats *stats;
static pthread_once_t init_once = PTHREAD_ONCE_INIT;
static __thread uint64_t pending_start_ns;
static __thread int pending_interval;

static uint64_t thread_cpu_ns(void) {
    struct timespec now;
    if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &now) != 0) return 0;
    return (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
}

static void initialize(void) {
    const char *path = getenv("PERFLOOP_PARSE_CPU_FILE");
    real_read_fn = dlsym(RTLD_NEXT, "read");
    if (path == NULL || real_read_fn == NULL) return;

    int fd = open(path, O_RDWR | O_CLOEXEC);
    if (fd < 0) return;
    struct stat file_stat;
    if (fstat(fd, &file_stat) != 0 || file_stat.st_size < (off_t)STATS_BYTES) {
        close(fd);
        return;
    }
    void *mapped = mmap(NULL, STATS_BYTES, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (mapped == MAP_FAILED) return;
    stats = mapped;
}

ssize_t read(int fd, void *buffer, size_t count) {
    pthread_once(&init_once, initialize);
    if (real_read_fn == NULL) {
        errno = ENOSYS;
        return -1;
    }

    if (stats != NULL && pending_interval) {
        uint64_t now = thread_cpu_ns();
        if (now >= pending_start_ns) {
            uint64_t interval = now - pending_start_ns;
            atomic_fetch_add_explicit(&stats->cpu_ns, interval, memory_order_relaxed);
            atomic_fetch_add_explicit(&stats->completed_intervals, 1, memory_order_relaxed);
        }
        pending_interval = 0;
    }

    ssize_t result = real_read_fn(fd, buffer, count);
    if (stats != NULL && result >= FULLY_BUFFERED_READ_BYTES) {
        pending_start_ns = thread_cpu_ns();
        pending_interval = pending_start_ns != 0;
        atomic_fetch_add_explicit(&stats->target_reads, 1, memory_order_relaxed);
    }
    return result;
}
