#define _GNU_SOURCE

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

#define NO_INSTRUMENT __attribute__((no_instrument_function))
#define TRACE_EVENT_WRITE_QUANTUM (64 * 1024)

static ssize_t (*real_writev_fn)(int fd, const struct iovec *iov, int iovcnt);
static int trace_fd = -1;
static pthread_once_t trace_init_once = PTHREAD_ONCE_INIT;
static void *write_to_client_fn;
static void *ping_command_fn;
static __thread long long write_to_client_start_ns;
static __thread int write_to_client_had_oversized_writev;
static __thread int resolving_targets;

static long long NO_INSTRUMENT monotonic_ns(void) {
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static void NO_INSTRUMENT trace_init(void) {
    const char *path = getenv("PERFLOOP_IO_TRACE");

    real_writev_fn = dlsym(RTLD_NEXT, "writev");
    if (path && *path) {
        trace_fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
    }
}

static void NO_INSTRUMENT ensure_trace_initialized(void) {
    pthread_once(&trace_init_once, trace_init);
}

__attribute__((constructor))
static void NO_INSTRUMENT initialize_trace_constructor(void) {
    ensure_trace_initialized();
}

static void NO_INSTRUMENT trace_record(char type, size_t offered,
                                       ssize_t accepted, long long duration_ns)
{
    char record[128];
    int length;

    if (trace_fd < 0) return;
    length = snprintf(record, sizeof(record), "%c %zu %zd %lld\n",
                      type, offered, accepted, duration_ns);
    if (length > 0) {
        syscall(SYS_write, trace_fd, record, (size_t)length);
    }
}

static void NO_INSTRUMENT find_trace_targets(void) {
    if ((write_to_client_fn && ping_command_fn) || resolving_targets) return;

    resolving_targets = 1;
    if (!write_to_client_fn) {
        write_to_client_fn = dlsym(RTLD_DEFAULT, "writeToClient");
    }
    if (!ping_command_fn) {
        ping_command_fn = dlsym(RTLD_DEFAULT, "pingCommand");
    }
    resolving_targets = 0;
}

void NO_INSTRUMENT __cyg_profile_func_enter(void *this_fn, void *call_site) {
    (void)call_site;

    ensure_trace_initialized();
    find_trace_targets();
    if (this_fn == write_to_client_fn) {
        write_to_client_start_ns = monotonic_ns();
        write_to_client_had_oversized_writev = 0;
    } else if (this_fn == ping_command_fn) {
        trace_record('P', 0, 0, monotonic_ns());
    }
}

void NO_INSTRUMENT __cyg_profile_func_exit(void *this_fn, void *call_site) {
    long long start_ns;

    (void)call_site;
    if (this_fn != write_to_client_fn) return;

    ensure_trace_initialized();
    start_ns = write_to_client_start_ns;
    write_to_client_start_ns = 0;
    if (start_ns) {
        long long duration_ns = monotonic_ns() - start_ns;
        trace_record('C', 0, 0, duration_ns);
        if (write_to_client_had_oversized_writev) {
            trace_record('O', 0, 0, duration_ns);
        }
    }
}

ssize_t NO_INSTRUMENT writev(int fd, const struct iovec *iov, int iovcnt) {
    size_t offered = 0;
    long long start_ns;
    long long elapsed_ns;
    ssize_t accepted;
    int saved_errno;

    ensure_trace_initialized();
    if (!real_writev_fn) {
        errno = ENOSYS;
        return -1;
    }

    for (int index = 0; index < iovcnt; index++) {
        offered += iov[index].iov_len;
    }

    start_ns = monotonic_ns();
    accepted = real_writev_fn(fd, iov, iovcnt);
    saved_errno = errno;
    elapsed_ns = monotonic_ns() - start_ns;

    if (accepted > 0) {
        if (write_to_client_start_ns && accepted > TRACE_EVENT_WRITE_QUANTUM) {
            write_to_client_had_oversized_writev = 1;
        }
        trace_record('W', offered, accepted, elapsed_ns);
    }

    errno = saved_errno;
    return accepted;
}
