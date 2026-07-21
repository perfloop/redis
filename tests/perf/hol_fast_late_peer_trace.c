#define _GNU_SOURCE

#include <arpa/inet.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

/*
 * Test-only interposer for the fast late-peer guard. It labels successful
 * server writes by the client source ports selected by the Python fixture, so
 * the fixture can establish whether the peer PONG was submitted before the
 * last large-response vector. Aggregate submission is recorded rather than a
 * short-write return value.
 */

static ssize_t (*real_write_fn)(int, const void *, size_t);
static ssize_t (*real_writev_fn)(int, const struct iovec *, int);
static pthread_once_t resolve_once = PTHREAD_ONCE_INIT;
static _Atomic uint64_t sequence;
static const char *trace_path;
static unsigned short bulk_port;
static unsigned short peer_port;

static unsigned short parse_port(const char *value) {
    if (value == NULL || *value == '\0') return 0;
    char *end = NULL;
    unsigned long port = strtoul(value, &end, 10);
    if (*end != '\0' || port == 0 || port > 65535) return 0;
    return (unsigned short)port;
}

static void resolve_once_fn(void) {
    real_write_fn = dlsym(RTLD_NEXT, "write");
    real_writev_fn = dlsym(RTLD_NEXT, "writev");
    trace_path = getenv("PERFLOOP_FAST_LATE_TRACE");
    bulk_port = parse_port(getenv("PERFLOOP_FAST_LATE_BULK_PORT"));
    peer_port = parse_port(getenv("PERFLOOP_FAST_LATE_PEER_PORT"));
    if (real_write_fn == NULL || real_writev_fn == NULL) _exit(127);
}

static char client_kind(int fd) {
    struct sockaddr_storage address;
    socklen_t length = sizeof(address);
    if (getpeername(fd, (struct sockaddr *)&address, &length) != 0) return '\0';

    unsigned short port = 0;
    if (address.ss_family == AF_INET) {
        port = ntohs(((struct sockaddr_in *)&address)->sin_port);
    } else if (address.ss_family == AF_INET6) {
        port = ntohs(((struct sockaddr_in6 *)&address)->sin6_port);
    }
    if (port == bulk_port) return 'B';
    if (port == peer_port) return 'P';
    return '\0';
}

static void record_write(int fd, size_t submitted, ssize_t result) {
    if (result <= 0 || trace_path == NULL || *trace_path == '\0') return;
    char kind = client_kind(fd);
    if (kind == '\0') return;

    uint64_t order = atomic_fetch_add_explicit(&sequence, 1, memory_order_relaxed) + 1;
    char line[128];
    int length = snprintf(line, sizeof(line), "%llu %c %zu %zd\n",
                          (unsigned long long)order, kind, submitted, result);
    if (length <= 0 || length >= (int)sizeof(line)) return;

    int output = open(trace_path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
    if (output < 0) return;
    (void)syscall(SYS_write, output, line, (size_t)length);
    close(output);
}

ssize_t write(int fd, const void *buffer, size_t count) {
    pthread_once(&resolve_once, resolve_once_fn);
    ssize_t result = real_write_fn(fd, buffer, count);
    record_write(fd, count, result);
    return result;
}

ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
    pthread_once(&resolve_once, resolve_once_fn);
    size_t submitted = 0;
    for (int index = 0; index < iovcnt; index++) {
        if (iov[index].iov_len > SIZE_MAX - submitted) {
            submitted = SIZE_MAX;
            break;
        }
        submitted += iov[index].iov_len;
    }
    ssize_t result = real_writev_fn(fd, iov, iovcnt);
    record_write(fd, submitted, result);
    return result;
}
