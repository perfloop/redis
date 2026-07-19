/*
 * Exercise the event-loop fairness boundary for a copy-avoided bulk reply.
 *
 * `--verify` is a correctness check: it observes the shared reply bytes from
 * CLIENT LIST while a transaction holds a large GET reply, then validates the
 * full RESP payload and ordering.  `--measure` runs a continuously draining
 * bulk GET client beside a latency-sensitive PING client.  A tiny LD_PRELOAD
 * helper aggregates every successful server writev() result into a histogram.
 * The build also applies a test-only timestamp probe around writeToClient() and tags
 * each small PING with a client monotonic timestamp, exposing the reply-vector
 * quantum, its event callback span, and server-side small-request wait.
 */
#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <netinet/tcp.h>

#define BULK_BYTES (4U * 1024U * 1024U)
#define EVENT_WRITE_QUANTUM (64U * 1024U)
#define MAX_LATENCIES 1000000U
#define SERVER_WAIT_ATTEMPTS 500
#define SERVER_WAIT_NS (10L * 1000L * 1000L)
#define TIMED_PING_PREFIX "PERFLOOP_HOL:"
#define WRITEV_TRACE_BUCKET_BYTES 1024U
#define WRITEV_TRACE_BUCKET_COUNT 8192U

typedef struct serverProcess {
    const char *binary;
    const char *probe;
    char directory[480];
    char trace_path[512];
    char callback_trace_path[512];
    char small_ping_wait_trace_path[512];
    int port;
    pid_t pid;
} serverProcess;

typedef struct writevTrace {
    unsigned long long count;
    unsigned long long sum;
    unsigned long long maximum;
    unsigned long long over_quantum_count;
    unsigned long long buckets[WRITEV_TRACE_BUCKET_COUNT];
} writevTrace;

typedef struct bulkWorker {
    int port;
    size_t payload_len;
    atomic_bool stop;
    atomic_bool started;
    atomic_bool failed;
    atomic_ulong completed;
    atomic_ulong bytes;
} bulkWorker;

static const char ping_command[] = "*1\r\n$4\r\nPING\r\n";
static const char multi_command[] = "*1\r\n$5\r\nMULTI\r\n";
static const char get_bulk_command[] = "*2\r\n$3\r\nGET\r\n$4\r\nbulk\r\n";
static const char client_list_command[] = "*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n";
static const char exec_command[] = "*1\r\n$4\r\nEXEC\r\n";
static const char debug_copy_avoidance_command[] =
    "*3\r\n$5\r\nDEBUG\r\n$20\r\nREPLY-COPY-AVOIDANCE\r\n$1\r\n1\r\n";

static uint64_t monotonicNs(void) {
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
        return 0;
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void sleepNs(long ns) {
    struct timespec request = {.tv_sec = 0, .tv_nsec = ns};

    while (nanosleep(&request, &request) != 0 && errno == EINTR) {
    }
}

static int pinToCpu(int cpu) {
    cpu_set_t set;

    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    return sched_setaffinity(0, sizeof(set), &set);
}

static int sendAll(int fd, const void *buffer, size_t length) {
    const unsigned char *position = buffer;

    while (length > 0) {
        ssize_t sent = send(fd, position, length, MSG_NOSIGNAL);
        if (sent > 0) {
            position += sent;
            length -= (size_t)sent;
            continue;
        }
        if (sent < 0 && errno == EINTR)
            continue;
        return -1;
    }
    return 0;
}

static int receiveAll(int fd, void *buffer, size_t length) {
    unsigned char *position = buffer;

    while (length > 0) {
        ssize_t received = recv(fd, position, length, 0);
        if (received > 0) {
            position += received;
            length -= (size_t)received;
            continue;
        }
        if (received < 0 && errno == EINTR)
            continue;
        return -1;
    }
    return 0;
}

static int readLine(int fd, char *line, size_t capacity) {
    size_t used = 0;

    while (used + 1 < capacity) {
        ssize_t received = recv(fd, line + used, 1, 0);
        if (received == 1) {
            used++;
            if (used >= 2 && line[used - 2] == '\r' && line[used - 1] == '\n') {
                line[used] = '\0';
                return 0;
            }
            continue;
        }
        if (received < 0 && errno == EINTR)
            continue;
        return -1;
    }
    return -1;
}

static int expectSimple(int fd, const char *expected) {
    char line[256];

    if (readLine(fd, line, sizeof(line)) != 0)
        return -1;
    return strcmp(line, expected) == 0 ? 0 : -1;
}

static int parseLengthLine(int fd, char marker, size_t *length) {
    char line[128];
    char *end = NULL;
    unsigned long long value;

    if (readLine(fd, line, sizeof(line)) != 0 || line[0] != marker)
        return -1;
    errno = 0;
    value = strtoull(line + 1, &end, 10);
    if (errno != 0 || end == line + 1 || strcmp(end, "\r\n") != 0 || value > SIZE_MAX)
        return -1;
    *length = (size_t)value;
    return 0;
}

static unsigned char payloadByte(size_t offset) {
    return (unsigned char)((offset * 17U + 31U) % 251U);
}

static void makePayload(unsigned char *payload, size_t length) {
    for (size_t index = 0; index < length; index++)
        payload[index] = payloadByte(index);
}

static int expectBulkString(int fd, const char *expected) {
    char value[64];
    char suffix[2];
    size_t length;

    if (parseLengthLine(fd, '$', &length) != 0 || length != strlen(expected) || length >= sizeof(value) ||
        receiveAll(fd, value, length) != 0 || receiveAll(fd, suffix, sizeof(suffix)) != 0)
    {
        return -1;
    }
    value[length] = '\0';
    return memcmp(suffix, "\r\n", sizeof(suffix)) == 0 && strcmp(value, expected) == 0 ? 0 : -1;
}

static int readBulkAndValidate(int fd, size_t expected_length) {
    unsigned char chunk[64 * 1024];
    char suffix[2];
    size_t length;
    size_t offset = 0;

    if (parseLengthLine(fd, '$', &length) != 0 || length != expected_length)
        return -1;
    while (offset < length) {
        size_t wanted = length - offset;
        if (wanted > sizeof(chunk))
            wanted = sizeof(chunk);
        if (receiveAll(fd, chunk, wanted) != 0)
            return -1;
        for (size_t index = 0; index < wanted; index++) {
            if (chunk[index] != payloadByte(offset + index))
                return -1;
        }
        offset += wanted;
    }
    if (receiveAll(fd, suffix, sizeof(suffix)) != 0)
        return -1;
    return suffix[0] == '\r' && suffix[1] == '\n' ? 0 : -1;
}

static int readBulkToBuffer(int fd, char **result, size_t *result_length) {
    char suffix[2];
    char *buffer;
    size_t length;

    if (parseLengthLine(fd, '$', &length) != 0 || length > 1024 * 1024)
        return -1;
    buffer = malloc(length + 1);
    if (buffer == NULL)
        return -1;
    if (receiveAll(fd, buffer, length) != 0 || receiveAll(fd, suffix, sizeof(suffix)) != 0 ||
        suffix[0] != '\r' || suffix[1] != '\n') {
        free(buffer);
        return -1;
    }
    buffer[length] = '\0';
    *result = buffer;
    *result_length = length;
    return 0;
}

static int connectToServer(int port) {
    struct sockaddr_in address = {0};
    int enabled = 1;
    int fd;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled)) != 0) {
        close(fd);
        return -1;
    }
    address.sin_family = AF_INET;
    address.sin_port = htons((uint16_t)port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int sendPing(int fd) {
    return sendAll(fd, ping_command, sizeof(ping_command) - 1) == 0 &&
                   expectSimple(fd, "+PONG\r\n") == 0
               ? 0
               : -1;
}

static int sendTimedPing(int fd, uint64_t *started_ns) {
    char timestamp[64];
    char command[128];
    int timestamp_length;
    int command_length;

    *started_ns = monotonicNs();
    if (*started_ns == 0)
        return -1;
    timestamp_length = snprintf(timestamp, sizeof(timestamp), TIMED_PING_PREFIX "%llu",
                                (unsigned long long)(*started_ns / 1000ULL));
    if (timestamp_length < 0 || (size_t)timestamp_length >= sizeof(timestamp))
        return -1;
    command_length = snprintf(command, sizeof(command), "*2\r\n$4\r\nPING\r\n$%d\r\n%s\r\n",
                              timestamp_length, timestamp);
    if (command_length < 0 || (size_t)command_length >= sizeof(command) ||
        sendAll(fd, command, (size_t)command_length) != 0)
    {
        return -1;
    }
    return expectBulkString(fd, timestamp);
}

static int setBulkValue(int fd, const unsigned char *payload, size_t length) {
    char header[128];
    int header_length;

    header_length = snprintf(header, sizeof(header), "*3\r\n$3\r\nSET\r\n$4\r\nbulk\r\n$%zu\r\n", length);
    if (header_length < 0 || (size_t)header_length >= sizeof(header))
        return -1;
    if (sendAll(fd, header, (size_t)header_length) != 0 || sendAll(fd, payload, length) != 0 ||
        sendAll(fd, "\r\n", 2) != 0)
        return -1;
    return expectSimple(fd, "+OK\r\n");
}

static int enableCopyAvoidance(int fd) {
    if (sendAll(fd, debug_copy_avoidance_command, sizeof(debug_copy_avoidance_command) - 1) != 0)
        return -1;
    return expectSimple(fd, "+OK\r\n");
}

static int setClientName(int fd, const char *name) {
    char header[256];
    int length;

    length = snprintf(header, sizeof(header), "*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$%zu\r\n%s\r\n",
                      strlen(name), name);
    if (length < 0 || (size_t)length >= sizeof(header) || sendAll(fd, header, (size_t)length) != 0)
        return -1;
    return expectSimple(fd, "+OK\r\n");
}

static int findAvailablePort(void) {
    struct sockaddr_in address = {0};
    socklen_t address_length = sizeof(address);
    int enabled = 1;
    int fd;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) != 0) {
        close(fd);
        return -1;
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (bind(fd, (struct sockaddr *)&address, sizeof(address)) != 0 ||
        getsockname(fd, (struct sockaddr *)&address, &address_length) != 0) {
        close(fd);
        return -1;
    }
    close(fd);
    return ntohs(address.sin_port);
}

static int serverIsReady(serverProcess *server) {
    for (int attempt = 0; attempt < SERVER_WAIT_ATTEMPTS; attempt++) {
        int status;
        int fd = connectToServer(server->port);

        if (fd >= 0) {
            int ready = sendPing(fd) == 0;
            close(fd);
            if (ready)
                return 0;
        }
        if (waitpid(server->pid, &status, WNOHANG) == server->pid) {
            server->pid = -1;
            return -1;
        }
        sleepNs(SERVER_WAIT_NS);
    }
    return -1;
}

static int startServer(serverProcess *server) {
    char port[16];
    char pidfile[512];
    int null_fd;

    server->port = findAvailablePort();
    if (server->port < 0)
        return -1;
    snprintf(server->trace_path, sizeof(server->trace_path), "%s/writev-max", server->directory);
    snprintf(server->callback_trace_path, sizeof(server->callback_trace_path), "%s/write-callback-max",
             server->directory);
    snprintf(server->small_ping_wait_trace_path, sizeof(server->small_ping_wait_trace_path),
             "%s/small-ping-wait-max", server->directory);
    snprintf(port, sizeof(port), "%d", server->port);
    snprintf(pidfile, sizeof(pidfile), "%s/redis.pid", server->directory);
    server->pid = fork();
    if (server->pid < 0)
        return -1;
    if (server->pid == 0) {
        if (pinToCpu(0) != 0)
            _exit(127);
        if (setenv("PERFLOOP_HOL_CALLBACK_PATH", server->callback_trace_path, 1) != 0 ||
            setenv("PERFLOOP_HOL_SMALL_QUEUE_WAIT_PATH", server->small_ping_wait_trace_path, 1) != 0)
        {
            _exit(127);
        }
        if (server->probe != NULL) {
            if (setenv("LD_PRELOAD", server->probe, 1) != 0 ||
                setenv("WRITEV_PROBE_PATH", server->trace_path, 1) != 0)
                _exit(127);
        }
        null_fd = open("/dev/null", O_RDWR);
        if (null_fd < 0)
            _exit(127);
        if (dup2(null_fd, STDOUT_FILENO) < 0 || dup2(null_fd, STDERR_FILENO) < 0)
            _exit(127);
        if (null_fd > STDERR_FILENO)
            close(null_fd);
        execl(server->binary, server->binary, "--bind", "127.0.0.1", "--port", port, "--save", "",
              "--appendonly", "no", "--enable-debug-command", "yes", "--protected-mode", "no", "--dir",
              server->directory, "--pidfile", pidfile, "--loglevel", "warning", (char *)NULL);
        _exit(127);
    }
    return serverIsReady(server);
}

static void stopServer(serverProcess *server) {
    int status;

    if (server->pid <= 0)
        return;
    kill(server->pid, SIGTERM);
    for (int attempt = 0; attempt < 100; attempt++) {
        if (waitpid(server->pid, &status, WNOHANG) == server->pid) {
            server->pid = -1;
            return;
        }
        sleepNs(SERVER_WAIT_NS);
    }
    kill(server->pid, SIGKILL);
    waitpid(server->pid, &status, 0);
    server->pid = -1;
}

static int initializeServer(serverProcess *server, const char *binary, const char *probe) {
    char template[] = "/tmp/redis-writev-hol-XXXXXX";
    char *directory;

    memset(server, 0, sizeof(*server));
    directory = mkdtemp(template);
    if (directory == NULL)
        return -1;
    server->binary = binary;
    server->probe = probe;
    server->pid = -1;
    strcpy(server->directory, directory);
    return 0;
}

static void cleanupServer(serverProcess *server) {
    stopServer(server);
    if (server->trace_path[0] != '\0')
        unlink(server->trace_path);
    if (server->callback_trace_path[0] != '\0')
        unlink(server->callback_trace_path);
    if (server->small_ping_wait_trace_path[0] != '\0')
        unlink(server->small_ping_wait_trace_path);
    if (server->directory[0] != '\0') {
        char pidfile[512];
        snprintf(pidfile, sizeof(pidfile), "%s/redis.pid", server->directory);
        unlink(pidfile);
        rmdir(server->directory);
    }
}

static void *runBulkWorker(void *argument) {
    bulkWorker *worker = argument;
    int fd;

    if (pinToCpu(2) != 0) {
        atomic_store(&worker->failed, true);
        atomic_store(&worker->started, true);
        return NULL;
    }
    fd = connectToServer(worker->port);
    if (fd < 0) {
        atomic_store(&worker->failed, true);
        atomic_store(&worker->started, true);
        return NULL;
    }
    atomic_store(&worker->started, true);
    while (!atomic_load(&worker->stop)) {
        if (sendAll(fd, get_bulk_command, sizeof(get_bulk_command) - 1) != 0 ||
            readBulkAndValidate(fd, worker->payload_len) != 0) {
            if (!atomic_load(&worker->stop))
                atomic_store(&worker->failed, true);
            break;
        }
        atomic_fetch_add(&worker->completed, 1);
        atomic_fetch_add(&worker->bytes, worker->payload_len);
    }
    close(fd);
    return NULL;
}

static int waitForBulkWorker(bulkWorker *worker, unsigned long minimum_completed) {
    uint64_t deadline = monotonicNs() + 5000000000ULL;

    while (monotonicNs() < deadline) {
        if (atomic_load(&worker->failed))
            return -1;
        if (atomic_load(&worker->completed) >= minimum_completed)
            return 0;
        sleepNs(SERVER_WAIT_NS);
    }
    return -1;
}

static int compareLatency(const void *left, const void *right) {
    const double a = *(const double *)left;
    const double b = *(const double *)right;

    return a < b ? -1 : a > b;
}

static int readLargestValue(const char *path, unsigned long long *largest) {
    FILE *file;
    unsigned long long value;

    file = fopen(path, "r");
    if (file == NULL)
        return -1;
    *largest = 0;
    while (fscanf(file, "%llu", &value) == 1) {
        if (value > *largest)
            *largest = value;
    }
    fclose(file);
    return *largest > 0 ? 0 : -1;
}

static int readWritevTrace(const char *path, writevTrace *trace) {
    char record[32];
    FILE *file;
    int saw_count = 0;
    int saw_sum = 0;
    int saw_max = 0;
    int saw_over_quantum = 0;

    file = fopen(path, "r");
    if (file == NULL)
        return -1;
    memset(trace, 0, sizeof(*trace));
    while (fscanf(file, "%31s", record) == 1) {
        if (strcmp(record, "count") == 0) {
            if (fscanf(file, "%llu", &trace->count) != 1)
                goto error;
            saw_count = 1;
        } else if (strcmp(record, "sum") == 0) {
            if (fscanf(file, "%llu", &trace->sum) != 1)
                goto error;
            saw_sum = 1;
        } else if (strcmp(record, "max") == 0) {
            if (fscanf(file, "%llu", &trace->maximum) != 1)
                goto error;
            saw_max = 1;
        } else if (strcmp(record, "over_quantum") == 0) {
            if (fscanf(file, "%llu", &trace->over_quantum_count) != 1)
                goto error;
            saw_over_quantum = 1;
        } else if (strcmp(record, "bucket") == 0) {
            size_t index;
            unsigned long long count;

            if (fscanf(file, "%zu %llu", &index, &count) != 2 || index >= WRITEV_TRACE_BUCKET_COUNT)
                goto error;
            trace->buckets[index] = count;
        } else {
            goto error;
        }
    }
    fclose(file);
    unsigned long long bucket_total = 0;
    for (size_t index = 0; index < WRITEV_TRACE_BUCKET_COUNT; index++)
        bucket_total += trace->buckets[index];
    return saw_count && saw_sum && saw_max && saw_over_quantum && trace->count > 0 && trace->maximum > 0 &&
                   bucket_total == trace->count
               ? 0
               : -1;

error:
    fclose(file);
    return -1;
}

static unsigned long long writevTraceQuantileUpperBound(const writevTrace *trace, unsigned basis_points) {
    unsigned long long rank = (trace->count * basis_points + 9999U) / 10000U;
    unsigned long long accumulated = 0;

    for (size_t index = 0; index < WRITEV_TRACE_BUCKET_COUNT; index++) {
        accumulated += trace->buckets[index];
        if (accumulated >= rank) {
            unsigned long long upper_bound;

            if (index == WRITEV_TRACE_BUCKET_COUNT - 1)
                return trace->maximum;
            upper_bound = (unsigned long long)(index + 1) * WRITEV_TRACE_BUCKET_BYTES - 1U;
            return upper_bound < trace->maximum ? upper_bound : trace->maximum;
        }
    }
    return 0;
}

static int runVerify(const char *binary) {
    static const char verification_name[] = "hol_writev_verify";
    unsigned char *payload = NULL;
    char *client_list = NULL;
    serverProcess server;
    unsigned long long shared = 0;
    const char *name_position;
    const char *shared_position;
    int fd = -1;
    int result = 1;
    size_t ignored_length;
    size_t array_length;

    if (initializeServer(&server, binary, NULL) != 0 || startServer(&server) != 0) {
        fprintf(stderr, "could not start Redis for verification\n");
        goto cleanup;
    }
    payload = malloc(BULK_BYTES);
    if (payload == NULL)
        goto cleanup;
    makePayload(payload, BULK_BYTES);
    fd = connectToServer(server.port);
    if (fd < 0 || enableCopyAvoidance(fd) != 0 || setBulkValue(fd, payload, BULK_BYTES) != 0 ||
        setClientName(fd, verification_name) != 0)
        goto cleanup;

    if (sendAll(fd, multi_command, sizeof(multi_command) - 1) != 0 || expectSimple(fd, "+OK\r\n") != 0 ||
        sendAll(fd, get_bulk_command, sizeof(get_bulk_command) - 1) != 0 || expectSimple(fd, "+QUEUED\r\n") != 0 ||
        sendAll(fd, client_list_command, sizeof(client_list_command) - 1) != 0 ||
        expectSimple(fd, "+QUEUED\r\n") != 0 || sendAll(fd, exec_command, sizeof(exec_command) - 1) != 0 ||
        parseLengthLine(fd, '*', &array_length) != 0 || array_length != 2 ||
        readBulkAndValidate(fd, BULK_BYTES) != 0 || readBulkToBuffer(fd, &client_list, &ignored_length) != 0)
        goto cleanup;

    name_position = strstr(client_list, "name=hol_writev_verify");
    shared_position = name_position == NULL ? NULL : strstr(name_position, "omem-shared=");
    if (shared_position == NULL || sscanf(shared_position, "omem-shared=%llu", &shared) != 1 ||
        shared < BULK_BYTES)
        goto cleanup;

    printf("HOL_WRITEV_VERIFY_OK shared=%llu payload=%u\n", shared, BULK_BYTES);
    result = 0;

cleanup:
    if (fd >= 0)
        close(fd);
    free(client_list);
    free(payload);
    cleanupServer(&server);
    return result;
}

static int runMeasurement(const char *binary, const char *probe, unsigned int duration_ms) {
    bulkWorker worker = {0};
    pthread_t worker_thread;
    serverProcess server;
    unsigned char *payload = NULL;
    double *latencies = NULL;
    unsigned long before_measure;
    unsigned long after_measure;
    unsigned long completed_during;
    unsigned long long largest_writev;
    unsigned long long largest_callback;
    unsigned long long largest_small_ping_wait;
    unsigned long long quantum_excess;
    writevTrace write_trace;
    uint64_t deadline;
    int small_fd = -1;
    int thread_started = 0;
    int result = 1;
    size_t latency_count = 0;

    if (pinToCpu(1) != 0) {
        fprintf(stderr, "could not pin small-request client to CPU 1\n");
        return 1;
    }
    if (initializeServer(&server, binary, probe) != 0 || startServer(&server) != 0) {
        fprintf(stderr, "could not start Redis for measurement\n");
        goto cleanup;
    }
    payload = malloc(BULK_BYTES);
    latencies = malloc(sizeof(*latencies) * MAX_LATENCIES);
    if (payload == NULL || latencies == NULL)
        goto cleanup;
    makePayload(payload, BULK_BYTES);

    small_fd = connectToServer(server.port);
    if (small_fd < 0 || enableCopyAvoidance(small_fd) != 0 || setBulkValue(small_fd, payload, BULK_BYTES) != 0)
        goto cleanup;

    worker.port = server.port;
    worker.payload_len = BULK_BYTES;
    if (pthread_create(&worker_thread, NULL, runBulkWorker, &worker) != 0)
        goto cleanup;
    thread_started = 1;
    while (!atomic_load(&worker.started))
        sleepNs(SERVER_WAIT_NS);
    if (waitForBulkWorker(&worker, 2) != 0)
        goto cleanup;

    for (int warmup = 0; warmup < 100; warmup++) {
        uint64_t ignored_started;

        if (sendTimedPing(small_fd, &ignored_started) != 0)
            goto cleanup;
    }
    before_measure = atomic_load(&worker.completed);
    deadline = monotonicNs() + (uint64_t)duration_ms * 1000000ULL;
    while (monotonicNs() < deadline && latency_count < MAX_LATENCIES) {
        uint64_t started;
        uint64_t finished;

        if (sendTimedPing(small_fd, &started) != 0)
            goto cleanup;
        finished = monotonicNs();
        if (finished <= started)
            goto cleanup;
        latencies[latency_count++] = (double)(finished - started) / 1000.0;
    }
    after_measure = atomic_load(&worker.completed);
    if (latency_count < 1000 || after_measure <= before_measure || atomic_load(&worker.failed))
        goto cleanup;
    atomic_store(&worker.stop, true);
    pthread_join(worker_thread, NULL);
    thread_started = 0;
    close(small_fd);
    small_fd = -1;
    stopServer(&server);
    if (readWritevTrace(server.trace_path, &write_trace) != 0 ||
        readLargestValue(server.callback_trace_path, &largest_callback) != 0 ||
        readLargestValue(server.small_ping_wait_trace_path, &largest_small_ping_wait) != 0)
    {
        goto cleanup;
    }
    largest_writev = write_trace.maximum;

    qsort(latencies, latency_count, sizeof(*latencies), compareLatency);
    completed_during = after_measure - before_measure;
    quantum_excess = largest_writev > EVENT_WRITE_QUANTUM ?
                         largest_writev - EVENT_WRITE_QUANTUM : 0;
    printf("{\"metric\":\"small_ping_p99_us\",\"value\":%.3f}\n",
           latencies[(latency_count * 99 + 99) / 100 - 1]);
    printf("{\"metric\":\"writev_accepted_bytes_count\",\"value\":%llu}\n", write_trace.count);
    printf("{\"metric\":\"writev_accepted_bytes_total\",\"value\":%llu}\n", write_trace.sum);
    printf("{\"metric\":\"writev_accepted_bytes_p99_upper_bound\",\"value\":%llu}\n",
           writevTraceQuantileUpperBound(&write_trace, 9900));
    printf("{\"metric\":\"writev_accepted_over_quantum_count\",\"value\":%llu}\n",
           write_trace.over_quantum_count);
    printf("{\"metric\":\"bulk_writev_max_bytes\",\"value\":%llu}\n", largest_writev);
    printf("{\"metric\":\"bulk_writev_quantum_excess_bytes\",\"value\":%llu}\n", quantum_excess);
    printf("{\"metric\":\"bulk_writev_quantum_excess_callback_byte_us\",\"value\":%llu}\n",
           quantum_excess * largest_callback);
    printf("{\"metric\":\"write_to_client_max_callback_us\",\"value\":%llu}\n", largest_callback);
    printf("{\"metric\":\"small_ping_server_queue_wait_max_us\",\"value\":%llu}\n",
           largest_small_ping_wait);
    printf("{\"metric\":\"bulk_gets_during\",\"value\":%lu}\n", completed_during);
    result = 0;

cleanup:
    if (thread_started) {
        atomic_store(&worker.stop, true);
        pthread_join(worker_thread, NULL);
    }
    if (small_fd >= 0)
        close(small_fd);
    free(latencies);
    free(payload);
    cleanupServer(&server);
    return result;
}

static void usage(const char *program) {
    fprintf(stderr,
            "usage: %s --verify --server <redis-server>\n"
            "       %s --measure --server <redis-server> --probe <writev-probe.so> [--duration-ms <ms>]\n",
            program, program);
}

int main(int argc, char **argv) {
    const char *binary = NULL;
    const char *probe = NULL;
    unsigned int duration_ms = 2000;
    bool verify = false;
    bool measure = false;

    for (int index = 1; index < argc; index++) {
        if (strcmp(argv[index], "--verify") == 0) {
            verify = true;
        } else if (strcmp(argv[index], "--measure") == 0) {
            measure = true;
        } else if (strcmp(argv[index], "--server") == 0 && index + 1 < argc) {
            binary = argv[++index];
        } else if (strcmp(argv[index], "--probe") == 0 && index + 1 < argc) {
            probe = argv[++index];
        } else if (strcmp(argv[index], "--duration-ms") == 0 && index + 1 < argc) {
            char *end = NULL;
            unsigned long value = strtoul(argv[++index], &end, 10);
            if (end == argv[index] || *end != '\0' || value == 0 || value > 60000) {
                usage(argv[0]);
                return 2;
            }
            duration_ms = (unsigned int)value;
        } else {
            usage(argv[0]);
            return 2;
        }
    }
    if (binary == NULL || verify == measure || (measure && probe == NULL)) {
        usage(argv[0]);
        return 2;
    }
    return verify ? runVerify(binary) : runMeasurement(binary, probe, duration_ms);
}
