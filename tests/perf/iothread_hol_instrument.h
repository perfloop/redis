#ifndef PERFLOOP_IOTHREAD_HOL_INSTRUMENT_H
#define PERFLOOP_IOTHREAD_HOL_INSTRUMENT_H

/*
 * This header is force-included only while compiling src/iothread.c for the
 * proof harness. It wraps the handoff-list operations without changing Redis
 * source files, records the handoff boundary in process-local state, and
 * writes a snapshot when the benchmark server exits normally.
 */

#include "server.h"

#include <inttypes.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#define HOL_CLIENT_SLOTS 1024
#define HOL_QUEUE_SAMPLES 65536
#define HOL_DRAIN_SAMPLES 65536

enum hol_client_class {
    HOL_CLIENT_UNKNOWN,
    HOL_CLIENT_SHORT,
    HOL_CLIENT_BULK,
};

typedef struct hol_client_slot {
    atomic_uintptr_t client_ptr;
    atomic_uint_fast64_t enqueue_us;
    atomic_uint_fast64_t commands_before;
    atomic_int inflight;
} hol_client_slot;

static pthread_once_t hol_once = PTHREAD_ONCE_INIT;
static int hol_enabled;
static char hol_metrics_path[PATH_MAX];
static hol_client_slot hol_clients[HOL_CLIENT_SLOTS];

/* These values are updated only by the main thread after a client leaves the
 * handoff list. The enqueue map is atomic because the IO thread writes it. */
static uint64_t hol_short_delays[HOL_QUEUE_SAMPLES];
static uint64_t hol_bulk_delays[HOL_QUEUE_SAMPLES];
static uint64_t hol_short_delay_count;
static uint64_t hol_bulk_delay_count;
static uint64_t hol_drain_clients[HOL_DRAIN_SAMPLES];
static uint64_t hol_drain_commands[HOL_DRAIN_SAMPLES];
static uint64_t hol_drain_residual[HOL_DRAIN_SAMPLES];
static uint64_t hol_drain_initial[HOL_DRAIN_SAMPLES];
static uint64_t hol_drain_count;

static _Atomic(list *) hol_pending_list;
static list *hol_processing_list;
static int hol_drain_active;
static uint64_t hol_active_clients;
static uint64_t hol_active_commands;
static uint64_t hol_active_initial;

static void hol_init(void) {
    const char *path = getenv("PERFLOOP_IOTHREAD_HOL_METRICS");
    if (path == NULL || *path == '\0' || strlen(path) >= sizeof(hol_metrics_path)) return;

    memcpy(hol_metrics_path, path, strlen(path) + 1);
    hol_enabled = 1;
}

static int hol_is_enabled(void) {
    pthread_once(&hol_once, hol_init);
    return hol_enabled;
}

static uint64_t hol_now_us(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return 0;
    return (uint64_t)ts.tv_sec * UINT64_C(1000000) + (uint64_t)ts.tv_nsec / 1000;
}

static hol_client_slot *hol_client_slot_for(client *c, int create) {
    uintptr_t key = (uintptr_t)c;
    size_t index = (key >> 4) & (HOL_CLIENT_SLOTS - 1);

    for (size_t probes = 0; probes < HOL_CLIENT_SLOTS; probes++) {
        hol_client_slot *slot = &hol_clients[(index + probes) & (HOL_CLIENT_SLOTS - 1)];
        uintptr_t observed = atomic_load_explicit(&slot->client_ptr, memory_order_acquire);
        if (observed == key) return slot;
        if (observed != 0 || !create) continue;
        if (atomic_compare_exchange_strong_explicit(&slot->client_ptr, &observed, key,
                                                    memory_order_release, memory_order_acquire)) {
            return slot;
        }
    }
    return NULL;
}

static int hol_object_equals(const robj *object, const char *text) {
    size_t text_len = strlen(text);
    if (object == NULL || object->type != OBJ_STRING || object->encoding == OBJ_ENCODING_INT)
        return 0;
    return sdslen(object->ptr) == text_len &&
           strncasecmp(object->ptr, text, text_len) == 0;
}

static enum hol_client_class hol_classify_client(const client *c) {
    const pendingCommand *command = c->pending_cmds.head;
    if (command == NULL || command->argc < 2 || command->argv == NULL) return HOL_CLIENT_UNKNOWN;

    if (hol_object_equals(command->argv[0], "GET") &&
        hol_object_equals(command->argv[1], "hol:short")) {
        return HOL_CLIENT_SHORT;
    }
    if (hol_object_equals(command->argv[0], "SET") &&
        hol_object_equals(command->argv[1], "hol:bulk")) {
        return HOL_CLIENT_BULK;
    }
    return HOL_CLIENT_UNKNOWN;
}

static void hol_store_sample(uint64_t *samples, uint64_t *count, uint64_t value) {
    samples[*count % HOL_QUEUE_SAMPLES] = value;
    (*count)++;
}

static void hol_finish_drain(size_t residual) {
    uint64_t index;
    if (!hol_drain_active) return;

    index = hol_drain_count % HOL_DRAIN_SAMPLES;
    hol_drain_clients[index] = hol_active_clients;
    hol_drain_commands[index] = hol_active_commands;
    hol_drain_residual[index] = residual;
    hol_drain_initial[index] = hol_active_initial;
    hol_drain_count++;
    hol_drain_active = 0;
}

static void perfloop_hol_list_join(list *destination, list *source) {
    list *pending;

    if (hol_is_enabled()) {
        /* This is the IO-thread -> main-thread transfer. */
        if (source == IOThreads[1].pending_clients_to_main_thread) {
            atomic_store_explicit(&hol_pending_list, destination, memory_order_release);
        } else {
            /* This is the start of one processClientsFromIOThread invocation. */
            pending = atomic_load_explicit(&hol_pending_list, memory_order_acquire);
            if (pending != NULL && source == pending) {
                if (hol_drain_active) hol_finish_drain(destination->len);
                hol_processing_list = destination;
                if (destination->len + source->len > 0) {
                    hol_drain_active = 1;
                    hol_active_clients = 0;
                    hol_active_commands = 0;
                    hol_active_initial = destination->len + source->len;
                }
            }
        }
    }

    (listJoin)(destination, source);
}

static void perfloop_hol_list_link_tail(list *target, listNode *node) {
    if (hol_is_enabled() && target == IOThreads[1].pending_clients_to_main_thread) {
        client *c = listNodeValue(node);
        hol_client_slot *slot = hol_client_slot_for(c, 1);
        if (slot != NULL) {
            atomic_store_explicit(&slot->enqueue_us, hol_now_us(), memory_order_release);
            atomic_store_explicit(&slot->inflight, 0, memory_order_release);
        }
    }

    (listLinkNodeTail)(target, node);
}

static void perfloop_hol_list_unlink(list *target, listNode *node) {
    if (hol_is_enabled() && hol_drain_active && target == hol_processing_list) {
        client *c = listNodeValue(node);
        hol_client_slot *slot = hol_client_slot_for(c, 0);
        uint64_t now = hol_now_us();

        hol_active_clients++;
        if (slot != NULL) {
            uint64_t enqueued = atomic_load_explicit(&slot->enqueue_us, memory_order_acquire);
            if (enqueued != 0 && now >= enqueued) {
                switch (hol_classify_client(c)) {
                case HOL_CLIENT_SHORT:
                    hol_store_sample(hol_short_delays, &hol_short_delay_count, now - enqueued);
                    break;
                case HOL_CLIENT_BULK:
                    hol_store_sample(hol_bulk_delays, &hol_bulk_delay_count, now - enqueued);
                    break;
                case HOL_CLIENT_UNKNOWN:
                    break;
                }
            }
            atomic_store_explicit(&slot->commands_before, c->commands_processed, memory_order_release);
            atomic_store_explicit(&slot->inflight, 1, memory_order_release);
        }
    }

    (listUnlinkNode)(target, node);
}

static void perfloop_hol_list_link_head(list *target, listNode *node) {
    if (hol_is_enabled()) {
        client *c = listNodeValue(node);
        hol_client_slot *slot = hol_client_slot_for(c, 0);
        if (slot != NULL && atomic_exchange_explicit(&slot->inflight, 0, memory_order_acq_rel)) {
            uint64_t before = atomic_load_explicit(&slot->commands_before, memory_order_acquire);
            if (hol_drain_active && c->commands_processed >= before)
                hol_active_commands += c->commands_processed - before;
            atomic_store_explicit(&slot->enqueue_us, 0, memory_order_release);
        }
    }

    (listLinkNodeHead)(target, node);
}

static int hol_compare_u64(const void *left, const void *right) {
    uint64_t a = *(const uint64_t *)left;
    uint64_t b = *(const uint64_t *)right;
    return (a > b) - (a < b);
}

static uint64_t hol_quantile(const uint64_t *samples, uint64_t total, size_t capacity, unsigned percentile) {
    static uint64_t scratch[HOL_DRAIN_SAMPLES];
    size_t count = total > capacity ? capacity : (size_t)total;
    size_t rank;

    if (count == 0) return 0;
    memcpy(scratch, samples, count * sizeof(*samples));
    qsort(scratch, count, sizeof(*scratch), hol_compare_u64);
    rank = ((size_t)percentile * count + 99) / 100;
    return scratch[rank - 1];
}

static uint64_t hol_maximum(const uint64_t *samples, uint64_t total, size_t capacity) {
    size_t count = total > capacity ? capacity : (size_t)total;
    uint64_t maximum = 0;

    for (size_t i = 0; i < count; i++) {
        if (samples[i] > maximum) maximum = samples[i];
    }
    return maximum;
}

static void hol_write_metric(FILE *file, const char *name, uint64_t value) {
    fprintf(file, "%s=%" PRIu64 "\n", name, value);
}

static void hol_write_snapshot(void) __attribute__((destructor));
static void hol_write_snapshot(void) {
    FILE *file;

    if (!hol_enabled) return;
    if (hol_drain_active) {
        size_t residual = hol_processing_list == NULL ? 0 : hol_processing_list->len;
        hol_finish_drain(residual);
    }

    file = fopen(hol_metrics_path, "w");
    if (file == NULL) return;

    hol_write_metric(file, "short_queue_delay_samples", hol_short_delay_count);
    hol_write_metric(file, "short_queue_delay_p50_us",
                     hol_quantile(hol_short_delays, hol_short_delay_count, HOL_QUEUE_SAMPLES, 50));
    hol_write_metric(file, "short_queue_delay_p99_us",
                     hol_quantile(hol_short_delays, hol_short_delay_count, HOL_QUEUE_SAMPLES, 99));
    hol_write_metric(file, "bulk_queue_delay_samples", hol_bulk_delay_count);
    hol_write_metric(file, "bulk_queue_delay_p50_us",
                     hol_quantile(hol_bulk_delays, hol_bulk_delay_count, HOL_QUEUE_SAMPLES, 50));
    hol_write_metric(file, "bulk_queue_delay_p99_us",
                     hol_quantile(hol_bulk_delays, hol_bulk_delay_count, HOL_QUEUE_SAMPLES, 99));
    hol_write_metric(file, "drain_invocation_samples", hol_drain_count);
    hol_write_metric(file, "drain_clients_per_invocation_p50",
                     hol_quantile(hol_drain_clients, hol_drain_count, HOL_DRAIN_SAMPLES, 50));
    hol_write_metric(file, "drain_clients_per_invocation_p99",
                     hol_quantile(hol_drain_clients, hol_drain_count, HOL_DRAIN_SAMPLES, 99));
    hol_write_metric(file, "drain_commands_per_invocation_p50",
                     hol_quantile(hol_drain_commands, hol_drain_count, HOL_DRAIN_SAMPLES, 50));
    hol_write_metric(file, "drain_commands_per_invocation_p99",
                     hol_quantile(hol_drain_commands, hol_drain_count, HOL_DRAIN_SAMPLES, 99));
    hol_write_metric(file, "drain_residual_clients_p50",
                     hol_quantile(hol_drain_residual, hol_drain_count, HOL_DRAIN_SAMPLES, 50));
    hol_write_metric(file, "drain_residual_clients_p99",
                     hol_quantile(hol_drain_residual, hol_drain_count, HOL_DRAIN_SAMPLES, 99));
    hol_write_metric(file, "drain_residual_clients_max",
                     hol_maximum(hol_drain_residual, hol_drain_count, HOL_DRAIN_SAMPLES));
    hol_write_metric(file, "drain_initial_clients_p99",
                     hol_quantile(hol_drain_initial, hol_drain_count, HOL_DRAIN_SAMPLES, 99));
    fclose(file);
}

#define listJoin(destination, source) perfloop_hol_list_join((destination), (source))
#define listLinkNodeTail(target, node) perfloop_hol_list_link_tail((target), (node))
#define listUnlinkNode(target, node) perfloop_hol_list_unlink((target), (node))
#define listLinkNodeHead(target, node) perfloop_hol_list_link_head((target), (node))

#endif
