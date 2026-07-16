#!/usr/bin/env python3
"""Additional no-regression selectors for the IO-thread fairness benchmark."""

import argparse
import json
import sys
import time
from pathlib import Path

import iothread_fairness as fairness


BULK_CLIENTS = 24
BULK_PIPELINE = 800
ROUNDS = 100


def close_clients(clients):
    for client in clients:
        try:
            client.close()
        except OSError:
            pass


def bulk_payloads(clients):
    return {
        client.client_id: fairness.encode_command("INCR", "iothread-fairness-guard:%d" % index)
        * BULK_PIPELINE
        for index, client in enumerate(clients)
    }


def verify_assignments(admin, bulk_clients, bulk_thread, short_client=None, short_thread=None):
    assignments = fairness.client_threads(admin)
    if any(assignments.get(client.client_id) != bulk_thread for client in bulk_clients):
        raise fairness.BenchmarkError("a bulk client moved off its expected IO thread")
    if short_client is not None and assignments.get(short_client.client_id) != short_thread:
        raise fairness.BenchmarkError("the short client moved onto the bulk IO thread")


def run_mixed_bulk_throughput(admin):
    """Measure only bulk completion while a distinct lane receives a PING.

    The PING is sent in each round to preserve the mixed workload, but it is
    deliberately read after the bulk completion timer stops.  Thus a faster
    PING cannot raise this bulk-throughput result merely by shortening the
    measured latency sample.
    """

    layout = fairness.create_lane_layout(admin)
    bulk_thread, short_thread, bulk_clients, short_client, fillers, retained_padding = layout
    all_clients = bulk_clients + fillers + retained_padding + [short_client]
    expected_values = {client.client_id: 0 for client in bulk_clients}
    payloads = bulk_payloads(bulk_clients)
    elapsed_ns = 0
    try:
        for client in bulk_clients:
            client.socket.sendall(payloads[client.client_id])
        fairness.drain_bulk_replies(bulk_clients, expected_values, BULK_PIPELINE)

        for _ in range(ROUNDS):
            round_start = time.perf_counter_ns()
            for client in bulk_clients:
                client.socket.sendall(payloads[client.client_id])
            short_client.socket.sendall(fairness.encode_command("PING"))
            fairness.drain_bulk_replies(bulk_clients, expected_values, BULK_PIPELINE)
            elapsed_ns += time.perf_counter_ns() - round_start
            if short_client.read() != "PONG":
                raise fairness.BenchmarkError("mixed-workload PING did not return PONG")

        verify_assignments(admin, bulk_clients, bulk_thread, short_client, short_thread)
        return ROUNDS * BULK_CLIENTS * BULK_PIPELINE / (elapsed_ns / 1_000_000_000.0)
    finally:
        close_clients(all_clients)


def run_one_lane_bulk_throughput(admin):
    """Measure completion of the same bulk shape on one worker IO thread."""

    assignments = fairness.client_threads(admin)
    bulk_thread = assignments.get(admin.client_id)
    if bulk_thread != 1:
        raise fairness.BenchmarkError("one-lane server did not assign its control client to worker 1")

    port = admin.socket.getpeername()[1]
    bulk_clients = []
    expected_values = {}
    try:
        for _ in range(BULK_CLIENTS):
            client = fairness.RespClient("127.0.0.1", port)
            if fairness.client_threads(admin).get(client.client_id) != bulk_thread:
                client.close()
                raise fairness.BenchmarkError("bulk client was not assigned to the only worker IO thread")
            bulk_clients.append(client)
            expected_values[client.client_id] = 0

        payloads = bulk_payloads(bulk_clients)
        for client in bulk_clients:
            client.socket.sendall(payloads[client.client_id])
        fairness.drain_bulk_replies(bulk_clients, expected_values, BULK_PIPELINE)

        elapsed_ns = 0
        for _ in range(ROUNDS):
            round_start = time.perf_counter_ns()
            for client in bulk_clients:
                client.socket.sendall(payloads[client.client_id])
            fairness.drain_bulk_replies(bulk_clients, expected_values, BULK_PIPELINE)
            elapsed_ns += time.perf_counter_ns() - round_start

        verify_assignments(admin, bulk_clients, bulk_thread)
        return ROUNDS * BULK_CLIENTS * BULK_PIPELINE / (elapsed_ns / 1_000_000_000.0)
    finally:
        close_clients(bulk_clients)


def start_guard_server(server_path, server_cpus, io_threads):
    fairness.IO_THREADS = io_threads
    return fairness.start_server(server_path, server_cpus)


def run_metric(arguments):
    server_path = Path(arguments.server)
    if not server_path.is_file():
        raise fairness.BenchmarkError("Redis server binary does not exist: %s" % server_path)

    if arguments.metric == "mixed_bulk_ops_per_sec":
        fairness.BULK_CLIENTS = BULK_CLIENTS
        fairness.BULK_PIPELINE = BULK_PIPELINE
        process, tempdir, _logfile, admin = start_guard_server(server_path, arguments.server_cpus, 3)
        try:
            value = run_mixed_bulk_throughput(admin)
        finally:
            admin.close()
            fairness.stop_server(process, tempdir)
        return "bulk_ops_per_sec", value

    if arguments.metric == "one_lane_bulk_ops_per_sec":
        process, tempdir, _logfile, admin = start_guard_server(server_path, arguments.server_cpus, 2)
        try:
            value = run_one_lane_bulk_throughput(admin)
        finally:
            admin.close()
            fairness.stop_server(process, tempdir)
        return "bulk_ops_per_sec", value

    fairness.BULK_CLIENTS = 3
    fairness.BULK_PIPELINE = BULK_PIPELINE
    process, tempdir, _logfile, admin = start_guard_server(server_path, arguments.server_cpus, 3)
    try:
        results = fairness.run_workload(admin, ROUNDS)
    finally:
        admin.close()
        fairness.stop_server(process, tempdir)
    return "short_ping_p99_us", results["short_p99_us"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default="src/redis-server")
    parser.add_argument("--server-cpus", default="")
    parser.add_argument(
        "--metric",
        choices=(
            "mixed_bulk_ops_per_sec",
            "one_lane_bulk_ops_per_sec",
            "below_quantum_short_ping_p99_us",
        ),
        required=True,
    )
    arguments = parser.parse_args()
    metric, value = run_metric(arguments)
    print(json.dumps({"metric": metric, "value": value}))


if __name__ == "__main__":
    try:
        main()
    except (fairness.BenchmarkError, OSError) as error:
        print("iothread fairness guard benchmark failed: %s" % error, file=sys.stderr)
        sys.exit(1)
