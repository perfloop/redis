#!/usr/bin/env python3
"""Measure short-request tail latency while another IO-thread lane is busy.

The workload pins 24 pipelined INCR clients to one IO thread and one PING client
onto the other IO thread.  The client-placement setup mirrors Redis's
least-populated IO-thread assignment and checks CLIENT LIST before and after
the run, so the measured PING is not accidentally placed in the bulk lane.
"""

import argparse
import json
import math
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path


BULK_CLIENTS = 24
BULK_PIPELINE = 800
PADDING_CLIENTS = 64
MEASURE_ROUNDS = 100
CHECK_ROUNDS = 8
IO_THREADS = 3


class BenchmarkError(RuntimeError):
    """A workload setup or response-validation failure."""


def encode_command(*parts):
    encoded_parts = []
    for part in parts:
        if not isinstance(part, bytes):
            part = str(part).encode()
        encoded_parts.append(b"$%d\r\n%s\r\n" % (len(part), part))
    return b"*%d\r\n" % len(encoded_parts) + b"".join(encoded_parts)


class RespClient:
    """Small RESP2 client with buffered reads for high-pipeline validation."""

    def __init__(self, host, port):
        self.socket = socket.create_connection((host, port), timeout=3)
        self.socket.settimeout(10)
        self.buffer = bytearray()
        self.client_id = int(self.command("CLIENT", "ID"))

    def close(self):
        self.socket.close()

    def command(self, *parts):
        self.socket.sendall(encode_command(*parts))
        return self.read()

    def read_exact(self, length):
        while len(self.buffer) < length:
            chunk = self.socket.recv(65536)
            if not chunk:
                raise BenchmarkError("server closed a benchmark connection")
            self.buffer.extend(chunk)
        result = bytes(self.buffer[:length])
        del self.buffer[:length]
        return result

    def read_line(self):
        while True:
            end = self.buffer.find(b"\r\n")
            if end >= 0:
                result = bytes(self.buffer[:end])
                del self.buffer[:end + 2]
                return result
            chunk = self.socket.recv(65536)
            if not chunk:
                raise BenchmarkError("server closed a benchmark connection")
            self.buffer.extend(chunk)

    def read(self):
        response_type = self.read_exact(1)
        if response_type == b"+":
            return self.read_line().decode()
        if response_type == b"-":
            raise BenchmarkError("Redis error reply: %s" % self.read_line().decode())
        if response_type == b":":
            return int(self.read_line())
        if response_type == b"$":
            length = int(self.read_line())
            if length < 0:
                return None
            return self.read_exact(length + 2)[:-2]
        raise BenchmarkError("unexpected RESP response type %r" % response_type)


def client_threads(admin):
    """Return the server-reported IO-thread assignment keyed by CLIENT ID."""

    assignments = {}
    for row in admin.command("CLIENT", "LIST").decode().splitlines():
        fields = dict(field.split("=", 1) for field in row.split() if "=" in field)
        assignments[int(fields["id"])] = int(fields["io-thread"])
    return assignments


def wait_for_removed_clients(admin, closed_client_ids):
    deadline = time.monotonic() + 3
    while True:
        active = client_threads(admin)
        if not closed_client_ids.intersection(active):
            return
        if time.monotonic() >= deadline:
            raise BenchmarkError("closed padding clients remained assigned to an IO thread")
        time.sleep(0.005)


def start_server(server_path, server_cpus):
    port_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port_socket.bind(("127.0.0.1", 0))
    port = port_socket.getsockname()[1]
    port_socket.close()

    tempdir = tempfile.TemporaryDirectory(prefix="redis-iothread-fairness-")
    logfile = Path(tempdir.name) / "redis.log"
    command = [
        str(server_path),
        "--port",
        str(port),
        "--bind",
        "127.0.0.1",
        "--save",
        "",
        "--appendonly",
        "no",
        "--io-threads",
        str(IO_THREADS),
        "--dir",
        tempdir.name,
        "--logfile",
        str(logfile),
    ]
    if server_cpus:
        taskset = shutil.which("taskset")
        if not taskset:
            tempdir.cleanup()
            raise BenchmarkError("--server-cpus requires taskset")
        command = [taskset, "-c", server_cpus] + command

    process = subprocess.Popen(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 5
    while True:
        try:
            admin = RespClient("127.0.0.1", port)
            return process, tempdir, logfile, admin
        except OSError:
            if time.monotonic() >= deadline:
                process.kill()
                process.wait()
                log = logfile.read_text(errors="replace") if logfile.exists() else ""
                tempdir.cleanup()
                raise BenchmarkError("server did not start:\n%s" % log)
            time.sleep(0.01)


def stop_server(process, tempdir):
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    tempdir.cleanup()


def create_lane_layout(admin):
    """Place bulk and short clients on distinct worker IO threads.

    The control connection is retained on one worker.  Temporary clients are
    first balanced across both workers, then clients from the control worker
    are closed.  This leaves the opposite worker deliberately more populated,
    making the next bulk clients choose the control worker.  Additional idle
    clients are then opened until the next connection is assigned to the other
    worker; that connection is the latency-sensitive PING client.
    """

    assignments = client_threads(admin)
    bulk_thread = assignments[admin.client_id]
    worker_threads = {1, 2}
    if bulk_thread not in worker_threads:
        raise BenchmarkError("control connection was not assigned to a worker IO thread")
    short_thread = next(iter(worker_threads - {bulk_thread}))

    padding = [RespClient("127.0.0.1", admin.socket.getpeername()[1]) for _ in range(PADDING_CLIENTS)]
    assignments = client_threads(admin)
    retained_padding = []
    closed_client_ids = set()
    for client in padding:
        if assignments.get(client.client_id) == short_thread:
            retained_padding.append(client)
        else:
            closed_client_ids.add(client.client_id)
            client.close()
    wait_for_removed_clients(admin, closed_client_ids)

    bulk_clients = []
    for _ in range(BULK_CLIENTS):
        client = RespClient("127.0.0.1", admin.socket.getpeername()[1])
        assignments = client_threads(admin)
        if assignments.get(client.client_id) != bulk_thread:
            client.close()
            raise BenchmarkError("bulk client was assigned to the short-request IO thread")
        bulk_clients.append(client)

    fillers = []
    short_client = None
    for _ in range(PADDING_CLIENTS + BULK_CLIENTS):
        client = RespClient("127.0.0.1", admin.socket.getpeername()[1])
        assignments = client_threads(admin)
        if assignments.get(client.client_id) == short_thread:
            short_client = client
            break
        fillers.append(client)
    if short_client is None:
        raise BenchmarkError("could not place the short-request client on the other IO thread")

    return bulk_thread, short_thread, bulk_clients, short_client, fillers, retained_padding


def drain_bulk_replies(clients, expected_values, replies_per_client):
    for client in clients:
        expected = expected_values[client.client_id]
        for _ in range(replies_per_client):
            expected += 1
            reply = client.read()
            if reply != expected:
                raise BenchmarkError(
                    "bulk reply order/value mismatch for client %d: expected %d, got %r"
                    % (client.client_id, expected, reply)
                )
        expected_values[client.client_id] = expected


def percentile(values, quantile):
    if not values:
        raise BenchmarkError("no short-request latency samples were collected")
    rank = max(0, math.ceil(quantile * len(values)) - 1)
    return sorted(values)[rank]


def run_workload(admin, rounds):
    layout = create_lane_layout(admin)
    bulk_thread, short_thread, bulk_clients, short_client, fillers, retained_padding = layout
    all_clients = bulk_clients + fillers + retained_padding + [short_client]
    expected_values = {client.client_id: 0 for client in bulk_clients}
    payloads = {
        client.client_id: encode_command("INCR", "iothread-fairness:%d" % index) * BULK_PIPELINE
        for index, client in enumerate(bulk_clients)
    }
    short_latencies_us = []
    try:
        # Warm the normal Redis command and I/O paths; warmup replies are also
        # checked so a compiler or scheduler cannot turn the workload into an
        # unconsumed write-only operation.
        for client in bulk_clients:
            client.socket.sendall(payloads[client.client_id])
        drain_bulk_replies(bulk_clients, expected_values, BULK_PIPELINE)

        start = time.perf_counter_ns()
        for _ in range(rounds):
            for client in bulk_clients:
                client.socket.sendall(payloads[client.client_id])

            request_start = time.perf_counter_ns()
            short_client.socket.sendall(encode_command("PING"))
            if short_client.read() != "PONG":
                raise BenchmarkError("short request did not return PONG")
            short_latencies_us.append((time.perf_counter_ns() - request_start) / 1000.0)

            drain_bulk_replies(bulk_clients, expected_values, BULK_PIPELINE)
        elapsed_seconds = (time.perf_counter_ns() - start) / 1_000_000_000.0

        assignments = client_threads(admin)
        if assignments.get(short_client.client_id) != short_thread:
            raise BenchmarkError("short-request client moved to the bulk IO thread")
        if any(assignments.get(client.client_id) != bulk_thread for client in bulk_clients):
            raise BenchmarkError("a bulk client moved off the bulk IO thread")

        return {
            "bulk_commands": rounds * BULK_CLIENTS * BULK_PIPELINE,
            "bulk_ops_per_sec": rounds * BULK_CLIENTS * BULK_PIPELINE / elapsed_seconds,
            "bulk_thread": bulk_thread,
            "short_p50_us": percentile(short_latencies_us, 0.50),
            "short_p99_us": percentile(short_latencies_us, 0.99),
            "short_samples": len(short_latencies_us),
            "short_thread": short_thread,
        }
    finally:
        for client in all_clients:
            try:
                client.close()
            except OSError:
                pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default="src/redis-server")
    parser.add_argument("--server-cpus", default="")
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--metric",
        choices=("short_ping_p99_us", "bulk_ops_per_sec"),
        help="emit exactly one proof JSONL metric",
    )
    arguments = parser.parse_args()
    if arguments.check == (arguments.metric is not None):
        parser.error("specify exactly one of --check or --metric")

    server_path = Path(arguments.server)
    if not server_path.is_file():
        raise BenchmarkError("Redis server binary does not exist: %s" % server_path)

    process = tempdir = admin = None
    try:
        process, tempdir, _logfile, admin = start_server(server_path, arguments.server_cpus)
        results = run_workload(admin, CHECK_ROUNDS if arguments.check else MEASURE_ROUNDS)
        if arguments.check:
            print(
                "iothread fairness check: PASS "
                "bulk-thread=%d short-thread=%d bulk-commands=%d short-samples=%d"
                % (
                    results["bulk_thread"],
                    results["short_thread"],
                    results["bulk_commands"],
                    results["short_samples"],
                )
            )
        elif arguments.metric == "short_ping_p99_us":
            print(json.dumps({"metric": "short_ping_p99_us", "value": results["short_p99_us"]}))
        else:
            print(json.dumps({"metric": "bulk_ops_per_sec", "value": results["bulk_ops_per_sec"]}))
    finally:
        if admin is not None:
            try:
                admin.close()
            except OSError:
                pass
        if process is not None:
            stop_server(process, tempdir)


if __name__ == "__main__":
    try:
        main()
    except (BenchmarkError, OSError, subprocess.SubprocessError) as error:
        print("iothread fairness benchmark failed: %s" % error, file=sys.stderr)
        sys.exit(1)
