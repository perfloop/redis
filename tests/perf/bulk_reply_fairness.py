#!/usr/bin/env python3
"""Exercise copy-avoided bulk replies alongside latency-sensitive clients.

The workload stores one raw 32 MiB string, streams it repeatedly to a fast
normal client, and measures serial PING round trips from another normal client.
The integrity check pipelines a trailing PING and verifies both the payload
digest and reply order, so the payload remains an observed part of the workload.
The trace-enabled server build records each writev acceptance and writeToClient
callback interval for the same sample, and separately records callback
intervals that contained an accepted write larger than the 64 KiB quantum. It
also pairs each serial small-client PING's CLOCK_MONOTONIC send timestamp with
the server pingCommand entry to
report the scheduling queue-wait distribution.
"""

import argparse
import hashlib
import json
import math
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path


HOST = "127.0.0.1"
KEY = b"perfloop:bulk-reply-fairness"
PAYLOAD_BYTES = 32 * 1024 * 1024
PING_REQUESTS = 4000
SOCKET_TIMEOUT_SECONDS = 10
NET_MAX_WRITES_PER_EVENT = 64 * 1024


def make_payload():
    block = b"0123456789abcdef:copy-avoided-bulk-reply:"
    repeats, remainder = divmod(PAYLOAD_BYTES, len(block))
    return block * repeats + block[:remainder]


PAYLOAD = make_payload()
PAYLOAD_DIGEST = hashlib.sha256(PAYLOAD).digest()
PAYLOAD_DIGEST_HEX = PAYLOAD_DIGEST.hex()


def encode_command(*parts):
    encoded = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        if isinstance(part, str):
            part = part.encode()
        encoded.extend((f"${len(part)}\r\n".encode(), part, b"\r\n"))
    return b"".join(encoded)


PING = encode_command("PING")
BULK_GET = encode_command("GET", KEY)
BULK_PIPELINE = BULK_GET + PING


def reserve_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((HOST, 0))
        return sock.getsockname()[1]


class RespConnection:
    def __init__(self, port):
        self.sock = socket.create_connection((HOST, port), SOCKET_TIMEOUT_SECONDS)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.settimeout(SOCKET_TIMEOUT_SECONDS)
        self.buffer = bytearray()

    def close(self):
        self.sock.close()

    def sendall(self, data):
        self.sock.sendall(data)

    def _fill(self):
        chunk = self.sock.recv(256 * 1024)
        if not chunk:
            raise RuntimeError("unexpected end of RESP stream")
        self.buffer.extend(chunk)

    def read_exact(self, length):
        while len(self.buffer) < length:
            self._fill()
        data = bytes(self.buffer[:length])
        del self.buffer[:length]
        return data

    def read_line(self):
        while True:
            end = self.buffer.find(b"\r\n")
            if end >= 0:
                line = bytes(self.buffer[:end])
                del self.buffer[: end + 2]
                return line
            self._fill()

    def read_status(self):
        prefix = self.read_exact(1)
        line = self.read_line()
        return prefix + line

    def read_bulk_bytes(self):
        if self.read_exact(1) != b"$":
            raise RuntimeError("expected a bulk-string reply")
        length = int(self.read_line())
        if length < 0:
            return None
        value = self.read_exact(length)
        if self.read_exact(2) != b"\r\n":
            raise RuntimeError("bulk-string reply does not end in CRLF")
        return value

    def read_bulk_digest(self):
        if self.read_exact(1) != b"$":
            raise RuntimeError("expected a bulk-string reply")
        length = int(self.read_line())
        if length != PAYLOAD_BYTES:
            raise RuntimeError(f"unexpected bulk-string length: {length}")

        digest = hashlib.sha256()
        remaining = length
        while remaining:
            if not self.buffer:
                self._fill()
            take = min(remaining, len(self.buffer))
            digest.update(self.buffer[:take])
            del self.buffer[:take]
            remaining -= take

        if self.read_exact(2) != b"\r\n":
            raise RuntimeError("bulk-string reply does not end in CRLF")
        if digest.digest() != PAYLOAD_DIGEST:
            raise RuntimeError("bulk-string reply digest mismatch")


def connect(port):
    return RespConnection(port)


class RedisServer:
    def __init__(self, server, trace=False):
        self.directory = tempfile.TemporaryDirectory(prefix="redis-bulk-reply-")
        self.trace_path = None
        self.port = reserve_port()
        env = os.environ.copy()
        if trace:
            self.trace_path = Path(self.directory.name) / "io.trace"
            env["PERFLOOP_IO_TRACE"] = str(self.trace_path)

        self.process = subprocess.Popen(
            [
                str(Path(server).resolve()),
                "--bind", HOST,
                "--port", str(self.port),
                "--save", "",
                "--appendonly", "no",
                "--protected-mode", "no",
                "--daemonize", "no",
                "--enable-debug-command", "yes",
                "--dir", self.directory.name,
                "--loglevel", "warning",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env=env,
        )
        self._wait_until_ready()

    def _wait_until_ready(self):
        deadline = time.monotonic() + SOCKET_TIMEOUT_SECONDS
        last_error = None
        while time.monotonic() < deadline:
            try:
                conn = connect(self.port)
                conn.sendall(PING)
                if conn.read_status() != b"+PONG":
                    raise RuntimeError("server did not return PONG during startup")
                conn.close()
                return
            except (OSError, RuntimeError) as error:
                last_error = error
                time.sleep(0.01)
        self.stop()
        raise RuntimeError(f"redis-server did not become ready: {last_error}")

    def stop(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=SOCKET_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=SOCKET_TIMEOUT_SECONDS)
        stderr = self.process.stderr.read().decode(errors="replace")
        if self.process.returncode not in (0, -15):
            raise RuntimeError(
                f"redis-server exited unexpectedly ({self.process.returncode}): {stderr.strip()}"
            )

    def close(self):
        try:
            self.stop()
        finally:
            self.directory.cleanup()


def prepare_copy_avoided_value(port):
    conn = connect(port)
    try:
        conn.sendall(encode_command("SET", KEY, PAYLOAD))
        if conn.read_status() != b"+OK":
            raise RuntimeError("SET did not succeed")

        conn.sendall(encode_command("DEBUG", "REPLY-COPY-AVOIDANCE", "1"))
        if conn.read_status() != b"+OK":
            raise RuntimeError("could not enable reply copy avoidance")

        conn.sendall(encode_command("OBJECT", "ENCODING", KEY))
        if conn.read_bulk_bytes() != b"raw":
            raise RuntimeError("bulk value is not raw encoded")
    finally:
        conn.close()


class BulkReader:
    def __init__(self, port):
        self.port = port
        self.started = threading.Event()
        self.stop_requested = threading.Event()
        self.thread = threading.Thread(target=self._run, name="bulk-reader")
        self.completed = 0
        self.error = None

    def start(self):
        self.thread.start()
        if not self.started.wait(SOCKET_TIMEOUT_SECONDS):
            raise RuntimeError("bulk reader did not enqueue its first request")

    def _run(self):
        conn = None
        try:
            conn = connect(self.port)
            while not self.stop_requested.is_set():
                # Keep PING exclusive to the measured small client so trace P records
                # can be paired with its serial request-send timestamps.
                conn.sendall(BULK_GET)
                self.started.set()
                conn.read_bulk_digest()
                self.completed += 1
        except BaseException as error:
            if not self.stop_requested.is_set():
                self.error = error
                self.started.set()
        finally:
            if conn is not None:
                conn.close()

    def stop(self):
        self.stop_requested.set()
        self.thread.join(SOCKET_TIMEOUT_SECONDS)
        if self.thread.is_alive():
            raise RuntimeError("bulk reader did not stop")
        if self.error is not None:
            raise RuntimeError(f"bulk reader failed: {self.error}")
        if self.completed < 1:
            raise RuntimeError("bulk reader completed no verified responses")


def collect_trace_metrics(trace_path, small_ping_sent_ns=()):
    writev_records = []
    callback_durations_ns = []
    oversized_callback_durations_ns = []
    ping_command_start_ns = []

    if trace_path is None:
        raise RuntimeError("the trace-enabled redis-server was not requested")
    try:
        with trace_path.open("rt", encoding="ascii") as trace:
            for line in trace:
                fields = line.split()
                if len(fields) != 4:
                    raise RuntimeError(f"malformed I/O trace record: {line!r}")
                record_type = fields[0]
                offered, accepted, duration_ns = map(int, fields[1:])
                if record_type == "W" and accepted > 0:
                    writev_records.append((offered, accepted, duration_ns))
                elif record_type == "C" and duration_ns > 0:
                    callback_durations_ns.append(duration_ns)
                elif record_type == "O" and duration_ns > 0:
                    oversized_callback_durations_ns.append(duration_ns)
                elif record_type == "P" and duration_ns > 0:
                    ping_command_start_ns.append(duration_ns)
                elif record_type not in ("W", "C", "O", "P"):
                    raise RuntimeError(f"unknown I/O trace record type: {record_type}")
    except FileNotFoundError as error:
        raise RuntimeError("the server did not produce an I/O trace") from error

    if not writev_records:
        raise RuntimeError("copy-avoided reply made no traced writev call")
    if not callback_durations_ns:
        raise RuntimeError("the server did not trace any writeToClient callback")

    max_writev_accepted_bytes = max(record[1] for record in writev_records)
    metrics = {
        "max_write_callback_duration_ns": max(callback_durations_ns),
        # This is zero precisely when no traced writeToClient callback accepted
        # an oversized writev span; it therefore binds the quantum-cap claim.
        "max_oversized_write_callback_duration_ns": max(
            oversized_callback_durations_ns, default=0
        ),
        "max_writev_accepted_bytes": max_writev_accepted_bytes,
        "max_writev_duration_ns": max(record[2] for record in writev_records),
        "writev_events": len(writev_records),
        "writev_quantum_bound": int(
            max_writev_accepted_bytes <= NET_MAX_WRITES_PER_EVENT
        ),
        # A discrete goal makes the claimed quantum cap verdict-visible: under
        # this workload any accepted span above the configured event quantum is
        # a violation, while an exact cap produces zero violations.
        "writev_quantum_violation": int(
            max_writev_accepted_bytes > NET_MAX_WRITES_PER_EVENT
        ),
    }

    if small_ping_sent_ns:
        if len(ping_command_start_ns) < len(small_ping_sent_ns):
            raise RuntimeError("the I/O trace omitted small-client PING commands")
        # Python and the trace helper both use the system-wide CLOCK_MONOTONIC
        # epoch. Only the startup PING precedes these serial small-client PINGs.
        queue_waits_us = []
        for sent_ns, command_start_ns in zip(
            small_ping_sent_ns, ping_command_start_ns[-len(small_ping_sent_ns) :]
        ):
            if command_start_ns < sent_ns:
                raise RuntimeError("small-client PING trace ordering is invalid")
            queue_waits_us.append((command_start_ns - sent_ns) / 1000.0)
        metrics["small_ping_queue_wait_p99_us"] = percentile_99(queue_waits_us)

    return metrics


def verify_integrity(server):
    redis = RedisServer(server, trace=True)
    try:
        prepare_copy_avoided_value(redis.port)
        bulk = connect(redis.port)
        small = connect(redis.port)
        try:
            bulk.sendall(BULK_PIPELINE)
            small.sendall(PING)
            if small.read_status() != b"+PONG":
                raise RuntimeError("small client did not receive PONG")
            bulk.read_bulk_digest()
            if bulk.read_status() != b"+PONG":
                raise RuntimeError("bulk response was not followed by its pipelined PONG")
        finally:
            bulk.close()
            small.close()

        redis.stop()
        report = {
            "bulk_reply_bytes": PAYLOAD_BYTES,
            "bulk_reply_sha256": PAYLOAD_DIGEST_HEX,
            "response_order": "bulk_then_pong",
            "small_client_reply": "PONG",
            **collect_trace_metrics(redis.trace_path),
        }
        print(json.dumps(report, sort_keys=True))
    finally:
        redis.close()


def percentile_99(values):
    if not values:
        raise RuntimeError("no small-client latency samples")
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * 0.99) - 1]


def measure(server):
    redis = RedisServer(server, trace=True)
    bulk = None
    try:
        prepare_copy_avoided_value(redis.port)
        bulk = BulkReader(redis.port)
        bulk.start()

        small = connect(redis.port)
        try:
            latencies_us = []
            small_ping_sent_ns = []
            for _ in range(PING_REQUESTS):
                start_ns = time.monotonic_ns()
                small.sendall(PING)
                small_ping_sent_ns.append(start_ns)
                if small.read_status() != b"+PONG":
                    raise RuntimeError("small client did not receive PONG")
                latencies_us.append((time.monotonic_ns() - start_ns) / 1000.0)
                if bulk.error is not None:
                    raise RuntimeError(f"bulk reader failed: {bulk.error}")
        finally:
            small.close()

        bulk.stop()
        redis.stop()
        trace_metrics = collect_trace_metrics(redis.trace_path, small_ping_sent_ns)
        print(json.dumps({"metric": "small_ping_p99_us", "value": percentile_99(latencies_us)}))
        print(json.dumps({"metric": "bulk_responses_completed", "value": bulk.completed}))
        for metric in (
            "max_writev_accepted_bytes",
            "max_write_callback_duration_ns",
            "max_oversized_write_callback_duration_ns",
            "small_ping_queue_wait_p99_us",
            "writev_quantum_bound",
            "writev_quantum_violation",
        ):
            print(json.dumps({"metric": metric, "value": trace_metrics[metric]}))
    finally:
        if bulk is not None and bulk.thread.is_alive():
            bulk.stop_requested.set()
            bulk.thread.join(SOCKET_TIMEOUT_SECONDS)
        redis.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--measure", action="store_true")
    mode.add_argument("--verify", action="store_true")
    args = parser.parse_args()

    if args.measure:
        measure(args.server)
    else:
        verify_integrity(args.server)


if __name__ == "__main__":
    try:
        main()
    except BaseException as error:
        print(f"bulk reply fairness harness failed: {error}", file=sys.stderr)
        raise SystemExit(1)
