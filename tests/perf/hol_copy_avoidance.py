#!/usr/bin/env python3
"""Measure small-request tail latency while a copy-avoided bulk reply drains.

Redis's built-in redis-benchmark measures one request class at a time.  This
fixture instead keeps one normal client draining a raw bulk GET while another
normal client issues PINGs.  It is test-only proof machinery: every invocation
starts a fresh server, validates a runtime-generated value, and emits one JSONL
sample per requested metric.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
from typing import Iterable, Optional


BULK_KEY = b"perfloop:copy-avoided-bulk"
BULK_BYTES = 4 * 1024 * 1024
NET_MAX_WRITES_PER_EVENT = 64 * 1024
RECV_CHUNK = 64 * 1024


class ProtocolError(RuntimeError):
    """The test server produced a reply different from the expected RESP2 reply."""


def command(*parts: bytes | str) -> bytes:
    encoded = []
    for part in parts:
        if isinstance(part, str):
            part = part.encode("ascii")
        encoded.append(part)
    request = [b"*", str(len(encoded)).encode("ascii"), b"\r\n"]
    for part in encoded:
        request.extend((b"$", str(len(part)).encode("ascii"), b"\r\n", part, b"\r\n"))
    return b"".join(request)


class RedisConnection:
    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self.buffer = bytearray()

    @classmethod
    def connect(cls, host: str, port: int, receive_buffer: Optional[int] = None) -> "RedisConnection":
        sock = socket.create_connection((host, port), timeout=5)
        sock.settimeout(10)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if receive_buffer is not None:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
        return cls(sock)

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass

    def send(self, *parts: bytes | str) -> None:
        self.sock.sendall(command(*parts))

    def send_raw(self, request: bytes) -> None:
        self.sock.sendall(request)

    def _fill(self) -> None:
        data = self.sock.recv(RECV_CHUNK)
        if not data:
            raise ProtocolError("unexpected EOF while reading RESP reply")
        self.buffer.extend(data)

    def read_line(self) -> bytes:
        while True:
            newline = self.buffer.find(b"\r\n")
            if newline >= 0:
                line = bytes(self.buffer[:newline])
                del self.buffer[: newline + 2]
                return line
            self._fill()

    def read_exact(self, size: int) -> bytes:
        while len(self.buffer) < size:
            self._fill()
        data = bytes(self.buffer[:size])
        del self.buffer[:size]
        return data

    def read_simple(self, expected: bytes) -> None:
        actual = self.read_line()
        if actual != expected:
            raise ProtocolError(f"expected {expected!r}, got {actual!r}")

    def read_bulk(self, expected_size: int, expected_value: Optional[bytes] = None) -> None:
        header = self.read_line()
        expected_header = b"$" + str(expected_size).encode("ascii")
        if header != expected_header:
            raise ProtocolError(f"expected bulk header {expected_header!r}, got {header!r}")

        remaining = expected_size
        offset = 0
        while remaining:
            take = min(remaining, RECV_CHUNK)
            data = self.read_exact(take)
            if expected_value is not None and data != expected_value[offset : offset + take]:
                raise ProtocolError("bulk response payload did not match the value written to Redis")
            offset += take
            remaining -= take

        if self.read_exact(2) != b"\r\n":
            raise ProtocolError("bulk response was missing its RESP terminator")


class RedisServer:
    def __init__(self, server: Path, directory: Path, trace_library: Optional[Path], trace_file: Path) -> None:
        self.server = server
        self.directory = directory
        self.trace_library = trace_library
        self.trace_file = trace_file
        self.port = reserve_port()
        self.process: Optional[subprocess.Popen[bytes]] = None
        self.server_cpu = choose_server_cpu()

    def __enter__(self) -> "RedisServer":
        env = os.environ.copy()
        if self.trace_library is not None:
            existing = env.get("LD_PRELOAD")
            env["LD_PRELOAD"] = (
                f"{self.trace_library}:{existing}" if existing else str(self.trace_library)
            )
            env["PERFLOOP_WRITEV_TRACE"] = str(self.trace_file)

        def pin_server() -> None:
            if self.server_cpu is not None:
                os.sched_setaffinity(0, {self.server_cpu})

        args = [
            str(self.server),
            "--bind",
            "127.0.0.1",
            "--port",
            str(self.port),
            "--save",
            "",
            "--appendonly",
            "no",
            "--protected-mode",
            "no",
            "--enable-debug-command",
            "yes",
            "--dir",
            str(self.directory),
            "--logfile",
            "",
        ]
        self.process = subprocess.Popen(
            args,
            cwd=self.directory,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            preexec_fn=pin_server if self.server_cpu is not None else None,
        )
        wait_for_server(self.process, self.port)
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)


def reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def choose_server_cpu() -> Optional[int]:
    """Pin Redis away from the Python clients when the host offers two CPUs."""
    if not hasattr(os, "sched_getaffinity"):
        return None
    try:
        cpus = sorted(os.sched_getaffinity(0))
        if len(cpus) < 2:
            return None
        server_cpu = cpus[0]
        os.sched_setaffinity(0, set(cpus[1:]))
        return server_cpu
    except OSError:
        return None


def wait_for_server(process: subprocess.Popen[bytes], port: int) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"redis-server exited during startup with status {process.returncode}")
        try:
            connection = RedisConnection.connect("127.0.0.1", port)
            connection.send("PING")
            connection.read_simple(b"+PONG")
            connection.close()
            return
        except (OSError, ProtocolError):
            time.sleep(0.01)
    raise RuntimeError("redis-server did not accept a PING within 10 seconds")


def setup_copy_avoided_value(port: int, payload: bytes) -> None:
    connection = RedisConnection.connect("127.0.0.1", port)
    try:
        connection.send("DEBUG", "REPLY-COPY-AVOIDANCE", "1")
        connection.read_simple(b"+OK")
        connection.send(b"SET", BULK_KEY, payload)
        connection.read_simple(b"+OK")

        # Verify the value before measuring.  The read consumes the actual RESP
        # response and confirms that the runtime-generated payload survived setup.
        connection.send(b"GET", BULK_KEY)
        connection.read_bulk(len(payload), payload)
    finally:
        connection.close()


class BulkLoad:
    """Continuously GET and consume a large raw string on one normal client."""

    def __init__(self, port: int, payload_size: int) -> None:
        self.connection = RedisConnection.connect("127.0.0.1", port, receive_buffer=8 * 1024 * 1024)
        self.payload_size = payload_size
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.completed = 0
        self.error: Optional[BaseException] = None
        self.thread = threading.Thread(target=self._run, name="bulk-get-reader", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def _run(self) -> None:
        try:
            while not self.stop_event.is_set():
                self.connection.send(b"GET", BULK_KEY)
                self.connection.read_bulk(self.payload_size)
                with self.lock:
                    self.completed += 1
        except BaseException as error:  # socket close is expected during shutdown
            if not self.stop_event.is_set():
                self.error = error

    def count(self) -> int:
        with self.lock:
            return self.completed

    def wait_for(self, target: int, timeout: float = 10) -> None:
        deadline = time.monotonic() + timeout
        while self.count() < target:
            if self.error is not None:
                raise RuntimeError("bulk GET client failed") from self.error
            if time.monotonic() >= deadline:
                raise RuntimeError("bulk GET client did not complete a reply")
            time.sleep(0.002)

    def stop(self) -> None:
        self.stop_event.set()
        try:
            self.connection.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.connection.close()
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            raise RuntimeError("bulk GET reader did not stop")
        if self.error is not None:
            raise RuntimeError("bulk GET client failed") from self.error


def p99(values: Iterable[int]) -> float:
    ordered = sorted(values)
    if len(ordered) < 500:
        raise RuntimeError(f"need at least 500 small-request samples, got {len(ordered)}")
    return float(ordered[math.ceil(len(ordered) * 0.99) - 1]) / 1000.0


def read_trace(trace_file: Path) -> tuple[int, float]:
    try:
        values = dict(line.split("=", 1) for line in trace_file.read_text().splitlines() if "=" in line)
        maximum = int(values["max_writev_accepted_bytes"])
        duration_us = int(values["max_writev_duration_ns"]) / 1000.0
    except (FileNotFoundError, KeyError, ValueError) as error:
        raise RuntimeError("writev probe did not emit a usable summary") from error
    if maximum <= 0 or duration_us <= 0:
        raise RuntimeError("writev probe recorded no successful writev call")
    return maximum, duration_us


def run_latency(server: Path, trace_library: Path, duration: float) -> tuple[float, int, float]:
    payload = os.urandom(BULK_BYTES)
    with tempfile.TemporaryDirectory(prefix=".perfloop-hol-", dir=".") as temp:
        directory = Path(temp)
        trace_file = directory / "writev.trace"
        with RedisServer(server, directory, trace_library, trace_file) as redis:
            setup_copy_avoided_value(redis.port, payload)
            bulk = BulkLoad(redis.port, len(payload))
            pinger = RedisConnection.connect("127.0.0.1", redis.port)
            try:
                bulk.start()
                bulk.wait_for(1)
                latencies = []
                end = time.monotonic() + duration
                while time.monotonic() < end:
                    start = time.perf_counter_ns()
                    pinger.send("PING")
                    pinger.read_simple(b"+PONG")
                    latencies.append(time.perf_counter_ns() - start)
                if bulk.count() < 2:
                    raise RuntimeError("bulk traffic did not continue during small-request sampling")
            finally:
                pinger.close()
                bulk.stop()
        # RedisServer's destructor flushes the interposer summary before leaving
        # this temporary directory.
        maximum, duration_us = read_trace(trace_file)
        return p99(latencies), maximum, duration_us


def run_probe(server: Path, trace_library: Path) -> tuple[int, float]:
    """Confirm that the copy-avoided workload reaches a measured writev call."""
    payload = os.urandom(BULK_BYTES)
    with tempfile.TemporaryDirectory(prefix=".perfloop-hol-", dir=".") as temp:
        directory = Path(temp)
        trace_file = directory / "writev.trace"
        with RedisServer(server, directory, trace_library, trace_file) as redis:
            setup_copy_avoided_value(redis.port, payload)
            connection = RedisConnection.connect("127.0.0.1", redis.port, receive_buffer=64 * 1024)
            try:
                connection.send(b"GET", BULK_KEY)
                connection.read_bulk(len(payload), payload)
            finally:
                connection.close()
        return read_trace(trace_file)


def run_throughput(server: Path, duration: float) -> float:
    payload = os.urandom(BULK_BYTES)
    with tempfile.TemporaryDirectory(prefix=".perfloop-hol-", dir=".") as temp:
        directory = Path(temp)
        with RedisServer(server, directory, None, directory / "unused.trace") as redis:
            setup_copy_avoided_value(redis.port, payload)
            bulk = BulkLoad(redis.port, len(payload))
            try:
                bulk.start()
                bulk.wait_for(1)
                completed_before = bulk.count()
                started = time.monotonic()
                while time.monotonic() - started < duration:
                    if bulk.error is not None:
                        raise RuntimeError("bulk GET client failed") from bulk.error
                    time.sleep(0.002)
                elapsed = time.monotonic() - started
                completed = bulk.count() - completed_before
            finally:
                bulk.stop()
    if completed <= 0:
        raise RuntimeError("bulk throughput run completed no full replies")
    return completed * BULK_BYTES / elapsed / (1024 * 1024)


def run_check(server: Path) -> None:
    """Validate a large copy-avoided reply and a following reply stay ordered."""
    payload = os.urandom(BULK_BYTES)
    with tempfile.TemporaryDirectory(prefix=".perfloop-hol-", dir=".") as temp:
        directory = Path(temp)
        with RedisServer(server, directory, None, directory / "unused.trace") as redis:
            setup_copy_avoided_value(redis.port, payload)
            connection = RedisConnection.connect("127.0.0.1", redis.port, receive_buffer=64 * 1024)
            try:
                # Sending both commands together makes RESP ordering observable: PONG
                # must follow all payload bytes, even if the large response is partial.
                connection.send_raw(command(b"GET", BULK_KEY) + command(b"PING"))
                connection.read_bulk(len(payload), payload)
                connection.read_simple(b"+PONG")
            finally:
                connection.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--mode", choices=("latency", "throughput", "check", "probe"), required=True)
    parser.add_argument("--duration", type=float, default=1.5)
    parser.add_argument("--writev-trace", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    server = args.server.resolve()
    if not server.is_file():
        raise RuntimeError(f"redis-server not found at {server}")
    if args.duration <= 0:
        raise RuntimeError("duration must be positive")

    if args.mode == "check":
        run_check(server)
        print("copy_avoided_bulk_reply_and_ordering_ok")
        return 0
    if args.mode == "throughput":
        value = run_throughput(server, args.duration)
        print(json.dumps({"metric": "bulk_get_mib_per_s", "value": value}, separators=(",", ":")))
        return 0

    if args.writev_trace is None:
        raise RuntimeError("latency mode requires --writev-trace")
    trace_library = args.writev_trace.resolve()
    if not trace_library.is_file():
        raise RuntimeError(f"writev trace library not found at {trace_library}")
    if args.mode == "probe":
        maximum, duration_us = run_probe(server, trace_library)
        print(
            "writev_probe_observed "
            f"max_writev_accepted_bytes={maximum} max_writev_duration_us={duration_us}"
        )
        return 0
    latency, maximum, duration_us = run_latency(server, trace_library, args.duration)
    print(json.dumps({"metric": "small_ping_p99_us", "value": latency}, separators=(",", ":")))
    print(json.dumps({"metric": "max_writev_accepted_bytes", "value": maximum}, separators=(",", ":")))
    print(json.dumps({"metric": "max_writev_duration_us", "value": duration_us}, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
