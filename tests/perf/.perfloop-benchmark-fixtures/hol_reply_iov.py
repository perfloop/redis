#!/usr/bin/env python3
"""Exercise reply-vector resume and price normal reply-list output under a peer.

This proof-only fixture complements the copy-avoided HOL benchmark.  It keeps a
second normal client connected while it checks the reply forms affected by the
ReplyIOV budget, and it can run the same public RESP sequence with one output
I/O worker.  The throughput mode emits one JSONL sample for the contended,
non-copy-avoided reply-list destination path.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
from typing import Iterator, Optional

from hol_copy_avoidance import (
    BULK_BYTES,
    BULK_KEY,
    BulkLoad,
    ProtocolError,
    RedisConnection,
    RedisServer,
    command,
    reserve_port,
    wait_for_server,
)

EMPTY_KEY = b"perfloop:reply-iov-empty"
CRLF_KEY = b"perfloop:reply-iov-crlf"
FIRST_KEY = b"perfloop:reply-iov-first"
SECOND_KEY = b"perfloop:reply-iov-second"


class PingLoad:
    """Keep a second normal client active while a large reply drains."""

    def __init__(self, port: int) -> None:
        self.connection = RedisConnection.connect("127.0.0.1", port)
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.completed = 0
        self.max_latency_us = 0.0
        self.error: Optional[BaseException] = None
        self.thread = threading.Thread(target=self._run, name="reply-iov-pinger", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def _run(self) -> None:
        try:
            while not self.stop_event.is_set():
                started = time.perf_counter_ns()
                self.connection.send("PING")
                self.connection.read_simple(b"+PONG")
                elapsed_us = (time.perf_counter_ns() - started) / 1000.0
                with self.lock:
                    self.completed += 1
                    self.max_latency_us = max(self.max_latency_us, elapsed_us)
        except BaseException as error:  # socket close is expected during shutdown
            if not self.stop_event.is_set():
                self.error = error

    def wait_for(self, target: int, timeout: float = 10) -> None:
        deadline = time.monotonic() + timeout
        while True:
            with self.lock:
                completed = self.completed
            if completed >= target:
                return
            if self.error is not None:
                raise RuntimeError("peer PING client failed") from self.error
            if time.monotonic() >= deadline:
                raise RuntimeError("peer PING client did not complete a reply")
            time.sleep(0.002)

    def snapshot(self) -> tuple[int, float]:
        with self.lock:
            return self.completed, self.max_latency_us

    def stop(self) -> None:
        self.stop_event.set()
        try:
            self.connection.sock.shutdown(2)
        except OSError:
            pass
        self.connection.close()
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            raise RuntimeError("peer PING client did not stop")
        if self.error is not None:
            raise RuntimeError("peer PING client failed") from self.error


def set_value(port: int, key: bytes, payload: bytes, copy_avoidance: bool) -> None:
    connection = RedisConnection.connect("127.0.0.1", port)
    try:
        connection.send("DEBUG", "REPLY-COPY-AVOIDANCE", "1" if copy_avoidance else "0")
        connection.read_simple(b"+OK")
        connection.send(b"SET", key, payload)
        connection.read_simple(b"+OK")
        connection.send(b"GET", key)
        connection.read_bulk(len(payload), payload)
    finally:
        connection.close()


def read_ordered_bulk_pair(connection: RedisConnection, first: bytes, second: bytes) -> None:
    connection.send_raw(command(b"GET", BULK_KEY) + command(b"GET", EMPTY_KEY) + command(b"PING"))
    connection.read_bulk(len(first), first)
    connection.read_bulk(len(second), second)
    connection.read_simple(b"+PONG")


def run_copy_avoided_resume_cases(port: int) -> None:
    """Cover empty encoded payloads and split BULK_STR_REF fragments under a peer."""
    large = os.urandom(BULK_BYTES)
    set_value(port, BULK_KEY, large, True)
    set_value(port, EMPTY_KEY, b"", True)
    set_value(port, CRLF_KEY, b"c" * 65527, True)
    set_value(port, FIRST_KEY, b"a" * 65523, True)
    set_value(port, SECOND_KEY, b"b" * 65523, True)

    peer = PingLoad(port)
    peer.start()
    peer.wait_for(1)
    connection = RedisConnection.connect("127.0.0.1", port, receive_buffer=64 * 1024)
    try:
        read_ordered_bulk_pair(connection, large, b"")

        # "$65527\r\n" plus this value uses 65,535 bytes, so the first CRLF
        # byte occupies the final byte of the quantum and the second resumes.
        connection.send_raw(command(b"GET", CRLF_KEY) + command(b"PING"))
        connection.read_bulk(65527, b"c" * 65527)
        connection.read_simple(b"+PONG")

        # The first complete reply leaves three bytes.  The second reply's
        # "$65523\r\n" prefix is therefore split over two write vectors.
        connection.send_raw(
            command(b"GET", FIRST_KEY) + command(b"GET", SECOND_KEY) + command(b"PING")
        )
        connection.read_bulk(65523, b"a" * 65523)
        connection.read_bulk(65523, b"b" * 65523)
        connection.read_simple(b"+PONG")
    finally:
        connection.close()
        peer.stop()


def run_normal_reply_list_case(port: int) -> None:
    """Force copied client-buffer plus reply-list output while a peer remains live."""
    large = os.urandom(BULK_BYTES)
    set_value(port, BULK_KEY, large, False)
    set_value(port, EMPTY_KEY, b"", False)

    peer = PingLoad(port)
    peer.start()
    peer.wait_for(1)
    connection = RedisConnection.connect("127.0.0.1", port, receive_buffer=64 * 1024)
    try:
        read_ordered_bulk_pair(connection, large, b"")
    finally:
        connection.close()
        peer.stop()


def run_check(server: Path) -> None:
    with tempfile.TemporaryDirectory(prefix=".perfloop-reply-iov-check-", dir=".") as temp:
        directory = Path(temp)
        with RedisServer(server, directory, None, directory / "unused.trace") as redis:
            run_copy_avoided_resume_cases(redis.port)
            run_normal_reply_list_case(redis.port)


def run_normal_throughput(server: Path, duration: float) -> float:
    payload = os.urandom(BULK_BYTES)
    with tempfile.TemporaryDirectory(prefix=".perfloop-reply-iov-normal-", dir=".") as temp:
        directory = Path(temp)
        with RedisServer(server, directory, None, directory / "unused.trace") as redis:
            set_value(redis.port, BULK_KEY, payload, False)
            peer = PingLoad(redis.port)
            bulk = BulkLoad(redis.port, len(payload))
            peer.start()
            peer.wait_for(1)
            try:
                bulk.start()
                bulk.wait_for(1)
                completed_before = bulk.count()
                started = time.monotonic()
                while time.monotonic() - started < duration:
                    if bulk.error is not None:
                        raise RuntimeError("normal bulk GET client failed") from bulk.error
                    if peer.error is not None:
                        raise RuntimeError("peer PING client failed") from peer.error
                    time.sleep(0.002)
                elapsed = time.monotonic() - started
                completed = bulk.count() - completed_before
            finally:
                bulk.stop()
                peer.stop()
    if completed <= 0:
        raise RuntimeError("normal contended bulk throughput run completed no full replies")
    return completed * BULK_BYTES / elapsed / (1024 * 1024)


class ThreadedRedisServer:
    """Fresh local Redis with exactly one output I/O worker (io-threads=2)."""

    def __init__(self, server: Path, directory: Path, trace_library: Path, trace_file: Path) -> None:
        self.server = server
        self.directory = directory
        self.trace_library = trace_library
        self.trace_file = trace_file
        self.port = reserve_port()
        self.process: Optional[subprocess.Popen[bytes]] = None

    def __enter__(self) -> "ThreadedRedisServer":
        env = os.environ.copy()
        existing = env.get("LD_PRELOAD")
        env["LD_PRELOAD"] = f"{self.trace_library}:{existing}" if existing else str(self.trace_library)
        env["PERFLOOP_WRITEV_TRACE"] = str(self.trace_file)
        args = [
            str(self.server),
            "--bind", "127.0.0.1",
            "--port", str(self.port),
            "--save", "",
            "--appendonly", "no",
            "--protected-mode", "no",
            "--enable-debug-command", "yes",
            "--io-threads", "2",
            "--dir", str(self.directory),
            "--logfile", "",
        ]
        self.process = subprocess.Popen(
            args,
            cwd=self.directory,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
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


def read_trace_max(trace_file: Path) -> int:
    try:
        values = dict(line.split("=", 1) for line in trace_file.read_text().splitlines() if "=" in line)
        maximum = int(values["max_writev_accepted_bytes"])
    except (FileNotFoundError, KeyError, ValueError) as error:
        raise RuntimeError("threaded writev probe did not emit a usable summary") from error
    if maximum <= 0:
        raise RuntimeError("threaded writev probe recorded no successful writev call")
    return maximum


def run_io_threads_check(server: Path, trace_library: Path) -> tuple[int, int, float]:
    """Use one worker for both normal clients and exercise partial ordered replies."""
    large = os.urandom(BULK_BYTES)
    with tempfile.TemporaryDirectory(prefix=".perfloop-reply-iov-threaded-", dir=".") as temp:
        directory = Path(temp)
        trace_file = directory / "writev.trace"
        with ThreadedRedisServer(server, directory, trace_library, trace_file) as redis:
            set_value(redis.port, BULK_KEY, large, True)
            set_value(redis.port, EMPTY_KEY, b"", True)
            peer = PingLoad(redis.port)
            peer.start()
            peer.wait_for(1)
            connection = RedisConnection.connect("127.0.0.1", redis.port, receive_buffer=64 * 1024)
            try:
                read_ordered_bulk_pair(connection, large, b"")
                peer.wait_for(2)
                completed, maximum_latency_us = peer.snapshot()
            finally:
                connection.close()
                peer.stop()
        maximum = read_trace_max(trace_file)
    return maximum, completed, maximum_latency_us


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--mode", choices=("check", "normal-throughput", "io-threads-check"), required=True)
    parser.add_argument("--duration", type=float, default=8.0)
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
        print("reply_iov_partial_ordering_ok")
        return 0
    if args.mode == "normal-throughput":
        value = run_normal_throughput(server, args.duration)
        print(json.dumps({"metric": "normal_contended_bulk_get_mib_per_s", "value": value}, separators=(",", ":")))
        return 0

    if args.writev_trace is None:
        raise RuntimeError("io-threads-check requires --writev-trace")
    trace_library = args.writev_trace.resolve()
    if not trace_library.is_file():
        raise RuntimeError(f"writev trace library not found at {trace_library}")
    maximum, completed, maximum_latency_us = run_io_threads_check(server, trace_library)
    print(
        "io_thread_reply_resume_ok "
        f"io_thread_max_writev_accepted_bytes={maximum} "
        f"io_thread_ping_count={completed} io_thread_ping_max_us={maximum_latency_us}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
