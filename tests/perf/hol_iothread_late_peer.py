#!/usr/bin/env python3
"""Measure a late peer while one output I/O worker drains a bulk reply.

The server has io-threads=2, leaving exactly one non-main output worker. A
copy-avoided 4 MiB GET starts first; after its reader consumes an actual payload
byte, a second normal client sends PING. The probe consumes the full ordered
large GET, empty GET, and PING response stream, requires the late PING before
the bulk reader finishes, and reports both peer latency and submitted-iovec
size from the test-only writev interposer.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time

from hol_copy_avoidance import BULK_BYTES, BULK_KEY, RedisConnection, command
from hol_reply_iov import EMPTY_KEY, ThreadedRedisServer, set_value


def read_trace(trace_file: Path) -> int:
    try:
        values = dict(line.split("=", 1) for line in trace_file.read_text().splitlines() if "=" in line)
        submitted = int(values["max_writev_submitted_bytes"])
        calls = int(values["writev_calls"])
    except (FileNotFoundError, KeyError, ValueError) as error:
        raise RuntimeError("late-peer writev probe did not emit a usable summary") from error
    if submitted <= 0 or calls <= 0:
        raise RuntimeError("late-peer writev probe observed no successful writev call")
    return submitted


def run(server: Path, trace_library: Path) -> tuple[float, int]:
    payload = os.urandom(BULK_BYTES)
    started = threading.Event()
    finished = threading.Event()
    errors: list[BaseException] = []

    with tempfile.TemporaryDirectory(prefix=".perfloop-iothread-late-peer-", dir=".") as temp:
        directory = Path(temp)
        trace_file = directory / "writev.trace"
        with ThreadedRedisServer(server, directory, trace_library, trace_file) as redis:
            set_value(redis.port, BULK_KEY, payload, True)
            set_value(redis.port, EMPTY_KEY, b"", True)

            def drain() -> None:
                connection: RedisConnection | None = None
                try:
                    connection = RedisConnection.connect("127.0.0.1", redis.port, receive_buffer=4096)
                    connection.send_raw(command(b"GET", BULK_KEY) + command(b"GET", EMPTY_KEY) + command(b"PING"))
                    header = connection.read_line()
                    if header != b"$" + str(len(payload)).encode():
                        raise RuntimeError(f"unexpected large bulk header: {header!r}")
                    if connection.read_exact(1) != payload[:1]:
                        raise RuntimeError("first bulk payload byte did not match")
                    started.set()
                    offset = 1
                    while offset < len(payload):
                        take = min(4096, len(payload) - offset)
                        if connection.read_exact(take) != payload[offset:offset + take]:
                            raise RuntimeError("large bulk payload did not match")
                        offset += take
                        time.sleep(0.0005)
                    if connection.read_exact(2) != b"\r\n":
                        raise RuntimeError("large bulk terminator was missing")
                    if connection.read_line() != b"$0":
                        raise RuntimeError("empty bulk header was missing")
                    if connection.read_exact(2) != b"\r\n":
                        raise RuntimeError("empty bulk terminator was missing")
                    connection.read_simple(b"+PONG")
                except BaseException as error:
                    errors.append(error)
                finally:
                    finished.set()
                    if connection is not None:
                        connection.close()

            reader = threading.Thread(target=drain, name="iothread-late-peer-reader", daemon=True)
            reader.start()
            if not started.wait(10):
                raise RuntimeError("large reply did not start draining")

            peer = RedisConnection.connect("127.0.0.1", redis.port)
            try:
                begun = time.perf_counter_ns()
                peer.send(b"PING")
                peer.read_simple(b"+PONG")
                latency_us = (time.perf_counter_ns() - begun) / 1000.0
            finally:
                peer.close()

            if finished.is_set():
                raise RuntimeError("late peer completed only after the large reply drained")
            reader.join(20)
            if reader.is_alive():
                raise RuntimeError("ordered large reply did not finish")
            if errors:
                raise errors[0]
            if redis.process is None or redis.process.poll() is not None:
                raise RuntimeError("server exited during late-peer check")
        submitted = read_trace(trace_file)
    return latency_us, submitted


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--writev-trace", type=Path, required=True)
    args = parser.parse_args()
    server = args.server.resolve()
    trace_library = args.writev_trace.resolve()
    if not server.is_file():
        raise RuntimeError(f"redis-server not found at {server}")
    if not trace_library.is_file():
        raise RuntimeError(f"writev trace library not found at {trace_library}")
    latency_us, submitted = run(server, trace_library)
    print(json.dumps({"metric": "io_thread_late_peer_ping_us", "value": latency_us}, separators=(",", ":")))
    print(json.dumps({"metric": "io_thread_late_peer_max_writev_submitted_bytes", "value": submitted}, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
