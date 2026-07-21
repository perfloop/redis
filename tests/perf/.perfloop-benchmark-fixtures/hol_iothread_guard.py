#!/usr/bin/env python3
"""Measure the copy-avoided reply-vector bound on one output I/O worker.

With ``io-threads=2`` Redis has exactly one non-main output worker, so the
large GET reader and the continuously active PING client share that worker.
The fixture consumes a pipelined large/empty reply sequence, requires a PING
to complete while that sequence drains, and emits the largest aggregate iovec
submission observed by a test-only writev interposer.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile

from hol_copy_avoidance import BULK_BYTES, BULK_KEY, RedisConnection
from hol_reply_iov import (
    EMPTY_KEY,
    PingLoad,
    ThreadedRedisServer,
    read_ordered_bulk_pair,
    set_value,
)


PING_LIVENESS_US = 100_000


def read_trace(trace_file: Path) -> tuple[int, int]:
    try:
        values = dict(line.split("=", 1) for line in trace_file.read_text().splitlines() if "=" in line)
        submitted = int(values["max_writev_submitted_bytes"])
        calls = int(values["writev_calls"])
    except (FileNotFoundError, KeyError, ValueError) as error:
        raise RuntimeError("I/O-thread writev probe did not emit a usable summary") from error
    if submitted <= 0 or calls <= 0:
        raise RuntimeError("I/O-thread writev probe recorded no successful writev call")
    return submitted, calls


def run(server: Path, trace_library: Path) -> tuple[int, float]:
    large = os.urandom(BULK_BYTES)
    with tempfile.TemporaryDirectory(prefix=".perfloop-iothread-guard-", dir=".") as temp:
        directory = Path(temp)
        trace_file = directory / "writev.trace"
        with ThreadedRedisServer(server, directory, trace_library, trace_file) as redis:
            set_value(redis.port, BULK_KEY, large, True)
            set_value(redis.port, EMPTY_KEY, b"", True)
            peer = PingLoad(redis.port)
            peer.start()
            peer.wait_for(1)
            completed_before, _ = peer.snapshot()
            connection = RedisConnection.connect("127.0.0.1", redis.port, receive_buffer=64 * 1024)
            try:
                read_ordered_bulk_pair(connection, large, b"")
                completed_after, maximum_ping_us = peer.snapshot()
                if completed_after <= completed_before:
                    raise RuntimeError("I/O-thread peer PING did not complete while the bulk reply drained")
                if maximum_ping_us > PING_LIVENESS_US:
                    raise RuntimeError("I/O-thread peer PING exceeded the 100 ms liveness bound")
            finally:
                connection.close()
                peer.stop()
        submitted, _ = read_trace(trace_file)
    return submitted, maximum_ping_us


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--writev-trace", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    server = args.server.resolve()
    trace_library = args.writev_trace.resolve()
    if not server.is_file():
        raise RuntimeError(f"redis-server not found at {server}")
    if not trace_library.is_file():
        raise RuntimeError(f"writev trace library not found at {trace_library}")
    submitted, maximum_ping_us = run(server, trace_library)
    print(json.dumps({"metric": "io_thread_max_writev_submitted_bytes", "value": submitted}, separators=(",", ":")))
    print(json.dumps({"metric": "io_thread_ping_max_us", "value": maximum_ping_us}, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
