#!/usr/bin/env python3
"""Measure a normal copied bulk reply that must use the reply-list writev path.

This is proof-only coverage for the generic ReplyIOV quantum clamp.  It keeps
reply-copy avoidance disabled, stores a runtime-generated 4 MiB value, consumes
every GET reply, and emits one end-to-end transfer-rate sample.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import time

from hol_copy_avoidance import BULK_BYTES, BULK_KEY, BulkLoad, RedisConnection, RedisServer


def setup_normal_value(port: int, payload: bytes) -> None:
    """Store and verify a raw value while forcing the copied reply representation."""
    connection = RedisConnection.connect("127.0.0.1", port)
    try:
        connection.send("DEBUG", "REPLY-COPY-AVOIDANCE", "0")
        connection.read_simple(b"+OK")
        connection.send(b"SET", BULK_KEY, payload)
        connection.read_simple(b"+OK")
        connection.send(b"GET", BULK_KEY)
        connection.read_bulk(len(payload), payload)
    finally:
        connection.close()


def run_throughput(server: Path, duration: float) -> float:
    payload = os.urandom(BULK_BYTES)
    with tempfile.TemporaryDirectory(prefix=".perfloop-hol-normal-", dir=".") as temp:
        directory = Path(temp)
        with RedisServer(server, directory, None, directory / "unused.trace") as redis:
            setup_normal_value(redis.port, payload)
            bulk = BulkLoad(redis.port, len(payload))
            try:
                bulk.start()
                bulk.wait_for(1)
                completed_before = bulk.count()
                started = time.monotonic()
                while time.monotonic() - started < duration:
                    if bulk.error is not None:
                        raise RuntimeError("normal bulk GET client failed") from bulk.error
                    time.sleep(0.002)
                elapsed = time.monotonic() - started
                completed = bulk.count() - completed_before
            finally:
                bulk.stop()
    if completed <= 0:
        raise RuntimeError("normal bulk throughput run completed no full replies")
    return completed * BULK_BYTES / elapsed / (1024 * 1024)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--duration", type=float, default=8.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    server = args.server.resolve()
    if not server.is_file():
        raise RuntimeError(f"redis-server not found at {server}")
    if args.duration <= 0:
        raise RuntimeError("duration must be positive")
    value = run_throughput(server, args.duration)
    print(json.dumps({"metric": "normal_bulk_get_mib_per_s", "value": value}, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
