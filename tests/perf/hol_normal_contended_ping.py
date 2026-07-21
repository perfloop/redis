#!/usr/bin/env python3
"""Guard peer PING tail latency during copied reply-list bulk output.

This is distinct from the copy-avoided HOL selector: reply-copy avoidance stays
disabled, so the large GET travels through ordinary PLAIN_REPLY/reply-list
output while a second normal client issues sequential PINGs.  The fixture
validates its runtime-generated value before timing and consumes every bulk
reply before reporting one JSONL sample for each metric.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import time

from hol_copy_avoidance import BULK_BYTES, BULK_KEY, BulkLoad, RedisConnection, RedisServer, p99


def setup_normal_value(port: int, payload: bytes) -> None:
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


def run(server: Path, duration: float) -> tuple[float, float]:
    payload = os.urandom(BULK_BYTES)
    with tempfile.TemporaryDirectory(prefix=".perfloop-normal-peer-", dir=".") as temp:
        directory = Path(temp)
        with RedisServer(server, directory, None, directory / "unused.trace") as redis:
            setup_normal_value(redis.port, payload)
            bulk = BulkLoad(redis.port, len(payload))
            peer = RedisConnection.connect("127.0.0.1", redis.port)
            try:
                bulk.start()
                bulk.wait_for(1)
                completed_before = bulk.count()
                latencies = []
                started = time.monotonic()
                while time.monotonic() - started < duration:
                    ping_started = time.perf_counter_ns()
                    peer.send("PING")
                    peer.read_simple(b"+PONG")
                    latencies.append(time.perf_counter_ns() - ping_started)
                    if bulk.error is not None:
                        raise RuntimeError("normal bulk GET client failed") from bulk.error
                elapsed = time.monotonic() - started
                completed = bulk.count() - completed_before
            finally:
                peer.close()
                bulk.stop()
    if completed <= 0:
        raise RuntimeError("normal peer run completed no full bulk replies")
    return p99(latencies), completed * BULK_BYTES / elapsed / (1024 * 1024)


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
    peer_p99_us, bulk_mib_per_s = run(server, args.duration)
    print(json.dumps({"metric": "normal_contended_peer_ping_p99_us", "value": peer_p99_us}, separators=(",", ":")))
    print(json.dumps({"metric": "normal_contended_bulk_get_mib_per_s", "value": bulk_mib_per_s}, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
