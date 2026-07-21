#!/usr/bin/env python3
"""Trace a fast late peer against a copy-avoided bulk reply.

The bulk reader has a large receive window and never intentionally sleeps. It
connects first, waits for a real payload byte, then a separate client connects
and sends PING. A test-only write/writev interposer labels output by the two
client source ports. This lets the fixture distinguish a peer PONG submitted
before the final bulk vector from a PONG observed only after all bulk vectors
were already queued to the kernel.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time

from hol_copy_avoidance import (BULK_BYTES, BULK_KEY, ProtocolError, RedisConnection,
                                 choose_server_cpu, setup_copy_avoided_value, wait_for_server)


FAST_RECEIVE_BUFFER = 8 * 1024 * 1024


def reserve_bound_socket() -> tuple[socket.socket, int]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.bind(("127.0.0.1", 0))
    return sock, int(sock.getsockname()[1])


def reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def start_server(server: Path, directory: Path, trace_library: Path, trace_file: Path,
                 bulk_port: int, peer_port: int, server_cpu: int | None) -> tuple[subprocess.Popen[bytes], int]:
    port = reserve_port()
    environment = os.environ.copy()
    prior_preload = environment.get("LD_PRELOAD")
    environment["LD_PRELOAD"] = f"{trace_library}:{prior_preload}" if prior_preload else str(trace_library)
    environment["PERFLOOP_FAST_LATE_TRACE"] = str(trace_file)
    environment["PERFLOOP_FAST_LATE_BULK_PORT"] = str(bulk_port)
    environment["PERFLOOP_FAST_LATE_PEER_PORT"] = str(peer_port)
    def pin_server() -> None:
        if server_cpu is not None:
            os.sched_setaffinity(0, {server_cpu})

    process = subprocess.Popen(
        [
            str(server), "--bind", "127.0.0.1", "--port", str(port),
            "--save", "", "--appendonly", "no", "--protected-mode", "no",
            "--enable-debug-command", "yes", "--dir", str(directory), "--logfile", "",
        ],
        cwd=directory,
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        preexec_fn=pin_server if server_cpu is not None else None,
    )
    wait_for_server(process, port)
    return process, port


def read_trace(trace_file: Path) -> tuple[bool, int]:
    try:
        rows = [line.split() for line in trace_file.read_text().splitlines()]
        events = [(int(order), kind, int(submitted), int(written)) for order, kind, submitted, written in rows]
    except (FileNotFoundError, ValueError) as error:
        raise RuntimeError("fast late-peer trace did not contain usable write events") from error
    events.sort()
    bulk = [event for event in events if event[1] == "B" and event[3] > 0]
    peer = [event for event in events if event[1] == "P" and event[3] > 0]
    if not bulk:
        raise RuntimeError("fast late-peer trace did not observe a bulk write")
    if not peer:
        raise RuntimeError("fast late-peer trace did not observe the peer PONG write")
    peer_order = peer[0][0]
    before_final_bulk_submission = peer_order < bulk[-1][0]
    return before_final_bulk_submission, max(event[2] for event in bulk)


def run(server: Path, trace_library: Path) -> tuple[bool, float, int]:
    server_cpu = choose_server_cpu()
    payload = os.urandom(BULK_BYTES)
    bulk_socket, bulk_port = reserve_bound_socket()
    peer_socket, peer_port = reserve_bound_socket()
    peer_sent = threading.Event()
    finished = threading.Event()
    errors: list[BaseException] = []
    peer_started_ns: list[int] = []

    with tempfile.TemporaryDirectory(prefix=".perfloop-fast-late-peer-", dir=".") as temp:
        directory = Path(temp)
        trace_file = directory / "fast-late.trace"
        process: subprocess.Popen[bytes] | None = None
        try:
            process, server_port = start_server(server, directory, trace_library, trace_file, bulk_port, peer_port, server_cpu)
            setup_copy_avoided_value(server_port, payload)

            def drain_bulk() -> None:
                connection: RedisConnection | None = None
                try:
                    bulk_socket.settimeout(10)
                    bulk_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, FAST_RECEIVE_BUFFER)
                    bulk_socket.connect(("127.0.0.1", server_port))
                    connection = RedisConnection(bulk_socket)
                    connection.send("GET", BULK_KEY)
                    header = connection.read_line()
                    expected_header = b"$" + str(len(payload)).encode()
                    if header != expected_header:
                        raise ProtocolError(f"unexpected large bulk header: {header!r}")
                    if connection.read_exact(1) != payload[:1]:
                        raise ProtocolError("first bulk payload byte did not match")
                    peer_socket.settimeout(10)
                    peer_socket.connect(("127.0.0.1", server_port))
                    peer = RedisConnection(peer_socket)
                    peer_started_ns.append(time.perf_counter_ns())
                    peer.send("PING")
                    peer_sent.set()
                    offset = 1
                    while offset < len(payload):
                        take = min(256 * 1024, len(payload) - offset)
                        data = connection.read_exact(take)
                        if data != payload[offset:offset + take]:
                            raise ProtocolError("fast bulk payload did not match")
                        offset += take
                    if connection.read_exact(2) != b"\r\n":
                        raise ProtocolError("large bulk terminator was missing")
                except BaseException as error:
                    errors.append(error)
                    peer_sent.set()
                finally:
                    finished.set()
                    if connection is not None:
                        connection.close()
                    else:
                        bulk_socket.close()

            reader = threading.Thread(target=drain_bulk, name="fast-late-bulk-reader", daemon=True)
            reader.start()
            if not peer_sent.wait(10):
                raise RuntimeError("fast bulk reply did not submit the late peer PING")
            if errors:
                raise errors[0]
            if not peer_started_ns:
                raise RuntimeError("fast bulk reply did not start late-peer timing")

            peer = RedisConnection(peer_socket)
            try:
                peer.read_simple(b"+PONG")
                ping_us = (time.perf_counter_ns() - peer_started_ns[0]) / 1000.0
            finally:
                peer.close()

            reader.join(20)
            if reader.is_alive():
                raise RuntimeError("fast bulk reader did not finish")
            if errors:
                raise errors[0]
            if process.poll() is not None:
                raise RuntimeError("server exited during the fast late-peer probe")
            before_final_bulk_submission, maximum_submission = read_trace(trace_file)
        finally:
            bulk_socket.close()
            peer_socket.close()
            if process is not None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
    return before_final_bulk_submission, ping_us, maximum_submission


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--fast-late-trace", type=Path, required=True)
    args = parser.parse_args()
    server = args.server.resolve()
    trace_library = args.fast_late_trace.resolve()
    if not server.is_file():
        raise RuntimeError(f"redis-server not found at {server}")
    if not trace_library.is_file():
        raise RuntimeError(f"fast late-peer trace library not found at {trace_library}")
    before_final_bulk_submission, ping_us, maximum_submission = run(server, trace_library)
    print(json.dumps({"metric": "main_thread_fast_late_peer_before_bulk_submission", "value": int(before_final_bulk_submission)}, separators=(",", ":")))
    print(json.dumps({"metric": "main_thread_fast_late_peer_ping_us", "value": ping_us}, separators=(",", ":")))
    print(json.dumps({"metric": "main_thread_fast_late_peer_max_writev_submitted_bytes", "value": maximum_submission}, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
