#!/usr/bin/env python3
"""Measure the reply-output shared-lane workload with live Redis sockets."""

import argparse
import json
import math
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

VALUE_BYTES = 4 * 1024 * 1024
VALUE_BYTE = b"x"
PING_SAMPLES = 240
CONTENDED_BULK_REPLIES = 48


class ProtocolError(RuntimeError):
    pass


def resp_command(*parts):
    encoded = []
    for part in parts:
        if isinstance(part, str):
            part = part.encode("ascii")
        encoded.extend((b"$%d\r\n" % len(part), part, b"\r\n"))
    return b"*%d\r\n" % len(parts) + b"".join(encoded)


class RedisClient:
    def __init__(self, sock):
        self.sock = sock
        self.buffer = bytearray()

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def send(self, *parts):
        self.sock.sendall(resp_command(*parts))

    def _read_exact(self, wanted):
        while len(self.buffer) < wanted:
            chunk = self.sock.recv(max(65536, wanted - len(self.buffer)))
            if not chunk:
                raise ProtocolError("unexpected EOF from Redis")
            self.buffer.extend(chunk)
        result = bytes(self.buffer[:wanted])
        del self.buffer[:wanted]
        return result

    def _read_line(self):
        while True:
            end = self.buffer.find(b"\r\n")
            if end >= 0:
                result = bytes(self.buffer[:end])
                del self.buffer[: end + 2]
                return result
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ProtocolError("unexpected EOF while reading RESP line")
            self.buffer.extend(chunk)

    def expect_simple(self, expected):
        kind = self._read_exact(1)
        value = self._read_line()
        actual = kind + value
        if actual != expected:
            raise ProtocolError("expected %r, got %r" % (expected, actual))

    def read_bulk_header(self):
        if self._read_exact(1) != b"$":
            raise ProtocolError("expected bulk reply")
        try:
            size = int(self._read_line())
        except ValueError as exc:
            raise ProtocolError("invalid bulk length") from exc
        if size < 0:
            raise ProtocolError("unexpected null bulk reply")
        return size

    def read_bulk_body(self, remaining, expected_byte=VALUE_BYTE):
        while remaining:
            chunk = self._read_exact(min(65536, remaining))
            if expected_byte is not None and chunk.count(expected_byte) != len(chunk):
                raise ProtocolError("bulk response content changed")
            remaining -= len(chunk)
        if self._read_exact(2) != b"\r\n":
            raise ProtocolError("bulk reply lacks CRLF")

    def read_bulk(self, expected_length, expected_byte=VALUE_BYTE):
        actual_length = self.read_bulk_header()
        if actual_length != expected_length:
            raise ProtocolError("expected bulk length %d, got %d" % (expected_length, actual_length))
        self.read_bulk_body(actual_length, expected_byte)


def reserve_port():
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    return port


class RedisServer:
    def __init__(self, io_threads):
        self.io_threads = io_threads
        self.port = reserve_port()
        self.workdir = Path(tempfile.mkdtemp(prefix=".perfloop-reply-iov-", dir=os.getcwd()))
        self.log_path = self.workdir / "redis.log"
        self.proc = None
        self.clients = []

    def start(self):
        command = [
            "./src/redis-server",
            "--port",
            str(self.port),
            "--bind",
            "127.0.0.1",
            "--save",
            "",
            "--appendonly",
            "no",
            "--protected-mode",
            "no",
            "--enable-debug-command",
            "yes",
            "--io-threads",
            str(self.io_threads),
        ]
        log = self.log_path.open("wb")
        self.proc = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        log.close()

        deadline = time.monotonic() + 10
        last_error = None
        while time.monotonic() < deadline:
            try:
                client = self.connect(timeout=0.2)
                client.send("PING")
                client.expect_simple(b"+PONG")
                return client
            except (OSError, ProtocolError) as exc:
                last_error = exc
                time.sleep(0.02)
        raise RuntimeError("Redis did not become ready: %s" % last_error)

    def connect(self, timeout=10):
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=timeout)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(timeout)
        client = RedisClient(sock)
        self.clients.append(client)
        return client

    def prepare_large_values(self, client):
        client.send("DEBUG", "REPLY-COPY-AVOIDANCE", "1")
        client.expect_simple(b"+OK")
        client.send("SET", "reply-iov-large", VALUE_BYTE * VALUE_BYTES)
        client.expect_simple(b"+OK")
        client.send("SET", "reply-iov-empty", b"")
        client.expect_simple(b"+OK")

    def close(self):
        for client in self.clients:
            client.close()
        self.clients = []
        if self.proc is not None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
            if self.proc.returncode not in (0, -15):
                try:
                    log = self.log_path.read_text(encoding="utf-8", errors="replace")
                except OSError:
                    log = ""
                raise RuntimeError("Redis exited with %d:\n%s" % (self.proc.returncode, log))
        shutil.rmtree(self.workdir, ignore_errors=True)


def with_server(io_threads, body):
    server = RedisServer(io_threads)
    try:
        first_client = server.start()
        return body(server, first_client)
    finally:
        server.close()


def percentile_99(values):
    if not values:
        raise RuntimeError("no ping samples")
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * 0.99) - 1]


def measure_contended_ping_p99():
    def run(server, bulk):
        server.prepare_large_values(bulk)
        pinger = server.connect()
        pinger.send("PING")
        pinger.expect_simple(b"+PONG")

        first_bulk_reply = threading.Event()
        drain_bulk_replies = threading.Event()
        worker_error = []

        def bulk_worker():
            try:
                for _ in range(CONTENDED_BULK_REPLIES):
                    bulk.send("GET", "reply-iov-large")
                first_size = bulk.read_bulk_header()
                if first_size != VALUE_BYTES:
                    raise ProtocolError("unexpected first bulk size %d" % first_size)
                first_byte = bulk._read_exact(1)
                if first_byte != VALUE_BYTE:
                    raise ProtocolError("unexpected first bulk byte")
                first_bulk_reply.set()
                if not drain_bulk_replies.wait(timeout=10):
                    raise RuntimeError("pinger did not start")
                bulk.read_bulk_body(first_size - 1)
                for _ in range(CONTENDED_BULK_REPLIES - 1):
                    bulk.read_bulk(VALUE_BYTES)
            except BaseException as exc:
                worker_error.append(exc)
                first_bulk_reply.set()

        worker = threading.Thread(target=bulk_worker, daemon=True)
        worker.start()
        if not first_bulk_reply.wait(timeout=10):
            raise RuntimeError("copy-avoided bulk stream did not begin")
        if worker_error:
            raise worker_error[0]

        drain_bulk_replies.set()
        latencies_us = []
        for _ in range(PING_SAMPLES):
            before = time.perf_counter_ns()
            pinger.send("PING")
            pinger.expect_simple(b"+PONG")
            latencies_us.append((time.perf_counter_ns() - before) / 1000.0)

        worker.join(timeout=20)
        if worker.is_alive():
            raise RuntimeError("bulk worker did not finish")
        if worker_error:
            raise worker_error[0]
        return percentile_99(latencies_us)

    return with_server(1, run)


def check_reply_order(io_threads):
    def run(server, client):
        server.prepare_large_values(client)
        client.send("GET", "reply-iov-large")
        client.send("GET", "reply-iov-empty")
        client.send("PING")
        client.read_bulk(VALUE_BYTES)
        client.read_bulk(0, expected_byte=None)
        client.expect_simple(b"+PONG")

    with_server(io_threads, run)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, choices=("contended", "check"))
    args = parser.parse_args()

    if args.mode == "contended":
        print(
            json.dumps(
                {"metric": "contended_ping_p99_us", "value": measure_contended_ping_p99()},
                separators=(",", ":"),
            ),
            flush=True,
        )
    else:
        check_reply_order(1)
        check_reply_order(2)
        print("REPLY_IOV_ORDER_CHECK_OK io_threads=1,2", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ProtocolError, RuntimeError, subprocess.SubprocessError) as exc:
        print("reply_iov_hol: %s" % exc, file=sys.stderr)
        raise SystemExit(1)
