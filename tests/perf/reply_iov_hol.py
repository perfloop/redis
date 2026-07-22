#!/usr/bin/env python3
"""Measure Redis's native benchmark clients on the reply-output shared lane."""

import argparse
import csv
import io
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

VALUE_BYTES = 4 * 1024 * 1024
VALUE_BYTE = b"x"
PING_REQUESTS = 50000
BULK_REQUESTS = 1000000


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
        actual = self._read_exact(1) + self._read_line()
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

        last_error = None
        while self.proc.poll() is None:
            try:
                client = self.connect()
                client.send("PING")
                client.expect_simple(b"+PONG")
                return client
            except (OSError, ProtocolError) as exc:
                last_error = exc
                time.sleep(0.02)
        raise RuntimeError("Redis exited before it became ready: %s" % last_error)

    def connect(self):
        sock = socket.create_connection(("127.0.0.1", self.port))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setblocking(True)
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


def native_contended_ping_p99():
    def run(server, setup_client):
        server.prepare_large_values(setup_client)
        setup_client.close()

        bulk_command = [
            "./src/redis-benchmark",
            "-p",
            str(server.port),
            "-n",
            str(BULK_REQUESTS),
            "-c",
            "1",
            "-P",
            "1",
            "-q",
            "GET",
            "reply-iov-large",
        ]
        ping_command = [
            "./src/redis-benchmark",
            "-p",
            str(server.port),
            "-n",
            str(PING_REQUESTS),
            "-c",
            "1",
            "-P",
            "1",
            "--csv",
            "PING",
        ]
        bulk = subprocess.Popen(bulk_command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        try:
            ping = subprocess.run(ping_command, capture_output=True, text=True, timeout=90, check=False)
        finally:
            bulk_was_running = bulk.poll() is None
            if bulk_was_running:
                bulk.terminate()
            try:
                _, bulk_stderr = bulk.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                bulk.kill()
                _, bulk_stderr = bulk.communicate(timeout=10)

        if ping.returncode != 0:
            raise RuntimeError("native PING benchmark failed: %s" % ping.stderr.strip())
        if not bulk_was_running:
            raise RuntimeError("native bulk GET stream ended before the PING sample completed")
        if bulk.returncode not in (-15, 0):
            raise RuntimeError("native bulk benchmark failed: %s" % bulk_stderr.strip())
        rows = list(csv.DictReader(io.StringIO(ping.stdout)))
        if len(rows) != 1 or rows[0].get("test") != "PING":
            raise RuntimeError("unexpected native PING benchmark output: %r" % ping.stdout)
        try:
            return float(rows[0]["p99_latency_ms"]) * 1000.0
        except (KeyError, ValueError) as exc:
            raise RuntimeError("native PING benchmark omitted p99: %r" % ping.stdout) from exc

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
                {"metric": "contended_ping_p99_us", "value": native_contended_ping_p99()},
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
