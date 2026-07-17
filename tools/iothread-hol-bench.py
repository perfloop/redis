#!/usr/bin/env python3
"""Measure short-request tail latency while an IO thread drains pipeline bursts.

The workload deliberately uses one Redis IO worker (``io-threads 2``), sixteen
bulk clients (the handoff threshold), and one non-pipelined short client.  The
traffic generators are Redis's checked-in ``redis-benchmark`` binary; this
wrapper only coordinates the mixed workload and normalizes its CSV output.
"""

import argparse
import csv
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


BULK_CLIENTS = 16
BULK_PIPELINE = 256
BULK_REQUESTS = 100_000_000
SHORT_REQUESTS = 20_000
WARMUP_REQUESTS = 10_000
MIN_ACTIVE_BULK_COMMANDS = 20_000


class BenchError(RuntimeError):
    """A setup or workload failure that makes a sample unusable."""


@dataclass(frozen=True)
class CpuLayout:
    server: tuple[int, ...]
    bulk: tuple[int, ...]
    short: tuple[int, ...]


@dataclass(frozen=True)
class LatencyResult:
    rps: float
    p50_us: float
    p99_us: float


def resp_command(parts: Iterable[str]) -> bytes:
    encoded = []
    values = list(parts)
    encoded.append(f"*{len(values)}\r\n".encode())
    for value in values:
        data = value.encode()
        encoded.append(f"${len(data)}\r\n".encode())
        encoded.append(data)
        encoded.append(b"\r\n")
    return b"".join(encoded)


class RedisConnection:
    def __init__(self, host: str, port: int, timeout: float = 10.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self._buffer = bytearray()

    def close(self) -> None:
        self.sock.close()

    def send(self, *parts: str) -> None:
        self.sock.sendall(resp_command(parts))

    def send_raw(self, payload: bytes) -> None:
        self.sock.sendall(payload)

    def _read_exact(self, count: int) -> bytes:
        while len(self._buffer) < count:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise BenchError("Redis closed a connection while a reply was pending")
            self._buffer.extend(chunk)
        result = bytes(self._buffer[:count])
        del self._buffer[:count]
        return result

    def _read_line(self) -> bytes:
        while True:
            newline = self._buffer.find(b"\r\n")
            if newline >= 0:
                result = bytes(self._buffer[:newline])
                del self._buffer[:newline + 2]
                return result
            chunk = self.sock.recv(65536)
            if not chunk:
                raise BenchError("Redis closed a connection while a line reply was pending")
            self._buffer.extend(chunk)

    def read(self) -> str:
        prefix = self._read_exact(1)
        if prefix == b"+":
            return self._read_line().decode()
        if prefix == b"-":
            raise BenchError(f"Redis returned an error: {self._read_line().decode()}")
        if prefix == b"$":
            length = int(self._read_line())
            if length < 0:
                return ""
            value = self._read_exact(length)
            if self._read_exact(2) != b"\r\n":
                raise BenchError("malformed bulk reply terminator")
            return value.decode()
        if prefix == b":":
            return self._read_line().decode()
        raise BenchError(f"unexpected RESP reply prefix {prefix!r}")

    def command(self, *parts: str) -> str:
        self.send(*parts)
        return self.read()


def cpu_layout() -> CpuLayout:
    if not hasattr(os, "sched_getaffinity"):
        raise BenchError("this benchmark requires Linux CPU affinity support")
    if shutil.which("taskset") is None:
        raise BenchError("this benchmark requires taskset")
    cpus = sorted(os.sched_getaffinity(0))
    if len(cpus) < 5:
        raise BenchError("this benchmark requires five available CPUs")
    return CpuLayout(server=tuple(cpus[:2]), bulk=tuple(cpus[2:4]), short=(cpus[4],))


def taskset_command(cpus: tuple[int, ...], command: list[str]) -> list[str]:
    return ["taskset", "-c", ",".join(str(cpu) for cpu in cpus), *command]


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


class RedisServer:
    def __init__(self, server_binary: Path, layout: CpuLayout):
        self.server_binary = server_binary
        self.layout = layout
        self.directory: Optional[tempfile.TemporaryDirectory[str]] = None
        self.proc: Optional[subprocess.Popen[bytes]] = None
        self.port: Optional[int] = None
        self.logfile: Optional[Path] = None

    def start(self) -> None:
        for _ in range(5):
            self.directory = tempfile.TemporaryDirectory(prefix="iothread-hol-")
            directory = Path(self.directory.name)
            self.port = find_free_port()
            self.logfile = directory / "redis.log"
            command = [
                str(self.server_binary),
                "--bind", "127.0.0.1",
                "--port", str(self.port),
                "--save", "",
                "--appendonly", "no",
                "--io-threads", "2",
                "--dir", str(directory),
                "--logfile", str(self.logfile),
                "--loglevel", "warning",
            ]
            self.proc = subprocess.Popen(
                taskset_command(self.layout.server, command),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                self.wait_ready()
                return
            except BenchError:
                self.stop()
        raise BenchError("Redis could not start on a free loopback port")

    def wait_ready(self) -> None:
        assert self.proc is not None
        assert self.port is not None
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                log = self.logfile.read_text(errors="replace") if self.logfile else ""
                raise BenchError(f"Redis exited during startup:\n{log}")
            try:
                connection = RedisConnection("127.0.0.1", self.port, timeout=0.2)
                try:
                    if connection.command("PING") == "PONG":
                        return
                finally:
                    connection.close()
            except (BenchError, OSError, socket.timeout):
                time.sleep(0.05)
        raise BenchError("Redis did not become ready within ten seconds")

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        self.proc = None
        if self.directory is not None:
            self.directory.cleanup()
            self.directory = None

    def total_commands_processed(self) -> int:
        assert self.port is not None
        connection = RedisConnection("127.0.0.1", self.port)
        try:
            stats = connection.command("INFO", "stats")
        finally:
            connection.close()
        match = re.search(r"^total_commands_processed:(\d+)\r?$", stats, re.MULTILINE)
        if match is None:
            raise BenchError("INFO stats did not contain total_commands_processed")
        return int(match.group(1))


class IothreadHolBenchmark:
    def __init__(self, source_dir: Path):
        self.source_dir = source_dir
        self.server = source_dir / "redis-server"
        self.benchmark = source_dir / "redis-benchmark"
        for binary in (self.server, self.benchmark):
            if not binary.is_file() or not os.access(binary, os.X_OK):
                raise BenchError(f"missing executable: {binary}")
        self.layout = cpu_layout()
        self.redis = RedisServer(self.server, self.layout)
        self.bulk_proc: Optional[subprocess.Popen[bytes]] = None

    def close(self) -> None:
        self.stop_bulk()
        self.redis.stop()

    def run_checked(self, command: list[str], timeout: float = 90.0) -> str:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
        if result.returncode != 0:
            stdout = result.stdout.decode(errors="replace")
            stderr = result.stderr.decode(errors="replace")
            raise BenchError(
                f"redis-benchmark failed ({result.returncode}):\nstdout:\n{stdout}\nstderr:\n{stderr}"
            )
        return result.stdout.decode(errors="replace")

    def benchmark_command(self, clients: int, pipeline: int, requests: int, threads: int,
                          csv_output: bool = False) -> list[str]:
        assert self.redis.port is not None
        command = [
            str(self.benchmark),
            "-h", "127.0.0.1",
            "-p", str(self.redis.port),
            "-n", str(requests),
            "-c", str(clients),
            "-P", str(pipeline),
            "--threads", str(threads),
        ]
        if csv_output:
            command.append("--csv")
        command.append("PING")
        return command

    @staticmethod
    def parse_latency(output: str) -> LatencyResult:
        rows = list(csv.DictReader(line for line in output.splitlines() if line.strip()))
        if len(rows) != 1 or rows[0].get("test", "").lower() != "ping":
            raise BenchError(f"unexpected redis-benchmark CSV output:\n{output}")
        row = rows[0]
        try:
            rps = float(row["rps"])
            p50_us = float(row["p50_latency_ms"]) * 1000.0
            p99_us = float(row["p99_latency_ms"]) * 1000.0
        except (KeyError, ValueError) as exc:
            raise BenchError(f"invalid redis-benchmark CSV row: {row}") from exc
        if rps <= 0.0 or p50_us <= 0.0 or p99_us <= 0.0:
            raise BenchError(f"non-positive latency sample: {row}")
        return LatencyResult(rps=rps, p50_us=p50_us, p99_us=p99_us)

    def warmup(self) -> None:
        self.run_checked(
            taskset_command(
                self.layout.bulk,
                self.benchmark_command(BULK_CLIENTS, 64, WARMUP_REQUESTS, 2),
            )
        )

    def short_run(self) -> LatencyResult:
        output = self.run_checked(
            taskset_command(
                self.layout.short,
                self.benchmark_command(1, 1, SHORT_REQUESTS, 1, csv_output=True),
            )
        )
        return self.parse_latency(output)

    def start_bulk(self) -> None:
        command = taskset_command(
            self.layout.bulk,
            self.benchmark_command(BULK_CLIENTS, BULK_PIPELINE, BULK_REQUESTS, 2),
        )
        self.bulk_proc = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

    def stop_bulk(self) -> None:
        if self.bulk_proc is None:
            return
        if self.bulk_proc.poll() is None:
            self.bulk_proc.send_signal(signal.SIGTERM)
            try:
                self.bulk_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.bulk_proc.kill()
                self.bulk_proc.wait(timeout=5)
        self.bulk_proc = None

    def wait_for_bulk_traffic(self) -> None:
        start = self.redis.total_commands_processed()
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if self.bulk_proc is None or self.bulk_proc.poll() is not None:
                raise BenchError("bulk pipeline benchmark exited before the short workload began")
            time.sleep(0.05)
            if self.redis.total_commands_processed() - start >= MIN_ACTIVE_BULK_COMMANDS:
                return
        raise BenchError("bulk pipeline traffic did not reach the required active rate")

    def measure(self) -> dict[str, float]:
        try:
            self.redis.start()
            self.warmup()
            idle = self.short_run()
            self.start_bulk()
            self.wait_for_bulk_traffic()
            before = self.redis.total_commands_processed()
            started = time.monotonic()
            mixed = self.short_run()
            elapsed = time.monotonic() - started
            after = self.redis.total_commands_processed()
            if elapsed <= 0.0 or after <= before:
                raise BenchError("mixed workload did not advance server command counters")
            return {
                "short_idle_p99_latency_us": idle.p99_us,
                "short_request_p50_latency_us": mixed.p50_us,
                "short_request_p99_latency_us": mixed.p99_us,
                "mixed_total_ops_per_sec": (after - before) / elapsed,
            }
        finally:
            self.close()

    def verify(self) -> None:
        bulk_connections: list[RedisConnection] = []
        short_connection: Optional[RedisConnection] = None
        try:
            self.redis.start()
            assert self.redis.port is not None
            for _ in range(BULK_CLIENTS):
                connection = RedisConnection("127.0.0.1", self.redis.port)
                if connection.command("PING") != "PONG":
                    raise BenchError("bulk preflight PING did not return PONG")
                bulk_connections.append(connection)
            short_connection = RedisConnection("127.0.0.1", self.redis.port)
            if short_connection.command("PING") != "PONG":
                raise BenchError("short-client preflight PING did not return PONG")

            commands_per_client = 128
            for client_id, connection in enumerate(bulk_connections):
                payload = b"".join(
                    resp_command(("ECHO", f"bulk:{client_id}:{sequence}"))
                    for sequence in range(commands_per_client)
                )
                connection.send_raw(payload)

            for _ in range(32):
                if short_connection.command("PING") != "PONG":
                    raise BenchError("short client lost request/reply correctness")

            for client_id, connection in enumerate(bulk_connections):
                for sequence in range(commands_per_client):
                    expected = f"bulk:{client_id}:{sequence}"
                    actual = connection.read()
                    if actual != expected:
                        raise BenchError(
                            f"bulk client {client_id} reply order changed: expected {expected!r}, got {actual!r}"
                        )
        finally:
            if short_connection is not None:
                short_connection.close()
            for connection in bulk_connections:
                connection.close()
            self.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--src-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "src",
        help="directory containing redis-server and redis-benchmark",
    )
    parser.add_argument(
        "--metric",
        choices=(
            "all",
            "short_idle_p99_latency_us",
            "short_request_p50_latency_us",
            "short_request_p99_latency_us",
            "mixed_total_ops_per_sec",
        ),
        default="all",
        help="print all proof metrics or one selected metric",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="exercise reply ordering and completion under the same IO-thread shape",
    )
    args = parser.parse_args()

    try:
        bench = IothreadHolBenchmark(args.src_dir)
        if args.verify:
            bench.verify()
            print("iothread-hol-verification: PASS")
            return 0
        metrics = bench.measure()
        if args.metric == "all":
            names = (
                "short_idle_p99_latency_us",
                "short_request_p50_latency_us",
                "short_request_p99_latency_us",
                "mixed_total_ops_per_sec",
            )
        else:
            names = (args.metric,)
        for name in names:
            print(json.dumps({"metric": name, "value": metrics[name]}, separators=(",", ":")))
        return 0
    except (BenchError, OSError, socket.timeout, subprocess.TimeoutExpired) as exc:
        print(f"iothread-hol-bench: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
