#!/usr/bin/env python3
"""Measure IO-thread handoff head-of-line behavior with an instrumented workload.

The mixed workload uses one Redis IO worker (``io-threads 2``), sixteen native
``redis-benchmark`` bulk clients with a pipeline of 256, and one non-pipelined
short ECHO stream.  The bulk clients run continuously while the short stream is
measured.  A separate finite native bulk probe overlaps the short stream to
report bulk-class p50/p99 latency.  A temporary Redis module command filter
timestamps each short command immediately before Redis executes it, groups the
tagged sustained workload commands by main event-loop turn, and records
submitted-but-unexecuted pipeline commands at turn boundaries.  The module is
measurement-only: it does not rewrite commands or alter Redis scheduling.

The short stream calibrates the module's monotonic-clock domain with minimum
round-trip samples before it sends its timestamped commands.  Its server-side
submission-to-execution metric is therefore distinct from client reply latency.
The bulk streams deliberately use the repository's redis-benchmark binary so
bulk class latency and throughput remain native benchmark measurements.
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
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional


BULK_CLIENTS = 16
BULK_PIPELINE = 256
# The sustained bulk generator has far more requests than one proof sample can
# consume. A finite native probe overlaps the short stream and yields the
# bulk-class latency histogram.
BULK_SUSTAINED_REQUESTS = BULK_CLIENTS * BULK_PIPELINE * 262_144
BULK_PROBE_CLIENTS = 1
BULK_PROBE_REQUESTS = BULK_PIPELINE * 4096
THROUGHPUT_REQUESTS = BULK_CLIENTS * BULK_PIPELINE * 1024
SHORT_REQUESTS = 5_000
WARMUP_REQUESTS = 10_000
MIN_ACTIVE_BULK_COMMANDS = 100_000
MIN_ACTIVE_BULK_PROBE_COMMANDS = 10_000
CLOCK_CALIBRATION_SAMPLES = 17

MIXED_METRICS = (
    "short_idle_p99_latency_us",
    "short_request_p50_latency_us",
    "short_request_p99_latency_us",
    "short_enqueue_to_execution_p50_us",
    "short_enqueue_to_execution_p99_us",
    "per_client_enqueue_to_execution_p50_us",
    "per_client_enqueue_to_execution_p99_us",
    "bulk_pipeline_p50_latency_us",
    "bulk_pipeline_p99_latency_us",
    "mixed_total_ops_per_sec",
    "main_callback_clients_per_invocation_max",
    "main_callback_commands_per_invocation_max",
    "residual_queue_depth_after_callback_max",
    "server_eventloop_commands_per_cycle_max",
    "instrumented_event_loop_cycles",
)


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
    values = list(parts)
    encoded = [f"*{len(values)}\r\n".encode()]
    for value in values:
        data = value.encode()
        encoded.extend((f"${len(data)}\r\n".encode(), data, b"\r\n"))
    return b"".join(encoded)


def percentile(values: list[float], percent: int) -> float:
    if not values:
        raise BenchError("cannot compute a percentile from an empty sample")
    ordered = sorted(values)
    index = (len(ordered) * percent + 99) // 100
    return ordered[max(index - 1, 0)]


class RedisConnection:
    def __init__(self, host: str, port: int, timeout: float = 10.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self._buffer = bytearray()

    def close(self) -> None:
        self.sock.close()

    def send(self, *parts: str) -> None:
        self.send_raw(resp_command(parts))

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

    def read(self) -> object:
        prefix = self._read_exact(1)
        if prefix == b"+":
            return self._read_line().decode()
        if prefix == b"-":
            raise BenchError(f"Redis returned an error: {self._read_line().decode()}")
        if prefix == b"$":
            length = int(self._read_line())
            if length < 0:
                return None
            value = self._read_exact(length)
            if self._read_exact(2) != b"\r\n":
                raise BenchError("malformed bulk reply terminator")
            return value.decode()
        if prefix == b":":
            return self._read_line().decode()
        if prefix == b"*":
            count = int(self._read_line())
            if count < 0:
                return None
            return [self.read() for _ in range(count)]
        raise BenchError(f"unexpected RESP reply prefix {prefix!r}")

    def command(self, *parts: str) -> object:
        self.send(*parts)
        return self.read()


def expect_text(reply: object, context: str) -> str:
    if not isinstance(reply, str):
        raise BenchError(f"{context} returned {reply!r}, expected a text reply")
    return reply


def cpu_layout() -> CpuLayout:
    if not hasattr(os, "sched_getaffinity") or not hasattr(os, "sched_setaffinity"):
        raise BenchError("this benchmark requires Linux CPU affinity support")
    if shutil.which("taskset") is None:
        raise BenchError("this benchmark requires taskset")
    cpus = sorted(os.sched_getaffinity(0))
    if len(cpus) < 5:
        raise BenchError("this benchmark requires five available CPUs")
    return CpuLayout(server=tuple(cpus[:2]), bulk=tuple(cpus[2:4]), short=(cpus[4],))


@contextmanager
def pinned_to(cpus: tuple[int, ...]) -> Iterator[None]:
    old_affinity = os.sched_getaffinity(0)
    os.sched_setaffinity(0, set(cpus))
    try:
        yield
    finally:
        os.sched_setaffinity(0, old_affinity)


def taskset_command(cpus: tuple[int, ...], command: list[str]) -> list[str]:
    return ["taskset", "-c", ",".join(str(cpu) for cpu in cpus), *command]


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def parse_info_value(info: str, name: str) -> int:
    match = re.search(rf"^{re.escape(name)}:(\d+)\r?$", info, re.MULTILINE)
    if match is None:
        raise BenchError(f"INFO output did not contain {name}")
    return int(match.group(1))


class RedisServer:
    def __init__(self, server_binary: Path, module_binary: Path, layout: CpuLayout):
        self.server_binary = server_binary
        self.module_binary = module_binary
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
                "--loadmodule", str(self.module_binary),
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


class IothreadHolBenchmark:
    def __init__(self, source_dir: Path, module_binary: Path):
        self.source_dir = source_dir.resolve()
        self.server = self.source_dir / "redis-server"
        self.benchmark = self.source_dir / "redis-benchmark"
        module_binary = module_binary.resolve()
        for binary in (self.server, self.benchmark, module_binary):
            if not binary.is_file() or not os.access(binary, os.X_OK):
                raise BenchError(f"missing executable: {binary}")
        self.layout = cpu_layout()
        self.redis = RedisServer(self.server, module_binary, self.layout)
        self.bulk_proc: Optional[subprocess.Popen[bytes]] = None
        self.bulk_probe_proc: Optional[subprocess.Popen[bytes]] = None

    @property
    def port(self) -> int:
        if self.redis.port is None:
            raise BenchError("Redis has not been started")
        return self.redis.port

    def close(self) -> None:
        self.stop_bulk_probe()
        self.stop_bulk()
        self.redis.stop()

    def connection(self, timeout: float = 10.0) -> RedisConnection:
        return RedisConnection("127.0.0.1", self.port, timeout=timeout)

    def run_checked(self, command: list[str], timeout: float = 120.0) -> str:
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
                          command_parts: tuple[str, ...], csv_output: bool = False) -> list[str]:
        command = [
            str(self.benchmark),
            "-h", "127.0.0.1",
            "-p", str(self.port),
            "-n", str(requests),
            "-c", str(clients),
            "-P", str(pipeline),
            "--threads", str(threads),
        ]
        if csv_output:
            command.append("--csv")
        return [*command, *command_parts]

    @staticmethod
    def parse_latency(output: str, expected_test: str) -> LatencyResult:
        rows = list(csv.DictReader(line for line in output.splitlines() if line.strip()))
        if len(rows) != 1 or rows[0].get("test", "").lower() != expected_test.lower():
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
                self.benchmark_command(BULK_CLIENTS, 64, WARMUP_REQUESTS, 2, ("PING",)),
            )
        )

    def total_commands_processed(self) -> int:
        connection = self.connection()
        try:
            info = expect_text(connection.command("INFO", "stats"), "INFO stats")
        finally:
            connection.close()
        return parse_info_value(info, "total_commands_processed")

    def server_eventloop_commands_max(self) -> int:
        connection = self.connection()
        try:
            info = expect_text(connection.command("INFO", "debug"), "INFO debug")
        finally:
            connection.close()
        return parse_info_value(info, "eventloop_cmd_per_cycle_max")

    def module_command(self, *parts: str) -> object:
        connection = self.connection()
        try:
            return connection.command(*parts)
        finally:
            connection.close()

    def module_stats(self) -> dict[str, int]:
        reply = self.module_command("HOL.STATS")
        if not isinstance(reply, list) or len(reply) % 2 != 0:
            raise BenchError(f"HOL.STATS returned malformed reply {reply!r}")
        stats: dict[str, int] = {}
        for index in range(0, len(reply), 2):
            name = reply[index]
            value = reply[index + 1]
            if not isinstance(name, str) or not isinstance(value, str):
                raise BenchError(f"HOL.STATS contains malformed metric pair {name!r}, {value!r}")
            try:
                stats[name] = int(value)
            except ValueError as exc:
                raise BenchError(f"HOL.STATS returned non-integer {name}={value!r}") from exc
        return stats

    def reset_module_measurement(self) -> None:
        if self.module_command("CONFIG", "RESETSTAT") != "OK":
            raise BenchError("CONFIG RESETSTAT did not return OK")
        if self.module_command("HOL.RESET", str(BULK_PIPELINE)) != "OK":
            raise BenchError("HOL.RESET did not return OK")
        if self.module_command("HOL.START") != "OK":
            raise BenchError("HOL.START did not return OK")

    def calibrate_server_clock_offset_us(self) -> int:
        samples: list[tuple[int, int]] = []
        connection = self.connection()
        try:
            for _ in range(CLOCK_CALIBRATION_SAMPLES):
                started_us = time.monotonic_ns() // 1000
                reply = expect_text(connection.command("HOL.CLOCK"), "HOL.CLOCK")
                finished_us = time.monotonic_ns() // 1000
                try:
                    server_us = int(reply)
                except ValueError as exc:
                    raise BenchError(f"HOL.CLOCK returned non-integer {reply!r}") from exc
                samples.append((finished_us - started_us, server_us - ((started_us + finished_us) // 2)))
        finally:
            connection.close()
        rtt_us, offset_us = min(samples, key=lambda sample: sample[0])
        if rtt_us > 10_000:
            raise BenchError(f"minimum HOL.CLOCK calibration RTT was too high: {rtt_us}us")
        return offset_us

    def short_run(self, request_count: int, marker_prefix: str,
                  clock_offset_us: Optional[int] = None) -> LatencyResult:
        latencies_us: list[float] = []
        connection = self.connection(timeout=30.0)
        try:
            for sequence in range(request_count):
                started_ns = time.monotonic_ns()
                if clock_offset_us is None:
                    token = f"{marker_prefix}:{sequence}"
                else:
                    estimated_server_send_us = started_ns // 1000 + clock_offset_us
                    token = f"{marker_prefix}:{estimated_server_send_us}:{sequence}"
                connection.send("ECHO", token)
                actual = expect_text(connection.read(), "short ECHO")
                finished_ns = time.monotonic_ns()
                if actual != token:
                    raise BenchError(
                        f"short request reply order changed: expected {token!r}, got {actual!r}"
                    )
                latencies_us.append((finished_ns - started_ns) / 1000.0)
        finally:
            connection.close()

        elapsed_us = sum(latencies_us)
        if elapsed_us <= 0.0:
            raise BenchError("short request run did not record positive elapsed time")
        return LatencyResult(
            rps=(request_count * 1_000_000.0) / elapsed_us,
            p50_us=percentile(latencies_us, 50),
            p99_us=percentile(latencies_us, 99),
        )

    def start_bulk(self) -> None:
        """Start a long native bulk stream; it is stopped after the probe."""
        command = taskset_command(
            self.layout.bulk,
            self.benchmark_command(
                BULK_CLIENTS,
                BULK_PIPELINE,
                BULK_SUSTAINED_REQUESTS,
                2,
                ("PING", "hol-bulk"),
            ),
        )
        self.bulk_proc = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

    def start_bulk_probe(self) -> None:
        """Run a finite native latency probe while the sustained stream is active."""
        command = taskset_command(
            self.layout.bulk,
            self.benchmark_command(
                BULK_PROBE_CLIENTS,
                BULK_PIPELINE,
                BULK_PROBE_REQUESTS,
                1,
                ("PING", "hol-bulk-probe"),
                csv_output=True,
            ),
        )
        self.bulk_probe_proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def finish_bulk_probe(self) -> LatencyResult:
        if self.bulk_probe_proc is None:
            raise BenchError("bulk latency probe was not started")
        try:
            stdout, stderr = self.bulk_probe_proc.communicate(timeout=120.0)
        except subprocess.TimeoutExpired as exc:
            self.bulk_probe_proc.kill()
            self.bulk_probe_proc.wait(timeout=5)
            raise BenchError("bulk latency probe did not finish within 120 seconds") from exc
        finally:
            process = self.bulk_probe_proc
            self.bulk_probe_proc = None
        if process.returncode != 0:
            raise BenchError(
                f"bulk latency probe failed ({process.returncode}):\n"
                f"stdout:\n{stdout.decode(errors='replace')}\n"
                f"stderr:\n{stderr.decode(errors='replace')}"
            )
        return self.parse_latency(stdout.decode(errors="replace"), "PING hol-bulk-probe")

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

    def stop_bulk_probe(self) -> None:
        if self.bulk_probe_proc is None:
            return
        if self.bulk_probe_proc.poll() is None:
            self.bulk_probe_proc.send_signal(signal.SIGTERM)
            try:
                self.bulk_probe_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.bulk_probe_proc.kill()
                self.bulk_probe_proc.wait(timeout=5)
        self.bulk_probe_proc = None

    def wait_for_bulk_traffic(self) -> None:
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            if self.bulk_proc is None or self.bulk_proc.poll() is not None:
                raise BenchError("sustained bulk pipeline benchmark exited before the short workload began")
            stats = self.module_stats()
            if stats.get("bulk_commands_executed", 0) >= MIN_ACTIVE_BULK_COMMANDS:
                return
            time.sleep(0.02)
        raise BenchError("sustained bulk pipeline traffic did not reach the required active command count")

    def wait_for_bulk_probe_traffic(self) -> None:
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            if self.bulk_probe_proc is None:
                raise BenchError("bulk latency probe was not started")
            stats = self.module_stats()
            if stats.get("bulk_latency_probe_commands", 0) >= MIN_ACTIVE_BULK_PROBE_COMMANDS:
                return
            if self.bulk_probe_proc.poll() is not None:
                raise BenchError("bulk latency probe exited before recording enough overlapped traffic")
            time.sleep(0.01)
        raise BenchError("bulk latency probe did not reach the required active command count")

    @staticmethod
    def require_stat(stats: dict[str, int], name: str, expected: int) -> None:
        actual = stats.get(name)
        if actual != expected:
            raise BenchError(f"HOL.STATS {name}={actual!r}, expected {expected}")

    @staticmethod
    def require_stat_at_least(stats: dict[str, int], name: str, minimum: int) -> None:
        actual = stats.get(name)
        if actual is None or actual < minimum:
            raise BenchError(f"HOL.STATS {name}={actual!r}, expected at least {minimum}")

    def validate_mixed_stats(self, stats: dict[str, int], short: LatencyResult) -> None:
        self.require_stat(stats, "bulk_client_count", BULK_CLIENTS)
        self.require_stat_at_least(stats, "bulk_commands_executed", MIN_ACTIVE_BULK_COMMANDS)
        self.require_stat(stats, "bulk_latency_probe_commands", BULK_PROBE_REQUESTS)
        self.require_stat(stats, "short_client_count", 1)
        self.require_stat(stats, "short_commands_executed", SHORT_REQUESTS)
        self.require_stat(stats, "short_enqueue_to_execution_samples", SHORT_REQUESTS)
        self.require_stat(stats, "bulk_pipeline_size", BULK_PIPELINE)
        if stats.get("delay_samples_dropped", 0) != 0:
            raise BenchError(f"HOL.STATS dropped delay samples: {stats['delay_samples_dropped']}")
        if stats.get("instrumented_event_loop_cycles", 0) <= 0:
            raise BenchError("HOL.STATS observed no event-loop cycles")
        if stats.get("main_callback_clients_per_invocation_max", 0) <= 0:
            raise BenchError("HOL.STATS observed no clients in a main callback")
        if stats.get("main_callback_commands_per_invocation_max", 0) <= 0:
            raise BenchError("HOL.STATS observed no commands in a main callback")
        queue_p99 = stats.get("short_enqueue_to_execution_p99_us", 0)
        if queue_p99 < 0 or queue_p99 > short.p99_us + 2_000.0:
            raise BenchError(
                "server-side enqueue-to-execution p99 is inconsistent with short end-to-end p99: "
                f"{queue_p99}us versus {short.p99_us:.1f}us"
            )

    def measure_mixed(self) -> dict[str, float]:
        try:
            self.redis.start()
            self.warmup()
            with pinned_to(self.layout.short):
                idle = self.short_run(SHORT_REQUESTS, "hol-idle")
            self.reset_module_measurement()
            clock_offset_us = self.calibrate_server_clock_offset_us()
            self.start_bulk()
            self.wait_for_bulk_traffic()
            self.start_bulk_probe()
            self.wait_for_bulk_probe_traffic()
            with pinned_to(self.layout.short):
                before = self.total_commands_processed()
                started = time.monotonic()
                mixed = self.short_run(SHORT_REQUESTS, "hol-short", clock_offset_us)
                elapsed = time.monotonic() - started
                after = self.total_commands_processed()
            if self.bulk_proc is None or self.bulk_proc.poll() is not None:
                raise BenchError("sustained bulk pipeline benchmark ended before the short workload completed")
            if elapsed <= 0.0 or after <= before:
                raise BenchError("mixed workload did not advance server command counters")
            bulk = self.finish_bulk_probe()
            if self.bulk_proc is None or self.bulk_proc.poll() is not None:
                raise BenchError("sustained bulk pipeline benchmark ended before the bulk latency probe completed")
            self.stop_bulk()
            stats = self.module_stats()
            self.validate_mixed_stats(stats, mixed)
            eventloop_commands = self.server_eventloop_commands_max()
            if eventloop_commands <= 0:
                raise BenchError("INFO debug recorded no commands in an event-loop cycle")
            return {
                "short_idle_p99_latency_us": idle.p99_us,
                "short_request_p50_latency_us": mixed.p50_us,
                "short_request_p99_latency_us": mixed.p99_us,
                "short_enqueue_to_execution_p50_us": float(stats["short_enqueue_to_execution_p50_us"]),
                "short_enqueue_to_execution_p99_us": float(stats["short_enqueue_to_execution_p99_us"]),
                "per_client_enqueue_to_execution_p50_us": float(
                    stats["per_client_enqueue_to_execution_p50_us"]
                ),
                "per_client_enqueue_to_execution_p99_us": float(
                    stats["per_client_enqueue_to_execution_p99_us"]
                ),
                "bulk_pipeline_p50_latency_us": bulk.p50_us,
                "bulk_pipeline_p99_latency_us": bulk.p99_us,
                "mixed_total_ops_per_sec": (after - before) / elapsed,
                "main_callback_clients_per_invocation_max": float(
                    stats["main_callback_clients_per_invocation_max"]
                ),
                "main_callback_commands_per_invocation_max": float(
                    stats["main_callback_commands_per_invocation_max"]
                ),
                "residual_queue_depth_after_callback_max": float(
                    stats["residual_queue_depth_after_callback_max"]
                ),
                "server_eventloop_commands_per_cycle_max": float(eventloop_commands),
                "instrumented_event_loop_cycles": float(stats["instrumented_event_loop_cycles"]),
            }
        finally:
            self.close()

    def measure_throughput(self) -> dict[str, float]:
        try:
            self.redis.start()
            self.warmup()
            output = self.run_checked(
                taskset_command(
                    self.layout.bulk,
                    self.benchmark_command(
                        BULK_CLIENTS,
                        BULK_PIPELINE,
                        THROUGHPUT_REQUESTS,
                        2,
                        ("PING",),
                        csv_output=True,
                    ),
                )
            )
            result = self.parse_latency(output, "PING")
            return {"bulk_pipeline_ops_per_sec": result.rps}
        finally:
            self.close()

    def verify(self) -> None:
        bulk_connections: list[RedisConnection] = []
        short_connection: Optional[RedisConnection] = None
        try:
            self.redis.start()
            self.reset_module_measurement()
            for _ in range(BULK_CLIENTS):
                bulk_connections.append(self.connection(timeout=30.0))
            short_connection = self.connection(timeout=30.0)

            # Tag a complete native-shaped pipeline for module accounting.
            tracked_payload = b"".join(
                resp_command(("PING", "hol-bulk")) for _ in range(BULK_PIPELINE)
            )
            for connection in bulk_connections:
                connection.send_raw(tracked_payload)
            for connection in bulk_connections:
                for _ in range(BULK_PIPELINE):
                    if expect_text(connection.read(), "tracked bulk PING") != "hol-bulk":
                        raise BenchError("tracked bulk PING returned an unexpected reply")

            # Separately check per-client reply ordering with distinct values.
            commands_per_client = 64
            for client_id, connection in enumerate(bulk_connections):
                payload = b"".join(
                    resp_command(("ECHO", f"bulk:{client_id}:{sequence}"))
                    for sequence in range(commands_per_client)
                )
                connection.send_raw(payload)
            for client_id, connection in enumerate(bulk_connections):
                for sequence in range(commands_per_client):
                    expected = f"bulk:{client_id}:{sequence}"
                    actual = expect_text(connection.read(), "bulk ECHO")
                    if actual != expected:
                        raise BenchError(
                            f"bulk client {client_id} reply order changed: "
                            f"expected {expected!r}, got {actual!r}"
                        )

            clock_offset_us = self.calibrate_server_clock_offset_us()
            assert short_connection is not None
            for sequence in range(32):
                estimated_server_send_us = time.monotonic_ns() // 1000 + clock_offset_us
                token = f"hol-short:{estimated_server_send_us}:{sequence}"
                short_connection.send("ECHO", token)
                actual = expect_text(short_connection.read(), "short ECHO")
                if actual != token:
                    raise BenchError(f"short client reply changed: expected {token!r}, got {actual!r}")

            stats = self.module_stats()
            self.require_stat(stats, "bulk_client_count", BULK_CLIENTS)
            self.require_stat(stats, "bulk_commands_executed", BULK_CLIENTS * BULK_PIPELINE)
            self.require_stat(stats, "short_client_count", 1)
            self.require_stat(stats, "short_commands_executed", 32)
            self.require_stat(stats, "short_enqueue_to_execution_samples", 32)
            if stats.get("delay_samples_dropped", 0) != 0:
                raise BenchError("instrumentation dropped short delay samples during verification")
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
        "--module",
        type=Path,
        default=Path(__file__).with_name("iothread-hol-module.so"),
        help="measurement-only Redis module compiled from iothread-hol-module.c",
    )
    parser.add_argument(
        "--mode",
        choices=("mixed", "throughput"),
        default="mixed",
        help="run the mixed proof selector or the standalone throughput guard",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="exercise reply ordering, completion, and module counters",
    )
    args = parser.parse_args()

    try:
        bench = IothreadHolBenchmark(args.src_dir, args.module)
        if args.verify:
            bench.verify()
            print("iothread-hol-verification: PASS")
            return 0
        metrics = bench.measure_mixed() if args.mode == "mixed" else bench.measure_throughput()
        names = MIXED_METRICS if args.mode == "mixed" else ("bulk_pipeline_ops_per_sec",)
        for name in names:
            print(json.dumps({"metric": name, "value": metrics[name]}, separators=(",", ":")))
        return 0
    except (BenchError, OSError, socket.timeout, subprocess.TimeoutExpired) as exc:
        print(f"iothread-hol-bench: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
