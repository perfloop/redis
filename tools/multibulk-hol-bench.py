#!/usr/bin/env python3
"""Exercise small-client latency while one client parses a buffered RESP request.

The attacker first sends a large bulk argument so Redis keeps a large private
query buffer for that client. It then sends a high-arity invalid PING request.
The invalid command deliberately isolates RESP argument parsing from command
execution; each request must still parse every argument before Redis returns
the expected arity error. Small clients send PING only after each attack write.
"""

import argparse
import json
import math
import os
import queue
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple


PRIME_BYTES = 4 * 1024 * 1024
ATTACK_ARGUMENTS = 50_000
ATTACK_REQUEST_POOL = 128
CPU_GUARD_BLOCK_ATTACKS = 64
WARMUP_ATTACKS = 64
BACKLOG_CLIENTS = 4
MAX_CONTINUATION_EVENTS_PER_CLIENT = 1024
PING_REQUEST = b"*1\r\n$4\r\nPING\r\n"


class BenchError(RuntimeError):
    pass


def encode_command(*parts: object) -> bytes:
    encoded = []
    for part in parts:
        if isinstance(part, bytes):
            encoded.append(part)
        else:
            encoded.append(str(part).encode("ascii"))
    out = [f"*{len(encoded)}\r\n".encode("ascii")]
    for part in encoded:
        out.extend((f"${len(part)}\r\n".encode("ascii"), part, b"\r\n"))
    return b"".join(out)


def build_attack_request(salt: int, arguments: int = ATTACK_ARGUMENTS) -> bytes:
    """Build a malformed PING with runtime-varied one-byte RESP arguments."""
    argument = bytes((ord("a") + (salt % 26),))
    bulk = b"$1\r\n" + argument + b"\r\n"
    return f"*{arguments + 1}\r\n$4\r\nPING\r\n".encode("ascii") + bulk * arguments


def build_attack_request_pool(
    rounds: int,
    arguments: int = ATTACK_ARGUMENTS,
    pool_size: int = ATTACK_REQUEST_POOL,
) -> List[bytes]:
    """Build before priming so clientsCron cannot shrink the primed query buffer.

    Repeating a bounded pool is safe here: every request is a consumed malformed
    PING, and Redis cannot cache its RESP decoding. The pool still contains
    runtime-varied payloads while keeping a long sample from retaining hundreds
    of megabytes and delaying the first attack past the query-buffer idle timer.
    """
    return [
        build_attack_request(int.from_bytes(os.urandom(1), "big") + index, arguments)
        for index in range(min(rounds, pool_size))
    ]


class RESPConnection:
    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.buffer = bytearray()

    @classmethod
    def connect(cls, port: int) -> "RESPConnection":
        sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # Let the client enqueue the complete attack before Redis reads it.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, PRIME_BYTES * 2)
        sock.settimeout(20)
        return cls(sock)

    def close(self) -> None:
        self.sock.close()

    def send(self, payload: bytes) -> None:
        self.sock.sendall(payload)

    def command(self, *parts: object) -> Tuple[str, bytes]:
        self.send(encode_command(*parts))
        return self.read_response()

    def _read_exact(self, length: int) -> bytes:
        while len(self.buffer) < length:
            chunk = self.sock.recv(max(64 * 1024, length - len(self.buffer)))
            if not chunk:
                raise BenchError("Redis closed the connection while reading a response")
            self.buffer.extend(chunk)
        result = bytes(self.buffer[:length])
        del self.buffer[:length]
        return result

    def _read_line(self) -> bytes:
        while True:
            end = self.buffer.find(b"\r\n")
            if end >= 0:
                result = bytes(self.buffer[:end])
                del self.buffer[:end + 2]
                return result
            chunk = self.sock.recv(64 * 1024)
            if not chunk:
                raise BenchError("Redis closed the connection while reading a line")
            self.buffer.extend(chunk)

    def read_response(self) -> Tuple[str, bytes]:
        kind = self._read_exact(1)
        if kind == b"+":
            return "simple", self._read_line()
        if kind == b"-":
            return "error", self._read_line()
        if kind == b"$":
            length = int(self._read_line())
            if length < 0:
                return "bulk", b""
            payload = self._read_exact(length)
            if self._read_exact(2) != b"\r\n":
                raise BenchError("malformed bulk response terminator")
            return "bulk", payload
        raise BenchError(f"unexpected RESP response type {kind!r}")


def require_response(response: Tuple[str, bytes], expected_kind: str, expected: bytes) -> None:
    if response != (expected_kind, expected):
        raise BenchError(f"unexpected Redis response {response!r}, expected {(expected_kind, expected)!r}")


def reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class RedisServer:
    def __init__(
        self,
        executable: Path,
        server_cpu: Optional[int],
        cpu_preload: Optional[Path],
    ):
        if not executable.is_file():
            raise BenchError(f"missing redis-server binary: {executable}")
        self.executable = executable
        self.server_cpu = server_cpu
        self.cpu_preload = cpu_preload
        self.port = reserve_port()
        self.tempdir: Optional[tempfile.TemporaryDirectory] = None
        self.process: Optional[subprocess.Popen] = None
        self.log_path: Optional[Path] = None
        self.parse_cpu_file: Optional[Path] = None

    def __enter__(self) -> "RedisServer":
        self.tempdir = tempfile.TemporaryDirectory(prefix="multibulk-hol-")
        self.log_path = Path(self.tempdir.name) / "redis.log"
        environment = None
        if self.cpu_preload is not None:
            if not self.cpu_preload.is_file():
                raise BenchError(f"missing CPU probe library: {self.cpu_preload}")
            self.parse_cpu_file = Path(self.tempdir.name) / "parse-cpu.bin"
            self.parse_cpu_file.write_bytes(b"\0" * 32)
            environment = os.environ.copy()
            previous_preload = environment.get("LD_PRELOAD")
            environment["LD_PRELOAD"] = (
                str(self.cpu_preload)
                if not previous_preload
                else f"{self.cpu_preload}:{previous_preload}"
            )
            environment["PERFLOOP_PARSE_CPU_FILE"] = str(self.parse_cpu_file)
        self.process = subprocess.Popen(
            [
                str(self.executable),
                "--bind", "127.0.0.1",
                "--port", str(self.port),
                "--save", "",
                "--appendonly", "no",
                "--io-threads", "1",
                "--dir", self.tempdir.name,
                "--logfile", str(self.log_path),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=environment,
        )
        if self.server_cpu is not None:
            try:
                os.sched_setaffinity(self.process.pid, {self.server_cpu})
            except OSError as exc:
                self.process.kill()
                self.process.wait(timeout=5)
                raise BenchError(f"could not isolate Redis on CPU {self.server_cpu}") from exc
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise BenchError(self._startup_error())
            try:
                probe = RESPConnection.connect(self.port)
                try:
                    require_response(probe.command("PING"), "simple", b"PONG")
                    return self
                finally:
                    probe.close()
            except OSError:
                time.sleep(0.01)
        raise BenchError(self._startup_error())

    def _startup_error(self) -> str:
        log = ""
        if self.log_path is not None and self.log_path.exists():
            log = self.log_path.read_text(encoding="utf-8", errors="replace")
        return f"redis-server did not start on port {self.port}: {log.strip()}"

    def reset_parse_cpu_stats(self) -> None:
        if self.parse_cpu_file is None:
            raise BenchError("CPU probe was not enabled")
        self.parse_cpu_file.write_bytes(b"\0" * 32)

    def parse_cpu_stats(self) -> Tuple[int, int, int]:
        if self.parse_cpu_file is None:
            raise BenchError("CPU probe was not enabled")
        data = self.parse_cpu_file.read_bytes()
        if len(data) != 32:
            raise BenchError("CPU probe returned an invalid stats file")
        cpu_ns, completed, target_reads, _ = struct.unpack("<QQQQ", data)
        return cpu_ns, completed, target_reads

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.tempdir is not None:
            self.tempdir.cleanup()


def client_info(control: RESPConnection, name: str) -> Dict[str, str]:
    kind, payload = control.command("CLIENT", "LIST")
    if kind != "bulk":
        raise BenchError(f"CLIENT LIST returned {kind!r}")
    for line in payload.decode("ascii", "strict").splitlines():
        fields = {
            key: value
            for field in line.split()
            if "=" in field
            for key, value in (field.split("=", 1),)
        }
        if fields.get("name") == name:
            return fields
    raise BenchError(f"CLIENT LIST did not contain {name!r}")


def prepare_attacker(
    server: RedisServer,
    control: RESPConnection,
    name: str,
    required_free_bytes: Optional[int] = None,
) -> RESPConnection:
    attacker = RESPConnection.connect(server.port)
    require_response(attacker.command("CLIENT", "SETNAME", name), "simple", b"OK")
    require_response(attacker.command("SET", "multibulk-hol-prime", b"p" * PRIME_BYTES), "simple", b"OK")
    free_bytes = int(client_info(control, name)["qbuf-free"])
    required_free_bytes = required_free_bytes or len(build_attack_request(0))
    if free_bytes < required_free_bytes:
        attacker.close()
        raise BenchError(
            f"attacker query buffer has only {free_bytes} free bytes after priming"
        )
    return attacker


def read_event_count(control: RESPConnection, name: str) -> int:
    return int(client_info(control, name)["read-events"])


def info_fields(control: RESPConnection, section: str) -> Dict[str, str]:
    kind, payload = control.command("INFO", section)
    if kind != "bulk":
        raise BenchError(f"INFO {section} returned {kind!r}")
    return {
        key: value
        for line in payload.decode("ascii", "strict").splitlines()
        if ":" in line and not line.startswith("#")
        for key, value in (line.split(":", 1),)
    }


def reset_server_stats(control: RESPConnection) -> None:
    require_response(control.command("CONFIG", "RESETSTAT"), "simple", b"OK")


def client_processing_event_count(control: RESPConnection) -> int:
    fields = info_fields(control, "STATS")
    try:
        return int(fields["total_client_processing_events"])
    except KeyError as exc:
        raise BenchError("INFO STATS did not expose total_client_processing_events") from exc


def parse_callback_eventloop_us(control: RESPConnection) -> float:
    """Return Redis's largest whole-event-loop slice since RESETSTAT.

    The benchmark resets server statistics immediately before one fully buffered
    malformed request. `eventloop_duration_max` therefore captures the parser's
    socket callback (and only its small reply/event-loop bookkeeping), rather
    than a client-side round trip. A yielding implementation should bound this
    maximum even though total parsing work remains necessary.
    """
    fields = info_fields(control, "DEBUG")
    try:
        return float(fields["eventloop_duration_max"])
    except KeyError as exc:
        raise BenchError("INFO DEBUG did not expose eventloop_duration_max") from exc


def percentile(values: List[float], fraction: float) -> float:
    if not values:
        raise BenchError("no latency samples collected")
    if not 0 < fraction <= 1:
        raise BenchError(f"invalid percentile fraction {fraction}")
    ordered = sorted(values)
    index = math.ceil(len(ordered) * fraction) - 1
    return ordered[index]


class PingWorkers:
    def __init__(self, port: int, count: int):
        self.port = port
        self.count = count
        self.start_events = [threading.Event() for _ in range(count)]
        self.results: queue.Queue[Tuple[int, Optional[float], Optional[str]]] = queue.Queue()
        self.ready: queue.Queue[Tuple[int, Optional[str]]] = queue.Queue()
        self.threads: List[threading.Thread] = []
        self.stop = False

    def start(self) -> None:
        for index in range(self.count):
            thread = threading.Thread(target=self._run, args=(index,), daemon=True)
            thread.start()
            self.threads.append(thread)
        for _ in range(self.count):
            index, error = self.ready.get(timeout=10)
            if error is not None:
                self.close()
                raise BenchError(f"small client {index} could not start: {error}")

    def _run(self, index: int) -> None:
        connection: Optional[RESPConnection] = None
        try:
            connection = RESPConnection.connect(self.port)
            self.ready.put((index, None))
            while True:
                self.start_events[index].wait()
                self.start_events[index].clear()
                if self.stop:
                    return
                start = time.perf_counter_ns()
                connection.send(PING_REQUEST)
                require_response(connection.read_response(), "simple", b"PONG")
                self.results.put((index, (time.perf_counter_ns() - start) / 1000.0, None))
        except Exception as exc:  # Report worker failures to the controlling thread.
            if connection is None:
                self.ready.put((index, str(exc)))
            else:
                self.results.put((index, None, str(exc)))
        finally:
            if connection is not None:
                connection.close()

    def run_round(self) -> List[float]:
        for event in self.start_events:
            event.set()
        latencies = []
        for _ in range(self.count):
            index, latency, error = self.results.get(timeout=20)
            if error is not None or latency is None:
                raise BenchError(f"small client {index} failed: {error}")
            latencies.append(latency)
        return latencies

    def close(self) -> None:
        self.stop = True
        for event in self.start_events:
            event.set()
        for thread in self.threads:
            thread.join(timeout=5)


class PingFlood:
    """Keep the fast class runnable while a large request must make progress."""
    def __init__(self, port: int, workers: int):
        self.port = port
        self.workers = workers
        self.stop = threading.Event()
        self.ready: queue.Queue[Optional[str]] = queue.Queue()
        self.errors: queue.Queue[str] = queue.Queue()
        self.threads: List[threading.Thread] = []
        self.lock = threading.Lock()
        self.completed = 0

    def start(self) -> None:
        for _ in range(self.workers):
            thread = threading.Thread(target=self._run, daemon=True)
            thread.start()
            self.threads.append(thread)
        for _ in range(self.workers):
            error = self.ready.get(timeout=10)
            if error is not None:
                self.close()
                raise BenchError(f"fast PING flood could not start: {error}")

    def _run(self) -> None:
        connection: Optional[RESPConnection] = None
        try:
            connection = RESPConnection.connect(self.port)
            self.ready.put(None)
            while not self.stop.is_set():
                connection.send(PING_REQUEST)
                require_response(connection.read_response(), "simple", b"PONG")
                with self.lock:
                    self.completed += 1
        except Exception as exc:  # Surface a post-start flood failure to the check.
            if connection is None:
                self.ready.put(str(exc))
            else:
                self.errors.put(str(exc))
        finally:
            if connection is not None:
                connection.close()

    def wait_for(self, minimum: int, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.lock:
                if self.completed >= minimum:
                    return
            self.raise_if_error()
            time.sleep(0.001)
        raise BenchError(f"fast PING flood completed fewer than {minimum} requests")

    def count(self) -> int:
        with self.lock:
            return self.completed

    def raise_if_error(self) -> None:
        try:
            raise BenchError(f"fast PING flood failed: {self.errors.get_nowait()}")
        except queue.Empty:
            return

    def close(self) -> None:
        self.stop.set()
        for thread in self.threads:
            thread.join(timeout=5)
        self.raise_if_error()


def run_check(server: RedisServer) -> None:
    control = RESPConnection.connect(server.port)
    attacker: Optional[RESPConnection] = None
    try:
        name = "multibulk-hol-check"
        attacker = prepare_attacker(server, control, name)
        before = read_event_count(control, name)
        # The correct PING must remain ordered after the malformed high-arity
        # command; a continuation must not expose a partially parsed command.
        attacker.send(build_attack_request(int.from_bytes(os.urandom(1), "big")) + PING_REQUEST)
        kind, payload = attacker.read_response()
        if kind != "error" or b"wrong number of arguments" not in payload:
            raise BenchError(f"high-arity request returned {(kind, payload)!r}")
        require_response(attacker.read_response(), "simple", b"PONG")
        if read_event_count(control, name) - before != 1:
            raise BenchError("high-arity request was not fully buffered in one read callback")
        require_response(control.command("PING"), "simple", b"PONG")
    finally:
        if attacker is not None:
            attacker.close()
        control.close()


def run_fairness_check(server: RedisServer) -> None:
    """Ensure fast traffic cannot permanently starve one yielded large request."""
    attack_requests = build_attack_request_pool(8)
    control = RESPConnection.connect(server.port)
    attacker: Optional[RESPConnection] = None
    flood: Optional[PingFlood] = None
    try:
        name = "multibulk-hol-fairness"
        attacker = prepare_attacker(server, control, name)
        flood = PingFlood(server.port, workers=8)
        flood.start()
        flood.wait_for(64, timeout=2)
        flood_before = flood.count()
        # A continuation implementation must finish this request while the
        # fast class remains continuously runnable, rather than re-enqueuing
        # the same client forever behind newly readable sockets.
        attacker.sock.settimeout(5)
        for attack in attack_requests:
            before = read_event_count(control, name)
            attacker.send(attack + PING_REQUEST)
            expect_attack_error(attacker)
            require_response(attacker.read_response(), "simple", b"PONG")
            if read_event_count(control, name) - before == 1:
                break
        else:
            raise BenchError("fairness attack was never fully buffered in one read callback")
        flood.wait_for(flood_before + 8, timeout=2)
        flood.raise_if_error()
    finally:
        if flood is not None:
            flood.close()
        if attacker is not None:
            attacker.close()
        control.close()


def run_backlog_check(server: RedisServer) -> None:
    """Bound continuation work for several simultaneously pending large clients."""
    attack_requests = build_attack_request_pool(BACKLOG_CLIENTS)
    control = RESPConnection.connect(server.port)
    attackers: List[Tuple[RESPConnection, str]] = []
    try:
        for index in range(BACKLOG_CLIENTS):
            name = f"multibulk-hol-backlog-{index}"
            attackers.append((prepare_attacker(server, control, name), name))
        read_counts = [(attacker, name, read_event_count(control, name)) for attacker, name in attackers]
        reset_server_stats(control)
        before_events = client_processing_event_count(control)
        for index, (attacker, _) in enumerate(attackers):
            attacker.sock.settimeout(5)
            attacker.send(attack_requests[index] + PING_REQUEST)
        for attacker, _ in attackers:
            expect_attack_error(attacker)
            require_response(attacker.read_response(), "simple", b"PONG")
        # Drive several fresh event-loop turns after every command completed.
        # A duplicated continuation that survives completion must be drained
        # and charged before the event-count bound is evaluated.
        for _ in range(BACKLOG_CLIENTS * 8):
            require_response(control.command("PING"), "simple", b"PONG")
        processing_events = client_processing_event_count(control) - before_events
        max_events = BACKLOG_CLIENTS * MAX_CONTINUATION_EVENTS_PER_CLIENT
        if processing_events > max_events:
            raise BenchError(
                f"processed {processing_events} continuation events, exceeding the {max_events} bound"
            )
        for attacker, name, before_reads in read_counts:
            if read_event_count(control, name) - before_reads != 1:
                raise BenchError("backlog attack was not fully buffered in one read callback")
    finally:
        for attacker, _ in attackers:
            attacker.close()
        control.close()


def expect_attack_error(attacker: RESPConnection) -> None:
    kind, payload = attacker.read_response()
    if kind != "error" or b"wrong number of arguments" not in payload:
        raise BenchError(f"high-arity request returned {(kind, payload)!r}")


def warmup_attacker(attacker: RESPConnection, attack_requests: List[bytes]) -> None:
    """Warm parser allocation paths outside the recorded sample."""
    for index in range(WARMUP_ATTACKS):
        attacker.send(attack_requests[index % len(attack_requests)])
        expect_attack_error(attacker)


def run_sample(server: RedisServer, rounds: int, workers: int) -> Dict[str, float]:
    # Construct the long-lived pool before prepare_attacker() reserves its
    # private buffer. Otherwise a long sample setup can let clientsCron shrink
    # that buffer before the first attack and change the intended read shape.
    attack_requests = build_attack_request_pool(rounds)
    control = RESPConnection.connect(server.port)
    attacker: Optional[RESPConnection] = None
    ping_workers: Optional[PingWorkers] = None
    try:
        name = "multibulk-hol-sample"
        ping_workers = PingWorkers(server.port, workers)
        ping_workers.start()

        idle_latencies: List[float] = []
        for _ in range(rounds):
            idle_latencies.extend(ping_workers.run_round())

        attacker = prepare_attacker(server, control, name)
        warmup_attacker(attacker, attack_requests)
        contended_latencies: List[float] = []
        callback_slices: List[float] = []
        fully_buffered = 0
        attempts = 0
        max_attempts = rounds + max(16, rounds // 8)
        while fully_buffered < rounds:
            if attempts == max_attempts:
                raise BenchError(
                    f"only {fully_buffered} of {rounds} attacks were fully buffered after {attempts} attempts"
                )
            attack = attack_requests[attempts % len(attack_requests)]
            attempts += 1
            # This must be immediately before the attack: INFO DEBUG exposes
            # Redis's maximum complete event-loop duration since this reset.
            reset_server_stats(control)
            before = read_event_count(control, name)
            attacker.send(attack)
            # These PINGs are issued only after the large request is handed to
            # the kernel, so they measure the fast class waiting behind parsing.
            round_latencies = ping_workers.run_round()
            expect_attack_error(attacker)
            if read_event_count(control, name) - before != 1:
                # A fragmented write does not exercise the fully-buffered
                # trigger. Its fast-client values are deliberately excluded.
                continue
            contended_latencies.extend(round_latencies)
            callback_slices.append(parse_callback_eventloop_us(control))
            fully_buffered += 1

        return {
            "contended_ping_p99_us": percentile(contended_latencies, 0.99),
            "contended_ping_p999_us": percentile(contended_latencies, 0.999),
            "idle_ping_p99_us": percentile(idle_latencies, 0.99),
            "fully_buffered_parse_callback_p99_us": percentile(callback_slices, 0.99),
            "fully_buffered_attack_count": float(fully_buffered),
            "attack_attempt_count": float(attempts),
        }
    finally:
        if ping_workers is not None:
            ping_workers.close()
        if attacker is not None:
            attacker.close()
        control.close()


def run_guard(server: RedisServer, rounds: int) -> Dict[str, float]:
    """Measure isolated throughput for the same buffered high-arity parser path."""
    attack_requests = build_attack_request_pool(rounds)
    control = RESPConnection.connect(server.port)
    attacker: Optional[RESPConnection] = None
    try:
        name = "multibulk-hol-throughput"
        attacker = prepare_attacker(server, control, name)
        warmup_attacker(attacker, attack_requests)
        before_events = read_event_count(control, name)
        start = time.perf_counter_ns()
        for round_index in range(rounds):
            attacker.send(attack_requests[round_index % len(attack_requests)])
            expect_attack_error(attacker)
        elapsed_us = (time.perf_counter_ns() - start) / 1000.0
        read_events = read_event_count(control, name) - before_events
        if elapsed_us <= 0:
            raise BenchError("guard workload did not produce a positive elapsed time")
        return {
            "high_arity_parse_ops_per_sec": rounds * 1_000_000.0 / elapsed_us,
            "attack_read_event_count": float(read_events),
            "attack_attempt_count": float(rounds),
        }
    finally:
        if attacker is not None:
            attacker.close()
        control.close()


def run_cpu_guard(server: RedisServer, rounds: int) -> Dict[str, float]:
    """Measure main-thread parse CPU for only fully buffered high-arity reads."""
    if server.cpu_preload is None:
        raise BenchError("--cpu-guard requires PERFLOOP_PARSE_CPU_PRELOAD")
    attack_requests = build_attack_request_pool(rounds)
    control = RESPConnection.connect(server.port)
    attacker: Optional[RESPConnection] = None
    try:
        name = "multibulk-hol-cpu"
        attacker = prepare_attacker(server, control, name)
        warmup_attacker(attacker, attack_requests)
        # Flush the final warm-up read interval before the first measured reset.
        read_event_count(control, name)
        fully_buffered = 0
        block_attempts = 0
        total_cpu_ns = 0
        max_block_attempts = (
            (rounds + CPU_GUARD_BLOCK_ATTACKS - 1) // CPU_GUARD_BLOCK_ATTACKS
        ) * 2
        while fully_buffered < rounds:
            if block_attempts == max_block_attempts:
                raise BenchError(
                    f"only {fully_buffered} CPU attacks were fully buffered after {block_attempts} blocks"
                )
            block_attempts += 1
            block_size = min(CPU_GUARD_BLOCK_ATTACKS, rounds - fully_buffered)
            server.reset_parse_cpu_stats()
            block_is_fully_buffered = True
            for offset in range(block_size):
                attack = attack_requests[(fully_buffered + offset) % len(attack_requests)]
                before_events = read_event_count(control, name)
                attacker.send(attack)
                expect_attack_error(attacker)
                # This control read completes the high-arity read interval without
                # charging the control request itself to the saved CPU counter.
                if read_event_count(control, name) - before_events != 1:
                    block_is_fully_buffered = False
            cpu_ns, completed_intervals, target_reads = server.parse_cpu_stats()
            if not block_is_fully_buffered:
                continue
            if (
                completed_intervals != block_size
                or target_reads != block_size
                or cpu_ns <= 0
            ):
                raise BenchError("CPU probe did not isolate the fully buffered parse block")
            total_cpu_ns += cpu_ns
            fully_buffered += block_size
        return {
            "fully_buffered_high_arity_parse_cpu_ns_per_argument": (
                total_cpu_ns / (fully_buffered * ATTACK_ARGUMENTS)
            ),
            "fully_buffered_cpu_attack_count": float(fully_buffered),
            "cpu_block_attempt_count": float(block_attempts),
        }
    finally:
        if attacker is not None:
            attacker.close()
        control.close()


def reserve_server_cpu() -> Optional[int]:
    """Keep the server's CPU counter separate from client worker activity."""
    try:
        allowed = sorted(os.sched_getaffinity(0))
        if len(allowed) < 2:
            return None
        server_cpu = allowed[-1]
        os.sched_setaffinity(0, set(allowed[:-1]))
        return server_cpu
    except (AttributeError, OSError):
        # A one-CPU or constrained environment can still run the functional
        # checks; the benchmark reports its ordinary thread CPU metric there.
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="src/redis-server", help="path to redis-server")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="validate ordered high-arity parsing")
    mode.add_argument(
        "--fairness-check", action="store_true", help="validate progress amid continuous fast traffic"
    )
    mode.add_argument(
        "--backlog-check", action="store_true", help="validate bounded continuation work"
    )
    mode.add_argument("--sample", action="store_true", help="emit one proof JSONL sample per metric")
    mode.add_argument("--guard", action="store_true", help="emit isolated parser throughput metrics")
    mode.add_argument("--cpu-guard", action="store_true", help="emit fully buffered parser CPU metrics")
    parser.add_argument("--rounds", type=int, default=512, help="attacks in one sample")
    parser.add_argument("--workers", type=int, default=8, help="small PING clients")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.rounds < 1 or args.workers < 1:
        raise BenchError("--rounds and --workers must be positive")
    try:
        server_cpu = reserve_server_cpu()
        preload_text = os.environ.get("PERFLOOP_PARSE_CPU_PRELOAD")
        cpu_preload = Path(preload_text).resolve() if preload_text else None
        with RedisServer(
            Path(args.server).resolve(),
            server_cpu,
            cpu_preload,
        ) as server:
            if args.check:
                run_check(server)
                check_message = "multibulk-hol check: PASS"
                metrics = None
            elif args.fairness_check:
                run_fairness_check(server)
                check_message = "multibulk-hol fairness check: PASS"
                metrics = None
            elif args.backlog_check:
                run_backlog_check(server)
                check_message = "multibulk-hol backlog check: PASS"
                metrics = None
            elif args.sample:
                metrics = run_sample(server, args.rounds, args.workers)
                check_message = None
            elif args.guard:
                metrics = run_guard(server, args.rounds)
                check_message = None
            else:
                metrics = run_cpu_guard(server, args.rounds)
                check_message = None
        if check_message is not None:
            print(check_message)
        else:
            assert metrics is not None
            for metric, value in metrics.items():
                print(json.dumps({"metric": metric, "value": value}, separators=(",", ":")))
        return 0
    except (BenchError, OSError, subprocess.SubprocessError, queue.Empty) as exc:
        print(f"multibulk-hol benchmark failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
