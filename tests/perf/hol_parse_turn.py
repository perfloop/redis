#!/usr/bin/env python3
"""Exercise fully buffered RESP parsing on one Redis event-loop lane.

The benchmark intentionally uses inline PING and control commands. The only
RESP requests in benchmark mode are one bootstrap PING and the fully buffered,
high-arity request, so the temporary trace probe records parser turns rather
than unrelated ready callbacks or event-loop housekeeping.
"""

import argparse
import csv
import json
import math
import os
import socket
import struct
import subprocess
import tempfile
import threading
import time
from pathlib import Path


PING = b"PING\r\n"
TRACE_CAPACITY = 1 << 20
SERVER_CPU = None


class BenchmarkError(RuntimeError):
    pass


class RespConnection:
    def __init__(self, sock):
        self.sock = sock
        self.buffer = bytearray()

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def send(self, payload):
        self.sock.sendall(payload)

    def command(self, command):
        self.send(command.encode("ascii") + b"\r\n")
        return self.read()

    def _fill(self, length):
        while len(self.buffer) < length:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise BenchmarkError("Redis closed a connection while reading a reply")
            self.buffer.extend(chunk)

    def _line(self):
        while True:
            marker = self.buffer.find(b"\r\n")
            if marker >= 0:
                line = bytes(self.buffer[:marker])
                del self.buffer[:marker + 2]
                return line
            self._fill(len(self.buffer) + 1)

    def _exact(self, length):
        self._fill(length)
        value = bytes(self.buffer[:length])
        del self.buffer[:length]
        return value

    def read(self):
        prefix = self._exact(1)
        if prefix in (b"+", b"-", b":"):
            value = self._line()
            if prefix == b"+":
                return ("simple", value)
            if prefix == b"-":
                return ("error", value)
            return ("integer", int(value))
        if prefix == b"$":
            length = int(self._line())
            if length == -1:
                return ("bulk", None)
            value = self._exact(length)
            if self._exact(2) != b"\r\n":
                raise BenchmarkError("malformed bulk response terminator")
            return ("bulk", value)
        if prefix == b"*":
            length = int(self._line())
            if length == -1:
                return ("array", None)
            return ("array", [self.read() for _ in range(length)])
        raise BenchmarkError("unexpected RESP prefix %r" % prefix)


def connect(port, timeout=2):
    sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.settimeout(timeout)
    return RespConnection(sock)


def expect_simple(reply, value):
    if reply != ("simple", value):
        raise BenchmarkError("expected simple response %r, got %r" % (value, reply))


def expect_integer(reply, value):
    if reply != ("integer", value):
        raise BenchmarkError("expected integer response %r, got %r" % (value, reply))


def reserve_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def pin_server_cpu():
    if SERVER_CPU is not None:
        os.sched_setaffinity(0, {SERVER_CPU})


def isolate_benchmark_processes():
    global SERVER_CPU
    try:
        available = sorted(os.sched_getaffinity(0))
        if len(available) > 1:
            SERVER_CPU = available[0]
            os.sched_setaffinity(0, set(available[1:]))
    except (AttributeError, OSError):
        pass


class RedisServer:
    def __init__(self, binary, io_threads, trace):
        self.binary = str(Path(binary).resolve())
        self.io_threads = io_threads
        self.temp = tempfile.TemporaryDirectory(prefix="redis-hol-parse-")
        self.port = reserve_port()
        self.trace_path = os.path.join(self.temp.name, "parse-turns.bin") if trace else None
        env = os.environ.copy()
        if self.trace_path:
            env["PERFLOOP_PARSE_TRACE"] = self.trace_path
        self.process = subprocess.Popen(
            [
                self.binary,
                "--port", str(self.port),
                "--bind", "127.0.0.1",
                "--protected-mode", "no",
                "--save", "",
                "--appendonly", "no",
                "--dir", self.temp.name,
                "--io-threads", str(io_threads),
            ],
            env=env,
            preexec_fn=pin_server_cpu if SERVER_CPU is not None else None,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._wait_ready()

    def _wait_ready(self):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise BenchmarkError("redis-server exited while starting")
            try:
                conn = connect(self.port)
                expect_simple(conn.command("PING"), b"PONG")
                conn.close()
                return
            except (BenchmarkError, OSError, socket.timeout):
                time.sleep(0.01)
        raise BenchmarkError("redis-server did not accept connections")

    def stop(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)

    def trace_values(self):
        if not self.trace_path:
            return []
        try:
            data = Path(self.trace_path).read_bytes()
        except FileNotFoundError as exc:
            raise BenchmarkError("the direct parser trace file was not created") from exc
        expected = TRACE_CAPACITY * 8
        if len(data) != expected:
            raise BenchmarkError("parser trace has %d bytes, expected %d" % (len(data), expected))
        values = struct.unpack("<%dQ" % TRACE_CAPACITY, data)
        return [value for value in values if value]

    def cleanup(self):
        self.stop()
        self.temp.cleanup()


def client_fields(control, name):
    reply = control.command("CLIENT LIST")
    if reply[0] != "bulk" or reply[1] is None:
        raise BenchmarkError("CLIENT LIST did not return a bulk response")
    for line in reply[1].decode("ascii").splitlines():
        fields = dict(item.split("=", 1) for item in line.split(" ") if "=" in item)
        if fields.get("name") == name:
            return fields
    return None


def wait_for_client(control, name, timeout=5, predicate=lambda fields: True):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        fields = client_fields(control, name)
        if fields and predicate(fields):
            return fields
        time.sleep(0.002)
    raise BenchmarkError("timed out waiting for client %s" % name)


def make_high_arity_ping(arguments, pipeline=True):
    if arguments < 2:
        raise BenchmarkError("high-arity request needs at least two arguments")
    payload = b"*%d\r\n$4\r\nPING\r\n" % arguments
    payload += b"$1\r\nx\r\n" * (arguments - 1)
    if pipeline:
        payload += PING
    return payload


def block_and_buffer(control, attacker, name, gate, payload):
    expect_simple(attacker.command("CLIENT SETNAME " + name), b"OK")
    attacker.send(("BLPOP %s 0\r\n" % gate).encode("ascii"))
    wait_for_client(control, name, predicate=lambda fields: "b" in fields.get("flags", ""))
    attacker.send(payload)
    return wait_for_client(
        control,
        name,
        predicate=lambda fields: int(fields.get("qbuf", "0")) >= len(payload),
    )


def expect_unblocked_pipeline(attacker, gate, commands=1):
    reply = attacker.read()
    expected = (
        "array",
        [("bulk", gate.encode("ascii")), ("bulk", b"v")],
    )
    if reply != expected:
        raise BenchmarkError("BLPOP response was %r, expected %r" % (reply, expected))
    for _ in range(commands):
        reply = attacker.read()
        if reply[0] != "error" or b"wrong number of arguments" not in reply[1]:
            raise BenchmarkError("high-arity PING did not return its expected error: %r" % (reply,))
        expect_simple(attacker.read(), b"PONG")


class Pinger:
    def __init__(self, port, request=PING):
        self.port = port
        self.request = request
        self.collect = threading.Event()
        self.stop_event = threading.Event()
        self.ready = threading.Event()
        self.values_ns = []
        self.error = None
        self.conn = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()
        if not self.ready.wait(5):
            raise BenchmarkError("a PING worker did not start")

    def _run(self):
        try:
            self.conn = connect(self.port)
            self.ready.set()
            while not self.stop_event.is_set():
                started = time.perf_counter_ns()
                self.conn.send(self.request)
                reply = self.conn.read()
                ended = time.perf_counter_ns()
                expect_simple(reply, b"PONG")
                if self.collect.is_set():
                    self.values_ns.append(ended - started)
        except (BenchmarkError, OSError, socket.timeout) as exc:
            if not self.stop_event.is_set():
                self.error = exc
            self.ready.set()
        finally:
            if self.conn:
                self.conn.close()

    def stop(self):
        self.stop_event.set()
        if self.conn:
            self.conn.close()
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            raise BenchmarkError("a PING worker did not stop")
        if self.error:
            raise BenchmarkError("PING worker failed: %s" % self.error)


def percentile(values, quantile):
    if not values:
        raise BenchmarkError("cannot calculate a percentile of no samples")
    ordered = sorted(values)
    return ordered[math.ceil(quantile * len(ordered)) - 1]


def bootstrap_trace(server):
    bootstrap = connect(server.port)
    bootstrap.send(b"*1\r\n$4\r\nPING\r\n")
    expect_simple(bootstrap.read(), b"PONG")
    bootstrap.close()


def run_benchmark(binary, rounds, arguments):
    server = RedisServer(binary, io_threads=1, trace=True)
    control = None
    idle_pingers = []
    pingers = []
    try:
        # This is the one trace record deliberately excluded from the metric.
        # It initializes the mmap sink before tail-latency collection begins.
        bootstrap_trace(server)
        control = connect(server.port)
        idle_pingers = [Pinger(server.port) for _ in range(4)]
        for pinger in idle_pingers:
            pinger.start()
            pinger.collect.set()
        time.sleep(0.4)
        for pinger in idle_pingers:
            pinger.collect.clear()
            pinger.stop()
        idle_latencies_ns = [
            value for pinger in idle_pingers for value in pinger.values_ns
        ]
        if len(idle_latencies_ns) < 2000:
            raise BenchmarkError(
                "only %d idle PING samples completed" % len(idle_latencies_ns)
            )

        pingers = [Pinger(server.port) for _ in range(4)]
        for pinger in pingers:
            pinger.start()

        payload = make_high_arity_ping(arguments)
        buffered_sizes = []
        for index in range(rounds):
            attacker = connect(server.port)
            name = "hol-attacker-%d" % index
            gate = "hol-gate-%d" % index
            fields = block_and_buffer(control, attacker, name, gate, payload)
            buffered_sizes.append(int(fields["qbuf"]))
            for pinger in pingers:
                pinger.collect.set()
            expect_integer(control.command("LPUSH %s v" % gate), 1)
            expect_unblocked_pipeline(attacker, gate)
            for pinger in pingers:
                pinger.collect.clear()
            attacker.close()

        for pinger in pingers:
            pinger.stop()
        latencies_ns = [value for pinger in pingers for value in pinger.values_ns]
        if len(latencies_ns) < rounds * 4:
            raise BenchmarkError(
                "only %d PING replies completed in release windows" % len(latencies_ns)
            )
        if min(buffered_sizes) < len(payload):
            raise BenchmarkError("a high-arity request was released before it was fully buffered")
    finally:
        if control:
            control.close()
        for pinger in idle_pingers + pingers:
            if pinger.thread.is_alive():
                pinger.stop()
        server.stop()

    trace_values = server.trace_values()
    server.temp.cleanup()
    # The bootstrap RESP PING is the only non-workload RESP request. All PING
    # workers and controller traffic use inline protocol, so every remaining
    # record belongs to an initial or resumed high-arity parser turn.
    if len(trace_values) < rounds + 1:
        raise BenchmarkError(
            "trace has %d records; expected bootstrap plus at least %d parser turns"
            % (len(trace_values), rounds)
        )
    parser_turns_ns = trace_values[1:]
    idle_p99_us = percentile(idle_latencies_ns, 0.99) / 1000.0
    hol_p99_us = percentile(latencies_ns, 0.99) / 1000.0
    return {
        "small_ping_hol_over_idle_p99": hol_p99_us / idle_p99_us,
        "small_ping_p99_us": hol_p99_us,
        "small_ping_p999_us": percentile(latencies_ns, 0.999) / 1000.0,
        "small_ping_idle_p99_us": idle_p99_us,
        "small_ping_idle_p999_us": percentile(idle_latencies_ns, 0.999) / 1000.0,
        "small_ping_idle_samples": len(idle_latencies_ns),
        "multibulk_parse_turn_p99_us": percentile(parser_turns_ns, 0.99) / 1000.0,
        "fully_buffered_qbuf_min_bytes": min(buffered_sizes),
        "small_ping_release_window_samples": len(latencies_ns),
        "multibulk_parse_turn_count": len(parser_turns_ns),
    }


def server_cpu_ns(pid):
    try:
        fields = Path("/proc/%d/stat" % pid).read_text().rpartition(")")[2].split()
        ticks = int(fields[11]) + int(fields[12])
        return ticks * (1_000_000_000 // os.sysconf("SC_CLK_TCK"))
    except (IndexError, OSError, ValueError) as exc:
        raise BenchmarkError("could not read redis-server CPU time") from exc


def run_steady_resp(binary, benchmark_binary):
    server = RedisServer(binary, io_threads=1, trace=False)
    try:
        start_cpu_ns = server_cpu_ns(server.process.pid)
        result = subprocess.run(
            [
                str(Path(benchmark_binary).resolve()),
                "-h", "127.0.0.1",
                "-p", str(server.port),
                "-n", "30000",
                "-c", "4",
                "-t", "ping_mbulk",
                "--csv",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        used_cpu_ns = server_cpu_ns(server.process.pid) - start_cpu_ns
        if result.returncode:
            raise BenchmarkError("redis-benchmark failed: %s" % result.stderr.strip())
        rows = list(csv.DictReader(result.stdout.splitlines()))
        if len(rows) != 1 or rows[0].get("test") != "PING_MBULK":
            raise BenchmarkError("unexpected redis-benchmark CSV: %r" % result.stdout)
        ops_per_sec = float(rows[0]["rps"])
        if ops_per_sec <= 0:
            raise BenchmarkError("redis-benchmark reported non-positive throughput")
        return {
            "small_resp_ping_ops_per_sec": ops_per_sec,
            "small_resp_server_cpu_ns_per_op": used_cpu_ns / 30000,
        }
    finally:
        server.cleanup()


def verify_trace_scope(binary):
    server = RedisServer(binary, io_threads=1, trace=True)
    pingers = [Pinger(server.port) for _ in range(4)]
    try:
        for pinger in pingers:
            pinger.start()
        # The concurrent PING traffic uses the inline protocol. The one RESP
        # PING below should therefore be the only direct parser-turn record,
        # even while its event-loop lane also runs unrelated file callbacks.
        traced = connect(server.port)
        traced.send(b"*1\r\n$4\r\nPING\r\n")
        expect_simple(traced.read(), b"PONG")
        traced.close()
        for pinger in pingers:
            pinger.stop()
        server.stop()
        records = server.trace_values()
        if len(records) != 1:
            raise BenchmarkError(
                "parser trace recorded %d turns for one RESP PING amid inline PING traffic"
                % len(records)
            )
    finally:
        for pinger in pingers:
            if pinger.thread.is_alive():
                pinger.stop()
        server.cleanup()


def verify_order(binary):
    server = RedisServer(binary, io_threads=1, trace=False)
    control = connect(server.port)
    attacker = connect(server.port)
    try:
        payload = make_high_arity_ping(50000)
        fields = block_and_buffer(control, attacker, "verify-order", "verify-order-gate", payload)
        if int(fields["qbuf"]) != len(payload):
            raise BenchmarkError("order test request was not wholly buffered")
        expect_integer(control.command("LPUSH verify-order-gate v"), 1)
        expect_unblocked_pipeline(attacker, "verify-order-gate")
    finally:
        attacker.close()
        control.close()
        server.cleanup()


def verify_competing_backlog(binary):
    server = RedisServer(binary, io_threads=1, trace=False)
    control = connect(server.port, timeout=10)
    attackers = []
    pingers = []
    try:
        commands_per_client = 4
        payload = make_high_arity_ping(50000) * commands_per_client
        names = []
        gates = []
        for index in range(16):
            name = "verify-backlog-%d" % index
            gate = "verify-backlog-gate-%d" % index
            attacker = connect(server.port, timeout=10)
            attackers.append(attacker)
            fields = block_and_buffer(control, attacker, name, gate, payload)
            if int(fields["qbuf"]) != len(payload):
                raise BenchmarkError("backlog test request was not wholly buffered")
            names.append(name)
            gates.append(gate)

        pingers = [Pinger(server.port) for _ in range(4)]
        for pinger in pingers:
            pinger.start()
        # Releasing all gates in one controller input turn creates competing
        # fully buffered parser continuations in a bounded candidate. Four
        # commands per client exercise repeated resumptions; exact ordered
        # replies and drained qbufs expose duplicate or stranded work.
        control.send(
            b"".join(("LPUSH %s v\r\n" % gate).encode("ascii") for gate in gates)
        )
        for _ in gates:
            expect_integer(control.read(), 1)
        for attacker, gate in zip(attackers, gates):
            expect_unblocked_pipeline(attacker, gate, commands_per_client)
        for name in names:
            fields = client_fields(control, name)
            if not fields or int(fields.get("qbuf", "-1")) != 0:
                raise BenchmarkError("backlog client did not drain: %s" % name)
        for pinger in pingers:
            pinger.stop()
        expect_simple(control.command("PING"), b"PONG")
        if server.process.poll() is not None:
            raise BenchmarkError("redis-server exited after competing parser backlog")
    finally:
        for pinger in pingers:
            if pinger.thread.is_alive():
                pinger.stop()
        for attacker in attackers:
            attacker.close()
        control.close()
        server.cleanup()


def verify_worker_teardown(binary):
    server = RedisServer(binary, io_threads=2, trace=False)
    control = connect(server.port)
    attacker = connect(server.port)
    try:
        name = "verify-worker"
        expect_simple(attacker.command("CLIENT SETNAME " + name), b"OK")
        fields = wait_for_client(
            control,
            name,
            predicate=lambda info: info.get("io-thread") == "1",
        )
        client_id = fields["id"]
        attacker.send(make_high_arity_ping(200000, pipeline=False))
        # A bounded parser leaves a continuation on this worker after it
        # starts the request. The baseline can finish its unbounded request
        # before this poll, but a candidate must remain safe in either case.
        deadline = time.monotonic() + 0.1
        while time.monotonic() < deadline:
            fields = client_fields(control, name)
            if not fields:
                break
            if fields.get("io-thread") != "1":
                raise BenchmarkError("worker client changed lanes before teardown: %r" % fields)
            if int(fields.get("argv-mem", "0")) > 0:
                break
            time.sleep(0.001)
        # CLIENT KILL executes on the main loop and fetches this worker-owned
        # client. It therefore validates cancellation through the IO/main
        # transfer and free path while a bounded candidate can have a queued
        # parser continuation.
        expect_integer(control.command("CLIENT KILL ID " + client_id), 1)
        for _ in range(8):
            expect_simple(control.command("PING"), b"PONG")
        if server.process.poll() is not None:
            raise BenchmarkError("redis-server exited after worker continuation teardown")
    finally:
        attacker.close()
        control.close()
        server.cleanup()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", required=True)
    parser.add_argument(
        "--mode",
        choices=("benchmark", "resp-steady", "trace-scope", "verify"),
        default="benchmark",
    )
    parser.add_argument("--rounds", type=int, default=512)
    parser.add_argument("--arguments", type=int, default=50000)
    parser.add_argument("--benchmark-bin", default="./src/redis-benchmark")
    args = parser.parse_args()

    isolate_benchmark_processes()

    if args.mode == "verify":
        verify_order(args.server)
        verify_competing_backlog(args.server)
        verify_worker_teardown(args.server)
        print("HOL_PARSE_VERIFY_OK")
        return
    if args.mode == "trace-scope":
        verify_trace_scope(args.server)
        print("HOL_PARSE_TRACE_SCOPE_OK")
        return
    if args.mode == "resp-steady":
        metrics = run_steady_resp(args.server, args.benchmark_bin)
        for metric, value in metrics.items():
            print(json.dumps({"metric": metric, "value": value}, separators=(",", ":")))
        return

    metrics = run_benchmark(args.server, args.rounds, args.arguments)
    for metric, value in metrics.items():
        print(json.dumps({"metric": metric, "value": value}, separators=(",", ":")))


if __name__ == "__main__":
    try:
        main()
    except (BenchmarkError, OSError, socket.timeout, subprocess.SubprocessError) as exc:
        raise SystemExit("hol_parse_turn: %s" % exc)
