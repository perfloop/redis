#!/usr/bin/env python3
"""Verify same-lane reentrant IO-thread handoff progress without timing it.

A test module enters a yielding long command. Its allow-busy marker and stop
commands report the module yield epoch in which they were dispatched. The
workload sends markers followed by stop on pre-accepted sockets in the single
worker lane. SLOWLOG records actual server dispatch order, so a qualified run
requires that at least eight marker commands really preceded stop; it does not
infer that order from client send order or reply contents.

A qualified full-drain attempt dispatches the actual marker set recorded
before stop in one module yield epoch. A bounded reentrant drain cannot do so
for that set once it has more markers than the normal quantum. Attempts whose
cross-socket arrivals span multiple blocked-event passes are retried rather
than treated as a scheduler failure. Socket deadlines are only a watchdog for
a hung test process, not a pass/fail latency budget.
"""

import argparse
import json
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import iothread_fairness as fairness


QUEUED_MARKERS = 32
MIN_MARKERS_BEFORE_CONTROL = 8
MAX_SETUP_ATTEMPTS = 20
LIVENESS_TIMEOUT_SECONDS = 10
MARKER_COMMAND = "iothreadtest.marker"
STOP_COMMAND = ("iothreadtest.stop",)


class CheckClient(fairness.RespClient):
    """RESP2 client that preserves errors and nested SLOWLOG arrays."""

    def read(self):
        return self.read_response()

    def read_response(self):
        response_type = self.read_exact(1)
        if response_type == b"+":
            return "status", self.read_line().decode()
        if response_type == b"-":
            return "error", self.read_line().decode()
        if response_type == b":":
            return "integer", int(self.read_line())
        if response_type == b"$":
            length = int(self.read_line())
            if length < 0:
                return "bulk", None
            return "bulk", self.read_exact(length + 2)[:-2]
        if response_type == b"*":
            length = int(self.read_line())
            if length < 0:
                return "array", None
            return "array", [self.read_response() for _ in range(length)]
        raise fairness.BenchmarkError("unexpected RESP response type %r" % response_type)

    def command(self, *parts):
        self.socket.sendall(fairness.encode_command(*parts))
        response_type, response = self.read()
        if response_type == "error":
            raise fairness.BenchmarkError("Redis error reply: %s" % response)
        return response


class BareCheckClient(CheckClient):
    """A pre-accepted client whose first command can be allow-busy stop."""

    def __init__(self, host, port):
        self.socket = socket.create_connection((host, port), timeout=3)
        self.socket.settimeout(LIVENESS_TIMEOUT_SECONDS)
        self.buffer = bytearray()
        self.client_id = None


def start_server(server_path, module_path, server_cpus):
    port_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port_socket.bind(("127.0.0.1", 0))
    port = port_socket.getsockname()[1]
    port_socket.close()

    tempdir = tempfile.TemporaryDirectory(prefix="redis-iothread-reentrant-order-")
    logfile = Path(tempdir.name) / "redis.log"
    command = [
        str(server_path),
        "--port",
        str(port),
        "--bind",
        "127.0.0.1",
        "--save",
        "",
        "--appendonly",
        "no",
        "--io-threads",
        "2",
        "--io-threads-do-reads",
        "yes",
        "--busy-reply-threshold",
        "1",
        "--hz",
        "10",
        "--dynamic-hz",
        "no",
        "--slowlog-log-slower-than",
        "0",
        "--slowlog-max-len",
        "1024",
        "--loadmodule",
        str(module_path.resolve()),
        "--dir",
        tempdir.name,
        "--logfile",
        str(logfile),
    ]
    if server_cpus:
        taskset = shutil.which("taskset")
        if not taskset:
            tempdir.cleanup()
            raise fairness.BenchmarkError("--server-cpus requires taskset")
        command = [taskset, "-c", server_cpus] + command

    process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + 5
    while True:
        try:
            admin = CheckClient("127.0.0.1", port)
            return process, tempdir, logfile, admin
        except OSError:
            if time.monotonic() >= deadline:
                process.kill()
                process.wait()
                log = logfile.read_text(errors="replace") if logfile.exists() else ""
                tempdir.cleanup()
                raise fairness.BenchmarkError("server did not start:\n%s" % log)
            time.sleep(0.01)


def stop_server(process, tempdir):
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    tempdir.cleanup()


def require_one_worker(admin, clients):
    assignments = fairness.client_threads(admin)
    worker_ids = {assignments.get(client.client_id) for client in clients}
    if worker_ids != {1}:
        raise fairness.BenchmarkError("workload clients were not all assigned to worker 1: %r" % worker_ids)


def wait_for_busy_operation(probe):
    """Observe a real busy reply instead of assuming the long command yielded."""

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        probe.socket.sendall(fairness.encode_command("PING"))
        response_type, response = probe.read()
        if response_type == "error" and response.startswith("BUSY Slow IO-thread test operation"):
            return
        if (response_type, response) != ("status", "PONG"):
            raise fairness.BenchmarkError("long-command probe reply was %r" % ((response_type, response),))
    raise fairness.BenchmarkError("long command did not enter allow-busy yield mode")


def collect_responses(expected):
    """Receive the existing burst only; do not inject a follow-up request."""

    pending = {client.socket: (label, client) for label, client in expected}
    replies = {}
    deadline = time.monotonic() + LIVENESS_TIMEOUT_SECONDS
    while pending:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            labels = sorted(label for label, _client in pending.values())
            raise fairness.BenchmarkError("liveness watchdog expired waiting for: %s" % labels)
        ready, _, _ = select.select(list(pending), [], [], remaining)
        for sock in ready:
            label, client = pending.pop(sock)
            replies[label] = client.read()
    return replies


def slowlog_command_name(entry):
    """Extract a lower-case command tuple from one RESP2 SLOWLOG entry."""

    if entry[0] != "array" or not entry[1] or len(entry[1]) < 4:
        return None
    fields = entry[1]
    command = fields[3]
    if command[0] != "array" or command[1] is None:
        return None
    parts = []
    for part_type, part in command[1]:
        if part_type != "bulk" or not isinstance(part, bytes):
            return None
        parts.append(part.decode().lower())
    return tuple(parts)


def marker_ids_dispatched_before_stop(slowlog_entries):
    names = [slowlog_command_name(entry) for entry in slowlog_entries]
    try:
        stop_index = names.index(STOP_COMMAND)
    except ValueError as error:
        raise fairness.BenchmarkError("SLOWLOG did not record iothreadtest.stop") from error

    # SLOWLOG returns newest first, so later list entries were dispatched first.
    marker_ids = set()
    for name in names[stop_index + 1:]:
        if not name or name[0] != MARKER_COMMAND:
            continue
        if len(name) != 2:
            raise fairness.BenchmarkError("SLOWLOG marker command did not include its identifier: %r" % (name,))
        try:
            marker_id = int(name[1])
        except ValueError as error:
            raise fairness.BenchmarkError("SLOWLOG marker identifier was not numeric: %r" % (name,)) from error
        if marker_id < 0 or marker_id >= QUEUED_MARKERS:
            raise fairness.BenchmarkError("SLOWLOG marker identifier was out of range: %r" % (name,))
        marker_ids.add(marker_id)
    return marker_ids


def run_attempt(admin, port):
    slow_client = CheckClient("127.0.0.1", port)
    probe = CheckClient("127.0.0.1", port)
    markers = [CheckClient("127.0.0.1", port) for _ in range(QUEUED_MARKERS)]
    # Pre-accept the stop socket before the long command. A normal PING proves
    # it is a server client while the server is not busy; its first burst
    # command can then be allow-busy stop without adding a setup event.
    control = BareCheckClient("127.0.0.1", port)
    clients = [slow_client, probe] + markers + [control]
    try:
        if control.command("PING") != "PONG":
            raise fairness.BenchmarkError("pre-accept PING did not receive PONG")
        admin.command("SLOWLOG", "RESET")
        require_one_worker(admin, [slow_client, probe] + markers)
        slow_client.socket.sendall(fairness.encode_command("iothreadtest.slow"))
        wait_for_busy_operation(probe)

        for index, marker in enumerate(markers):
            marker.socket.sendall(fairness.encode_command(MARKER_COMMAND, str(index)))
        control.socket.sendall(fairness.encode_command(*STOP_COMMAND))

        expected = [("slow", slow_client), ("control", control)]
        expected.extend(("marker-%d" % index, marker) for index, marker in enumerate(markers))
        replies = collect_responses(expected)

        if replies["control"][0] != "integer" or replies["slow"][0] != "integer":
            raise fairness.BenchmarkError("long/control replies were %r and %r" % (replies["slow"], replies["control"]))
        control_epoch = replies["control"][1]
        if replies["slow"][1] != control_epoch:
            raise fairness.BenchmarkError("long command ended at a different yield epoch")

        marker_epochs = {}
        for index in range(QUEUED_MARKERS):
            response_type, response = replies["marker-%d" % index]
            if response_type != "integer":
                raise fairness.BenchmarkError("marker %d reply was %r" % (index, (response_type, response)))
            marker_epochs[index] = response

        slowlog_entries = admin.command("SLOWLOG", "GET", "1024")
        marker_ids = marker_ids_dispatched_before_stop(slowlog_entries)
        if len(marker_ids) < MIN_MARKERS_BEFORE_CONTROL:
            # The actual dispatch log says control won the cross-socket race;
            # retry rather than treating sender order as evidence.
            return None
        pre_stop_epochs = {marker_epochs[index] for index in marker_ids}
        if pre_stop_epochs != {control_epoch}:
            # These marker sockets reached different blocked-event passes before
            # stop. That is legal cross-socket arrival, not proof of a bounded
            # drain; retry until SLOWLOG identifies one authoritative batch.
            return None
        return control_epoch, len(marker_ids)
    finally:
        for client in clients:
            try:
                client.close()
            except OSError:
                pass


def run_measurement(server_path, module_path, server_cpus):
    process = tempdir = admin = None
    try:
        process, tempdir, _logfile, admin = start_server(server_path, module_path, server_cpus)
        for attempt in range(1, MAX_SETUP_ATTEMPTS + 1):
            result = run_attempt(admin, admin.socket.getpeername()[1])
            if result is not None:
                control_epoch, marker_count = result
                return attempt, control_epoch, marker_count
        raise fairness.BenchmarkError("could not establish a logged same-lane residual order")
    finally:
        if admin is not None:
            try:
                admin.close()
            except OSError:
                pass
        if process is not None:
            stop_server(process, tempdir)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default="src/redis-server")
    parser.add_argument("--module", default="tests/modules/iothreadtest.so")
    parser.add_argument("--server-cpus", default="")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--metric", choices=("reentrant_control_epoch",))
    arguments = parser.parse_args()
    if arguments.check == (arguments.metric is not None):
        parser.error("specify exactly one of --check or --metric")

    server_path = Path(arguments.server)
    module_path = Path(arguments.module)
    if not server_path.is_file():
        raise fairness.BenchmarkError("Redis server binary does not exist: %s" % server_path)
    if not module_path.is_file():
        raise fairness.BenchmarkError("test module does not exist: %s" % module_path)

    attempt, control_epoch, marker_count = run_measurement(server_path, module_path, arguments.server_cpus)
    if arguments.check:
        print(
            "iothread reentrant order check: PASS dispatch-order=slowlog "
            "markers-before-control=%d marker-epochs=%d control-epoch=%d worker=1 attempt=%d"
            % (marker_count, control_epoch, control_epoch, attempt)
        )
    else:
        print(json.dumps({"metric": "reentrant_control_epoch", "value": control_epoch}))


if __name__ == "__main__":
    try:
        main()
    except (fairness.BenchmarkError, OSError, subprocess.SubprocessError) as error:
        print("iothread reentrant order check failed: %s" % error, file=sys.stderr)
        sys.exit(1)
