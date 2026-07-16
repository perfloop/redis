#!/usr/bin/env python3
"""Measure a busy-script end-to-end IO-thread reply burst.

The workload uses one worker IO thread. It observes a real BUSY reply from a
timed-out script, then sends 64 ordinary requests followed by SCRIPT KILL
without sending another request. A sample is retained only when all ordinary
requests also observe the busy script. This is a progress metric, not a
command-dispatch-order assertion; iothread_reentrant_order.py covers that
semantic invariant with a server-side dispatch log and yield epochs.
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


QUEUED_PINGS = 64
MAX_SETUP_ATTEMPTS = 20


class CheckClient(fairness.RespClient):
    """RESP client that retains Redis error replies for assertions."""

    def read(self):
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
        raise fairness.BenchmarkError("unexpected RESP response type %r" % response_type)

    def command(self, *parts):
        self.socket.sendall(fairness.encode_command(*parts))
        response_type, response = self.read()
        if response_type == "error":
            raise fairness.BenchmarkError("Redis error reply: %s" % response)
        return response


def start_server(server_path, server_cpus):
    port_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port_socket.bind(("127.0.0.1", 0))
    port = port_socket.getsockname()[1]
    port_socket.close()

    tempdir = tempfile.TemporaryDirectory(prefix="redis-iothread-reentrant-")
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


def wait_for_busy_script(probe):
    """Observe timeout mode rather than guessing it with a wall-clock sleep."""

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        probe.socket.sendall(fairness.encode_command("PING"))
        response_type, response = probe.read()
        if response_type == "error" and response.startswith("BUSY"):
            return
        if (response_type, response) != ("status", "PONG"):
            raise fairness.BenchmarkError("script-start probe reply was %r" % ((response_type, response),))
    raise fairness.BenchmarkError("script did not enter timed-out mode")


def collect_responses(expected):
    """Collect one reply per client without injecting another request event."""

    pending = {client.socket: (label, client) for label, client in expected}
    replies = {}
    deadline = time.monotonic() + 5
    while pending:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            labels = sorted(label for label, _client in pending.values())
            raise fairness.BenchmarkError("timed out waiting for queued replies: %s" % labels)
        ready, _, _ = select.select(list(pending), [], [], remaining)
        if not ready:
            continue
        for sock in ready:
            label, client = pending.pop(sock)
            replies[label] = client.read()
    return replies


def run_attempt(admin):
    port = admin.socket.getpeername()[1]
    script_client = CheckClient("127.0.0.1", port)
    probe = CheckClient("127.0.0.1", port)
    pingers = [CheckClient("127.0.0.1", port) for _ in range(QUEUED_PINGS)]
    killer = CheckClient("127.0.0.1", port)
    clients = [script_client, probe] + pingers + [killer]
    try:
        require_one_worker(admin, clients)
        script_client.socket.sendall(fairness.encode_command("EVAL", "while true do end", "0"))
        wait_for_busy_script(probe)

        # The BUSY probe proves that the script entered its reentrant event
        # loop. No command is sent after this one burst of same-lane handoffs.
        start = time.perf_counter_ns()
        for client in pingers:
            client.socket.sendall(fairness.encode_command("PING"))
        killer.socket.sendall(fairness.encode_command("SCRIPT", "KILL"))

        expected = [("script", script_client), ("kill", killer)]
        expected.extend(("ping-%d" % index, client) for index, client in enumerate(pingers))
        replies = collect_responses(expected)

        if replies["kill"] != ("status", "OK"):
            raise fairness.BenchmarkError("SCRIPT KILL reply was %r" % (replies["kill"],))
        if replies["script"][0] != "error" or "killed by user" not in replies["script"][1]:
            raise fairness.BenchmarkError("script did not report a user kill: %r" % (replies["script"],))
        for index in range(QUEUED_PINGS):
            response_type, response = replies["ping-%d" % index]
            if (response_type, response) == ("status", "PONG"):
                # Keep only bursts whose ordinary requests were served while
                # the script was still busy. This does not establish dispatch
                # order; the separate order check does that.
                return None
            if response_type != "error" or not response.startswith("BUSY"):
                raise fairness.BenchmarkError("queued PING %d reply was %r" % (index, (response_type, response)))

        return (time.perf_counter_ns() - start) / 1000.0
    finally:
        for client in clients:
            try:
                client.close()
            except OSError:
                pass


def run_measurement(server_path, server_cpus):
    process = tempdir = admin = None
    try:
        process, tempdir, _logfile, admin = start_server(server_path, server_cpus)
        for attempt in range(1, MAX_SETUP_ATTEMPTS + 1):
            progress_us = run_attempt(admin)
            if progress_us is not None:
                return attempt, progress_us
        raise fairness.BenchmarkError("could not observe a busy-script reply burst")
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
    parser.add_argument("--server-cpus", default="")
    parser.add_argument("--metric", choices=("reentrant_progress_us",), required=True)
    arguments = parser.parse_args()

    server_path = Path(arguments.server)
    if not server_path.is_file():
        raise fairness.BenchmarkError("Redis server binary does not exist: %s" % server_path)
    attempt, progress_us = run_measurement(server_path, arguments.server_cpus)

    print(json.dumps({"metric": "reentrant_progress_us", "value": progress_us}))


if __name__ == "__main__":
    try:
        main()
    except (fairness.BenchmarkError, OSError, subprocess.SubprocessError) as error:
        print("iothread reentrant progress check failed: %s" % error, file=sys.stderr)
        sys.exit(1)
