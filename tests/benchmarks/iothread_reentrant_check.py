#!/usr/bin/env python3
"""Exercise IO-thread handoff progress while a timed-out Lua script reenters.

The check deliberately sends all queued requests before waiting for any reply.
With one worker IO thread, the busy script, ordinary requests, and SCRIPT KILL
share the same handoff lane.  SCRIPT KILL must still be consumed without a new
socket event after the notifier has been drained.
"""

import argparse
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


QUEUED_PINGS = 12


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


def collect_responses(expected):
    """Collect exactly one reply per client without injecting another event."""

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


def run_check(server_path, server_cpus):
    process = tempdir = admin = None
    clients = []
    try:
        process, tempdir, _logfile, admin = start_server(server_path, server_cpus)
        port = admin.socket.getpeername()[1]
        script_client = CheckClient("127.0.0.1", port)
        pingers = [CheckClient("127.0.0.1", port) for _ in range(QUEUED_PINGS)]
        killer = CheckClient("127.0.0.1", port)
        clients = [script_client] + pingers + [killer]
        require_one_worker(admin, clients)

        script_client.socket.sendall(fairness.encode_command("EVAL", "while true do end", "0"))
        # Allow the script to enter timed-out mode before making a single burst
        # of same-lane handoffs.  No command is sent after this burst.
        time.sleep(0.025)
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
            if response_type != "error" or not response.startswith("BUSY"):
                raise fairness.BenchmarkError("queued PING %d reply was %r" % (index, (response_type, response)))

        print("iothread reentrant progress check: PASS queued-pings=%d worker=1" % QUEUED_PINGS)
    finally:
        for client in clients:
            try:
                client.close()
            except OSError:
                pass
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
    arguments = parser.parse_args()
    server_path = Path(arguments.server)
    if not server_path.is_file():
        raise fairness.BenchmarkError("Redis server binary does not exist: %s" % server_path)
    run_check(server_path, arguments.server_cpus)


if __name__ == "__main__":
    try:
        main()
    except (fairness.BenchmarkError, OSError, subprocess.SubprocessError) as error:
        print("iothread reentrant progress check failed: %s" % error, file=sys.stderr)
        sys.exit(1)
