#!/usr/bin/env python3
"""Exercise residual IO-thread client lifetime and nested-event-loop progress."""

from __future__ import annotations

import argparse
import contextlib
from pathlib import Path
import socket
import subprocess
import tempfile
import time
from typing import Iterator


HOST = "127.0.0.1"
IO_THREADS = 4
RESIDUAL_CLIENTS = 16
LIFETIME_PIPELINE = 8192
REENTRANT_CLIENTS = 48
REENTRANT_PIPELINE = 256


class RegressionError(RuntimeError):
    pass


class RedisConnection:
    def __init__(self, port: int, timeout: float = 10.0):
        self.sock = socket.create_connection((HOST, port), timeout=timeout)
        self.sock.settimeout(timeout)

    def close(self) -> None:
        self.sock.close()

    def send_raw(self, payload: bytes) -> None:
        self.sock.sendall(payload)

    def command(self, *parts: str) -> str:
        self.send_raw(resp_command(parts))
        return self.read()

    def read(self) -> str:
        prefix = self._read_exact(1)
        if prefix == b"+":
            return self._readline().decode()
        if prefix == b"-":
            raise RegressionError(self._readline().decode())
        if prefix == b":":
            return self._readline().decode()
        if prefix == b"$":
            length = int(self._readline())
            if length == -1:
                return ""
            value = self._read_exact(length)
            if self._read_exact(2) != b"\r\n":
                raise RegressionError("malformed bulk response")
            return value.decode()
        raise RegressionError(f"unsupported RESP type {prefix!r}")

    def _read_exact(self, length: int) -> bytes:
        chunks: list[bytes] = []
        while length:
            chunk = self.sock.recv(length)
            if not chunk:
                raise RegressionError("connection closed while reading response")
            chunks.append(chunk)
            length -= len(chunk)
        return b"".join(chunks)

    def _readline(self) -> bytes:
        data = bytearray()
        while not data.endswith(b"\r\n"):
            data.extend(self._read_exact(1))
        return bytes(data[:-2])


def resp_command(parts: tuple[str, ...]) -> bytes:
    encoded = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        value = part.encode()
        encoded.extend((f"${len(value)}\r\n".encode(), value, b"\r\n"))
    return b"".join(encoded)


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((HOST, 0))
        return int(probe.getsockname()[1])


@contextlib.contextmanager
def redis_server(server: Path, module: Path | None = None) -> Iterator[int]:
    with tempfile.TemporaryDirectory(prefix="iothread-residual-") as directory:
        root = Path(directory)
        port = find_free_port()
        stderr = (root / "stderr.log").open("wb")
        command = [
            str(server),
            "--bind", HOST,
            "--port", str(port),
            "--save", "",
            "--appendonly", "no",
            "--io-threads", str(IO_THREADS),
            "--io-threads-do-reads", "yes",
            "--dir", str(root),
            "--logfile", str(root / "redis.log"),
            "--loglevel", "warning",
        ]
        if module is not None:
            command.extend(("--loadmodule", str(module)))
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=stderr)
        try:
            deadline = time.monotonic() + 15.0
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RegressionError(f"server exited during startup ({process.returncode})")
                try:
                    connection = RedisConnection(port, timeout=0.2)
                    try:
                        if connection.command("PING") == "PONG":
                            break
                    finally:
                        connection.close()
                except (OSError, TimeoutError, socket.timeout, RegressionError):
                    time.sleep(0.01)
            else:
                raise RegressionError("server did not become ready")
            yield port
            if process.poll() is not None:
                raise RegressionError(f"server exited unexpectedly ({process.returncode})")
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
            stderr.close()
            if process.returncode not in (0, -15):
                detail = (root / "stderr.log").read_text(errors="replace")
                detail += (root / "redis.log").read_text(errors="replace")
                raise RegressionError(f"server exited with {process.returncode}:\n{detail}")


def residual_payload(key: str, count: int) -> bytes:
    return b"".join(resp_command(("INCR", key)) for _ in range(count))


def open_residual_clients(port: int, count: int = RESIDUAL_CLIENTS) -> tuple[list[RedisConnection], list[str]]:
    victims: list[RedisConnection] = []
    ids: list[str] = []
    for _ in range(count):
        victim = RedisConnection(port, timeout=30.0)
        ids.append(victim.command("CLIENT", "ID"))
        victims.append(victim)
    return victims, ids


def send_residual_pipelines(port: int, key: str, count: int) -> tuple[list[RedisConnection], list[str]]:
    victims, ids = open_residual_clients(port)
    payload = residual_payload(key, count)
    for victim in victims:
        victim.send_raw(payload)
    return victims, ids


def assert_server_responsive(control: RedisConnection) -> None:
    for _ in range(100):
        if control.command("PING") != "PONG":
            raise RegressionError("server did not reply to PING")


def run_direct_kill(server: Path) -> None:
    with redis_server(server) as port:
        control = RedisConnection(port, timeout=30.0)
        victims: list[RedisConnection] = []
        try:
            victims, ids = send_residual_pipelines(port, "hol-direct-kill", LIFETIME_PIPELINE)
            time.sleep(0.05)
            if control.command("CLIENT", "KILL", "ID", ids[0], "SKIPME", "no") != "1":
                raise RegressionError("CLIENT KILL did not kill the residual victim")
            assert_server_responsive(control)
        finally:
            for victim in victims:
                victim.close()
            control.close()


def run_async_close(server: Path) -> None:
    username = "hol_residual_user"
    password = "hol-residual-password"
    with redis_server(server) as port:
        control = RedisConnection(port, timeout=30.0)
        victims: list[RedisConnection] = []
        try:
            if control.command("ACL", "SETUSER", username, "on", f">{password}", "allcommands", "allkeys") != "OK":
                raise RegressionError("ACL SETUSER failed")
            payload = residual_payload("hol-async-close", LIFETIME_PIPELINE)
            for _ in range(RESIDUAL_CLIENTS):
                victim = RedisConnection(port, timeout=30.0)
                if victim.command("AUTH", username, password) != "OK":
                    raise RegressionError("victim authentication failed")
                victims.append(victim)
            for victim in victims:
                victim.send_raw(payload)
            time.sleep(0.05)
            if control.command("ACL", "DELUSER", username) != "1":
                raise RegressionError("ACL DELUSER did not remove the residual user")
            assert_server_responsive(control)
        finally:
            for victim in victims:
                victim.close()
            control.close()


def run_reentrant_residual(server: Path, module: Path) -> tuple[int, int]:
    key = "hol-reentrant-residual"
    # The blocker reaches its third command after two decoded batches. Its 47
    # peers retain multiple turns each from a 256-command pipeline. They are
    # sent while the blocker sleeps and generate no further socket event once
    # its nested event loop starts, leaving more than 64 queued scheduler turns.
    expected = (REENTRANT_CLIENTS - 1) * REENTRANT_PIPELINE + 2
    with redis_server(server, module) as port:
        victims, _ = open_residual_clients(port, REENTRANT_CLIENTS)
        blocker = victims[0]
        try:
            blocker.send_raw(
                resp_command(("INCR", key))
                + resp_command(("INCR", key))
                + resp_command(("HOLRESIDUAL.BLOCK", "1000", key, str(expected)))
            )
            # Give the first queue entry time to enter the blocked command,
            # then stage all peer input before its nested event loop begins.
            time.sleep(0.05)
            payload = residual_payload(key, REENTRANT_PIPELINE)
            for victim in victims[1:]:
                victim.send_raw(payload)
            try:
                int(blocker.read())
                int(blocker.read())
                progress = blocker.read()
                before_text, first_after_text, after_text = progress.split(":", 2)
                before, first_after, after = int(before_text), int(first_after_text), int(after_text)
            except ValueError as exc:
                raise RegressionError("malformed nested event-loop progress") from exc
            if before != 2:
                raise RegressionError(
                    f"blocked probe ran {before - 2} peer commands before entering the nested event loop"
                )
            if first_after <= before:
                raise RegressionError("first nested event-loop pass made no scheduler progress")
            if after != expected:
                raise RegressionError(
                    f"nested event-loop probe completed {after}, expected {expected} commands before return"
                )
            return before, after
        finally:
            for victim in victims:
                victim.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--module", type=Path, required=True)
    args = parser.parse_args()
    args.server = args.server.resolve()
    args.module = args.module.resolve()
    if not args.server.is_file():
        raise RegressionError(f"missing server binary: {args.server}")
    if not args.module.is_file():
        raise RegressionError(f"missing regression module: {args.module}")

    run_direct_kill(args.server)
    run_async_close(args.server)
    before, after = run_reentrant_residual(args.server, args.module)
    print(f"iothread-residual-regressions: PASS (reentrant_commands={before}->{after})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
