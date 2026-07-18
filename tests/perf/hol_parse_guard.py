#!/usr/bin/env python3
"""Guard workloads for fully buffered high-arity RESP parser continuations."""

import argparse
import json
import time

import hol_parse_turn as hol


def stop_pingers(pingers):
    for pinger in pingers:
        if pinger.thread.is_alive():
            pinger.stop()


def buffer_high_arity_clients(control, port, prefix, count, payload):
    attackers = []
    gates = []
    buffered_sizes = []
    try:
        for index in range(count):
            attacker = hol.connect(port, timeout=20)
            attackers.append(attacker)
            name = "%s-%d" % (prefix, index)
            gate = "%s-gate-%d" % (prefix, index)
            fields = hol.block_and_buffer(control, attacker, name, gate, payload)
            buffered = int(fields["qbuf"])
            if buffered < len(payload):
                raise hol.BenchmarkError("high-arity request was not wholly buffered")
            gates.append(gate)
            buffered_sizes.append(buffered)
        return attackers, gates, buffered_sizes
    except Exception:
        for attacker in attackers:
            attacker.close()
        raise


def run_high_arity_phase(binary, rounds, arguments, with_pings):
    server = hol.RedisServer(binary, io_threads=1, trace=False)
    control = None
    attackers = []
    pingers = []
    try:
        control = hol.connect(server.port, timeout=20)
        payload = hol.make_high_arity_ping(arguments)
        attackers, gates, buffered_sizes = buffer_high_arity_clients(
            control, server.port, "high-arity-guard", rounds, payload
        )
        if with_pings:
            pingers = [hol.Pinger(server.port) for _ in range(4)]
            for pinger in pingers:
                pinger.start()

        start_cpu_ns = hol.server_cpu_ns(server.process.pid)
        phase_start_ns = time.perf_counter_ns()
        completions_ns = []
        for attacker, gate in zip(attackers, gates):
            start_ns = time.perf_counter_ns()
            hol.expect_integer(control.command("LPUSH %s v" % gate), 1)
            hol.expect_unblocked_pipeline(attacker, gate)
            completions_ns.append(time.perf_counter_ns() - start_ns)
        elapsed_ns = time.perf_counter_ns() - phase_start_ns
        used_cpu_ns = hol.server_cpu_ns(server.process.pid) - start_cpu_ns

        for index, (attacker, gate) in enumerate(zip(attackers, gates)):
            fields = hol.client_fields(control, "high-arity-guard-%d" % index)
            if not fields or int(fields.get("qbuf", "-1")) != 0:
                raise hol.BenchmarkError("high-arity client did not drain: %s" % gate)
        if elapsed_ns <= 0:
            raise hol.BenchmarkError("high-arity phase had non-positive duration")

        prefix = "high_arity_50k_with_small_pings" if with_pings else "high_arity_50k_alone"
        return {
            prefix + "_completion_p99_us": hol.percentile(completions_ns, 0.99) / 1000.0,
            prefix + "_ops_per_sec": rounds * 1_000_000_000.0 / elapsed_ns,
            prefix + "_server_cpu_ns_per_request": used_cpu_ns / rounds,
            prefix + "_qbuf_min_bytes": min(buffered_sizes),
        }
    finally:
        stop_pingers(pingers)
        for attacker in attackers:
            attacker.close()
        if control:
            control.close()
        server.cleanup()


def run_competing_backlog(binary, arguments, commands_per_client):
    server = hol.RedisServer(binary, io_threads=1, trace=False)
    control = None
    attackers = []
    pingers = []
    try:
        control = hol.connect(server.port, timeout=20)
        payload = hol.make_high_arity_ping(arguments) * commands_per_client
        names = []
        gates = []
        buffered_sizes = []
        for index in range(16):
            name = "guard-backlog-%d" % index
            gate = "guard-backlog-gate-%d" % index
            attacker = hol.connect(server.port, timeout=20)
            attackers.append(attacker)
            fields = hol.block_and_buffer(control, attacker, name, gate, payload)
            buffered = int(fields["qbuf"])
            if buffered < len(payload):
                raise hol.BenchmarkError("competing request was not wholly buffered")
            names.append(name)
            gates.append(gate)
            buffered_sizes.append(buffered)

        pingers = [hol.Pinger(server.port) for _ in range(4)]
        for pinger in pingers:
            pinger.start()
            pinger.collect.set()

        start_cpu_ns = hol.server_cpu_ns(server.process.pid)
        phase_start_ns = time.perf_counter_ns()
        control.send(
            b"".join(("LPUSH %s v\r\n" % gate).encode("ascii") for gate in gates)
        )
        for _ in gates:
            hol.expect_integer(control.read(), 1)
        for attacker, gate in zip(attackers, gates):
            hol.expect_unblocked_pipeline(attacker, gate, commands_per_client)
        elapsed_ns = time.perf_counter_ns() - phase_start_ns
        used_cpu_ns = hol.server_cpu_ns(server.process.pid) - start_cpu_ns

        for pinger in pingers:
            pinger.collect.clear()
            pinger.stop()
        values_ns = [value for pinger in pingers for value in pinger.values_ns]
        if len(values_ns) < 16:
            raise hol.BenchmarkError(
                "only %d PING replies completed during competing backlog" % len(values_ns)
            )
        for name in names:
            fields = hol.client_fields(control, name)
            if not fields or int(fields.get("qbuf", "-1")) != 0:
                raise hol.BenchmarkError("competing client did not drain: %s" % name)
        if elapsed_ns <= 0:
            raise hol.BenchmarkError("competing phase had non-positive duration")

        request_count = len(attackers) * commands_per_client
        return {
            "competing_16_high_arity_small_ping_p99_us": hol.percentile(values_ns, 0.99) / 1000.0,
            "competing_16_high_arity_drain_ops_per_sec": request_count * 1_000_000_000.0 / elapsed_ns,
            "competing_16_high_arity_server_cpu_ns_per_request": used_cpu_ns / request_count,
            "competing_16_high_arity_ping_samples": len(values_ns),
            "competing_16_high_arity_qbuf_min_bytes": min(buffered_sizes),
        }
    finally:
        stop_pingers(pingers)
        for attacker in attackers:
            attacker.close()
        if control:
            control.close()
        server.cleanup()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", required=True)
    parser.add_argument(
        "--mode",
        required=True,
        choices=("high-alone", "high-with-pings", "competing-backlog"),
    )
    parser.add_argument("--arguments", type=int, default=50000)
    parser.add_argument("--rounds", type=int, default=64)
    parser.add_argument("--commands-per-client", type=int, default=4)
    args = parser.parse_args()

    hol.isolate_benchmark_processes()
    if args.mode == "high-alone":
        metrics = run_high_arity_phase(args.server, args.rounds, args.arguments, False)
    elif args.mode == "high-with-pings":
        metrics = run_high_arity_phase(args.server, args.rounds, args.arguments, True)
    else:
        metrics = run_competing_backlog(args.server, args.arguments, args.commands_per_client)
    for metric, value in metrics.items():
        print(json.dumps({"metric": metric, "value": value}, separators=(",", ":")))


if __name__ == "__main__":
    main()
