#!/usr/bin/env python3
"""Emit validated IO-thread fairness guard measurements."""

import argparse
import json
import math
import sys

import iothread_fairness as fairness
import iothread_fairness_guards as guards


EXPECTED_METRICS = {
    "below_quantum_short_ping_p99_us": "short_ping_p99_us",
    "mixed_bulk_ops_per_sec": "bulk_ops_per_sec",
    "one_lane_bulk_ops_per_sec": "bulk_ops_per_sec",
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default="src/redis-server")
    parser.add_argument("--server-cpus", default="")
    parser.add_argument("--metric", choices=tuple(EXPECTED_METRICS), required=True)
    arguments = parser.parse_args()

    metric, value = guards.run_metric(arguments)
    if metric != EXPECTED_METRICS[arguments.metric]:
        raise fairness.BenchmarkError(
            "guard selector %s emitted %s instead of %s"
            % (arguments.metric, metric, EXPECTED_METRICS[arguments.metric])
        )
    if not math.isfinite(value) or value <= 0:
        raise fairness.BenchmarkError("guard selector %s emitted an invalid value %r" % (arguments.metric, value))
    print(json.dumps({"metric": metric, "value": value}))


if __name__ == "__main__":
    try:
        main()
    except (fairness.BenchmarkError, OSError) as error:
        print("iothread fairness guard benchmark failed: %s" % error, file=sys.stderr)
        sys.exit(1)
