#!/usr/bin/env python3
"""Emit the mixed workload's native bulk-latency budget as proof JSONL."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys


# The baseline's largest recorded p99 was 6,279us. The adjacent bulk class may
# use at most a 20% tail budget while the primary short-request tail is reduced.
BULK_P99_BUDGET_US = 7_500.0
# The p50 ceiling remains below 1.31x the baseline median, preventing a broad
# bulk slowdown while retaining headroom for the intended tail trade-off.
BULK_P50_BUDGET_US = 5_000.0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bench", type=Path, required=True)
    parser.add_argument("--src-dir", type=Path, required=True)
    parser.add_argument("--module", type=Path, required=True)
    args = parser.parse_args()

    result = subprocess.run(
        [
            sys.executable,
            str(args.bench),
            "--src-dir",
            str(args.src_dir),
            "--module",
            str(args.module),
            "--mode",
            "mixed",
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode:
        sys.stderr.write(result.stderr)
        sys.stderr.write(result.stdout)
        return result.returncode

    metrics: dict[str, float] = {}
    for line in result.stdout.splitlines():
        try:
            sample = json.loads(line)
        except json.JSONDecodeError:
            continue
        metric = sample.get("metric")
        value = sample.get("value")
        if metric in {"bulk_pipeline_p50_latency_us", "bulk_pipeline_p99_latency_us"}:
            if not isinstance(value, (int, float)):
                raise RuntimeError(f"non-numeric {metric!r} sample")
            metrics[metric] = float(value)

    missing = {"bulk_pipeline_p50_latency_us", "bulk_pipeline_p99_latency_us"} - metrics.keys()
    if missing:
        raise RuntimeError(f"mixed workload did not emit {sorted(missing)!r}")

    within_budget = (
        metrics["bulk_pipeline_p50_latency_us"] <= BULK_P50_BUDGET_US
        and metrics["bulk_pipeline_p99_latency_us"] <= BULK_P99_BUDGET_US
    )
    for metric in ("bulk_pipeline_p50_latency_us", "bulk_pipeline_p99_latency_us"):
        print(json.dumps({"metric": metric, "value": metrics[metric]}, separators=(",", ":")))
    print(
        json.dumps(
            {"metric": "bulk_pipeline_latency_budget_pass", "value": int(within_budget)},
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
