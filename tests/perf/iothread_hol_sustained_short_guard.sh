#!/usr/bin/env bash
# Guard sustained fast-client latency and record matching handoff queue delay.
set -euo pipefail

if [[ -n "${PERFLOOP_REPO_ROOT:-}" ]]; then
    root=$PERFLOOP_REPO_ROOT
else
    root=$(cd -- "$(dirname -- "$0")/../.." && pwd)
fi
cd "$root"

server="$root/src/redis-server"
instrumented_server="$root/src/redis-server.hol-instrumented"
cli="$root/src/redis-cli"
benchmark="$root/src/redis-benchmark"
for binary in "$server" "$instrumented_server" "$cli" "$benchmark"; do
    [[ -x "$binary" ]] || { echo "missing Redis build artifact: $binary" >&2; exit 1; }
done

workdir=$(mktemp -d "${TMPDIR:-/tmp}/redis-iothread-sustained-short-guard.XXXXXX")
server_pid=""
bulk_pid=""
port=""

cleanup() {
    if [[ -n "$bulk_pid" ]]; then
        kill "$bulk_pid" >/dev/null 2>&1 || true
        wait "$bulk_pid" >/dev/null 2>&1 || true
    fi
    if [[ -n "$server_pid" ]]; then
        "$cli" -h 127.0.0.1 -p "$port" shutdown nosave >/dev/null 2>&1 || true
        kill "$server_pid" >/dev/null 2>&1 || true
        wait "$server_pid" >/dev/null 2>&1 || true
    fi
    rm -rf "$workdir"
}
trap cleanup EXIT

fail() {
    echo "iothread-sustained-short-guard: $*" >&2
    exit 1
}

require_number() {
    local value=$1
    local name=$2
    [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "missing numeric $name"
}

metric_value() {
    local file=$1
    local metric=$2
    awk -v metric="$metric" '
        $0 ~ ("^\\{\\\"metric\\\":\\\"" metric "\\\",\\\"value\\\":") {
            value = $0
            sub(/^.*"value":/, "", value)
            sub(/}$/, "", value)
            print value
            exit
        }
    ' "$file"
}

start_server() {
    local binary=$1
    local attempt wait_iteration

    for attempt in $(seq 1 20); do
        port=$((20000 + RANDOM % 20000))
        "$binary" --bind 127.0.0.1 --port "$port" --save '' --appendonly no \
            --io-threads 2 --dir "$workdir" --dbfilename dump.rdb \
            --logfile "$workdir/redis-$port.log" >"$workdir/redis-$port.stdout" 2>&1 &
        server_pid=$!

        for wait_iteration in $(seq 1 100); do
            if "$cli" -h 127.0.0.1 -p "$port" ping >/dev/null 2>&1; then
                return
            fi
            if ! kill -0 "$server_pid" >/dev/null 2>&1; then
                break
            fi
            sleep 0.02
        done

        wait "$server_pid" >/dev/null 2>&1 || true
        server_pid=""
    done

    fail "server did not start on a free loopback port"
}

stop_server() {
    [[ -n "$server_pid" ]] || return
    "$cli" -h 127.0.0.1 -p "$port" shutdown nosave >/dev/null 2>&1 || \
        kill "$server_pid" >/dev/null 2>&1 || true
    wait "$server_pid" >/dev/null 2>&1 || true
    server_pid=""
}

info_metric() {
    local metric=$1
    "$cli" -h 127.0.0.1 -p "$port" --raw info all |
        awk -F: -v metric="$metric" '$1 == metric {gsub(/\r/, "", $2); print $2; exit}'
}

wait_for_metrics_file() {
    local metrics_file=$1
    local iteration

    for iteration in $(seq 1 100); do
        [[ -s "$metrics_file" ]] && return
        sleep 0.02
    done
    fail "instrumented server did not write $metrics_file"
}

instrumentation_metric() {
    local metrics_file=$1
    local metric=$2
    awk -F= -v metric="$metric" '$1 == metric { print $2; exit }' "$metrics_file"
}

# The normal half is the established long, co-resident P=128 bulk plus P=1
# fast-client workload. Capture its endpoint p99 without recorder overhead.
normal_metrics="$workdir/normal.jsonl"
unset PERFLOOP_IOTHREAD_HOL_METRICS
PERFLOOP_REPO_ROOT="$root" "$root/tests/perf/iothread_hol_sustained_bulk.sh" >"$normal_metrics"
short_p99=$(metric_value "$normal_metrics" sustained_mixed_short_get_p99_latency_us)
require_number "$short_p99" sustained_mixed_short_get_p99_latency_us

# Repeat the same process counts, pipeline depth, command mix, and run lengths
# with the recorder twin. The short/bulk keys select the recorder's two classes.
metrics_file="$workdir/handoff-metrics"
export PERFLOOP_IOTHREAD_HOL_METRICS="$metrics_file"
start_server "$instrumented_server"
active=$(info_metric io_threads_active)
[[ "$active" == "1" ]] || fail "io_threads_active was ${active:-missing}, expected 1"
"$cli" -h 127.0.0.1 -p "$port" set hol:short value >/dev/null

bulk_csv="$workdir/bulk.csv"
short_csv="$workdir/short.csv"
"$benchmark" -h 127.0.0.1 -p "$port" -c 32 -n 12000000 -P 128 --threads 2 \
    --csv SET hol:bulk __rand_int__ >"$bulk_csv" 2>"$workdir/bulk.stderr" &
bulk_pid=$!
sleep 0.20
kill -0 "$bulk_pid" >/dev/null 2>&1 || fail "bulk workload ended before short requests started"
"$benchmark" -h 127.0.0.1 -p "$port" -c 1 -n 500000 -P 1 --threads 1 \
    --csv GET hol:short >"$short_csv" 2>"$workdir/short.stderr"
wait "$bulk_pid"
bulk_pid=""

# Start another handoff callback before shutdown so the recorder finalizes the
# preceding invocation rather than only its destructor's last partial sample.
"$cli" -h 127.0.0.1 -p "$port" get hol:flush >/dev/null
stop_server
wait_for_metrics_file "$metrics_file"

short_queue_samples=$(instrumentation_metric "$metrics_file" short_queue_delay_samples)
short_queue_p50=$(instrumentation_metric "$metrics_file" short_queue_delay_p50_us)
short_queue_p99=$(instrumentation_metric "$metrics_file" short_queue_delay_p99_us)
require_number "$short_queue_samples" short_queue_delay_samples
require_number "$short_queue_p50" sustained_short_queue_delay_p50_us
require_number "$short_queue_p99" sustained_short_queue_delay_p99_us
if (( short_queue_samples < 1000 || short_queue_p99 == 0 )); then
    fail "instrumentation did not observe sustained short-client queue delay"
fi

printf '{"metric":"sustained_mixed_short_get_p99_latency_us","value":%s}\n' "$short_p99"
printf '{"metric":"sustained_short_queue_delay_p50_us","value":%s}\n' "$short_queue_p50"
printf '{"metric":"sustained_short_queue_delay_p99_us","value":%s}\n' "$short_queue_p99"
