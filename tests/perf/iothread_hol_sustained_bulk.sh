#!/usr/bin/env bash
# Longer co-resident P=128 bulk sample for the threaded-I/O HOL guard.
set -euo pipefail

if [[ -n "${PERFLOOP_REPO_ROOT:-}" ]]; then
    root=$PERFLOOP_REPO_ROOT
else
    root=$(cd -- "$(dirname -- "$0")/../.." && pwd)
fi
cd "$root"

server="$root/src/redis-server"
cli="$root/src/redis-cli"
benchmark="$root/src/redis-benchmark"
for binary in "$server" "$cli" "$benchmark"; do
    [[ -x "$binary" ]] || { echo "missing Redis build artifact: $binary" >&2; exit 1; }
done

workdir=$(mktemp -d "${TMPDIR:-/tmp}/redis-iothread-sustained.XXXXXX")
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
    echo "iothread-sustained: $*" >&2
    exit 1
}

for attempt in $(seq 1 20); do
    port=$((20000 + RANDOM % 20000))
    "$server" --bind 127.0.0.1 --port "$port" --save '' --appendonly no \
        --io-threads 2 --dir "$workdir" --dbfilename dump.rdb \
        --logfile "$workdir/redis-$port.log" >"$workdir/redis-$port.stdout" 2>&1 &
    server_pid=$!
    for wait_iteration in $(seq 1 100); do
        if "$cli" -h 127.0.0.1 -p "$port" ping >/dev/null 2>&1; then
            break 2
        fi
        if ! kill -0 "$server_pid" >/dev/null 2>&1; then
            break
        fi
        sleep 0.02
    done
    wait "$server_pid" >/dev/null 2>&1 || true
    server_pid=""
done
[[ -n "$server_pid" ]] || fail "server did not start on a free loopback port"

active=$("$cli" -h 127.0.0.1 -p "$port" --raw info all | awk -F: '$1 == "io_threads_active" {gsub(/\r/, "", $2); print $2; exit}')
[[ "$active" == "1" ]] || fail "io_threads_active was ${active:-missing}, expected 1"
"$cli" -h 127.0.0.1 -p "$port" set hol:sustained-short value >/dev/null

bulk_csv="$workdir/bulk.csv"
short_csv="$workdir/short.csv"
# The 12M bulk request stream keeps the 32 P=128 clients co-resident well
# past the 500k independent GET stream, reducing start-up timing variance
# without combining multiple samples into this one emitted observation.
"$benchmark" -h 127.0.0.1 -p "$port" -c 32 -n 12000000 -P 128 --threads 2 \
    --csv SET hol:sustained-bulk __rand_int__ >"$bulk_csv" 2>"$workdir/bulk.stderr" &
bulk_pid=$!
sleep 0.20
kill -0 "$bulk_pid" >/dev/null 2>&1 || fail "bulk workload ended before short requests started"
"$benchmark" -h 127.0.0.1 -p "$port" -c 1 -n 500000 -P 1 --threads 1 \
    --csv GET hol:sustained-short >"$short_csv" 2>"$workdir/short.stderr"
wait "$bulk_pid"
bulk_pid=""

csv_field() {
    local csv=$1
    local field=$2
    awk -F, -v field="$field" '
        /^"test",/ { next }
        /^"/ {
            value = $field
            gsub(/"/, "", value)
            gsub(/\r/, "", value)
            print value
            exit
        }
    ' "$csv"
}

require_number() {
    local value=$1
    local name=$2
    [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "missing numeric $name"
}

microseconds() {
    awk -v milliseconds="$1" 'BEGIN { printf "%.0f", milliseconds * 1000 }'
}

bulk_p99_ms=$(csv_field "$bulk_csv" 7)
bulk_rps=$(csv_field "$bulk_csv" 2)
short_p99_ms=$(csv_field "$short_csv" 7)
require_number "$bulk_p99_ms" sustained_mixed_bulk_set_p99_latency_ms
require_number "$bulk_rps" sustained_mixed_bulk_set_ops_per_sec
require_number "$short_p99_ms" sustained_mixed_short_get_p99_latency_ms

printf '{"metric":"sustained_mixed_bulk_set_p99_latency_us","value":%s}\n' "$(microseconds "$bulk_p99_ms")"
printf '{"metric":"sustained_mixed_bulk_set_ops_per_sec","value":%s}\n' "$bulk_rps"
printf '{"metric":"sustained_mixed_short_get_p99_latency_us","value":%s}\n' "$(microseconds "$short_p99_ms")"
