#!/usr/bin/env bash
# Isolated P=128 bulk guard for the threaded-I/O handoff experiment.
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

workdir=$(mktemp -d "${TMPDIR:-/tmp}/redis-iothread-isolated-bulk.XXXXXX")
server_pid=""
port=""

cleanup() {
    if [[ -n "$server_pid" ]]; then
        "$cli" -h 127.0.0.1 -p "$port" shutdown nosave >/dev/null 2>&1 || true
        kill "$server_pid" >/dev/null 2>&1 || true
        wait "$server_pid" >/dev/null 2>&1 || true
    fi
    rm -rf "$workdir"
}
trap cleanup EXIT

started=0
for attempt in $(seq 1 20); do
    port=$((20000 + RANDOM % 20000))
    "$server" --bind 127.0.0.1 --port "$port" --save '' --appendonly no \
        --io-threads 2 --dir "$workdir" --dbfilename dump.rdb \
        --logfile "$workdir/redis-$port.log" >"$workdir/redis-$port.stdout" 2>&1 &
    server_pid=$!
    for wait_iteration in $(seq 1 100); do
        if "$cli" -h 127.0.0.1 -p "$port" ping >/dev/null 2>&1; then
            started=1
            break
        fi
        if ! kill -0 "$server_pid" >/dev/null 2>&1; then
            break
        fi
        sleep 0.02
    done
    (( started )) && break
    kill "$server_pid" >/dev/null 2>&1 || true
    wait "$server_pid" >/dev/null 2>&1 || true
    server_pid=""
done

(( started )) || { echo "server did not start on a free loopback port" >&2; exit 1; }

active=$("$cli" -h 127.0.0.1 -p "$port" --raw info all |
    awk -F: '$1 == "io_threads_active" {gsub(/\r/, "", $2); print $2; exit}')
[[ "$active" == "1" ]] || { echo "io_threads_active was ${active:-missing}, expected 1" >&2; exit 1; }

csv="$workdir/bulk.csv"
"$benchmark" -h 127.0.0.1 -p "$port" -c 32 -n 3000000 -P 128 --threads 2 \
    --csv SET hol:bulk __rand_int__ >"$csv"
rps=$(awk -F, '
    /^"test",/ { next }
    /^"/ {
        value = $2
        gsub(/"/, "", value)
        gsub(/\r/, "", value)
        print value
        exit
    }
' "$csv")
[[ "$rps" =~ ^[0-9]+([.][0-9]+)?$ ]] || { echo "missing numeric bulk_set_ops_per_sec" >&2; exit 1; }
printf '{"metric":"isolated_bulk_set_ops_per_sec","value":%s}\n' "$rps"
