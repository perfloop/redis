#!/usr/bin/env bash
# Verify sustained short-client latency with full-span co-resident bulk traffic.
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

workdir=$(mktemp -d "${TMPDIR:-/tmp}/redis-iothread-verified-sustained.XXXXXX")
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
    echo "iothread-verified-sustained: $*" >&2
    exit 1
}

require_number() {
    local value=$1
    local name=$2
    [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "missing numeric $name"
}

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

microseconds() {
    awk -v milliseconds="$1" 'BEGIN { printf "%.0f", milliseconds * 1000 }'
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

command_calls() {
    local command=$1
    "$cli" -h 127.0.0.1 -p "$port" --raw info commandstats |
        awk -F: -v metric="cmdstat_${command}" '$1 == metric {
            split($2, fields, ",")
            sub(/^calls=/, "", fields[1])
            gsub(/\r/, "", fields[1])
            print fields[1]
            exit
        }'
}

assert_bulk_running() {
    local phase=$1
    local state

    kill -0 "$bulk_pid" >/dev/null 2>&1 || fail "bulk workload ended $phase"
    state=$(ps -o stat= -p "$bulk_pid" 2>/dev/null | tr -d '[:space:]')
    [[ -n "$state" && "$state" != Z* ]] || fail "bulk workload is not running $phase"
}

stop_bulk() {
    [[ -n "$bulk_pid" ]] || return
    kill "$bulk_pid" >/dev/null 2>&1 || true
    wait "$bulk_pid" >/dev/null 2>&1 || true
    bulk_pid=""
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

# The bulk generator loops until this script stops it after the GET arm. Its
# status and per-interval counters make an early termination a hard failure
# rather than silently measuring isolated GETs.
run_co_resident_stream() {
    local bulk_csv=$1
    local short_csv=$2
    local label=$3
    local active set_before set_after handoff_before handoff_after
    local set_during handoff_during

    active=$(info_metric io_threads_active)
    [[ "$active" == "1" ]] || fail "$label io_threads_active was ${active:-missing}, expected 1"
    "$cli" -h 127.0.0.1 -p "$port" set hol:short value >/dev/null

    "$benchmark" -h 127.0.0.1 -p "$port" -c 32 -l -P 128 --threads 2 \
        --csv SET hol:bulk __rand_int__ >"$bulk_csv" 2>"$workdir/$label-bulk.stderr" &
    bulk_pid=$!
    sleep 0.20
    assert_bulk_running "before $label short requests started"

    set_before=$(command_calls set)
    handoff_before=$(info_metric io_threaded_total_prefetch_entries)
    require_number "$set_before" "$label bulk SET calls before short requests"
    require_number "$handoff_before" "$label handoff entries before short requests"

    "$benchmark" -h 127.0.0.1 -p "$port" -c 1 -n 500000 -P 1 --threads 1 \
        --csv GET hol:short >"$short_csv" 2>"$workdir/$label-short.stderr"

    set_after=$(command_calls set)
    handoff_after=$(info_metric io_threaded_total_prefetch_entries)
    require_number "$set_after" "$label bulk SET calls after short requests"
    require_number "$handoff_after" "$label handoff entries after short requests"
    assert_bulk_running "when $label short requests completed"

    set_during=$((set_after - set_before))
    handoff_during=$((handoff_after - handoff_before))
    if (( set_during < 1000 || handoff_during <= 0 )); then
        fail "$label bulk/handoff processing did not advance during short requests"
    fi

    if [[ "$label" == "endpoint" ]]; then
        endpoint_bulk_set_calls_during_short=$set_during
        endpoint_handoff_entries_during_short=$handoff_during
    else
        probe_bulk_set_calls_during_short=$set_during
        probe_handoff_entries_during_short=$handoff_during
    fi

    stop_bulk
}

normal_bulk_csv="$workdir/endpoint-bulk.csv"
normal_short_csv="$workdir/endpoint-short.csv"
unset PERFLOOP_IOTHREAD_HOL_METRICS
start_server "$server"
run_co_resident_stream "$normal_bulk_csv" "$normal_short_csv" endpoint
endpoint_short_p99_ms=$(csv_field "$normal_short_csv" 7)
require_number "$endpoint_short_p99_ms" verified_sustained_mixed_short_get_p99_latency_ms
stop_server

metrics_file="$workdir/handoff-metrics"
export PERFLOOP_IOTHREAD_HOL_METRICS="$metrics_file"
probe_bulk_csv="$workdir/probe-bulk.csv"
probe_short_csv="$workdir/probe-short.csv"
start_server "$instrumented_server"
run_co_resident_stream "$probe_bulk_csv" "$probe_short_csv" probe

# Trigger another handoff callback so the recorder finalizes the preceding
# invocation before the shutdown snapshot is written.
"$cli" -h 127.0.0.1 -p "$port" get hol:flush >/dev/null
stop_server
wait_for_metrics_file "$metrics_file"

short_queue_samples=$(instrumentation_metric "$metrics_file" short_queue_delay_samples)
short_queue_p50=$(instrumentation_metric "$metrics_file" short_queue_delay_p50_us)
short_queue_p99=$(instrumentation_metric "$metrics_file" short_queue_delay_p99_us)
require_number "$short_queue_samples" verified_sustained_short_queue_delay_samples
require_number "$short_queue_p50" verified_sustained_short_queue_delay_p50_us
require_number "$short_queue_p99" verified_sustained_short_queue_delay_p99_us
if (( short_queue_samples < 1000 )); then
    fail "instrumentation did not observe enough sustained short-client queue samples"
fi

printf '{"metric":"verified_sustained_mixed_short_get_p99_latency_us","value":%s}\n' "$(microseconds "$endpoint_short_p99_ms")"
printf '{"metric":"verified_sustained_short_queue_delay_p50_us","value":%s}\n' "$short_queue_p50"
printf '{"metric":"verified_sustained_short_queue_delay_p99_us","value":%s}\n' "$short_queue_p99"
printf '{"metric":"verified_sustained_endpoint_bulk_set_calls_during_short","value":%s}\n' "$endpoint_bulk_set_calls_during_short"
printf '{"metric":"verified_sustained_endpoint_handoff_entries_during_short","value":%s}\n' "$endpoint_handoff_entries_during_short"
printf '{"metric":"verified_sustained_probe_bulk_set_calls_during_short","value":%s}\n' "$probe_bulk_set_calls_during_short"
printf '{"metric":"verified_sustained_probe_handoff_entries_during_short","value":%s}\n' "$probe_handoff_entries_during_short"
