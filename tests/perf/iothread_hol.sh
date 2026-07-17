#!/usr/bin/env bash
# Exercise the threaded-I/O handoff with one worker, pipelined bulk traffic,
# and an independent single-command client.
#
# Usage:
#   tests/perf/iothread_hol.sh mixed
#   tests/perf/iothread_hol.sh bulk
#   tests/perf/iothread_hol.sh check

set -euo pipefail

mode=${1:-}
case "$mode" in
    mixed|bulk|check) ;;
    *)
        echo "usage: $0 {mixed|bulk|check}" >&2
        exit 2
        ;;
esac

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
for binary in "$server" "$cli" "$benchmark"; do
    if [[ ! -x "$binary" ]]; then
        echo "missing Redis build artifact: $binary" >&2
        exit 1
    fi
done

workdir=$(mktemp -d "${TMPDIR:-/tmp}/redis-iothread-hol.XXXXXX")
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
    echo "iothread-hol: $*" >&2
    exit 1
}

start_server() {
    local server_binary=${1:-$server}
    local attempt wait_iteration

    for attempt in $(seq 1 20); do
        port=$((20000 + RANDOM % 20000))
        "$server_binary" --bind 127.0.0.1 --port "$port" --save '' --appendonly no \
            --io-threads 2 --dir "$workdir" --dbfilename dump.rdb \
            --logfile "$workdir/redis-$port.log" >"$workdir/redis-$port.stdout" 2>&1 &
        server_pid=$!

        for wait_iteration in $(seq 1 100); do
            if "$cli" -h 127.0.0.1 -p "$port" ping >/dev/null 2>&1; then
                return 0
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
    if [[ -z "$server_pid" ]]; then
        return
    fi

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

require_threaded_io() {
    local active
    active=$(info_metric io_threads_active)
    [[ "$active" == "1" ]] || fail "io_threads_active was ${active:-missing}, expected 1"
}

wait_for_metrics_file() {
    local metrics_file=$1
    local iteration

    for iteration in $(seq 1 100); do
        if [[ -s "$metrics_file" ]]; then
            return
        fi
        sleep 0.02
    done
    fail "instrumented server did not write $metrics_file"
}

instrumentation_metric() {
    local metrics_file=$1
    local metric=$2
    awk -F= -v metric="$metric" '$1 == metric { print $2; exit }' "$metrics_file"
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

require_number() {
    local value=$1
    local name=$2
    [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "missing numeric $name"
}

milliseconds_to_microseconds() {
    local milliseconds=$1
    awk -v milliseconds="$milliseconds" 'BEGIN { printf "%.0f", milliseconds * 1000 }'
}

run_short_get() {
    local csv=$1
    "$benchmark" -h 127.0.0.1 -p "$port" -c 1 -n 10000 -P 1 --threads 1 \
        --csv GET hol:short >"$csv"
}

bulk_csv="$workdir/bulk.csv"
short_csv="$workdir/short.csv"
bulk_entries_during_short=""

run_bulk_and_short() {
    local entries_before entries_after

    "$benchmark" -h 127.0.0.1 -p "$port" -c 32 -n 3000000 -P 128 --threads 2 \
        --csv SET hol:bulk __rand_int__ >"$bulk_csv" 2>"$workdir/bulk.stderr" &
    bulk_pid=$!

    sleep 0.20
    if ! kill -0 "$bulk_pid" >/dev/null 2>&1; then
        fail "bulk workload ended before the short client was issued"
    fi

    entries_before=$(info_metric io_threaded_total_prefetch_entries)
    require_number "$entries_before" io_threaded_total_prefetch_entries
    run_short_get "$short_csv"
    entries_after=$(info_metric io_threaded_total_prefetch_entries)
    require_number "$entries_after" io_threaded_total_prefetch_entries

    bulk_entries_during_short=$((entries_after - entries_before))
    if (( bulk_entries_during_short <= 0 )); then
        fail "no threaded-I/O bulk commands were processed while short requests ran"
    fi

    wait "$bulk_pid"
    bulk_pid=""
}

emit_metric() {
    local metric=$1
    local value=$2
    printf '{"metric":"%s","value":%s}\n' "$metric" "$value"
}

run_measurement() {
    local control_csv="$workdir/control.csv"
    local metrics_file="$workdir/handoff-metrics"
    local control_p99_ms control_p99_us short_p50_ms short_p99_ms bulk_p50_ms bulk_p99_ms bulk_rps
    local short_p50_us short_p99_us bulk_p50_us bulk_p99_us ratio
    local normal_bulk_entries_during_short
    local short_queue_samples short_queue_p50 short_queue_p99 bulk_queue_samples bulk_queue_p50 bulk_queue_p99
    local drain_samples drain_initial_p99 drain_clients_p50 drain_clients_p99 drain_commands_p50 drain_commands_p99
    local drain_residual_p50 drain_residual_p99 drain_residual_max

    if [[ "$mode" == "mixed" && ! -x "$instrumented_server" ]]; then
        fail "missing instrumented Redis build artifact: $instrumented_server"
    fi

    # Measure user-visible latency with the normal production binary. The
    # instrumented twin below is a separate controlled probe for the handoff
    # mechanics, so recorder calls do not perturb the primary latency metric.
    unset PERFLOOP_IOTHREAD_HOL_METRICS
    if [[ "$mode" == "mixed" ]]; then
        start_server "$server"
        require_threaded_io
        "$cli" -h 127.0.0.1 -p "$port" set hol:short value >/dev/null
        run_short_get "$control_csv"
        control_p99_ms=$(csv_field "$control_csv" 7)
        require_number "$control_p99_ms" isolated_short_get_p99_latency_ms
        control_p99_us=$(milliseconds_to_microseconds "$control_p99_ms")
        stop_server
    fi

    start_server "$server"
    require_threaded_io
    "$cli" -h 127.0.0.1 -p "$port" set hol:short value >/dev/null
    run_bulk_and_short

    short_p50_ms=$(csv_field "$short_csv" 5)
    short_p99_ms=$(csv_field "$short_csv" 7)
    bulk_p50_ms=$(csv_field "$bulk_csv" 5)
    bulk_p99_ms=$(csv_field "$bulk_csv" 7)
    bulk_rps=$(csv_field "$bulk_csv" 2)
    normal_bulk_entries_during_short=$bulk_entries_during_short
    require_number "$short_p50_ms" short_get_p50_latency_ms
    require_number "$short_p99_ms" short_get_p99_latency_ms
    require_number "$bulk_p50_ms" bulk_set_p50_latency_ms
    require_number "$bulk_p99_ms" bulk_set_p99_latency_ms
    require_number "$bulk_rps" bulk_set_ops_per_sec
    stop_server

    short_p50_us=$(milliseconds_to_microseconds "$short_p50_ms")
    short_p99_us=$(milliseconds_to_microseconds "$short_p99_ms")
    bulk_p50_us=$(milliseconds_to_microseconds "$bulk_p50_ms")
    bulk_p99_us=$(milliseconds_to_microseconds "$bulk_p99_ms")

    if [[ "$mode" == "bulk" ]]; then
        emit_metric bulk_set_ops_per_sec "$bulk_rps"
        return
    fi

    export PERFLOOP_IOTHREAD_HOL_METRICS="$metrics_file"
    start_server "$instrumented_server"
    require_threaded_io
    "$cli" -h 127.0.0.1 -p "$port" set hol:short value >/dev/null
    run_bulk_and_short

    # Force a later handoff callback so the recorder finalizes the previous
    # drain invocation before the server writes its shutdown snapshot.
    "$cli" -h 127.0.0.1 -p "$port" get hol:flush >/dev/null
    stop_server
    wait_for_metrics_file "$metrics_file"

    short_queue_samples=$(instrumentation_metric "$metrics_file" short_queue_delay_samples)
    short_queue_p50=$(instrumentation_metric "$metrics_file" short_queue_delay_p50_us)
    short_queue_p99=$(instrumentation_metric "$metrics_file" short_queue_delay_p99_us)
    bulk_queue_samples=$(instrumentation_metric "$metrics_file" bulk_queue_delay_samples)
    bulk_queue_p50=$(instrumentation_metric "$metrics_file" bulk_queue_delay_p50_us)
    bulk_queue_p99=$(instrumentation_metric "$metrics_file" bulk_queue_delay_p99_us)
    drain_samples=$(instrumentation_metric "$metrics_file" drain_invocation_samples)
    drain_initial_p99=$(instrumentation_metric "$metrics_file" drain_initial_clients_p99)
    drain_clients_p50=$(instrumentation_metric "$metrics_file" drain_clients_per_invocation_p50)
    drain_clients_p99=$(instrumentation_metric "$metrics_file" drain_clients_per_invocation_p99)
    drain_commands_p50=$(instrumentation_metric "$metrics_file" drain_commands_per_invocation_p50)
    drain_commands_p99=$(instrumentation_metric "$metrics_file" drain_commands_per_invocation_p99)
    drain_residual_p50=$(instrumentation_metric "$metrics_file" drain_residual_clients_p50)
    drain_residual_p99=$(instrumentation_metric "$metrics_file" drain_residual_clients_p99)
    drain_residual_max=$(instrumentation_metric "$metrics_file" drain_residual_clients_max)
    for metric_name in short_queue_samples short_queue_p50 short_queue_p99 bulk_queue_samples bulk_queue_p50 bulk_queue_p99 drain_samples drain_initial_p99 drain_clients_p50 drain_clients_p99 drain_commands_p50 drain_commands_p99 drain_residual_p50 drain_residual_p99 drain_residual_max; do
        require_number "${!metric_name}" "$metric_name"
    done
    if (( short_queue_samples < 1000 || bulk_queue_samples < 1000 || drain_samples < 1000 )); then
        fail "instrumentation did not capture enough handoff samples"
    fi
    if (( short_queue_p99 == 0 || bulk_queue_p99 == 0 )); then
        fail "instrumentation did not observe queue delay"
    fi
    if (( drain_clients_p99 == 0 || drain_commands_p99 == 0 )); then
        fail "instrumentation did not observe clients and commands in a drain"
    fi

    ratio=$(awk -v mixed="$short_p99_us" -v isolated="$control_p99_us" \
        'BEGIN { if (isolated <= 0) exit 1; printf "%.6f", mixed / isolated }')
    require_number "$ratio" mixed_to_isolated_p99_ratio

    emit_metric short_get_p99_latency_us "$short_p99_us"
    emit_metric short_get_p50_latency_us "$short_p50_us"
    emit_metric isolated_short_get_p99_latency_us "$control_p99_us"
    emit_metric mixed_to_isolated_p99_ratio "$ratio"
    emit_metric bulk_set_p50_latency_us "$bulk_p50_us"
    emit_metric bulk_set_p99_latency_us "$bulk_p99_us"
    emit_metric bulk_set_ops_per_sec "$bulk_rps"
    emit_metric short_queue_delay_p50_us "$short_queue_p50"
    emit_metric short_queue_delay_p99_us "$short_queue_p99"
    emit_metric bulk_queue_delay_p50_us "$bulk_queue_p50"
    emit_metric bulk_queue_delay_p99_us "$bulk_queue_p99"
    emit_metric drain_initial_clients_p99 "$drain_initial_p99"
    emit_metric drain_clients_per_invocation_p50 "$drain_clients_p50"
    emit_metric drain_clients_per_invocation_p99 "$drain_clients_p99"
    emit_metric drain_commands_per_invocation_p50 "$drain_commands_p50"
    emit_metric drain_commands_per_invocation_p99 "$drain_commands_p99"
    emit_metric drain_residual_clients_p50 "$drain_residual_p50"
    emit_metric drain_residual_clients_p99 "$drain_residual_p99"
    emit_metric drain_residual_clients_max "$drain_residual_max"
    emit_metric bulk_prefetch_entries_during_short "$normal_bulk_entries_during_short"
}

run_correctness_check() {
    local expected actual entries short_reply
    local order_output="$workdir/order.out"
    local check_bulk_output="$workdir/check-bulk.csv"

    start_server
    require_threaded_io

    {
        for value in $(seq 0 511); do
            printf '*3\r\n$5\r\nRPUSH\r\n$11\r\nhol:ordered\r\n$%s\r\n%s\r\n' \
                "${#value}" "$value"
        done
    } | "$cli" -h 127.0.0.1 -p "$port" --pipe >"$order_output"
    grep -q 'errors: 0' "$order_output" || fail "ordered pipeline reported a Redis error"

    expected=$(seq 0 511 | paste -sd, -)
    actual=$("$cli" -h 127.0.0.1 -p "$port" --raw lrange hol:ordered 0 -1 | paste -sd, -)
    [[ "$actual" == "$expected" ]] || fail "per-client pipeline order changed"

    "$benchmark" -h 127.0.0.1 -p "$port" -c 16 -n 1000000 -P 128 --threads 2 \
        --csv SET hol:check-bulk __rand_int__ >"$check_bulk_output" 2>"$workdir/check-bulk.stderr" &
    bulk_pid=$!
    sleep 0.10
    if ! kill -0 "$bulk_pid" >/dev/null 2>&1; then
        fail "bulk traffic ended before the independent short request"
    fi

    short_reply=$(timeout 5 "$cli" -h 127.0.0.1 -p "$port" --raw ping)
    [[ "$short_reply" == "PONG" ]] || fail "short client did not complete during bulk traffic"
    wait "$bulk_pid"
    bulk_pid=""

    entries=$(info_metric io_threaded_total_prefetch_entries)
    require_number "$entries" io_threaded_total_prefetch_entries
    if (( entries <= 0 )); then
        fail "threaded-I/O prefetch did not observe the test traffic"
    fi

    printf 'IOTHREAD_ORDER_AND_PROGRESS_OK\n'
}

if [[ "$mode" == "check" ]]; then
    run_correctness_check
else
    run_measurement
fi
