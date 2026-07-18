#!/bin/sh
# Build a local Redis binary with a temporary, direct parser-turn trace probe.
# The probe is applied only while compiling; it is not part of the server source.
set -eu

output=${PERFLOOP_BENCH_BIN:-src/redis-server}
staging=$(mktemp -d .perfloop-hol-parse.XXXXXX)
restore_source() {
    cp "$staging/networking.c" src/networking.c
    rm -rf "$staging"
}
trap restore_source EXIT HUP INT TERM

cp src/networking.c "$staging/networking.c"
# Generated Make settings are local build state. Remove them and clean so this
# proof binary is not linked from artifacts built by a different measured arm.
rm -f src/.make-settings src/.make-prerequisites
make -C src clean
patch -p1 --batch < tests/perf/parse_turn_trace.patch
make -C src -j"$(getconf _NPROCESSORS_ONLN)" redis-server REDIS_CFLAGS='-DPERFLOOP_PARSE_TRACE'
mkdir -p "$(dirname "$output")"
cp src/redis-server "$output"
