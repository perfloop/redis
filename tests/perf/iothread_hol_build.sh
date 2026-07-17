#!/usr/bin/env bash
# Build the uninstrumented server used for latency measurement and a matching
# instrumented twin used only for handoff observability.
set -euo pipefail

root=$(cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"

instrumented_server="$root/src/redis-server.hol-instrumented"
header="$root/tests/perf/iothread_hol_instrument.h"
compiler_wrapper="$root/tests/perf/iothread_hol_cc.sh"

rm -f "$instrumented_server"

# Make does not consider a CC change an object dependency. Clean explicitly so
# both binaries are rebuilt from this worktree rather than reusing the other
# arm's iothread.o.
make -C src clean
CC=gcc make -j"$(nproc)" build redis MALLOC=libc

# Only iothread.c differs between the two server binaries. Rebuild that
# object and relink it against the normal build so the controlled probe is
# otherwise byte-identical to the latency-measurement binary.
rm -f src/iothread.o src/iothread.d
PERFLOOP_IOTHREAD_HOL_HEADER="$header" PERFLOOP_REAL_CC=gcc CC="$compiler_wrapper" \
    make -C src -j"$(nproc)" redis-server
cp src/redis-server "$instrumented_server"

# Restore a normal object as well as the normal executable so a later ordinary
# relink cannot accidentally include the force-included recorder.
rm -f src/iothread.o src/iothread.d
CC=gcc make -C src -j"$(nproc)" redis-server MALLOC=libc

if [[ -n "${PERFLOOP_BENCH_BIN:-}" ]]; then
    install -m 755 tests/perf/iothread_hol.sh "$PERFLOOP_BENCH_BIN"
fi
