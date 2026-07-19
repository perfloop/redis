#!/usr/bin/env bash
# Build the test-instrumented Redis binary and the zero-copy HOL harness.
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "usage: $0 <benchmark-binary>" >&2
    exit 2
fi

benchmark_binary=$1
patch=tests/benchmarks/hol_callback_probe.patch

mkdir -p "$(dirname "$benchmark_binary")"
git apply "$patch"
make -C src -j16 redis-server REDIS_CFLAGS='-DPERFLOOP_HOL_CALLBACK_PROBE'
git apply -R "$patch"
cc -std=gnu11 -O2 -Wall -Wextra -Werror -pthread \
    -o "$benchmark_binary" tests/benchmarks/zero_copy_writev_hol.c
cc -std=gnu11 -O2 -Wall -Wextra -Werror -fPIC -shared \
    -o "$benchmark_binary.writev-probe.so" tests/benchmarks/writev_max_probe.c -ldl
