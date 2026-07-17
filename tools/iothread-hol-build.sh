#!/bin/sh
# Build the isolated IO-thread HOL benchmark artifacts into the requested path.
set -eu

if [ "$#" -ne 1 ]; then
    echo "usage: $0 BENCHMARK_OUTPUT_DIRECTORY" >&2
    exit 64
fi

output_dir=$1

# The benchmark server gets direct, build-only hooks around the target function.
# This script is run only in disposable proof worktrees; the committed server
# source remains unchanged.
python3 tools/iothread-hol-instrument-source.py src/iothread.c
make -C src -j2 redis-server redis-benchmark \
    MALLOC=libc BUILD_TLS=no SKIP_VEC_SETS=yes USE_SYSTEMD=no REDIS_CFLAGS='-Werror'
mkdir -p "$output_dir"
cc -std=c99 -fPIC -shared -Wall -Wextra -Werror -I src -I tools \
    -o "$output_dir/iothread-hol-module.so" tools/iothread-hol-module.c -ldl
cp tools/iothread-hol-bench.py "$output_dir/iothread-hol-bench.py"
chmod +x "$output_dir/iothread-hol-bench.py"
