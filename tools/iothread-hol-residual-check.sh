#!/bin/sh
# Build an isolated ASAN server and exercise residual IO-thread lifetimes.
set -eu

work=.perfloop-iothread-residual-check
cleanup() {
    rm -rf "$work"
}
trap cleanup EXIT HUP INT TERM

rm -rf "$work"
mkdir -p "$work"
cp -a src deps utils tests "$work"/

make -C "$work/src" clean
make -C "$work/src" -j2 redis-server \
    MALLOC=libc BUILD_TLS=no SKIP_VEC_SETS=yes USE_SYSTEMD=no \
    SANITIZER=address REDIS_CFLAGS='-Werror'
cc -std=c99 -fPIC -shared -Wall -Wextra -Werror \
    -I "$work/src" \
    -I "$work/deps/hiredis" \
    -I "$work/deps/linenoise" \
    -I "$work/deps/lua/src" \
    -I "$work/deps/hdr_histogram" \
    -I "$work/deps/fpconv" \
    -I "$work/deps/xxhash" \
    -o "$work/iothread-hol-residual-regression-module.so" \
    tools/iothread-hol-residual-regression-module.c
ASAN_OPTIONS=detect_leaks=0:abort_on_error=1 \
    python3 tools/iothread-hol-residual-regression.py \
        --server "$work/src/redis-server" \
        --module "$work/iothread-hol-residual-regression-module.so"
