#!/bin/sh
# Force the proof-only handoff recorder into iothread.c and nowhere else.

set -eu

: "${PERFLOOP_IOTHREAD_HOL_HEADER:?PERFLOOP_IOTHREAD_HOL_HEADER must name the recorder header}"
real_cc=${PERFLOOP_REAL_CC:-gcc}
has_dependency_scan=0
has_iothread_source=0

for arg in "$@"; do
    case "$arg" in
        -MM) has_dependency_scan=1 ;;
        iothread.c|*/iothread.c) has_iothread_source=1 ;;
    esac
done

if [ "$has_dependency_scan" -eq 0 ] && [ "$has_iothread_source" -eq 1 ]; then
    header_dir=$(dirname "$PERFLOOP_IOTHREAD_HOL_HEADER")
    src_dir=$(cd "$header_dir/../../src" && pwd)
    exec "$real_cc" -I "$src_dir" -include "$PERFLOOP_IOTHREAD_HOL_HEADER" "$@"
fi

exec "$real_cc" "$@"
