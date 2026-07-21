# Measurement-only fixture bootstrap.
#
# The immutable benchmark base predates the post-seal I/O-thread and normal
# output fixtures.  Perfloop reconstructs that base by applying only files
# added by amendment harness commits.  Keep canonical fixture copies under a
# new harness-only path and materialize them in the fresh measured worktree
# before each sealed build, so both arms execute the same committed sources.
include Makefile

.PHONY: perfloop-benchmark-fixtures
perfloop-benchmark-fixtures:
	@cp -f ../tests/perf/.perfloop-benchmark-fixtures/hol_iothread_guard.py ../tests/perf/hol_iothread_guard.py
	@cp -f ../tests/perf/.perfloop-benchmark-fixtures/hol_iothread_late_peer.py ../tests/perf/hol_iothread_late_peer.py
	@cp -f ../tests/perf/.perfloop-benchmark-fixtures/hol_iothread_trace.c ../tests/perf/hol_iothread_trace.c
	@cp -f ../tests/perf/.perfloop-benchmark-fixtures/hol_mainthread_late_peer.py ../tests/perf/hol_mainthread_late_peer.py
	@cp -f ../tests/perf/.perfloop-benchmark-fixtures/hol_normal_output.py ../tests/perf/hol_normal_output.py
	@cp -f ../tests/perf/.perfloop-benchmark-fixtures/hol_reply_iov.py ../tests/perf/hol_reply_iov.py

redis-server: | perfloop-benchmark-fixtures
